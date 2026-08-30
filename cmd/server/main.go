// VeridicalDB - A Modern Database Built From Scratch
// Main entry point for the database server.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/internal/build"
	"github.com/JayabrataBasu/VeridicalDB/internal/cli"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/config"
	"github.com/JayabrataBasu/VeridicalDB/pkg/engine"
	"github.com/JayabrataBasu/VeridicalDB/pkg/log"
	"github.com/JayabrataBasu/VeridicalDB/pkg/observability"
	"github.com/JayabrataBasu/VeridicalDB/pkg/pgwire"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/parse"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "", "Path to configuration file (YAML or JSON)")
	showVersion := flag.Bool("version", false, "Show version and exit")
	showHelp := flag.Bool("help", false, "Show help and exit")
	interactive := flag.Bool("interactive", true, "Start in interactive REPL mode")
	enableUI := flag.Bool("ui", false, "Enable web UI server")
	uiPort := flag.Int("ui-port", 8080, "Port to serve web UI on")

	flag.Parse()

	// Handle version flag
	if *showVersion {
		fmt.Printf("VeridicalDB version %s\n", build.Full())
		os.Exit(0)
	}

	// Handle help flag
	if *showHelp {
		printUsage()
		os.Exit(0)
	}

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	logger, err := log.NewFromConfig(cfg.Logging.Level, cfg.Logging.Format, cfg.Logging.Output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing logger: %v\n", err)
		os.Exit(1)
	}
	log.SetDefault(logger)

	logger.Info("VeridicalDB starting",
		"version", build.Version,
		"data_dir", cfg.Storage.DataDir,
		"port", cfg.Server.Port,
	)

	// If enabled, serve static files under ./web at /ui/.
	if *enableUI {
		uiDir := "./web"
		fs := http.FileServer(http.Dir(uiDir))
		http.Handle("/ui/", http.StripPrefix("/ui/", fs))
		// Note: API endpoints are registered after the database (MTM) is initialized so
		// handlers can access the table manager and execute SQL.
	}

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.Storage.DataDir, 0755); err != nil {
		logger.Error("Failed to create data directory", "path", cfg.Storage.DataDir, "error", err)
		os.Exit(1)
	}

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("Received signal, shutting down", "signal", sig)
		os.Exit(0)
	}()

	// Assemble the database. The server runs in durable mode: WAL, crash
	// recovery on open, and a background checkpointer that db.Close() stops.
	db, err := engine.Open(engine.Config{
		DataDir:  cfg.Storage.DataDir,
		PageSize: cfg.Storage.PageSize,
		Durable:  true,
		Logger:   logger,
	})
	if err != nil {
		logger.Error("Failed to open database", "error", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	tm := db.TableManager()
	logger.Info("Database opened", "tables", len(tm.ListTables()))

	// Initialize SystemCatalog for observability
	sysCatalog := observability.NewSystemCatalog(db.TxnManager(), nil, db.Catalog())

	// Register observability HTTP endpoints (always available for monitoring)
	// These run on a separate port to avoid conflicts with the main server
	observabilityPort := 8081
	if cfg.Server.ObservabilityPort > 0 {
		observabilityPort = cfg.Server.ObservabilityPort
	}

	// Start observability HTTP server in a goroutine
	go func() {
		obsMux := http.NewServeMux()

		// Prometheus metrics endpoint
		obsMux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			metrics := sysCatalog.PrometheusMetrics()
			_, _ = fmt.Fprint(w, metrics)
		})

		// Health check endpoint
		obsMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			health := map[string]interface{}{
				"status":    "healthy",
				"timestamp": time.Now().UTC().Format(time.RFC3339),
				"tables":    len(tm.ListTables()),
				"version":   build.Version,
			}
			_ = json.NewEncoder(w).Encode(health)
		})

		// Readiness probe (for Kubernetes)
		obsMux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
		})

		// Liveness probe (for Kubernetes)
		obsMux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "alive"})
		})

		// pprof endpoints for Go profiling
		obsMux.HandleFunc("/debug/pprof/", pprof.Index)
		obsMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		obsMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		obsMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		obsMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		logger.Info("Starting observability server", "port", observabilityPort)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", observabilityPort), obsMux); err != nil {
			logger.Error("Observability server failed", "error", err)
		}
	}()

	// If UI is enabled, register API endpoints that can use the MTM to run read-only queries.
	if *enableUI {
		http.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "ok",
				"tables": len(tm.ListTables()),
			})
		})

		http.HandleFunc("/api/query", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			type Req struct {
				SQL string `json:"sql"`
			}
			var req Req
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}

			// Very small safety check: only allow SELECT statements from the UI for now.
			parser := parse.NewParser(req.SQL)
			stmt, err := parser.Parse()
			if err != nil {
				http.Error(w, "syntax error: "+err.Error(), http.StatusBadRequest)
				return
			}
			switch stmt.(type) {
			case *ast.SelectStmt:
				// allowed
			default:
				http.Error(w, "only SELECT statements are allowed via the UI", http.StatusForbidden)
				return
			}

			// Create a temporary session and execute the query (read-only)
			sess := db.NewSession()
			res, err := sess.ExecuteSQL(req.SQL)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			// Convert rows to JSON-friendly values
			rows := make([][]interface{}, 0, len(res.Rows))
			for _, row := range res.Rows {
				rvals := make([]interface{}, len(row))
				for i, v := range row {
					if v.IsNull {
						rvals[i] = nil
						continue
					}
					switch v.Type {
					case catalog.TypeInt32:
						rvals[i] = int64(v.Int32)
					case catalog.TypeInt64:
						rvals[i] = v.Int64
					case catalog.TypeFloat64:
						rvals[i] = v.Float64
					case catalog.TypeBool:
						rvals[i] = v.Bool
					case catalog.TypeText:
						rvals[i] = v.Text
					case catalog.TypeTimestamp:
						rvals[i] = v.Timestamp.Format(time.RFC3339)
					case catalog.TypeJSON:
						rvals[i] = v.JSON
					default:
						rvals[i] = v.String()
					}
				}
				rows = append(rows, rvals)
			}

			resp := map[string]interface{}{
				"columns": res.Columns,
				"rows":    rows,
				"message": res.Message,
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
		})
	}

	// If UI enabled, start the UI server. (Handlers are registered above.)
	if *enableUI {
		uiDir := "./web"
		go func() {
			logger.Info("Starting UI server", "port", *uiPort, "dir", uiDir)
			if err := http.ListenAndServe(fmt.Sprintf(":%d", *uiPort), nil); err != nil {
				logger.Error("UI server failed", "error", err)
			}
		}()
	}

	// (The WAL checkpointer is started and stopped by engine.Open / db.Close.)

	// Run in interactive mode
	if *interactive {
		if err := cli.RunInteractive(db, cfg.Storage.DataDir); err != nil {
			logger.Error("REPL error", "error", err)
			os.Exit(1)
		}
	} else {
		// Run as a PostgreSQL wire protocol server
		logger.Info("Starting PostgreSQL wire protocol server", "port", cfg.Server.Port)

		// Build TLS configuration for pgwire if enabled
		tlsCfg, err := cfg.PgWire.TLS.BuildTLSConfig()
		if err != nil {
			logger.Error("invalid TLS configuration", "error", err)
			os.Exit(1)
		}

		pgServer := pgwire.NewServer(pgwire.ServerConfig{
			Port:          cfg.Server.Port,
			Logger:        logger,
			NewSession:    db.NewSession,
			ServerVersion: build.Version,
			TLSConfig:     tlsCfg,
		})

		if err := pgServer.Start(cfg.Server.Port); err != nil {
			logger.Error("Failed to start pgwire server", "error", err)
			os.Exit(1)
		}
		defer func() { _ = pgServer.Stop() }()

		logger.Info("PostgreSQL wire protocol server started", "port", cfg.Server.Port)
		fmt.Printf("VeridicalDB is ready to accept connections on port %d\n", cfg.Server.Port)
		fmt.Println("Connect using: psql -h localhost -p", cfg.Server.Port)

		// Wait for shutdown signal
		<-sigChan
		logger.Info("Shutting down server...")
	}

	logger.Info("VeridicalDB shutdown complete")
}

// printUsage displays usage information.
func printUsage() {
	fmt.Printf(`VeridicalDB v%s - A Modern Database Built From Scratch

Usage:
  veridicaldb [options]

Options:
  --config <path>    Path to configuration file (YAML or JSON)
  --interactive      Start in interactive REPL mode (default: true)
  --version          Show version and exit
  --help             Show this help message

Examples:
  veridicaldb                        Start with defaults
  veridicaldb --config config.yaml   Start with custom config
  veridicaldb --version              Show version

Configuration File Example (config.yaml):
  server:
    port: 5432
    host: "127.0.0.1"
  storage:
    data_dir: "./data"
    page_size: 8192
  logging:
    level: "info"
    format: "text"
    output: "stdout"

For more information, visit: https://github.com/JayabrataBasu/VeridicalDB

`, build.Version)
}
