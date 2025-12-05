// VeridicalDB - A Modern Database Built From Scratch
// Main entry point for the database server.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/JayabrataBasu/VeridicalDB/pkg/cli"
	"github.com/JayabrataBasu/VeridicalDB/pkg/config"
	"github.com/JayabrataBasu/VeridicalDB/pkg/log"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "", "Path to configuration file (YAML or JSON)")
	showVersion := flag.Bool("version", false, "Show version and exit")
	showHelp := flag.Bool("help", false, "Show help and exit")
	interactive := flag.Bool("interactive", true, "Start in interactive REPL mode")

	flag.Parse()

	// Handle version flag
	if *showVersion {
		fmt.Printf("VeridicalDB version %s\n", cli.Version)
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
		"version", cli.Version,
		"data_dir", cfg.Storage.DataDir,
		"port", cfg.Server.Port,
	)

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

	// Run in interactive mode
	if *interactive {
		if err := cli.RunInteractive(logger); err != nil {
			logger.Error("REPL error", "error", err)
			os.Exit(1)
		}
	} else {
		// Future: Run as a TCP server (Stage 5)
		logger.Info("Non-interactive mode not yet implemented")
		fmt.Println("Non-interactive (server) mode will be available in Stage 5.")
		fmt.Println("For now, use --interactive=true (default) to start the REPL.")
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

`, cli.Version)
}
