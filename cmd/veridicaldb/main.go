// VeridicalDB - A modern, embeddable database engine
// Main entry point for the database server

package main

import (
	"fmt"
	"os"

	"github.com/JayabrataBasu/VeridicalDB/internal/cli"
	"github.com/JayabrataBasu/VeridicalDB/internal/config"
	"github.com/JayabrataBasu/VeridicalDB/internal/logger"
	"github.com/spf13/cobra"
)

var (
	version   = "0.1.0"
	buildDate = "dev"
	cfgFile   string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "veridicaldb",
		Short: "VeridicalDB - A modern database engine",
		Long: `VeridicalDB is a full-featured database engine supporting both
row-based and columnar storage, MVCC transactions, and SQL queries.

Start the interactive shell:
  veridicaldb

Start with a specific config file:
  veridicaldb --config /path/to/config.yaml`,
		Run: runServer,
	}

	// Global flags
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file path")

	// Version command
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("VeridicalDB %s (built %s)\n", version, buildDate)
		},
	})

	// Init command - initialize a new database
	rootCmd.AddCommand(&cobra.Command{
		Use:   "init [directory]",
		Short: "Initialize a new database directory",
		Args:  cobra.MaximumNArgs(1),
		Run:   initDatabase,
	})

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runServer(cmd *cobra.Command, args []string) {
	// Load configuration
	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	log, err := logger.New(cfg.Log.Level, cfg.Log.Format, cfg.Log.Output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing logger: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = log.Sync() }()

	log.Info("Starting VeridicalDB",
		"version", version,
		"data_dir", cfg.Storage.DataDir,
		"port", cfg.Server.Port,
	)

	// Validate data directory exists
	if err := config.ValidateDataDir(cfg.Storage.DataDir); err != nil {
		log.Error("Data directory validation failed", "error", err)
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		fmt.Fprintf(os.Stderr, "Run 'veridicaldb init' to create a new database\n")
		os.Exit(1)
	}

	// Start the CLI REPL
	repl := cli.NewREPL(cfg, log)
	if err := repl.Run(); err != nil {
		log.Error("REPL error", "error", err)
		os.Exit(1)
	}
}

func initDatabase(cmd *cobra.Command, args []string) {
	dir := "./data"
	if len(args) > 0 {
		dir = args[0]
	}

	fmt.Printf("Initializing new VeridicalDB database in: %s\n", dir)

	if err := config.InitDataDir(dir); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Create default config file
	cfgPath := "veridicaldb.yaml"
	if err := config.CreateDefaultConfig(cfgPath, dir); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Could not create config file: %v\n", err)
	} else {
		fmt.Printf("Created config file: %s\n", cfgPath)
	}

	fmt.Println("Database initialized successfully!")
	fmt.Printf("Start the database with: veridicaldb --config %s\n", cfgPath)
}
