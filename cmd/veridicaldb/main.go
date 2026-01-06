// VeridicalDB - A modern, embeddable database engine
// Main entry point for the database server

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/internal/cli"
	"github.com/JayabrataBasu/VeridicalDB/internal/config"
	"github.com/JayabrataBasu/VeridicalDB/internal/logger"
	"github.com/JayabrataBasu/VeridicalDB/pkg/backup"
	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
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

	// Backup commands
	backupCmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup and restore operations",
	}

	// Base backup command
	var backupOutput string
	baseBackupCmd := &cobra.Command{
		Use:   "basebackup",
		Short: "Create a full base backup",
		Long: `Create a full base backup of the database.
The backup includes all data files and records the starting WAL position
for point-in-time recovery.

Examples:
  veridicaldb backup basebackup
  veridicaldb backup basebackup --output /backups/mybackup`,
		Run: func(cmd *cobra.Command, args []string) {
			runBaseBackup(cmd, args, backupOutput)
		},
	}
	baseBackupCmd.Flags().StringVarP(&backupOutput, "output", "o", "", "Output path for backup")
	backupCmd.AddCommand(baseBackupCmd)

	// List backups command
	backupCmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List all available backups",
		Run:   listBackups,
	})

	// Verify backup command
	backupCmd.AddCommand(&cobra.Command{
		Use:   "verify [backup-path]",
		Short: "Verify backup integrity",
		Args:  cobra.ExactArgs(1),
		Run:   verifyBackup,
	})

	rootCmd.AddCommand(backupCmd)

	// Restore command
	var restoreTargetTime, restoreTargetLSN string
	var restoreArchiveDir string
	restoreCmd := &cobra.Command{
		Use:   "restore [base-backup-path] [target-dir]",
		Short: "Restore database from backup",
		Long: `Restore the database from a base backup with optional point-in-time recovery.

Examples:
  # Restore to latest state
  veridicaldb restore /backups/backup_20260106_120000.tar.gz /data/restored

  # Restore to specific time (PITR)
  veridicaldb restore /backups/backup_20260106_120000.tar.gz /data/restored \
    --target-time "2026-01-06T15:30:00Z"

  # Restore to specific LSN
  veridicaldb restore /backups/backup_20260106_120000.tar.gz /data/restored \
    --target-lsn 1234567890`,
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			runRestore(cmd, args, restoreTargetTime, restoreTargetLSN, restoreArchiveDir)
		},
	}
	restoreCmd.Flags().StringVar(&restoreTargetTime, "target-time", "", "Target time for PITR (RFC3339 format)")
	restoreCmd.Flags().StringVar(&restoreTargetLSN, "target-lsn", "", "Target LSN for PITR")
	restoreCmd.Flags().StringVar(&restoreArchiveDir, "archive-dir", "", "WAL archive directory")
	rootCmd.AddCommand(restoreCmd)

	// WAL archive command
	walCmd := &cobra.Command{
		Use:   "wal",
		Short: "WAL management operations",
	}

	walCmd.AddCommand(&cobra.Command{
		Use:   "archive",
		Short: "Archive current WAL segment",
		Run:   archiveWAL,
	})

	walCmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List archived WAL segments",
		Run:   listArchivedWAL,
	})

	rootCmd.AddCommand(walCmd)

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

// runBaseBackup creates a base backup of the database.
func runBaseBackup(cmd *cobra.Command, args []string, output string) {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Initialize WAL
	walDir := cfg.Storage.WalDir
	if walDir == "" {
		walDir = cfg.Storage.DataDir + "/wal"
	}
	walMgr, err := wal.Open(walDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening WAL: %v\n", err)
		os.Exit(1)
	}
	defer walMgr.Close()

	// Create backup manager
	backupCfg := &backup.Config{
		BackupDir:  cfg.Storage.DataDir + "/backups",
		ArchiveDir: cfg.Storage.DataDir + "/wal_archive",
		Compress:   true,
	}
	mgr, err := backup.NewManager(backupCfg, cfg.Storage.DataDir, walMgr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating backup manager: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Creating base backup...")
	meta, err := mgr.CreateBaseBackup(output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating backup: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nBackup completed successfully!\n")
	fmt.Printf("  ID:        %s\n", meta.ID)
	fmt.Printf("  Size:      %d bytes\n", meta.Size)
	fmt.Printf("  Start LSN: %d\n", meta.StartLSN)
	fmt.Printf("  End LSN:   %d\n", meta.EndLSN)
	fmt.Printf("  Duration:  %v\n", meta.EndTime.Sub(meta.StartTime))
	fmt.Printf("  Checksum:  %s\n", meta.Checksum[:16]+"...")
}

// listBackups shows all available backups.
func listBackups(cmd *cobra.Command, args []string) {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	backupCfg := &backup.Config{
		BackupDir: cfg.Storage.DataDir + "/backups",
	}
	mgr, err := backup.NewManager(backupCfg, cfg.Storage.DataDir, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	backups, err := mgr.ListBackups()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listing backups: %v\n", err)
		os.Exit(1)
	}

	if len(backups) == 0 {
		fmt.Println("No backups found.")
		return
	}

	fmt.Printf("%-30s %-20s %-12s %-15s\n", "ID", "Time", "Size", "LSN Range")
	fmt.Println(string(make([]byte, 80)))
	for _, b := range backups {
		fmt.Printf("%-30s %-20s %-12d %d-%d\n",
			b.ID,
			b.StartTime.Format("2006-01-02 15:04:05"),
			b.Size,
			b.StartLSN,
			b.EndLSN,
		)
	}
}

// verifyBackup checks backup integrity.
func verifyBackup(cmd *cobra.Command, args []string) {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	backupCfg := &backup.Config{
		BackupDir: cfg.Storage.DataDir + "/backups",
	}
	mgr, err := backup.NewManager(backupCfg, cfg.Storage.DataDir, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Verifying backup: %s\n", args[0])
	if err := mgr.VerifyBackup(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "Verification failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Backup verification successful!")
}

// runRestore restores from a backup.
func runRestore(cmd *cobra.Command, args []string, targetTime, targetLSN, archiveDir string) {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	backupPath := args[0]
	targetDir := args[1]

	opts := backup.RestoreOptions{
		BaseBackupPath: backupPath,
		TargetDir:      targetDir,
		ArchiveDir:     archiveDir,
	}

	if archiveDir == "" {
		opts.ArchiveDir = cfg.Storage.DataDir + "/wal_archive"
	}

	// Parse target time if provided
	if targetTime != "" {
		t, err := time.Parse(time.RFC3339, targetTime)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid target time format (use RFC3339): %v\n", err)
			os.Exit(1)
		}
		opts.TargetTime = &t
	}

	// Parse target LSN if provided
	if targetLSN != "" {
		var lsn uint64
		if _, err := fmt.Sscanf(targetLSN, "%d", &lsn); err != nil {
			fmt.Fprintf(os.Stderr, "Invalid target LSN: %v\n", err)
			os.Exit(1)
		}
		opts.TargetLSN = &lsn
	}

	backupCfg := &backup.Config{
		BackupDir:  cfg.Storage.DataDir + "/backups",
		ArchiveDir: opts.ArchiveDir,
	}
	mgr, err := backup.NewManager(backupCfg, cfg.Storage.DataDir, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Restoring from: %s\n", backupPath)
	fmt.Printf("Target directory: %s\n", targetDir)
	if opts.TargetTime != nil {
		fmt.Printf("Target time (PITR): %s\n", opts.TargetTime.Format(time.RFC3339))
	}
	if opts.TargetLSN != nil {
		fmt.Printf("Target LSN (PITR): %d\n", *opts.TargetLSN)
	}

	result, err := mgr.Restore(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Restore failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nRestore completed successfully!\n")
	fmt.Printf("  Base Backup: %s\n", result.BaseBackupID)
	fmt.Printf("  Files Restored: %d\n", result.FilesRestored)
	fmt.Printf("  WAL Segments Applied: %d\n", result.WALSegmentsApplied)
	fmt.Printf("  Restored LSN: %d\n", result.RestoredLSN)
	fmt.Printf("  Duration: %v\n", result.EndTime.Sub(result.StartTime))
}

// archiveWAL archives the current WAL segment.
func archiveWAL(cmd *cobra.Command, args []string) {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	walDir := cfg.Storage.WalDir
	if walDir == "" {
		walDir = cfg.Storage.DataDir + "/wal"
	}

	walMgr, err := wal.Open(walDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening WAL: %v\n", err)
		os.Exit(1)
	}
	defer walMgr.Close()

	archiveCfg := &backup.Config{
		ArchiveDir: cfg.Storage.DataDir + "/wal_archive",
	}
	archiver, err := backup.NewArchiver(archiveCfg, walDir, walMgr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Archiving WAL...")
	if err := archiver.ArchiveCurrentWAL(); err != nil {
		fmt.Fprintf(os.Stderr, "Archive failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("WAL archived successfully (LSN: %d)\n", archiver.LastArchivedLSN())
}

// listArchivedWAL shows archived WAL segments.
func listArchivedWAL(cmd *cobra.Command, args []string) {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	archiveCfg := &backup.Config{
		ArchiveDir: cfg.Storage.DataDir + "/wal_archive",
	}
	archiver, err := backup.NewArchiver(archiveCfg, "", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	segments, err := archiver.ListArchivedSegments()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if len(segments) == 0 {
		fmt.Println("No archived WAL segments found.")
		return
	}

	fmt.Printf("%-45s %-20s %-12s %-15s\n", "Name", "Timestamp", "Size", "LSN")
	fmt.Println(string(make([]byte, 95)))
	for _, seg := range segments {
		fmt.Printf("%-45s %-20s %-12d %d\n",
			seg.Name,
			seg.Timestamp.Format("2006-01-02 15:04:05"),
			seg.Size,
			seg.LSN,
		)
	}
}

// Ensure unused imports are satisfied
var _ = json.Marshal
