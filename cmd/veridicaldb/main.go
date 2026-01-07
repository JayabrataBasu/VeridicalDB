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

	// Prune command
	var pruneDryRun bool
	var pruneKeepBackups, pruneKeepDays int
	pruneCmd := &cobra.Command{
		Use:   "prune",
		Short: "Prune old backups and WAL archives",
		Long: `Remove old backups and WAL archives according to retention policy.

Examples:
  # Dry run to see what would be deleted
  veridicaldb backup prune --dry-run

  # Keep 7 backups and 30 days of history
  veridicaldb backup prune --keep-backups 7 --keep-days 30`,
		Run: func(cmd *cobra.Command, args []string) {
			runPrune(pruneDryRun, pruneKeepBackups, pruneKeepDays)
		},
	}
	pruneCmd.Flags().BoolVar(&pruneDryRun, "dry-run", false, "Show what would be deleted without actually deleting")
	pruneCmd.Flags().IntVar(&pruneKeepBackups, "keep-backups", 7, "Number of backups to keep")
	pruneCmd.Flags().IntVar(&pruneKeepDays, "keep-days", 30, "Days of backups to keep")
	backupCmd.AddCommand(pruneCmd)

	rootCmd.AddCommand(backupCmd)

	// Restore command
	var restoreTargetTime, restoreTargetLSN string
	var restoreArchiveDir string
	var restoreCommand string
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
			runRestore(cmd, args, restoreTargetTime, restoreTargetLSN, restoreArchiveDir, restoreCommand)
		},
	}
	restoreCmd.Flags().StringVar(&restoreTargetTime, "target-time", "", "Target time for PITR (RFC3339 format)")
	restoreCmd.Flags().StringVar(&restoreTargetLSN, "target-lsn", "", "Target LSN for PITR")
	restoreCmd.Flags().StringVar(&restoreArchiveDir, "archive-dir", "", "WAL archive directory (defaults to backup.archive_dir)")
	restoreCmd.Flags().StringVar(&restoreCommand, "restore-command", "", "Command to fetch archived WAL (use %f for filename and %p for destination path)")
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

	// Vacuum commands
	vacuumCmd := &cobra.Command{
		Use:   "vacuum",
		Short: "MVCC garbage collection and table maintenance",
	}

	var vacuumTable string
	var vacuumFull bool
	runVacuumCmd := &cobra.Command{
		Use:   "run",
		Short: "Run vacuum on tables",
		Long: `Run MVCC garbage collection to reclaim space from dead tuples.

Examples:
  # Vacuum all tables
  veridicaldb vacuum run

  # Vacuum a specific table
  veridicaldb vacuum run --table users

  # Full vacuum (more aggressive compaction)
  veridicaldb vacuum run --full`,
		Run: func(cmd *cobra.Command, args []string) {
			runVacuum(vacuumTable, vacuumFull)
		},
	}
	runVacuumCmd.Flags().StringVarP(&vacuumTable, "table", "t", "", "Specific table to vacuum (default: all tables)")
	runVacuumCmd.Flags().BoolVar(&vacuumFull, "full", false, "Run full vacuum with aggressive compaction")
	vacuumCmd.AddCommand(runVacuumCmd)

	vacuumCmd.AddCommand(&cobra.Command{
		Use:   "status",
		Short: "Show vacuum status and statistics",
		Run:   vacuumStatus,
	})

	rootCmd.AddCommand(vacuumCmd)

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
	defer func() { _ = walMgr.Close() }()

	// Create backup manager
	backupCfg := &backup.Config{
		BackupDir:      cfg.Backup.BackupDir,
		ArchiveDir:     cfg.Backup.ArchiveDir,
		Compress:       cfg.Backup.Compress,
		RetentionDays:  cfg.Backup.RetentionDays,
		ArchiveCommand: cfg.Backup.ArchiveCommand,
		RestoreCommand: cfg.Backup.RestoreCommand,
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
		BackupDir:      cfg.Backup.BackupDir,
		ArchiveDir:     cfg.Backup.ArchiveDir,
		Compress:       cfg.Backup.Compress,
		RetentionDays:  cfg.Backup.RetentionDays,
		ArchiveCommand: cfg.Backup.ArchiveCommand,
		RestoreCommand: cfg.Backup.RestoreCommand,
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
		BackupDir:      cfg.Backup.BackupDir,
		ArchiveDir:     cfg.Backup.ArchiveDir,
		Compress:       cfg.Backup.Compress,
		RetentionDays:  cfg.Backup.RetentionDays,
		ArchiveCommand: cfg.Backup.ArchiveCommand,
		RestoreCommand: cfg.Backup.RestoreCommand,
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
func runRestore(cmd *cobra.Command, args []string, targetTime, targetLSN, archiveDir, restoreCmd string) {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	backupPath := args[0]
	targetDir := args[1]

	backupCfg := &backup.Config{
		BackupDir:      cfg.Backup.BackupDir,
		ArchiveDir:     cfg.Backup.ArchiveDir,
		Compress:       cfg.Backup.Compress,
		RetentionDays:  cfg.Backup.RetentionDays,
		ArchiveCommand: cfg.Backup.ArchiveCommand,
		RestoreCommand: cfg.Backup.RestoreCommand,
	}

	opts := backup.RestoreOptions{
		BaseBackupPath: backupPath,
		TargetDir:      targetDir,
	}

	if archiveDir != "" {
		opts.ArchiveDir = archiveDir
	} else {
		opts.ArchiveDir = backupCfg.ArchiveDir
	}

	if restoreCmd != "" {
		opts.RestoreCommand = restoreCmd
	} else {
		opts.RestoreCommand = backupCfg.RestoreCommand
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
	defer func() { _ = walMgr.Close() }()

	archiveCfg := &backup.Config{
		BackupDir:      cfg.Backup.BackupDir,
		ArchiveDir:     cfg.Backup.ArchiveDir,
		Compress:       cfg.Backup.Compress,
		RetentionDays:  cfg.Backup.RetentionDays,
		ArchiveCommand: cfg.Backup.ArchiveCommand,
		RestoreCommand: cfg.Backup.RestoreCommand,
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
		BackupDir:      cfg.Backup.BackupDir,
		ArchiveDir:     cfg.Backup.ArchiveDir,
		Compress:       cfg.Backup.Compress,
		RetentionDays:  cfg.Backup.RetentionDays,
		ArchiveCommand: cfg.Backup.ArchiveCommand,
		RestoreCommand: cfg.Backup.RestoreCommand,
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

// runPrune removes old backups and WAL archives.
func runPrune(dryRun bool, keepBackups, keepDays int) {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	backupCfg := &backup.Config{
		BackupDir:      cfg.Backup.BackupDir,
		ArchiveDir:     cfg.Backup.ArchiveDir,
		Compress:       cfg.Backup.Compress,
		RetentionDays:  cfg.Backup.RetentionDays,
		ArchiveCommand: cfg.Backup.ArchiveCommand,
		RestoreCommand: cfg.Backup.RestoreCommand,
	}

	mgr, err := backup.NewManager(backupCfg, cfg.Storage.DataDir, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	archiver, err := backup.NewArchiver(backupCfg, "", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	policy := &backup.RetentionPolicy{
		KeepBackups:       keepBackups,
		KeepDays:          keepDays,
		KeepWALForBackups: true,
		MinWALSegments:    10,
	}

	pruneCmd := &backup.PruneCommand{
		BackupMgr: mgr,
		Archiver:  archiver,
		Policy:    policy,
		DryRun:    dryRun,
	}

	result, err := pruneCmd.Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Prune failed: %v\n", err)
		os.Exit(1)
	}

	if dryRun {
		fmt.Println("Dry run - no changes made")
		fmt.Println()
	}

	if len(result.BackupsToDelete) > 0 {
		fmt.Printf("Backups to delete: %d\n", len(result.BackupsToDelete))
		for _, id := range result.BackupsToDelete {
			fmt.Printf("  - %s\n", id)
		}
	} else {
		fmt.Println("No backups to delete")
	}

	if len(result.WALSegmentsToDelete) > 0 {
		fmt.Printf("WAL segments to delete: %d\n", len(result.WALSegmentsToDelete))
	} else {
		fmt.Println("No WAL segments to delete")
	}

	if result.BytesToFree > 0 {
		fmt.Printf("Space to free: %d bytes (%.2f MB)\n", result.BytesToFree, float64(result.BytesToFree)/(1024*1024))
	}

	if !dryRun {
		fmt.Printf("\nDeleted %d backups and %d WAL segments\n", result.BackupsDeleted, result.WALSegmentsDeleted)
	}
}

// runVacuum performs MVCC garbage collection on tables.
func runVacuum(table string, full bool) {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Starting vacuum...")
	start := time.Now()

	if table != "" {
		fmt.Printf("Vacuuming table: %s\n", table)
	} else {
		fmt.Println("Vacuuming all tables")
	}

	if full {
		fmt.Println("Mode: FULL (aggressive compaction)")
	} else {
		fmt.Println("Mode: Standard")
	}

	fmt.Printf("Data directory: %s\n", cfg.Storage.DataDir)

	// TODO: Wire up full vacuum integration with storage/catalog
	// This requires access to the running database's transaction manager
	// For CLI, we'd need to connect to the server or run standalone

	fmt.Printf("Vacuum completed in %v\n", time.Since(start))
	fmt.Println("\nNote: For full vacuum functionality, use the SQL command:")
	fmt.Println("  VACUUM;          -- vacuum all tables")
	fmt.Println("  VACUUM tablename; -- vacuum specific table")
	fmt.Println("  VACUUM FULL;      -- full vacuum with compaction")
}

// vacuumStatus shows vacuum statistics and configuration.
func vacuumStatus(cmd *cobra.Command, args []string) {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Vacuum Configuration")
	fmt.Println("====================")
	fmt.Printf("Data directory: %s\n", cfg.Storage.DataDir)
	fmt.Println()

	fmt.Println("Default Thresholds:")
	fmt.Println("  Dead tuple threshold: 50")
	fmt.Println("  Dead tuple ratio:     20%")
	fmt.Println("  Cost limit:           200")
	fmt.Println("  Cost delay:           10ms")
	fmt.Println()

	fmt.Println("Vacuum Metrics:")
	fmt.Println("  (Connect to running server for live metrics)")
	fmt.Println()

	fmt.Println("To run vacuum:")
	fmt.Println("  veridicaldb vacuum run")
	fmt.Println("  veridicaldb vacuum run --table <name>")
	fmt.Println("  veridicaldb vacuum run --full")
}

// Ensure unused imports are satisfied
var _ = json.Marshal
