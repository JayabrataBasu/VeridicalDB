// Package cli provides the command-line interface and REPL for VeridicalDB
package cli

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/internal/build"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/config"
	"github.com/JayabrataBasu/VeridicalDB/pkg/engine"
	"github.com/JayabrataBasu/VeridicalDB/pkg/log"
	"github.com/JayabrataBasu/VeridicalDB/pkg/shard"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
	"github.com/chzyer/readline"
)

// REPL implements the Read-Eval-Print Loop for VeridicalDB
type REPL struct {
	config  *config.Config
	log     *log.Logger
	rl      *readline.Instance
	db      *engine.DB
	ownsDB  bool // true when this REPL opened db and must close it
	session *sql.Session
	coord   *shard.Coordinator
}

func (r *REPL) catalog() *catalog.Catalog { return r.db.Catalog() }

// NewREPL creates a new REPL instance
func NewREPL(cfg *config.Config, log *log.Logger) *REPL {
	return &REPL{
		config: cfg,
		log:    log,
	}
}

// Initialize opens the database engine and creates the REPL's session.
func (r *REPL) Initialize() error {
	db, err := engine.Open(engine.Config{
		DataDir:  r.config.Storage.DataDir,
		PageSize: r.config.Storage.PageSize,
		Logger:   r.log,
	})
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	r.db = db
	r.ownsDB = true
	r.session = db.NewSession()

	coord, err := SetupShardCoordinator(r.config, r.session)
	if err != nil {
		return fmt.Errorf("failed to initialize shard coordinator: %w", err)
	}
	r.coord = coord
	if r.coord != nil {
		r.log.Info("Shard coordinator connected",
			"shards", len(r.config.Sharding.Nodes),
			"shard_key", r.config.Sharding.ShardKeyColumn,
		)
	}
	return nil
}

func (r *REPL) closeResources() {
	if r.coord != nil {
		if err := r.coord.Close(); err != nil {
			r.log.Warn("failed to close shard coordinator", "error", err)
		}
	}
	if r.session != nil {
		r.session.Close()
	}
	if r.ownsDB && r.db != nil {
		if err := r.db.Close(); err != nil {
			r.log.Warn("failed to close database", "error", err)
		}
	}
}

// Run starts the REPL loop
func (r *REPL) Run() error {
	// Initialize the database components
	if err := r.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}
	defer r.closeResources()

	// Configure readline
	rlConfig := &readline.Config{
		Prompt:          "veridicaldb> ",
		HistoryFile:     getHistoryFile(),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
		AutoComplete:    newCompleter(),
	}

	rl, err := readline.NewEx(rlConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize readline: %w", err)
	}
	defer func() { _ = rl.Close() }()
	r.rl = rl

	// Print welcome message
	r.printWelcome()

	// Main REPL loop
	var multilineBuffer strings.Builder
	inMultiline := false

	for {
		// Update prompt for multiline input
		if inMultiline {
			rl.SetPrompt("         -> ")
		} else {
			rl.SetPrompt("veridicaldb> ")
		}

		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if inMultiline {
				// Cancel multiline input
				multilineBuffer.Reset()
				inMultiline = false
				fmt.Println("^C")
				continue
			}
			continue
		} else if err == io.EOF {
			fmt.Println("\nGoodbye!")
			return nil
		} else if err != nil {
			return fmt.Errorf("readline error: %w", err)
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Handle multiline input
		multilineBuffer.WriteString(line)
		fullInput := multilineBuffer.String()

		// Check if command is complete (ends with semicolon for SQL, immediate for backslash commands)
		if strings.HasPrefix(fullInput, "\\") || strings.HasSuffix(fullInput, ";") {
			// Process complete command
			result := r.processCommand(strings.TrimSuffix(fullInput, ";"))
			if result == commandExit {
				fmt.Println("Goodbye!")
				return nil
			}
			multilineBuffer.Reset()
			inMultiline = false
		} else {
			// Continue collecting multiline input
			multilineBuffer.WriteString(" ")
			inMultiline = true
		}
	}
}

type commandResult int

const (
	commandOK commandResult = iota
	commandExit
	commandError
)

func (r *REPL) processCommand(input string) commandResult {
	input = strings.TrimSpace(input)
	upperInput := strings.ToUpper(input)

	// Handle backslash commands
	if strings.HasPrefix(input, "\\") {
		return r.handleBackslashCommand(input)
	}

	// Handle SQL-like commands
	switch {
	case upperInput == "EXIT" || upperInput == "QUIT" || upperInput == "\\Q":
		return commandExit

	case upperInput == "HELP" || upperInput == "\\?" || upperInput == "\\HELP":
		r.printHelp()
		return commandOK

	case strings.HasPrefix(upperInput, "BEGIN"),
		strings.HasPrefix(upperInput, "COMMIT"),
		strings.HasPrefix(upperInput, "ROLLBACK"):
		// Route transaction commands through session for proper handling
		return r.executeSQL(input)

	default:
		// Try to parse and execute as SQL
		return r.executeSQL(input)
	}
}

func (r *REPL) executeSQL(input string) commandResult {
	res, err := r.session.ExecuteSQL(input)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return commandError
	}
	if res.Message != "" {
		fmt.Println(res.Message)
	}
	if len(res.Columns) > 0 {
		r.printTable(res.Columns, res.Rows)
	}
	if res.RowsAffected > 0 && res.Message == "" {
		fmt.Printf("%d row(s) affected\n", res.RowsAffected)
	}
	return commandOK
}

func (r *REPL) printTable(columns []string, rows [][]catalog.Value) {
	if len(rows) == 0 {
		fmt.Println("(0 rows)")
		return
	}

	// Calculate column widths
	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = len(col)
	}
	for _, row := range rows {
		for i, val := range row {
			s := fmt.Sprintf("%v", val)
			if len(s) > widths[i] {
				widths[i] = len(s)
			}
		}
	}

	// Print header
	fmt.Print("|")
	for i, col := range columns {
		fmt.Printf(" %-*s |", widths[i], col)
	}
	fmt.Println()

	// Print separator
	fmt.Print("+")
	for _, w := range widths {
		fmt.Print(strings.Repeat("-", w+2) + "+")
	}
	fmt.Println()

	// Print rows
	for _, row := range rows {
		fmt.Print("|")
		for i, val := range row {
			fmt.Printf(" %-*v |", widths[i], val)
		}
		fmt.Println()
	}

	fmt.Printf("(%d row(s))\n", len(rows))
}

func (r *REPL) handleBackslashCommand(input string) commandResult {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return commandOK
	}

	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "\\q", "\\quit", "\\exit":
		return commandExit

	case "\\?", "\\help":
		r.printHelp()
		return commandOK

	case "\\dt", "\\tables":
		tables := r.catalog().ListTables()
		if len(tables) == 0 {
			fmt.Println("No tables found.")
		} else {
			fmt.Println("\nTables")
			fmt.Println("======")
			for _, t := range tables {
				fmt.Printf("  %s\n", t)
			}
			fmt.Println()
		}
		return commandOK

	case "\\di", "\\indexes":
		fmt.Println("No indexes defined.")
		return commandOK

	case "\\d":
		if len(parts) > 1 {
			tableName := parts[1]
			meta, err := r.catalog().GetTable(tableName)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				return commandError
			}
			fmt.Printf("\nTable: %s\n", meta.Name)
			fmt.Println(strings.Repeat("-", 40))
			fmt.Printf("%-20s %-15s %s\n", "Column", "Type", "Nullable")
			fmt.Println(strings.Repeat("-", 40))
			for _, col := range meta.Columns {
				nullable := "YES"
				if col.NotNull {
					nullable = "NO"
				}
				fmt.Printf("%-20s %-15s %s\n", col.Name, col.Type, nullable)
			}
			fmt.Println()
		} else {
			fmt.Println("Usage: \\d <table_name>")
		}
		return commandOK

	case "\\status":
		r.printStatus()
		return commandOK

	case "\\config":
		r.printConfig()
		return commandOK

	case "\\clear":
		fmt.Print("\033[H\033[2J") // ANSI clear screen
		return commandOK

	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		fmt.Println("Type \\? for help")
		return commandError
	}
}

func (r *REPL) printWelcome() {
	fmt.Printf(`
██╗   ██╗███████╗██████╗ ██╗██████╗ ██╗ ██████╗ █████╗ ██╗     ██████╗ ██████╗ 
██║   ██║██╔════╝██╔══██╗██║██╔══██╗██║██╔════╝██╔══██╗██║     ██╔══██╗██╔══██╗
██║   ██║█████╗  ██████╔╝██║██║  ██║██║██║     ███████║██║     ██║  ██║██████╔╝
╚██╗ ██╔╝██╔══╝  ██╔══██╗██║██║  ██║██║██║     ██╔══██║██║     ██║  ██║██╔══██╗
 ╚████╔╝ ███████╗██║  ██║██║██████╔╝██║╚██████╗██║  ██║███████╗██████╔╝██████╔╝
  ╚═══╝  ╚══════╝╚═╝  ╚═╝╚═╝╚═════╝ ╚═╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚═════╝ ╚═════╝

    Version %s
    Type HELP; or \? for available commands

`, build.String())
}

func (r *REPL) printHelp() {
	fmt.Println(`
VeridicalDB Commands
====================

SQL Commands:
  CREATE TABLE name (columns...)   Create a new table
  DROP TABLE name                  Drop a table
  INSERT INTO table VALUES (...)   Insert rows
  SELECT cols FROM table [WHERE]   Query data
  UPDATE table SET ... [WHERE]     Update rows
  DELETE FROM table [WHERE]        Delete rows

Transaction Commands:
  BEGIN                            Start a transaction
  COMMIT                           Commit transaction
  ROLLBACK                         Rollback transaction

Backslash Commands:
  \dt, \tables                     List all tables
  \di, \indexes                    List all indexes  
  \d <table>                       Describe a table
  \status                          Show server status
  \config                          Show configuration
  \clear                           Clear screen
  \?, \help                        Show this help
  \q, \quit                        Exit

Other:
  EXIT; or QUIT;                   Exit the shell
  HELP;                            Show this help

Note: Commands must end with ; (semicolon)
      Backslash commands do not need ;`)
}

func (r *REPL) printStatus() {
	tableCount := len(r.catalog().ListTables())
	fmt.Println("\nVeridicalDB Status")
	fmt.Println("==================")
	fmt.Printf("Version:    %s\n", build.String())
	fmt.Printf("Data Dir:   %s\n", r.config.Storage.DataDir)
	fmt.Printf("Port:       %d\n", r.config.Server.Port)
	fmt.Printf("Tables:     %d\n", tableCount)
	fmt.Printf("Log Level:  %s\n", r.config.Logging.Level)
	fmt.Println()
}

func (r *REPL) printConfig() {
	fmt.Println("\nCurrent Configuration")
	fmt.Println("=====================")
	fmt.Printf("Server:\n")
	fmt.Printf("  Host:             %s\n", r.config.Server.Host)
	fmt.Printf("  Port:             %d\n", r.config.Server.Port)
	fmt.Printf("  Max Connections:  %d\n", r.config.Server.MaxConnections)
	fmt.Printf("\nStorage:\n")
	fmt.Printf("  Data Directory:   %s\n", r.config.Storage.DataDir)
	fmt.Printf("  Page Size:        %d bytes\n", r.config.Storage.PageSize)
	fmt.Printf("  Buffer Pool:      %d MB\n", r.config.Storage.BufferPoolMB)
	fmt.Printf("  WAL Directory:    %s\n", r.config.Storage.WalDir)
	fmt.Printf("\nLogging:\n")
	fmt.Printf("  Level:            %s\n", r.config.Logging.Level)
	fmt.Printf("  Format:           %s\n", r.config.Logging.Format)
	fmt.Printf("  Output:           %s\n", r.config.Logging.Output)
	fmt.Println()
}

func getHistoryFile() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return home + "/.veridicaldb_history"
}

// newCompleter creates an auto-completer for the REPL
func newCompleter() *readline.PrefixCompleter {
	return readline.NewPrefixCompleter(
		readline.PcItem("SELECT"),
		readline.PcItem("INSERT"),
		readline.PcItem("UPDATE"),
		readline.PcItem("DELETE"),
		readline.PcItem("CREATE",
			readline.PcItem("TABLE"),
			readline.PcItem("INDEX"),
		),
		readline.PcItem("DROP",
			readline.PcItem("TABLE"),
			readline.PcItem("INDEX"),
		),
		readline.PcItem("BEGIN"),
		readline.PcItem("COMMIT"),
		readline.PcItem("ROLLBACK"),
		readline.PcItem("HELP"),
		readline.PcItem("EXIT"),
		readline.PcItem("QUIT"),
		readline.PcItem("\\dt"),
		readline.PcItem("\\di"),
		readline.PcItem("\\d"),
		readline.PcItem("\\status"),
		readline.PcItem("\\config"),
		readline.PcItem("\\clear"),
		readline.PcItem("\\help"),
		readline.PcItem("\\q"),
	)
}

// RunInteractive is a compatibility function that starts the REPL in interactive mode.
// This function provides compatibility with the cmd/server main.go interface.
// RunInteractive starts the REPL against an already-open engine.DB. The caller
// retains ownership of db and is responsible for closing it.
func RunInteractive(db *engine.DB, dataDir string) error {
	// Build a logger for the REPL; the exact level here is not important.
	replLog, err := log.NewFromConfig("info", "text", "stdout")
	if err != nil {
		replLog = log.NewNop()
	}

	cfg := &config.Config{
		Storage: config.StorageConfig{
			DataDir:  dataDir,
			PageSize: 4096,
		},
	}

	repl := &REPL{
		config:  cfg,
		log:     replLog,
		db:      db,
		ownsDB:  false,
		session: db.NewSession(),
	}

	coord, err := SetupShardCoordinator(repl.config, repl.session)
	if err != nil {
		return fmt.Errorf("failed to initialize shard coordinator: %w", err)
	}
	repl.coord = coord
	defer repl.closeResources()

	// Configure readline
	rlConfig := &readline.Config{
		Prompt:          "veridicaldb> ",
		HistoryFile:     getHistoryFile(),
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
		AutoComplete:    newCompleter(),
	}

	rl, err := readline.NewEx(rlConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize readline: %w", err)
	}
	defer func() { _ = rl.Close() }()
	repl.rl = rl

	// Print welcome message
	repl.printWelcome()

	// Main REPL loop
	var multilineBuffer strings.Builder
	inMultiline := false

	for {
		// Update prompt for multiline input
		if inMultiline {
			rl.SetPrompt("         -> ")
		} else {
			rl.SetPrompt("veridicaldb> ")
		}

		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if inMultiline {
				// Cancel multiline input
				multilineBuffer.Reset()
				inMultiline = false
				fmt.Println("^C")
				continue
			}
			continue
		} else if err == io.EOF {
			fmt.Println("\nGoodbye!")
			return nil
		} else if err != nil {
			return fmt.Errorf("readline error: %w", err)
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Handle multiline input
		multilineBuffer.WriteString(line)
		fullInput := multilineBuffer.String()

		// Check if command is complete (ends with semicolon for SQL, immediate for backslash commands)
		if strings.HasPrefix(fullInput, "\\") || strings.HasSuffix(fullInput, ";") {
			// Process complete command
			result := repl.processCommand(strings.TrimSuffix(fullInput, ";"))
			if result == commandExit {
				fmt.Println("Goodbye!")
				return nil
			}
			multilineBuffer.Reset()
			inMultiline = false
		} else {
			// Continue collecting multiline input
			multilineBuffer.WriteString(" ")
			inMultiline = true
		}
	}
}
