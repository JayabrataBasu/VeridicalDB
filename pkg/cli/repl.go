// Package cli provides the command-line interface and REPL for VeridicalDB.
package cli

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/auth"
	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/log"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

const (
	// Version of VeridicalDB
	Version = "0.1.0"

	// Prompt displayed to users
	Prompt = "veridical> "

	// TxPrompt displayed when in a transaction
	TxPrompt = "veridical*> "

	// ContinuePrompt for multi-line statements
	ContinuePrompt = "       ... "
)

// REPL provides an interactive Read-Eval-Print Loop for VeridicalDB.
type REPL struct {
	in       io.Reader
	out      io.Writer
	logger   *log.Logger
	tm       *catalog.TableManager
	executor *sql.Executor

	// MVCC support
	mtm     *catalog.MVCCTableManager
	session *sql.Session

	// running tracks if the REPL is currently active
	running bool
}

// NewREPL creates a new REPL instance.
func NewREPL(in io.Reader, out io.Writer, logger *log.Logger, tm *catalog.TableManager, txnMgr *txn.Manager, txnLogger *wal.TxnLogger) *REPL {
	var executor *sql.Executor
	var mtm *catalog.MVCCTableManager
	var session *sql.Session

	if tm != nil {
		executor = sql.NewExecutor(tm)

		// Create MVCC layer
		if txnMgr == nil {
			txnMgr = txn.NewManager()
		}
		mtm = catalog.NewMVCCTableManager(tm, txnMgr, txnLogger)
		session = sql.NewSession(mtm)
	}

	return &REPL{
		in:       in,
		out:      out,
		logger:   logger,
		tm:       tm,
		executor: executor,
		mtm:      mtm,
		session:  session,
	}
}

// RunInteractive starts the REPL in interactive mode.
func RunInteractive(logger *log.Logger, tm *catalog.TableManager, txnMgr *txn.Manager, txnLogger *wal.TxnLogger, dataDir string) error {
	repl := NewREPL(os.Stdin, os.Stdout, logger, tm, txnMgr, txnLogger)

	// If we have a session, wire in optional components: DatabaseManager and UserCatalog
	if repl.session != nil {
		if dbMgr, err := catalog.NewDatabaseManager(dataDir); err == nil {
			repl.session.SetDatabaseManager(dbMgr)
		} else {
			// Non-fatal: warn and continue
			repl.logger.Warn("database manager not available", "error", err)
		}

		if uc, err := auth.NewUserCatalog(dataDir); err == nil {
			repl.session.SetUserCatalog(uc)
		} else {
			repl.logger.Warn("user catalog not available", "error", err)
		}

		// Wire IndexManager (optional)
		if idxMgr, err := btree.NewIndexManager(dataDir, 4096); err == nil {
			repl.session.SetIndexManager(idxMgr)
		} else {
			repl.logger.Warn("index manager not available", "error", err)
		}

		// Wire TriggerCatalog (optional)
		if tc, err := catalog.NewTriggerCatalog(dataDir); err == nil {
			repl.session.SetTriggerCatalog(tc)
		} else {
			repl.logger.Warn("trigger catalog not available", "error", err)
		}

		// Wire ProcedureCatalog (optional)
		if pc, err := catalog.NewProcedureCatalog(dataDir); err == nil {
			repl.session.SetProcedureCatalog(pc)
		} else {
			repl.logger.Warn("procedure catalog not available", "error", err)
		}

		// If DatabaseManager is present but no database selected, print a short hint
		if repl.session.HasDatabaseManager() && repl.session.CurrentDatabase() == "" {
			fmt.Fprintln(repl.out, "No database selected. Run: CREATE DATABASE <name>; then USE <name> to select it.")
		}
	}

	return repl.Run()
}

// Run starts the REPL loop.
func (r *REPL) Run() error {
	r.running = true
	r.printWelcome()

	scanner := bufio.NewScanner(r.in)
	var buffer strings.Builder

	for r.running {
		// Print appropriate prompt (with transaction indicator if active)
		if buffer.Len() == 0 {
			fmt.Fprint(r.out, r.getPrompt())
		} else {
			fmt.Fprint(r.out, ContinuePrompt)
		}

		// Read input
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()

		// Handle empty lines
		if strings.TrimSpace(line) == "" {
			continue
		}

		// Accumulate input
		if buffer.Len() > 0 {
			buffer.WriteString(" ")
		}
		buffer.WriteString(line)

		// Check if statement is complete (ends with semicolon)
		input := strings.TrimSpace(buffer.String())
		if !strings.HasSuffix(input, ";") {
			// Multi-line statement, continue reading
			continue
		}

		// Execute the statement
		r.execute(input)
		buffer.Reset()
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	fmt.Fprintln(r.out, "Goodbye!")
	return nil
}

// getPrompt returns the appropriate prompt based on transaction state.
func (r *REPL) getPrompt() string {
	var db string
	if r.session != nil {
		db = r.session.CurrentDatabase()
	}

	inTx := r.session != nil && r.session.InTransaction()

	if db != "" {
		if inTx {
			return fmt.Sprintf("veridical[%s] [tx]> ", db)
		}
		return fmt.Sprintf("veridical[%s]> ", db)
	}

	if inTx {
		return "veridical [tx]> "
	}
	return Prompt
}

// printWelcome displays the welcome banner.
func (r *REPL) printWelcome() {
	fmt.Fprintf(r.out, `
╔═══════════════════════════════════════════════════════════╗
║                      VeridicalDB v%s                     ║
║          A Modern Database Built From Scratch             ║
╠═══════════════════════════════════════════════════════════╣
║  Type HELP; for available commands                        ║
║  Type EXIT; to quit                                       ║
╚═══════════════════════════════════════════════════════════╝

`, Version)
}

// execute processes a complete SQL statement or command.
func (r *REPL) execute(input string) {
	// Remove trailing semicolon for command processing
	cmd := strings.TrimSuffix(strings.TrimSpace(input), ";")
	cmd = strings.TrimSpace(cmd)
	cmdUpper := strings.ToUpper(cmd)

	r.logger.Debug("executing command", "cmd", cmd)

	switch {
	case cmdUpper == "EXIT" || cmdUpper == "QUIT" || cmdUpper == "\\Q":
		r.running = false

	case cmdUpper == "HELP" || cmdUpper == "\\H" || cmdUpper == "\\?":
		r.printHelp()

	case cmdUpper == "VERSION" || cmdUpper == "\\V":
		fmt.Fprintf(r.out, "VeridicalDB version %s\n", Version)

	case cmdUpper == "\\LIST" || cmdUpper == "\\DT":
		r.listTables()

	case strings.HasPrefix(cmdUpper, "\\DESCRIBE ") || strings.HasPrefix(cmdUpper, "\\D "):
		parts := strings.Fields(cmd)
		if len(parts) < 2 {
			fmt.Fprintln(r.out, "Usage: \\describe <table_name>")
		} else {
			r.describeTable(parts[1])
		}

	case cmdUpper == "STATUS" || cmdUpper == "\\S":
		r.printStatus()

	case cmdUpper == "\\DI":
		r.listIndexes()

	case cmdUpper == "\\STATS":
		r.printStatistics()

	case cmdUpper == "\\LOCKS":
		r.printLocks()

	case cmdUpper == "\\TXNS":
		r.printTransactions()

	case cmdUpper == "\\MEMORY" || cmdUpper == "\\MEM":
		r.printMemoryStats()

	case cmdUpper == "CLEAR" || cmdUpper == "\\C":
		// Clear screen (ANSI escape code)
		fmt.Fprint(r.out, "\033[2J\033[H")

	case strings.HasPrefix(cmdUpper, "CREATE TABLE"):
		r.executeSQL(input)

	case strings.HasPrefix(cmdUpper, "DROP TABLE"):
		r.executeSQL(input)

	case strings.HasPrefix(cmdUpper, "INSERT"):
		r.executeSQL(input)

	case strings.HasPrefix(cmdUpper, "SELECT"):
		r.executeSQL(input)

	case strings.HasPrefix(cmdUpper, "UPDATE"):
		r.executeSQL(input)

	case strings.HasPrefix(cmdUpper, "DELETE"):
		r.executeSQL(input)

	case strings.HasPrefix(cmdUpper, "BEGIN"):
		r.executeSQL(input)

	case strings.HasPrefix(cmdUpper, "COMMIT"):
		r.executeSQL(input)

	case strings.HasPrefix(cmdUpper, "ROLLBACK"):
		r.executeSQL(input)

	default:
		fmt.Fprintf(r.out, "Unknown command: %s\nType HELP; for available commands.\n", cmd)
	}
}

// printHelp displays available commands.
func (r *REPL) printHelp() {
	fmt.Fprintln(r.out, `
Available Commands:
═══════════════════════════════════════════════════════════

  HELP;              Show this help message
  VERSION;           Show VeridicalDB version
  STATUS;            Show server status
  EXIT;              Exit the REPL
  CLEAR;             Clear the screen

Meta Commands:
  \dt, \list         List all tables
  \di                List all indexes
  \d <table>         Describe a table
  \h, \?             Show help
  \q                 Quit
  \v                 Show version
  \s                 Show status
  \c                 Clear screen

Monitoring Commands:
  \stats             Show database statistics
  \locks             Show active locks
  \txns              Show active transactions
  \mem, \memory      Show memory statistics

SQL Commands:
  CREATE TABLE name (col type, ...);   Create a new table
  DROP TABLE name;                     Drop a table
  INSERT INTO name VALUES (...);       Insert rows
  SELECT ... FROM name [WHERE ...];    Query data
  UPDATE name SET ... [WHERE ...];     Update rows
  DELETE FROM name [WHERE ...];        Delete rows

Transaction Commands:
  BEGIN;             Start a new transaction
  COMMIT;            Commit current transaction
  ROLLBACK;          Rollback current transaction

═══════════════════════════════════════════════════════════`)
}

// printStatus displays current server status.
func (r *REPL) printStatus() {
	tableCount := 0
	if r.tm != nil {
		tableCount = len(r.tm.ListTables())
	}
	txnStatus := "Not active"
	if r.session != nil && r.session.InTransaction() {
		txnStatus = "Active"
	}
	fmt.Fprintf(r.out, `
Server Status:
═══════════════════════════════════════════════════════════
  Version:           %s (Stage 4 - MVCC)
  Status:            Running
  Tables:            %d
  Storage Engine:    Heap (row store)
  SQL Support:       CREATE, DROP, INSERT, SELECT, UPDATE, DELETE
  Transactions:      MVCC (Snapshot Isolation)
  Current Txn:       %s
  Connections:       CLI only (TCP not implemented)
═══════════════════════════════════════════════════════════
`, Version, tableCount, txnStatus)
}

// listTables prints all tables in the database.
func (r *REPL) listTables() {
	if r.tm == nil {
		fmt.Fprintln(r.out, "TableManager not initialized.")
		return
	}
	tables := r.tm.ListTables()
	if len(tables) == 0 {
		fmt.Fprintln(r.out, "No tables found.")
		return
	}
	sort.Strings(tables)
	fmt.Fprintln(r.out, "\nTables:")
	fmt.Fprintln(r.out, "═══════════════════════════════════════")
	for _, t := range tables {
		fmt.Fprintf(r.out, "  %s\n", t)
	}
	fmt.Fprintf(r.out, "\n(%d table(s))\n", len(tables))
}

// describeTable prints column info for a table.
func (r *REPL) describeTable(name string) {
	if r.tm == nil {
		fmt.Fprintln(r.out, "TableManager not initialized.")
		return
	}
	cols, err := r.tm.DescribeTable(name)
	if err != nil {
		fmt.Fprintf(r.out, "Error: %v\n", err)
		return
	}
	fmt.Fprintf(r.out, "\nTable: %s\n", name)
	fmt.Fprintln(r.out, "═══════════════════════════════════════════════════════")
	fmt.Fprintf(r.out, "%-4s %-20s %-12s %-10s\n", "ID", "Column", "Type", "Nullable")
	fmt.Fprintln(r.out, "───────────────────────────────────────────────────────")
	for _, c := range cols {
		nullable := "YES"
		if c.NotNull {
			nullable = "NO"
		}
		fmt.Fprintf(r.out, "%-4d %-20s %-12s %-10s\n", c.ID, c.Name, c.Type.String(), nullable)
	}
	fmt.Fprintln(r.out, "═══════════════════════════════════════════════════════")
}

// executeSQL parses and executes a SQL statement.
func (r *REPL) executeSQL(input string) {
	// Prefer session (MVCC-enabled) over executor
	if r.session != nil {
		result, err := r.session.ExecuteSQL(input)
		if err != nil {
			fmt.Fprintf(r.out, "Error: %v\n", err)
			return
		}
		r.displayResult(result)
		return
	}

	// Fallback to legacy executor if no session
	if r.executor == nil {
		fmt.Fprintln(r.out, "SQL executor not initialized.")
		return
	}

	// Parse the SQL statement
	parser := sql.NewParser(input)
	stmt, err := parser.Parse()
	if err != nil {
		fmt.Fprintf(r.out, "Syntax error: %v\n", err)
		return
	}

	// Execute the statement
	result, err := r.executor.Execute(stmt)
	if err != nil {
		fmt.Fprintf(r.out, "Error: %v\n", err)
		return
	}

	// Display the result
	r.displayResult(result)
}

// displayResult formats and prints a query result.
func (r *REPL) displayResult(result *sql.Result) {
	// If it's a message-only result (CREATE, INSERT, UPDATE, DELETE)
	if result.Message != "" && result.Columns == nil {
		fmt.Fprintln(r.out, result.Message)
		return
	}

	// It's a SELECT query result
	if len(result.Columns) == 0 {
		fmt.Fprintln(r.out, "(no columns)")
		return
	}

	// Calculate column widths
	widths := make([]int, len(result.Columns))
	for i, col := range result.Columns {
		widths[i] = len(col)
	}
	for _, row := range result.Rows {
		for i, val := range row {
			str := formatValue(val)
			if len(str) > widths[i] {
				widths[i] = len(str)
			}
		}
	}

	// Cap max column width at 40 characters
	for i := range widths {
		if widths[i] > 40 {
			widths[i] = 40
		}
	}

	// Print header
	fmt.Fprint(r.out, "\n")
	for i, col := range result.Columns {
		fmt.Fprintf(r.out, " %-*s ", widths[i], truncate(col, widths[i]))
		if i < len(result.Columns)-1 {
			fmt.Fprint(r.out, "│")
		}
	}
	fmt.Fprintln(r.out)

	// Print separator
	for i := range result.Columns {
		fmt.Fprint(r.out, strings.Repeat("─", widths[i]+2))
		if i < len(result.Columns)-1 {
			fmt.Fprint(r.out, "┼")
		}
	}
	fmt.Fprintln(r.out)

	// Print rows
	for _, row := range result.Rows {
		for i, val := range row {
			str := formatValue(val)
			fmt.Fprintf(r.out, " %-*s ", widths[i], truncate(str, widths[i]))
			if i < len(row)-1 {
				fmt.Fprint(r.out, "│")
			}
		}
		fmt.Fprintln(r.out)
	}

	// Print row count
	fmt.Fprintf(r.out, "\n(%d row(s))\n", len(result.Rows))
}

// formatValue converts a catalog.Value to a string for display.
func formatValue(v catalog.Value) string {
	if v.IsNull {
		return "NULL"
	}
	switch v.Type {
	case catalog.TypeInt32:
		return fmt.Sprintf("%d", v.Int32)
	case catalog.TypeInt64:
		return fmt.Sprintf("%d", v.Int64)
	case catalog.TypeText:
		return v.Text
	case catalog.TypeBool:
		if v.Bool {
			return "true"
		}
		return "false"
	case catalog.TypeTimestamp:
		return v.Timestamp.Format("2006-01-02 15:04:05")
	default:
		return "?"
	}
}

// truncate limits a string to maxLen characters.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen < 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

// listIndexes prints all indexes in the database.
func (r *REPL) listIndexes() {
	// Indexes are managed by btree.IndexManager, which we don't have direct access to
	// For now, show a placeholder message
	fmt.Fprintln(r.out, "\nIndexes:")
	fmt.Fprintln(r.out, "═══════════════════════════════════════")
	fmt.Fprintln(r.out, "  (Index listing requires IndexManager integration)")
	fmt.Fprintln(r.out, "  Use CREATE INDEX to create indexes on tables")
	fmt.Fprintln(r.out, "")
}

// printStatistics prints database statistics.
func (r *REPL) printStatistics() {
	fmt.Fprintln(r.out, "\nDatabase Statistics:")
	fmt.Fprintln(r.out, "═══════════════════════════════════════════════════════")

	tableCount := 0
	if r.tm != nil {
		tableCount = len(r.tm.ListTables())
	}

	txnStatus := "None"
	if r.session != nil && r.session.InTransaction() {
		txnStatus = "Active"
	}

	fmt.Fprintf(r.out, "  %-25s %d\n", "Tables:", tableCount)
	fmt.Fprintf(r.out, "  %-25s %s\n", "Current Transaction:", txnStatus)
	fmt.Fprintln(r.out, "")
	fmt.Fprintln(r.out, "  Note: Full statistics available via SystemCatalog API")
	fmt.Fprintln(r.out, "═══════════════════════════════════════════════════════")
}

// printLocks prints current lock information.
func (r *REPL) printLocks() {
	fmt.Fprintln(r.out, "\nActive Locks:")
	fmt.Fprintln(r.out, "═══════════════════════════════════════════════════════")
	fmt.Fprintln(r.out, "  (Lock information requires LockManager integration)")
	fmt.Fprintln(r.out, "  Use SystemCatalog.GetLocks() for programmatic access")
	fmt.Fprintln(r.out, "═══════════════════════════════════════════════════════")
}

// printTransactions prints active transaction information.
func (r *REPL) printTransactions() {
	fmt.Fprintln(r.out, "\nActive Transactions:")
	fmt.Fprintln(r.out, "═══════════════════════════════════════════════════════")

	if r.session == nil {
		fmt.Fprintln(r.out, "  (Session not initialized)")
	} else if r.session.InTransaction() {
		fmt.Fprintln(r.out, "  Current session has an active transaction")
	} else {
		fmt.Fprintln(r.out, "  No active transaction in current session")
	}

	fmt.Fprintln(r.out, "")
	fmt.Fprintln(r.out, "  Use SystemCatalog.GetActiveTransactions() for full list")
	fmt.Fprintln(r.out, "═══════════════════════════════════════════════════════")
}

// printMemoryStats prints memory statistics.
func (r *REPL) printMemoryStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Fprintln(r.out, "\nMemory Statistics:")
	fmt.Fprintln(r.out, "═══════════════════════════════════════════════════════")
	fmt.Fprintf(r.out, "  %-25s %d MB\n", "Heap Allocated:", m.HeapAlloc/1024/1024)
	fmt.Fprintf(r.out, "  %-25s %d MB\n", "Heap System:", m.HeapSys/1024/1024)
	fmt.Fprintf(r.out, "  %-25s %d\n", "Heap Objects:", m.HeapObjects)
	fmt.Fprintf(r.out, "  %-25s %d\n", "Goroutines:", runtime.NumGoroutine())
	fmt.Fprintf(r.out, "  %-25s %d\n", "GC Cycles:", m.NumGC)
	fmt.Fprintf(r.out, "  %-25s %.2f ms\n", "Last GC Pause:", float64(m.PauseNs[(m.NumGC+255)%256])/1e6)
	fmt.Fprintln(r.out, "═══════════════════════════════════════════════════════")
}
