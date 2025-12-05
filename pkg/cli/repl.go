// Package cli provides the command-line interface and REPL for VeridicalDB.
package cli

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/log"
)

const (
	// Version of VeridicalDB
	Version = "0.1.0"

	// Prompt displayed to users
	Prompt = "veridical> "

	// ContinuePrompt for multi-line statements
	ContinuePrompt = "       ... "
)

// REPL provides an interactive Read-Eval-Print Loop for VeridicalDB.
type REPL struct {
	in     io.Reader
	out    io.Writer
	logger *log.Logger

	// running tracks if the REPL is currently active
	running bool
}

// NewREPL creates a new REPL instance.
func NewREPL(in io.Reader, out io.Writer, logger *log.Logger) *REPL {
	return &REPL{
		in:     in,
		out:    out,
		logger: logger,
	}
}

// Run starts the REPL loop.
func (r *REPL) Run() error {
	r.running = true
	r.printWelcome()

	scanner := bufio.NewScanner(r.in)
	var buffer strings.Builder

	for r.running {
		// Print appropriate prompt
		if buffer.Len() == 0 {
			fmt.Fprint(r.out, Prompt)
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
		// List tables (stub for now)
		fmt.Fprintln(r.out, "No tables yet. (Storage engine not implemented)")

	case strings.HasPrefix(cmdUpper, "\\DESCRIBE ") || strings.HasPrefix(cmdUpper, "\\D "):
		// Describe table (stub for now)
		parts := strings.Fields(cmd)
		if len(parts) < 2 {
			fmt.Fprintln(r.out, "Usage: \\describe <table_name>")
		} else {
			fmt.Fprintf(r.out, "Table '%s' not found. (Storage engine not implemented)\n", parts[1])
		}

	case cmdUpper == "STATUS" || cmdUpper == "\\S":
		r.printStatus()

	case cmdUpper == "CLEAR" || cmdUpper == "\\C":
		// Clear screen (ANSI escape code)
		fmt.Fprint(r.out, "\033[2J\033[H")

	case strings.HasPrefix(cmdUpper, "CREATE TABLE"):
		fmt.Fprintln(r.out, "CREATE TABLE: Not yet implemented (Stage 2)")

	case strings.HasPrefix(cmdUpper, "INSERT"):
		fmt.Fprintln(r.out, "INSERT: Not yet implemented (Stage 3)")

	case strings.HasPrefix(cmdUpper, "SELECT"):
		fmt.Fprintln(r.out, "SELECT: Not yet implemented (Stage 3)")

	case strings.HasPrefix(cmdUpper, "UPDATE"):
		fmt.Fprintln(r.out, "UPDATE: Not yet implemented (Stage 3)")

	case strings.HasPrefix(cmdUpper, "DELETE"):
		fmt.Fprintln(r.out, "DELETE: Not yet implemented (Stage 3)")

	case strings.HasPrefix(cmdUpper, "BEGIN"):
		fmt.Fprintln(r.out, "BEGIN: Not yet implemented (Stage 4)")

	case strings.HasPrefix(cmdUpper, "COMMIT"):
		fmt.Fprintln(r.out, "COMMIT: Not yet implemented (Stage 4)")

	case strings.HasPrefix(cmdUpper, "ROLLBACK"):
		fmt.Fprintln(r.out, "ROLLBACK: Not yet implemented (Stage 4)")

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
  \d <table>         Describe a table
  \h, \?             Show help
  \q                 Quit
  \v                 Show version
  \s                 Show status
  \c                 Clear screen

SQL Commands (Coming Soon):
  CREATE TABLE ...   Create a new table (Stage 2)
  INSERT INTO ...    Insert rows (Stage 3)
  SELECT ...         Query data (Stage 3)
  UPDATE ...         Update rows (Stage 3)
  DELETE ...         Delete rows (Stage 3)
  BEGIN;             Start transaction (Stage 4)
  COMMIT;            Commit transaction (Stage 4)
  ROLLBACK;          Rollback transaction (Stage 4)

═══════════════════════════════════════════════════════════`)
}

// printStatus displays current server status.
func (r *REPL) printStatus() {
	fmt.Fprintln(r.out, `
Server Status:
═══════════════════════════════════════════════════════════
  Version:           0.1.0 (Stage 0 - Skeleton)
  Status:            Running
  Storage Engine:    Not yet implemented
  Transactions:      Not yet implemented
  Connections:       CLI only (TCP not implemented)
═══════════════════════════════════════════════════════════`)
}

// RunInteractive starts an interactive REPL using stdin/stdout.
func RunInteractive(logger *log.Logger) error {
	repl := NewREPL(os.Stdin, os.Stdout, logger)
	return repl.Run()
}
