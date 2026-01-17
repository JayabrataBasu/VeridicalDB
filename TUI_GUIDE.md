# Testing the VeridicalDB TUI

## Quick Start

The Terminal User Interface (TUI) is built with Bubble Tea and provides an interactive SQL editor with results viewer.

### Prerequisites

Make sure the database is initialized:

```bash
./build/veridicaldb init
```

This creates the `./data` directory and default configuration.

### Launch the TUI

```bash
# Using the built binary
./build/veridicaldb --tui

# Or build and run in one step
go build -o build/veridicaldb ./cmd/veridicaldb && ./build/veridicaldb --tui
```

### Keyboard Navigation

**Home Screen (Menu):**

- `j` / `down` - Navigate down
- `k` / `up` - Navigate up
- `enter` - Select option
- `q` / `exit` - Quit

**SQL Editor Screen:**

- `F5` or `Ctrl+Enter` - Execute query
- `Ctrl+K` or `Ctrl+L` - Clear editor
- `Ctrl+↑` / `Ctrl+↓` - Navigate query history
- `Esc` - Return to menu

**Results Screen:**

- `PgUp` / `PgDn` - Navigate pages
- `←` / `→` (or `h` / `l`) - Scroll columns
- `Home` / `End` - Jump to first/last page
- `Ctrl+E` - Export (coming soon)
- `Esc` / `q` - Return to editor

### Features

✓ SQL Query Editor with line numbers
✓ Query History navigation
✓ Results viewer with pagination
✓ Column scrolling for wide result sets
✓ Error display with suggestions
✓ Dark/Light/High-contrast themes (via configuration)
✓ Session management with transaction support

### Example Workflow

1. Launch: `./build/veridicaldb --tui`
2. Select "New Query" from menu
3. Type a SQL query:

   ```sql
   SELECT 1 as test;
   ```
  
4. Press `F5` to execute
5. Navigate results with arrow keys
6. Press `Esc` to return to editor for next query

### Troubleshooting

- **"No database selected"**: Create a database first:

  ```sql
  CREATE DATABASE mydb;
  USE mydb;
  ```

- **Terminal display issues**: Ensure your terminal supports 256 colors and Unicode
  
- **Clipboard not working**: `Ctrl+V` may not work; copy/paste depends on terminal support

### Development

To rebuild after changes:

```bash
go build -o build/veridicaldb ./cmd/veridicaldb
```

Run tests:

```bash
go test ./internal/tui/...
```

See the implementation:

- Entry point: [cmd/veridicaldb/main.go](cmd/veridicaldb/main.go)
- TUI core: [internal/tui/app.go](internal/tui/app.go)
- Screens: [internal/tui/screens/](internal/tui/screens/)
