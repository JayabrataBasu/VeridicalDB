# Plan: Build Beautiful, Extensible VeridicalDB TUI with Bubble Tea

Build a production-grade Terminal User Interface using Bubble Tea that provides **comprehensive access to all VeridicalDB capabilities** with convenience and accessibility as core philosophies. The TUI will be modular, themeable, and architected for long-term extensibility. Reuse existing REPL display logic through a shared display package, integrate with VeridicalDB's mature Session API and MVCC engine, and plan for both local single-user and networked multi-user deployment models.

## Phase Overview: 3 Major Phases

**Phase 1: Foundation & Core** (Weeks 1-2) - Display layer, Bubble Tea structure, SQL editor, results viewer, basic integration
**Phase 2: Feature Parity** (Weeks 3-4) - Database browser, monitoring dashboard, user management, backup interface, all high-priority features from VeridicalDB  
**Phase 3: Polish & Extensibility** (Week 5+) - Theming, advanced UX, plugin architecture, production hardening

---

## Phase 1: Foundation & Core Execution

### Step 1.1: Extract & Create Shared Display Package

Create `pkg/display/` package to decouple output formatting from REPL/TUI execution:

- `formatter.go` - `TableFormatter` struct with `FormatResult()` method (extract displayResult logic from `pkg/cli/repl.go` lines 150-220)
- `value_formatter.go` - `ValueFormatter` for type-specific formatting (consolidate formatValue + Value.String())
- `prompt.go` - `PromptBuilder` for context display (database, transaction state, user)
- `errors.go` - Standardized error formatting for CLI + TUI consumption
- Remove `io.Writer` dependencies; return formatted strings instead of writing directly
- Unit tests for all formatters

**Why This First**: Enables REPL refactoring and gives TUI clean display primitives immediately without duplicating code.

### Step 1.2: Create Bubble Tea Application Architecture

Create `internal/tui/` package structure for TUI implementation:

```go
internal/tui/
├── app.go              # Main TUI application struct (MVU model)
├── models.go           # Domain models (QueryResult, SessionState, etc)
├── messages.go         # Message types for Bubble Tea event system
├── styles.go           # Lipgloss styling definitions
└── screens/            # Screen implementations (see Step 1.3)
    ├── screen.go       # Screen interface
    ├── home.go         # Home/menu screen
    └── ...
```

- Implement core MVU architecture: `Model`, `Update(msg Msg) (Model, Cmd)`, `View() string`
- Design message routing system for screen transitions (`GotoScreen(screenID)` command)
- Session lifecycle: create on app startup, close on shutdown (graceful rollback)
- State container for shared app state: current database, user, transaction status, theme
- Command queue for async operations (queries, backups) using goroutines + channels

### Step 1.3: Build SQL Editor Screen

Create interactive SQL statement editor as primary screen (`internal/tui/screens/editor.go`):

- **Text Input**: Use `bubbles/textinput.TextInput` for single-line, multi-line for statement body
- **Syntax Highlighting**: For SQL keywords (manual coloring via lipgloss or integrate `chroma` library)
- **Statement Separator Detection**: Track multi-line statements, detect statement completion (`;` terminator)
- **Execution Trigger**: `Ctrl+Enter` or F5 to execute, show loading indicator
- **Async Execution**: Execute query in goroutine, send result back via channel+message
- **Keybindings**:  
  - `Ctrl+Enter` / `F5` - Execute
  - `Ctrl+A` - Select all
  - `Ctrl+K` - Clear editor
  - `Ctrl+/` - Comment/uncomment line
  - `Tab` - Insert spaces (4-space indent)
- **Error Display**: Show SQL parsing/execution errors in dedicated area, keep editor intact

**Key Design**: Non-blocking execution - user can edit while query runs, status bar shows "Running query...".

### Step 1.4: Build Results Viewer Screen

Create display screen for query results (`internal/tui/screens/results.go`):

- **Table Rendering**: Use `TableFormatter` to get formatted output, render via lipgloss grid or custom widget
- **Pagination**: Show "Row 1-50 of 1000" with `PgUp`/`PgDn` or arrow keys to navigate (load on-demand)
- **Column Navigation**: `Left`/`Right` arrows to scroll horizontally for wide tables
- **Column Filtering**: `Ctrl+F` to show/hide columns (checkbox list)
- **Export Functions**: `Ctrl+E` to export as CSV/JSON
- **Row Count**: Display row count prominently
- **Back Button**: Return to editor screen

**Key Design**: Delegate formatting to shared `TableFormatter`; implement streaming for large result sets (fetch 50 rows at a time).

### Step 1.5: Create Home/Menu Screen

Create navigation hub (`internal/tui/screens/home.go`):

- **Menu Items** (selectable):
     1. New Query (go to editor screen)
     2. Database Browser (see Step 2.2)
     3. Monitoring Dashboard (see Step 2.3)
     4. User Management (see Step 2.5)
     5. Backup & Restore (see Step 2.4)
     6. Settings
     7. About / Help
     8. Exit
- **Display Context**: Show current database, user, transaction status in header
- **Prompt**: Display prompt from `PromptBuilder`
- **Recent Queries**: List last 5 executed queries with timestamps

### Step 1.6: Integrate Session & Execute Queries

Modify `internal/tui/app.go` to:

- Create `Session` instance on app startup (share with existing database instance)
- Implement `ExecuteQuery(sql string)` method that:
  - Calls `session.ExecuteSQL(sql)`
  - Handles both DDL responses (`result.Message`) and query results (`result.Columns`, `result.Rows`)
  - Returns formatted output or error
- Track transaction state: `session.InTransaction()` for prompt display
- Implement transaction control commands: `BEGIN`, `COMMIT`, `ROLLBACK` via `session.ExecuteSQL()`

### Step 1.7: Implement Basic Keybindings & Navigation

Create `internal/tui/keybindings.go`:

- Define all hotkeys as constants
- Screen-specific keybindings (editor vs results vs home)
- Global keybindings:
  - `Ctrl+C` / `Esc` - Return to home or cancel operation
  - `Ctrl+L` - Clear terminal
  - `F1` / `Ctrl+H` - Help overlay
  - `Ctrl+Q` - Quit (with confirmation if transaction active)
- Create help text with all keybindings

### Step 1.8: Implement Theming Foundation

Create `internal/tui/theme/` package:

- Define `Theme` struct with colors, fonts, borders
- Default theme (light + dark variants)
- High-contrast and colorblind-friendly themes
- Apply themes consistently via lipgloss `Renderer` with color profile detection
- Store theme preference in user config file

### Step 1.9: Add Error & Status Messages

Create unified error/status display:

- Toast notifications for non-critical messages (query completed, backup started)
- Error modal for failures (with error details and recovery suggestions)
- Status bar at bottom showing:
  - Current mode (editing, executing, viewing results)
  - Query status (running / completed / failed)
  - Transaction status (IN TRANSACTION / autocommit)
  - Current time

### Step 1.10: Testing & Local Verification

- Unit tests for `pkg/display/` extractors (formatter, value_formatter)
- Integration test: execute simple SELECT query end-to-end
- Manual testing: run TUI locally, execute queries, verify formatting
- Refactor existing `pkg/cli/repl.go` to use new `pkg/display` package (removes code duplication)

---

## Phase 2: Feature Parity with VeridicalDB

### Step 2.1: Database & Schema Browser

Create `internal/tui/screens/browser.go` - tree-style navigator:

- **Root**: List of databases (fetched from `DatabaseManager.ListDatabases()`)
- **Expand Database**: Show tables
- **Expand Table**: Show columns with types, constraints, indexes
- **Context Menu** (right-click or menu key):
  - `SELECT * FROM table` - Template query in editor
  - View table metadata (row count, disk size, last modified)
  - Show indexes on table
  - Truncate table (with confirmation)
  - Drop table (with confirmation)
- **Search**: `Ctrl+F` to filter by name (databases, tables, columns)

### Step 2.2: Monitoring Dashboard

Create `internal/tui/screens/monitoring.go` - real-time observability:

- **System Metrics** (via `SystemCatalog`):
  - Active transactions count, list with TxID, status, duration
  - Active locks (table locks, row locks)
  - Memory usage (heap, goroutines, GC stats)
  - Query statistics (count, avg duration, success rate)
- **Refresh Rate**: Auto-refresh every 1-2 seconds (configurable)
- **Drilling Down**: Click transaction → show query, locks held, isolation level
- **Kill Transaction**: Option to abort a transaction (security check: require confirmation)
- **Charts** (if using `go-echarts` or similar):
  - Memory usage over time (sparkline)
  - Transaction rate (queries/sec)
  - Lock contention heatmap

### Step 2.3: User & Authentication Management

Create `internal/tui/screens/users.go`:

- **User List** (from `UserCatalog.ListUsers()`):
  - Show username, is_superuser flag, creation timestamp
- **Create User** (modal form):
  - Input: username, password, confirm password
  - Checkbox: "Grant SUPERUSER privilege"
  - Validation: username format, password strength
  - Call `UserCatalog.CreateUser()`
- **Edit User** (modal):
  - Change password (require old password)
  - Toggle superuser privilege
  - Disable/enable user (future feature)
- **Delete User** (with confirmation)
- **Grant/Revoke Privileges** (modal):
  - Select table, select privilege (SELECT, INSERT, UPDATE, DELETE), grant/revoke
  - Use `UserCatalog.Grant()/Revoke()`
- **Current User Display**: Show logged-in user in status bar

### Step 2.4: Backup & Disaster Recovery Interface

Create `internal/tui/screens/backup.go`:

- **Tab 1: Create Backup**:
  - Call `Manager.CreateBaseBackup()`
  - Show progress (percentage, ETA)
  - Backup location and metadata on completion
  - Option to verify backup
- **Tab 2: Backup List** (from `Manager.ListBackups()`):
  - Show all backups with: timestamp, size, LSN range, status
  - Actions: Download, Verify, Delete, Restore
- **Tab 3: Point-in-Time Recovery (PITR)**:
  - Calendar date picker for target time
  - Or LSN input field
  - Preview what will be restored
  - Call `Manager.Restore()` with options
  - Show restore progress
- **Tab 4: WAL Management** (from `Archiver`):
  - List archived WAL segments
  - Show archiving status
  - Manual trigger for `WAL ARCHIVE`
- **Error Handling**: Show recovery logs if restore fails

### Step 2.5: Index Management

Create `internal/tui/screens/indexes.go`:

- **Index List** (from `IndexManager.ListIndexes()`):
  - Table-format: index name, table, columns, type (B-Tree/FTS), unique flag
- **Create Index** (modal form):
  - Select table
  - Input index name
  - Multi-select columns
  - Checkbox: "Make UNIQUE"
  - Checkbox: "Full-Text Search" (vs B-Tree)
  - Call `IndexManager.CreateIndex()`
- **Drop Index** (with confirmation)
- **Index Statistics**:
  - Pages used, entries, average lookup time
  - Last updated timestamp
- **Query Hints**: Show which indexes are available for optimization

### Step 2.6: Query History & Saved Queries

Create `internal/tui/screens/history.go`:

- **History Pane** (modal or sidebar):
  - List last 100 executed queries with timestamps
  - Search by keyword (`Ctrl+R` reverse search like bash)
  - Select to insert into editor
  - Mark favorites (star icon)
- **Saved Query Templates**:
  - YAML/JSON file in config directory: `~/.veridicaldb/queries.yaml`
  - Organize by category (Admin, Analytics, Reporting, etc.)
  - Quick-insert via command palette
  - Create new from current query (with name/category)
  - Edit/delete existing templates

### Step 2.7: Transaction Management Screen

Create `internal/tui/screens/transactions.go`:

- **Active Transactions**:
  - Table: TxID, status, isolation level, start time, age, current query (if any)
- **Transaction Controls**:
  - Savepoint creation (name input)
  - Rollback to savepoint (list savepoints)
  - Set isolation level (dropdown)
- **Deadlock Detection** (if implemented):
  - Show deadlock cycles
  - Recommend victim transaction to abort
- **Locks Held**: Show locks held by current transaction

### Step 2.8: Advanced SQL Features Panel

Create `internal/tui/screens/advanced.go`:

- **EXPLAIN Plans**: Execute `EXPLAIN` on query in editor, show execution plan tree
- **Prepared Statements** (from `Session.preparedStmts`):
  - List prepared statements
  - EXECUTE statement with parameter input
  - DEALLOCATE statement
- **Stored Procedures/Functions**:
  - Call stored procedure with parameter form
  - Show procedure source
- **Triggers**: List, enable/disable, show source
- **Views**: List, show definition

---

## Phase 3: Polish, Extensibility & Production Readiness

### Step 3.1: Implement Plugin/Extension Architecture

Create `internal/tui/extensions/` framework:

- Define `Screen` interface for custom screens
- Define `Command` interface for custom commands
- Registry pattern: screens/commands register at startup
- Feature flags in config to enable/disable screens
- Allows future features (replication, sharding dashboards) to be plugged in

### Step 3.2: Accessibility Enhancements

- **Screen Reader Support**: Proper semantic output for complex widgets
- **Keyboard Navigation**: Full keyboard-first operation (no mouse required)
- **Mouse Support**: Optional mouse clicks for menus, table selection (Bubble Tea built-in)
- **High Contrast Theme**: Black/white with max contrast
- **Colorblind Themes**: Deuteranopia, Protanopia, Tritanopia variants
- **Font Resizing** (terminal-level): Document compatible terminals
- **Alt-Text**: For complex output (query plans, metrics charts)

### Step 3.3: Configuration & Customization

Create `~/.veridicaldb/config.yaml` with TUI options:

- Theme selection (light, dark, high-contrast, colorblind-X)
- Color scheme customization (custom hex colors)
- Keybinding overrides (JSON map: action → key)
- Default query template
- Auto-connect to database on startup
- Logging level
- Query timeout (milliseconds)
- Result pagination size
- Auto-save session state (restore on restart)

### Step 3.4: Session State Persistence

Implement session recovery:

- Save on exit: current database, open query in editor, scroll position, command history
- Load on startup: restore previous session (with opt-out flag)
- Session file: `~/.veridicaldb/session.json`
- Include: database, editor content, history, recent queries, favorites

### Step 3.5: Advanced UX Features

- **Command Palette** (`Ctrl+P` / `Cmd+P`):
  - Fuzzy search for: SQL commands, database operations, settings
  - Show keyboard shortcut next to each command
  - Recent commands at top
- **Autocomplete** (`Tab`):
  - Context-aware: table names after `FROM`, column names after `SELECT`
  - Function names, keywords
  - Show preview of selected suggestion
- **Inline Help**: Press `?` in any screen for context-sensitive help
- **Undo/Redo** (`Ctrl+Z` / `Ctrl+Shift+Z`): For editor input
- **Multi-Tab Support** (optional advanced feature):
  - Open multiple query tabs
  - Switch with `Ctrl+Tab` or tab selector
  - Save/load query tabs

### Step 3.6: Error Handling & Recovery

- **Graceful Degradation**: If non-critical feature unavailable, disable in UI (e.g., if monitoring query fails)
- **Automatic Reconnection**: If connection drops, try reconnecting (with backoff)
- **Transaction Safety**: Warn if quitting with active transaction
- **Crash Recovery**: Log session to temp file, offer recovery on restart
- **Detailed Error Messages**: Show root cause + suggested action (e.g., "Check syntax" for parse error)

### Step 3.7: Performance Optimization

- **Lazy Loading**: Load data on-demand (table schemas, monitoring data)
- **Caching**: Cache table/column lists (invalidate on DDL)
- **Debouncing**: Debounce typing in search/filter fields
- **Pagination**: Fetch results in chunks (50 rows at a time)
- **Background Updates**: Monitoring dashboard auto-refresh without blocking UI

### Step 3.8: Testing & CI/CD

- **Unit Tests**: Test all display formatters, message handlers
- **Integration Tests**: End-to-end query execution, screen navigation
- **Visual Regression Tests** (if possible): Snapshot TUI output for screens
- **Performance Tests**: Measure response time for large result sets
- **CI Pipeline**: GitHub Actions to run tests on every commit

### Step 3.9: Documentation & Help System

- **Built-in Help** (`F1`): Screen-specific help with examples
- **User Guide**: Markdown documentation covering:
  - Getting started
  - All screens and features
  - Keyboard shortcuts reference
  - Configuration options
  - Troubleshooting
- **In-app Tutorials**: Interactive walkthroughs for first-time users
- **Example Queries**: Pre-loaded sample queries for learning

### Step 3.10: Production Hardening

- **Security**:
  - Don't log passwords (filter from history)
  - Clear sensitive data on exit
  - Respect Unix file permissions for config/session files
  - Option for strict password requirements
- **Stability**:
  - Panic recovery (catch panics, show error modal, continue)
  - Connection timeout configuration
  - Statement timeout for long-running queries
- **Monitoring**:
  - Internal metrics (TUI response time, memory usage)
  - Logs to file in `.veridicaldb/logs/`
  - Option to send telemetry (opt-in)

---

## Further Considerations: Addressing Key Design Decisions

### 1. **Single-User Local vs. Multi-User Networked**

**Current Design**: TUI operates **locally** on same machine as database server.

**Rationale**:

- No network overhead
- Direct file system access (backups, config)
- Can run alongside pgwire server (both share `MVCCTableManager`)
- Simpler initial implementation

**Future Extension (Phase 4)**:

- Remote TUI mode: Connect to pgwire server via TCP
- Use existing PostgreSQL wire protocol (no new protocol)
- Reduced feature set for remote (no file access for backups)

**Implementation Detail**: Session created locally uses shared `MVCCTableManager`; remote mode would use `Session` created via network client.

---

### 2. **Extraction of Display Package & REPL Refactoring**

**Critical First Step**: Extract `pkg/display/` package before building TUI.

**REPL Refactoring** (`pkg/cli/repl.go`):

- Replace internal `displayResult()` with `display.TableFormatter.FormatResult()`
- Replace internal `formatValue()` with `display.ValueFormatter.Format()`
- Use `display.PromptBuilder` for prompt generation
- **Result**: REPL becomes 30% smaller, eliminates code duplication with TUI

**Benefits**:

- Single source of truth for formatting
- Easier to maintain consistent output across tools
- CSV/JSON exporters can build on same formatters

---

### 3. **Theming Strategy: Customization vs. Simplicity**

**Approach**: Defined themes (light, dark, high-contrast, colorblind variants) + customization layer.

**Why Not Full Customization First?**

- Defined themes ensure accessibility is built-in
- Simpler to test and maintain
- Prevents bad color combinations that harm visibility

**Customization Path** (Phase 3):

- Allow hex color overrides for defined colors in theme
- Predefined theme variants in config
- Community themes shareable via files

---

### 4. **Bubble Tea vs. Other TUI Frameworks**

**Bubble Tea Confirmed Best Choice**:

- MVU architecture handles screen transitions elegantly
- Lipgloss + bubbles ecosystem is mature and well-documented
- Used in production tools (Gum, Soft Serve, etc.)
- Active maintenance and community
- Async command support perfect for non-blocking query execution

**Why Not Termui?**

- Dashboard-focused, not ideal for multi-screen app with forms
- Smaller ecosystem

**Why Not Cobra-only Hybrid?**

- Would require manual terminal manipulation
- Harder to implement rich interactivity
- Limits future extensions (graphs, trees, etc.)

---

### 5. **Async Query Execution: Critical for UX**

**Problem**: Long-running queries freeze UI

**Solution**: Execute in goroutine, send result via channel.

**Pattern**:

```go
// In Update() message handler
case ExecuteQueryMsg:
    go func() {
        result, err := app.session.ExecuteSQL(msg.SQL)
        app.msgChan <- QueryCompletedMsg{result, err}
    }()
    return app, tea.Batch(
        tea.Every(100*time.Millisecond, func() tea.Msg {
            return RefreshStatusMsg{}  // Show loading indicator
        }),
    )
```

**UI Behavior**: Show "Running query..." with animated spinner, keep editor responsive.

---

### 6. **Extensibility: Plugin System for Future Features**

**Design Pattern**:

```go
// internal/tui/extensions/screen.go
type Screen interface {
    Init(ctx *AppContext) error
    Update(msg tea.Msg) (Screen, tea.Cmd)
    View() string
    ID() string  // Unique screen identifier
}

// Registry
type ScreenRegistry struct {
    screens map[string]Screen
}

func (r *ScreenRegistry) Register(s Screen, enabled bool) {
    if enabled {
        r.screens[s.ID()] = s
    }
}
```

**Future Screens** (can be added as separate packages):

- Replication Status Monitor
- Shard/Partition Dashboard
- Full-Text Search Manager
- Trigger/Procedure Manager
- Custom Metrics Dashboard

**Feature Flags** (in config):

```yaml
features:
  backup: true
  monitoring: true
  replication: false  # Disabled if not running replication
  sharding: false
```

---

### 7. **Concurrent Connections: TUI Isolated from Network Server**

**Architecture**:

- TUI Session = local, non-networked
- pgwire Server = networked clients (separate Connections)
- Both access same `MVCCTableManager`
- MVCC ensures isolation between concurrent connections

**Benefit**: No new concurrency issues introduced. Existing MVCC + lock manager handle multi-connection safety.

---

### 8. **Backward Compatibility: REPL Coexistence**

**Current Plan**:  

- Keep `pkg/cli/repl.go` functional
- Refactor to use `pkg/display/` extractors
- Support both `veridicaldb` (REPL) and new TUI via separate binary or flag

**Alternative** (Phase 3):

- Make REPL a "classic mode" within TUI
- Existing scripts/workflows using REPL still work

---

### 9. **Data Export & Portability**

**Formats Supported**:

- **CSV**: Tab-separated or comma-separated
- **JSON**: Array of objects format
- **SQL**: INSERT statements (for data migration)

**Implementation**: Use `ValueFormatter.FormatForCSV()` and `ValueFormatter.FormatForJSON()` methods.

**Storage**: Export to file (user selects path via file picker, or clipboard copy).

---

### 10. **Security: Passwords, Credentials, Sensitive Data**

**Practices**:

- Never log or display passwords in history
- Filter from session save file
- Encrypt session file if contains sensitive queries (future)
- Respect `.gitignore` for config/session directories
- Optional "private mode" that doesn't save history
- Clear sensitive data from memory on exit (use `mlock` on Linux?)

---

## Implementation Timeline & Deliverables

### **Week 1-2 (Phase 1)**

- ✅ Extract `pkg/display/` package (3 days)
- ✅ Refactor REPL to use display package (2 days)
- ✅ Build Bubble Tea foundation (`internal/tui/app.go`, screen router) (3 days)
- ✅ Implement SQL editor screen + results viewer (3 days)
- ✅ Home screen + basic navigation (2 days)
- ✅ Session integration + query execution (2 days)
- ✅ Basic theming + status bar (2 days)

**Deliverable**: Functional TUI that can execute SELECT/INSERT/UPDATE/DELETE queries with formatted output. Parity with minimal REPL.

---

### **Week 3-4 (Phase 2)**

- ✅ Database/table browser (3 days)
- ✅ Monitoring dashboard (4 days)
- ✅ User management screen (2 days)
- ✅ Backup/restore interface (3 days)
- ✅ Index management (2 days)
- ✅ Query history + saved templates (2 days)
- ✅ Advanced features panel (2 days)

**Deliverable**: Feature-complete TUI covering all high-priority VeridicalDB capabilities. Full parity with planned system_catalog and admin features.

---

### **Week 5+ (Phase 3)**

- ✅ Plugin/extension architecture (2 days)
- ✅ Accessibility enhancements (3 days)
- ✅ Configuration system (2 days)
- ✅ Session persistence (1 day)
- ✅ Advanced UX (command palette, autocomplete) (3 days)
- ✅ Comprehensive testing (3 days)
- ✅ Documentation (2 days)
- ✅ Production hardening (2 days)

**Deliverable**: Production-ready, accessible, extensible TUI suitable for distribution.

---

## File Structure (Final)

```go
VeridicalDB/
├── pkg/
│   ├── display/                     # NEW: Shared display formatting
│   │   ├── formatter.go             # TableFormatter
│   │   ├── value_formatter.go       # ValueFormatter
│   │   ├── prompt.go                # PromptBuilder
│   │   ├── errors.go                # Error formatting
│   │   └── display_test.go
│   ├── cli/
│   │   ├── repl.go                  # REFACTORED: Use pkg/display
│   │   └── repl_test.go
│   └── ... (existing packages)
│
├── internal/
│   └── tui/                         # NEW: Bubble Tea TUI
│       ├── app.go                   # Main MVU model
│       ├── models.go                # Domain models
│       ├── messages.go              # Bubble Tea messages
│       ├── keybindings.go           # Hotkey definitions
│       ├── theme/
│       │   ├── theme.go
│       │   ├── colors.go
│       │   └── themes/
│       │       ├── light.go
│       │       ├── dark.go
│       │       ├── highcontrast.go
│       │       └── colorblind_*.go
│       ├── screens/
│       │   ├── screen.go            # Screen interface
│       │   ├── home.go              # Home/menu
│       │   ├── editor.go            # SQL editor
│       │   ├── results.go           # Results viewer
│       │   ├── browser.go           # Database browser
│       │   ├── monitoring.go        # Monitoring dashboard
│       │   ├── users.go             # User management
│       │   ├── backup.go            # Backup/restore
│       │   ├── indexes.go           # Index management
│       │   ├── history.go           # Query history
│       │   ├── transactions.go      # Transaction management
│       │   └── advanced.go          # Advanced features
│       ├── extensions/
│       │   └── extension.go         # Plugin architecture
│       ├── widgets/                 # Reusable components
│       │   ├── form.go
│       │   ├── table.go
│       │   ├── tree.go
│       │   └── ...
│       └── tui_test.go
│
├── cmd/
│   ├── veridicaldb/
│   │   └── main.go                  # UPDATED: Add TUI mode option
│   ├── server/
│   │   └── main.go                  # Existing server
│   └── tui/                         # NEW: Optional separate TUI binary
│       └── main.go
│
├── ~/.veridicaldb/                  # User config directory (created at first run)
│   ├── config.yaml                  # TUI configuration
│   ├── session.json                 # Session state persistence
│   ├── queries.yaml                 # Saved query templates
│   ├── keybindings.yaml             # Custom keybindings
│   └── logs/                        # TUI logs
│
└── ... (existing files/packages)
```

---

## Success Criteria

### **MVP (End of Phase 1)**

- [ ] TUI launches, connects to local database
- [ ] Execute SELECT query, display formatted results
- [ ] Transaction control (BEGIN/COMMIT/ROLLBACK)
- [ ] Database/table listing
- [ ] Query history
- [ ] Error handling and display

### **Feature-Complete (End of Phase 2)**

- [ ] All high-priority screens implemented
- [ ] Backup/restore functionality accessible
- [ ] User management UI
- [ ] Monitoring dashboard with live updates
- [ ] Export (CSV/JSON)
- [ ] Comprehensive help system

### **Production-Ready (End of Phase 3)**

- [ ] Accessibility compliance (WCAG AA equivalent for TUI)
- [ ] Plugin system operational with sample extension
- [ ] >80% code coverage with tests
- [ ] Comprehensive documentation
- [ ] Performance: <100ms response time for UI interactions
- [ ] Security review completed

---

## Open Questions for Refinement

1. **Separate TUI Binary vs. Integrated Mode**?
   - Option A: `veridicaldb --tui` flag in main binary
   - Option B: Separate `veridicaldb-tui` command
   - **Recommendation**: Start with integrated flag, consider separate binary later if TUI becomes large

2. **Exported Data Location**?
   - File picker modal for save location?
   - Or default to `~/.veridicaldb/exports/`?

3. **Authentication for TUI**?
   - Assume local OS user authentication (Unix socket)?
   - Or prompt for DB user/password on startup?
   - **Recommendation**: Default to OS user, with optional password entry for clarity

4. **Real-Time Monitoring Refresh Rate**?
   - 1 second auto-refresh is safe?
   - Or configurable per user?

5. **When Should TUI Save State**?
   - On every action (aggressive)?
   - On exit only (safe)?
   - Every N seconds (compromise)?

---

## Notes for Implementation

This plan is **comprehensive, actionable, and extensible**. Each step builds on previous ones, with clear dependencies and integration points. The architecture anticipates future features (replication, sharding, custom screens) while remaining focused on delivering a beautiful, accessible, production-grade TUI.

**Next Steps**: Begin Phase 1 implementation starting with Step 1.1 (Extract Display Package).
