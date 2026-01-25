# Keyboard Philosophy & Design

VeridicalDB follows a comprehensive keyboard-driven philosophy to maximize efficiency and accessibility for database administrators and developers working in the terminal.

## Core Principles

### 1. **Modal Navigation**

- Primary mode: SQL Editor (edit SQL queries)
- Panel mode: Results/Database Browser (navigate with arrow keys)
- Help overlay: Context-sensitive, accessible via `?` or `F1`

### 2. **Vim-Inspired Navigation** (where applicable)

- `h/j/k/l` or arrow keys: Move cursor/selection
- `gg`: Jump to beginning
- `G`: Jump to end
- `Ctrl+D`: Page down
- `Ctrl+U`: Page up

### 3. **Screen Switching**

- `Tab`: Cycle forward between main panels (Editor → Results → Dashboard → Browser)
- `Shift+Tab`: Cycle backward
- `Alt+1/2/3/4`: Direct jump to specific screen
  - `Alt+1`: SQL Editor
  - `Alt+2`: Results/Query Results
  - `Alt+3`: Dashboard (Metrics)
  - `Alt+4`: Database Browser

### 4. **Query Execution**

- `Ctrl+Enter`: Execute selected query or current line
- `Ctrl+Shift+I`: Execute and inspect (show execution plan)
- `Ctrl+;`: Execute and time (show query duration)

### 5. **Editor Operations**

- `Ctrl+A`: Select all
- `Ctrl+X`: Cut
- `Ctrl+C`: Copy
- `Ctrl+V`: Paste
- `Ctrl+Z`: Undo
- `Ctrl+Y`: Redo
- `Ctrl+F`: Find in query
- `Ctrl+H`: Find and replace
- `Ctrl+L`: Clear editor
- `Ctrl+Up/Down`: Navigate query history

### 6. **Autocomplete & Suggestions**

- `Ctrl+Space`: Trigger autocomplete dropdown
- `Up/Down` (in dropdown): Navigate suggestions
- `Enter`: Accept suggestion and insert
- `Esc`: Close autocomplete without inserting
- `Tab` (in dropdown): Accept and move to next field

### 7. **Results Navigation**

- `Arrow Keys`: Navigate result rows/columns
- `Page Up/Down`: Scroll by page
- `Home/End`: Jump to first/last row
- `Ctrl+Home/End`: Jump to first/last column
- `Enter`: View full row details (detail view)
- `Esc`: Back to table view

### 8. **Database Browser Operations**

- `Tab/Shift+Tab`: Switch between panels (DB → Tables → Columns)
- `Up/Down`: Navigate within current panel
- `Left/Right`: Move between panels
- `Enter`: Expand/collapse or view details
- `Space`: Select/deselect item

### 9. **Help & Documentation**

- `?`: Show context-sensitive help overlay
- `F1`: Global help documentation
- `Ctrl+?`: Keyboard shortcuts reference
- `Ctrl+T`: Tutorial mode

### 10. **Global Commands**

- `q` or `Ctrl+C`: Quit/Exit application
- `Ctrl+S`: Save query to file
- `Ctrl+O`: Open query from file
- `:set`: Enter command mode (like vim)
- `:help`: Show help command
- `:status`: Show connection status
- `:config`: Show configuration

## Context-Specific Bindings

### In SQL Editor

- `Tab` (no selection): Insert 2 spaces (respects indentation)
- `Shift+Tab`: Dedent line
- `Ctrl+/`: Toggle line comment
- `Ctrl+Shift+/`: Toggle block comment

### In Results Panel

- `d`: Delete row (after confirmation)
- `e`: Edit cell (inline edit)
- `Ctrl+E`: Export results (CSV/JSON/Excel)
- `c`: Copy cell value
- `r`: Refresh results

### In Database Browser

- `d`: Show table details
- `i`: Show index information
- `c`: Show column info
- `s`: Show statistics

### In Dashboard

- `r`: Refresh metrics
- `p`: Pause updates
- `e`: Expand detail view
- `t`: Toggle chart type

## Accessibility Features

### Discoverable Shortcuts

- Every feature has a keyboard binding
- Help text shows available keys in status bar
- Mouse support is optional but not required
- Screen reader compatibility (where applicable)

### Consistency Rules

1. **Modifier patterns**: Similar operations use same modifiers
   - `Ctrl+*`: Core operations (execute, save, find)
   - `Alt+*`: Screen navigation
   - `Shift+*`: Extended operations (backward, uppercase)

2. **Navigation consistency**:
   - Arrow keys work everywhere
   - Page Up/Down for scrolling
   - Home/End for boundaries
   - Enter for confirm/open
   - Esc for cancel/back

3. **Mnemonic mnemonics**:
   - `Ctrl+E`: Execute
   - `Ctrl+S`: Save
   - `Ctrl+O`: Open
   - `Ctrl+F`: Find
   - `Ctrl+H`: Help (context)/History

## Status Bar Indicators

The status bar shows available keys for current context:

```menu
[Esc] Close  [↑↓] Navigate  [Enter] Select  [?] Help
```

## Learning Path

**Beginner**: Focus on core navigation

- Arrow keys, Enter, Esc, Tab, Ctrl+Enter

**Intermediate**: Add editor shortcuts

- Ctrl+Z/Y, Ctrl+X/C/V, Ctrl+F

**Advanced**: Use vim-style nav and command mode

- h/j/k/l, gg/G, :command mode

## Implementation Notes

- All bindings are remappable via config file
- Custom keybindings supported in `~/.veridicaldb/keybindings.yaml`
- Profile-based presets (vi, emacs, default)
- Conflict detection and warnings during startup

## Future Enhancements

- [ ] Macro recording (for complex operations)
- [ ] Key sequence binding (e.g., `jj` for Esc)
- [ ] Command palette (`Ctrl+Shift+P`)
- [ ] Chord support (e.g., `Ctrl+K Ctrl+C` for comment)
