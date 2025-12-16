package sql

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/auth"
	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/lock"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// Session represents a database session with transaction state.
// Each REPL connection has its own session.
type Session struct {
	mtm        *catalog.MVCCTableManager
	executor   *MVCCExecutor
	txnMgr     *txn.Manager
	lockMgr    *lock.Manager             // Optional, can be nil for single-threaded mode
	idxMgr     *btree.IndexManager       // Optional, can be nil if indexes not used
	userCat    *auth.UserCatalog         // Optional, can be nil if auth disabled
	dbMgr      *catalog.DatabaseManager  // Optional, for multi-database support
	triggerCat *catalog.TriggerCatalog   // Optional, for trigger support
	procCat    *catalog.ProcedureCatalog // Optional, for stored procedures/functions

	// currentTx is the current transaction, or nil if in autocommit mode.
	currentTx *txn.Transaction

	// autocommit mode: if true, each statement runs in its own transaction
	autocommit bool

	// preparedStmts stores prepared statements by name
	preparedStmts map[string]Statement

	// currentUser is the authenticated user for this session
	currentUser string

	// currentDatabase is the current database namespace for this session
	currentDatabase string
}

// NewSession creates a new database session.
func NewSession(mtm *catalog.MVCCTableManager) *Session {
	return &Session{
		mtm:           mtm,
		executor:      NewMVCCExecutor(mtm),
		txnMgr:        mtm.TxnManager(),
		lockMgr:       nil, // No locking by default
		idxMgr:        nil, // No indexes by default
		autocommit:    true,
		preparedStmts: make(map[string]Statement),
	}
}

// NewSessionWithLocks creates a new database session with lock support.
func NewSessionWithLocks(mtm *catalog.MVCCTableManager, lockMgr *lock.Manager) *Session {
	return &Session{
		mtm:           mtm,
		executor:      NewMVCCExecutor(mtm),
		txnMgr:        mtm.TxnManager(),
		lockMgr:       lockMgr,
		idxMgr:        nil, // No indexes by default
		autocommit:    true,
		preparedStmts: make(map[string]Statement),
	}
}

// SetLockManager sets the lock manager for concurrent access control.
func (s *Session) SetLockManager(lockMgr *lock.Manager) {
	s.lockMgr = lockMgr
}

// SetIndexManager sets the index manager for index operations.
func (s *Session) SetIndexManager(idxMgr *btree.IndexManager) {
	s.idxMgr = idxMgr
	// Also set on executor for DML index maintenance
	s.executor.SetIndexManager(idxMgr)
}

// SetUserCatalog sets the user catalog for authentication.
func (s *Session) SetUserCatalog(userCat *auth.UserCatalog) {
	s.userCat = userCat
}

// SetDatabaseManager sets the database manager for multi-database support.
func (s *Session) SetDatabaseManager(dbMgr *catalog.DatabaseManager) {
	s.dbMgr = dbMgr
}

// HasDatabaseManager returns true if a DatabaseManager is configured for this session.
func (s *Session) HasDatabaseManager() bool {
	return s.dbMgr != nil
}

// requireDatabaseSelected ensures a current database is selected and exists.
func (s *Session) requireDatabaseSelected() error {
	// If DatabaseManager is not configured (single-database/embedded mode), allow DDL
	if s.dbMgr == nil {
		return nil
	}
	if s.currentDatabase == "" {
		return fmt.Errorf("no database selected; run CREATE DATABASE <name> then USE <name>")
	}
	if !s.dbMgr.DatabaseExists(s.currentDatabase) {
		return fmt.Errorf("current database %q does not exist", s.currentDatabase)
	}
	return nil
}

// SetTriggerCatalog sets the trigger catalog for trigger support.
func (s *Session) SetTriggerCatalog(triggerCat *catalog.TriggerCatalog) {
	s.triggerCat = triggerCat
	// Also set on executor for DML trigger firing
	s.executor.SetTriggerCatalog(triggerCat)
}

// SetProcedureCatalog sets the procedure catalog for stored procedure support.
func (s *Session) SetProcedureCatalog(procCat *catalog.ProcedureCatalog) {
	s.procCat = procCat
	// Also set on executor for trigger function execution
	s.executor.SetProcedureCatalog(procCat)
	// Give executor a reference to this session for PL interpreter creation
	s.executor.SetSession(s)
}

// Catalog returns the underlying catalog for table metadata.
func (s *Session) Catalog() *catalog.Catalog {
	return s.mtm.Catalog()
}

// CurrentDatabase returns the current database for this session.
func (s *Session) CurrentDatabase() string {
	return s.currentDatabase
}

// SetCurrentDatabase sets the current database for this session.
func (s *Session) SetCurrentDatabase(dbName string) error {
	if s.dbMgr != nil {
		if !s.dbMgr.DatabaseExists(dbName) {
			return fmt.Errorf("database %q does not exist", dbName)
		}
	}
	s.currentDatabase = dbName
	return nil
}

// SetCurrentUser sets the authenticated user for this session.
func (s *Session) SetCurrentUser(username string) {
	s.currentUser = username
}

// CurrentUser returns the authenticated user for this session.
func (s *Session) CurrentUser() string {
	return s.currentUser
}

// Authenticate verifies the username and password.
func (s *Session) Authenticate(username, password string) error {
	if s.userCat == nil {
		// No authentication required if user catalog not set
		s.currentUser = username
		return nil
	}
	_, err := s.userCat.Authenticate(username, password)
	if err != nil {
		return err
	}
	s.currentUser = username
	return nil
}

// ExecuteSQL parses and executes a SQL string.
func (s *Session) ExecuteSQL(input string) (*Result, error) {
	parser := NewParser(input)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, fmt.Errorf("syntax error: %w", err)
	}
	return s.Execute(stmt)
}

// Execute executes a SQL statement and returns the result.
func (s *Session) Execute(stmt Statement) (*Result, error) {
	// Handle transaction control statements specially
	switch stmt.(type) {
	case *BeginStmt:
		return s.handleBegin()
	case *CommitStmt:
		return s.handleCommit()
	case *RollbackStmt:
		return s.handleRollback()
	}

	// Handle prepared statements
	switch typedStmt := stmt.(type) {
	case *PrepareStmt:
		return s.handlePrepare(typedStmt)
	case *ExecuteStmt:
		return s.handleExecute(typedStmt)
	case *DeallocateStmt:
		return s.handleDeallocate(typedStmt)
	}

	// For DDL statements (CREATE/DROP), we don't need a transaction
	switch typedStmt := stmt.(type) {
	case *CreateTableStmt:
		if err := s.requireDatabaseSelected(); err != nil {
			return nil, err
		}
		return s.executor.Execute(stmt, nil)
	case *DropTableStmt:
		if err := s.requireDatabaseSelected(); err != nil {
			return nil, err
		}
		return s.executor.Execute(stmt, nil)
	case *CreateIndexStmt:
		if err := s.requireDatabaseSelected(); err != nil {
			return nil, err
		}
		return s.handleCreateIndex(typedStmt)
	case *DropIndexStmt:
		if err := s.requireDatabaseSelected(); err != nil {
			return nil, err
		}
		return s.handleDropIndex(typedStmt)
	case *CreateViewStmt:
		if err := s.requireDatabaseSelected(); err != nil {
			return nil, err
		}
		return s.executor.Execute(stmt, nil)
	case *DropViewStmt:
		if err := s.requireDatabaseSelected(); err != nil {
			return nil, err
		}
		return s.executor.Execute(stmt, nil)
	case *CreateTriggerStmt:
		if err := s.requireDatabaseSelected(); err != nil {
			return nil, err
		}
		return s.handleCreateTrigger(typedStmt)
	case *DropTriggerStmt:
		if err := s.requireDatabaseSelected(); err != nil {
			return nil, err
		}
		return s.handleDropTrigger(typedStmt)
	// User management statements
	case *CreateUserStmt:
		return s.handleCreateUser(typedStmt)
	case *DropUserStmt:
		return s.handleDropUser(typedStmt)
	case *AlterUserStmt:
		return s.handleAlterUser(typedStmt)
	case *GrantStmt:
		return s.handleGrant(typedStmt)
	case *RevokeStmt:
		return s.handleRevoke(typedStmt)
	// Database management statements
	case *CreateDatabaseStmt:
		return s.handleCreateDatabase(typedStmt)
	case *DropDatabaseStmt:
		return s.handleDropDatabase(typedStmt)
	case *UseDatabaseStmt:
		return s.handleUseDatabase(typedStmt)
	// Stored procedure/function statements
	case *CreateProcedureStmt:
		return s.handleCreateProcedure(typedStmt)
	case *DropProcedureStmt:
		return s.handleDropProcedure(typedStmt)
	case *CreateFunctionStmt:
		return s.handleCreateFunction(typedStmt)
	case *DropFunctionStmt:
		return s.handleDropFunction(typedStmt)
	case *CallStmt:
		return s.handleCall(typedStmt)
	case *ShowProceduresStmt:
		return s.handleShowProcedures(typedStmt)
	case *ShowFunctionsStmt:
		return s.handleShowFunctions(typedStmt)
	case *ShowStmt:
		return s.handleShow(typedStmt)
	}

	// For DML statements, we need a transaction
	tx, shouldCommit, err := s.ensureTransaction()
	if err != nil {
		return nil, err
	}

	result, err := s.executor.Execute(stmt, tx)
	if err != nil {
		// If autocommit and error, abort the transaction
		if shouldCommit {
			// Release locks
			if s.lockMgr != nil {
				s.lockMgr.ReleaseAll(tx.ID)
			}
			_ = s.txnMgr.Abort(tx.ID)
			s.currentTx = nil
		}
		return nil, err
	}

	// If autocommit, commit the transaction and release locks
	if shouldCommit {
		// Release locks
		if s.lockMgr != nil {
			s.lockMgr.ReleaseAll(tx.ID)
		}
		if err := s.txnMgr.Commit(tx.ID); err != nil {
			return nil, fmt.Errorf("autocommit failed: %w", err)
		}
		s.currentTx = nil
	}

	return result, nil
}

// ensureTransaction ensures there's an active transaction.
// Returns the transaction and whether it should be auto-committed.
func (s *Session) ensureTransaction() (*txn.Transaction, bool, error) {
	if s.currentTx != nil {
		// Already in a transaction
		return s.currentTx, false, nil
	}

	// Start a new transaction (autocommit mode)
	tx := s.txnMgr.Begin()
	if s.autocommit {
		return tx, true, nil
	}

	s.currentTx = tx
	return tx, false, nil
}

// handleBegin starts a new explicit transaction.
func (s *Session) handleBegin() (*Result, error) {
	if s.currentTx != nil {
		return nil, fmt.Errorf("transaction already in progress")
	}

	s.currentTx = s.txnMgr.Begin()
	s.autocommit = false

	return &Result{
		Message: fmt.Sprintf("BEGIN (txid=%d)", s.currentTx.ID),
	}, nil
}

// handleCommit commits the current transaction.
func (s *Session) handleCommit() (*Result, error) {
	if s.currentTx == nil {
		return nil, fmt.Errorf("no transaction in progress")
	}

	txid := s.currentTx.ID

	// Release all locks held by this transaction
	if s.lockMgr != nil {
		s.lockMgr.ReleaseAll(txid)
	}

	if err := s.txnMgr.Commit(txid); err != nil {
		return nil, err
	}

	s.currentTx = nil
	s.autocommit = true

	return &Result{
		Message: fmt.Sprintf("COMMIT (txid=%d)", txid),
	}, nil
}

// handleRollback aborts the current transaction.
func (s *Session) handleRollback() (*Result, error) {
	if s.currentTx == nil {
		return nil, fmt.Errorf("no transaction in progress")
	}

	txid := s.currentTx.ID

	// Release all locks held by this transaction
	if s.lockMgr != nil {
		s.lockMgr.ReleaseAll(txid)
	}

	if err := s.txnMgr.Abort(txid); err != nil {
		return nil, err
	}

	s.currentTx = nil
	s.autocommit = true

	return &Result{
		Message: fmt.Sprintf("ROLLBACK (txid=%d)", txid),
	}, nil
}

// InTransaction returns true if there's an active explicit transaction.
func (s *Session) InTransaction() bool {
	return s.currentTx != nil
}

// CurrentTxID returns the current transaction ID, or 0 if none.
func (s *Session) CurrentTxID() txn.TxID {
	if s.currentTx == nil {
		return txn.InvalidTxID
	}
	return s.currentTx.ID
}

// Close cleans up the session, aborting any open transaction.
func (s *Session) Close() {
	if s.currentTx != nil {
		txid := s.currentTx.ID
		// Release all locks
		if s.lockMgr != nil {
			s.lockMgr.ReleaseAll(txid)
		}
		_ = s.txnMgr.Abort(txid)
		s.currentTx = nil
	}
}

// handleCreateIndex creates a new index.
func (s *Session) handleCreateIndex(stmt *CreateIndexStmt) (*Result, error) {
	if s.idxMgr == nil {
		return nil, fmt.Errorf("index manager not configured")
	}

	// Get table metadata
	cat := s.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table '%s' not found", stmt.TableName)
	}

	// Validate columns exist
	for _, colName := range stmt.Columns {
		col, _ := meta.Schema.ColumnByName(colName)
		if col == nil {
			return nil, fmt.Errorf("column '%s' not found in table '%s'", colName, stmt.TableName)
		}
	}

	// Create the index
	err = s.idxMgr.CreateIndex(btree.IndexMeta{
		Name:      stmt.IndexName,
		TableName: stmt.TableName,
		Columns:   stmt.Columns,
		Unique:    stmt.Unique,
	})
	if err != nil {
		return nil, fmt.Errorf("create index: %w", err)
	}

	// Populate the index with existing data
	// Use a read-only transaction to scan all visible rows
	tempTx := s.txnMgr.Begin()
	defer s.txnMgr.Commit(tempTx.ID)

	rowCount := 0
	err = s.mtm.Scan(stmt.TableName, tempTx, func(row *catalog.MVCCRow) (bool, error) {
		// Build index key
		key, err := s.buildIndexKeyForRow(stmt.Columns, row.Values, meta.Schema)
		if err != nil {
			return false, err
		}

		// Insert into index
		if err := s.idxMgr.Insert(stmt.IndexName, key, row.RID); err != nil {
			// For unique indexes, this would fail on duplicates
			return false, fmt.Errorf("index insert: %w", err)
		}
		rowCount++
		return true, nil
	})
	if err != nil {
		// Clean up the index if population failed
		s.idxMgr.DropIndex(stmt.IndexName)
		return nil, fmt.Errorf("populate index: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("Index '%s' created on table '%s'.", stmt.IndexName, stmt.TableName),
	}, nil
}

// buildIndexKeyForRow builds an index key from row values.
func (s *Session) buildIndexKeyForRow(columns []string, values []catalog.Value, schema *catalog.Schema) ([]byte, error) {
	if len(columns) == 1 {
		col, idx := schema.ColumnByName(columns[0])
		if col == nil {
			return nil, fmt.Errorf("unknown column: %s", columns[0])
		}
		return encodeValueForIndex(values[idx])
	}

	// Composite index
	var parts [][]byte
	for _, colName := range columns {
		col, idx := schema.ColumnByName(colName)
		if col == nil {
			return nil, fmt.Errorf("unknown column: %s", colName)
		}
		part, err := encodeValueForIndex(values[idx])
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	return btree.EncodeCompositeKey(parts...), nil
}

// encodeValueForIndex encodes a catalog.Value for use as an index key.
func encodeValueForIndex(v catalog.Value) ([]byte, error) {
	if v.IsNull {
		return []byte{0x00}, nil
	}

	switch v.Type {
	case catalog.TypeInt32:
		return btree.EncodeIntKey(int64(v.Int32)), nil
	case catalog.TypeInt64:
		return btree.EncodeIntKey(v.Int64), nil
	case catalog.TypeBool:
		if v.Bool {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case catalog.TypeText:
		return append([]byte{0x01}, []byte(v.Text)...), nil
	default:
		return nil, fmt.Errorf("unsupported type for index: %v", v.Type)
	}
}

// handleDropIndex removes an index.
func (s *Session) handleDropIndex(stmt *DropIndexStmt) (*Result, error) {
	if s.idxMgr == nil {
		return nil, fmt.Errorf("index manager not configured")
	}

	if err := s.idxMgr.DropIndex(stmt.IndexName); err != nil {
		return nil, fmt.Errorf("drop index: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("Index '%s' dropped.", stmt.IndexName),
	}, nil
}

// Prepare stores a prepared statement.
func (s *Session) Prepare(name string, stmt Statement) {
	s.preparedStmts[name] = stmt
}

// GetPrepared retrieves a prepared statement.
func (s *Session) GetPrepared(name string) Statement {
	return s.preparedStmts[name]
}

// Deallocate removes a prepared statement.
func (s *Session) Deallocate(name string) {
	delete(s.preparedStmts, name)
}

func (s *Session) handlePrepare(stmt *PrepareStmt) (*Result, error) {
	s.preparedStmts[stmt.Name] = stmt.Statement
	return &Result{Message: "PREPARE"}, nil
}

func (s *Session) handleDeallocate(stmt *DeallocateStmt) (*Result, error) {
	if stmt.Name == "ALL" {
		s.preparedStmts = make(map[string]Statement)
	} else {
		delete(s.preparedStmts, stmt.Name)
	}
	return &Result{Message: "DEALLOCATE"}, nil
}

func (s *Session) handleExecute(stmt *ExecuteStmt) (*Result, error) {
	prepared, ok := s.preparedStmts[stmt.Name]
	if !ok {
		return nil, fmt.Errorf("prepared statement %q does not exist", stmt.Name)
	}

	// Evaluate parameters
	params := make([]catalog.Value, len(stmt.Params))
	for i, expr := range stmt.Params {
		val, err := s.executor.EvaluateExpression(expr)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate parameter %d: %w", i+1, err)
		}
		params[i] = val
	}

	// Substitute parameters
	newStmt, err := SubstituteParams(prepared, params)
	if err != nil {
		return nil, fmt.Errorf("failed to substitute parameters: %w", err)
	}

	// Execute the substituted statement
	return s.Execute(newStmt)
}

// handleCreateUser handles CREATE USER statement.
func (s *Session) handleCreateUser(stmt *CreateUserStmt) (*Result, error) {
	if s.userCat == nil {
		return nil, fmt.Errorf("user management not enabled (no user catalog)")
	}

	// Check if current user is superuser (must be authenticated to create users)
	if s.currentUser != "" {
		user, err := s.userCat.GetUser(s.currentUser)
		if err != nil {
			return nil, fmt.Errorf("cannot verify current user: %w", err)
		}
		if !user.Superuser {
			return nil, fmt.Errorf("permission denied: only superusers can create users")
		}
	}

	if err := s.userCat.CreateUser(stmt.Username, stmt.Password, stmt.Superuser); err != nil {
		return nil, fmt.Errorf("create user: %w", err)
	}

	return &Result{Message: fmt.Sprintf("CREATE USER %s", stmt.Username)}, nil
}

// handleDropUser handles DROP USER statement.
func (s *Session) handleDropUser(stmt *DropUserStmt) (*Result, error) {
	if s.userCat == nil {
		return nil, fmt.Errorf("user management not enabled (no user catalog)")
	}

	// Check if current user is superuser
	if s.currentUser != "" {
		user, err := s.userCat.GetUser(s.currentUser)
		if err != nil {
			return nil, fmt.Errorf("cannot verify current user: %w", err)
		}
		if !user.Superuser {
			return nil, fmt.Errorf("permission denied: only superusers can drop users")
		}
	}

	if err := s.userCat.DropUser(stmt.Username, stmt.IfExists); err != nil {
		return nil, fmt.Errorf("drop user: %w", err)
	}

	return &Result{Message: fmt.Sprintf("DROP USER %s", stmt.Username)}, nil
}

// handleAlterUser handles ALTER USER statement.
func (s *Session) handleAlterUser(stmt *AlterUserStmt) (*Result, error) {
	if s.userCat == nil {
		return nil, fmt.Errorf("user management not enabled (no user catalog)")
	}

	// Check if current user is superuser (or altering themselves for password only)
	if s.currentUser != "" {
		user, err := s.userCat.GetUser(s.currentUser)
		if err != nil {
			return nil, fmt.Errorf("cannot verify current user: %w", err)
		}
		// Users can change their own password, but not superuser status
		if !user.Superuser && s.currentUser != stmt.Username {
			return nil, fmt.Errorf("permission denied: only superusers can alter other users")
		}
		if !user.Superuser && (stmt.SetSuperuser || stmt.UnsetSuperuser) {
			return nil, fmt.Errorf("permission denied: only superusers can change superuser status")
		}
	}

	if stmt.NewPassword != "" {
		if err := s.userCat.AlterPassword(stmt.Username, stmt.NewPassword); err != nil {
			return nil, fmt.Errorf("alter password: %w", err)
		}
	}

	if stmt.SetSuperuser {
		if err := s.userCat.SetSuperuser(stmt.Username, true); err != nil {
			return nil, fmt.Errorf("set superuser: %w", err)
		}
	}

	if stmt.UnsetSuperuser {
		if err := s.userCat.SetSuperuser(stmt.Username, false); err != nil {
			return nil, fmt.Errorf("unset superuser: %w", err)
		}
	}

	return &Result{Message: fmt.Sprintf("ALTER USER %s", stmt.Username)}, nil
}

// handleGrant handles GRANT statement.
func (s *Session) handleGrant(stmt *GrantStmt) (*Result, error) {
	if s.userCat == nil {
		return nil, fmt.Errorf("user management not enabled (no user catalog)")
	}

	// Check if current user is superuser
	if s.currentUser != "" {
		user, err := s.userCat.GetUser(s.currentUser)
		if err != nil {
			return nil, fmt.Errorf("cannot verify current user: %w", err)
		}
		if !user.Superuser {
			return nil, fmt.Errorf("permission denied: only superusers can grant privileges")
		}
	}

	if err := s.userCat.Grant(stmt.Username, stmt.TableName, auth.Priv(stmt.Privilege)); err != nil {
		return nil, fmt.Errorf("grant: %w", err)
	}

	return &Result{Message: fmt.Sprintf("GRANT %s ON %s TO %s", stmt.Privilege, stmt.TableName, stmt.Username)}, nil
}

// handleRevoke handles REVOKE statement.
func (s *Session) handleRevoke(stmt *RevokeStmt) (*Result, error) {
	if s.userCat == nil {
		return nil, fmt.Errorf("user management not enabled (no user catalog)")
	}

	// Check if current user is superuser
	if s.currentUser != "" {
		user, err := s.userCat.GetUser(s.currentUser)
		if err != nil {
			return nil, fmt.Errorf("cannot verify current user: %w", err)
		}
		if !user.Superuser {
			return nil, fmt.Errorf("permission denied: only superusers can revoke privileges")
		}
	}

	if err := s.userCat.Revoke(stmt.Username, stmt.TableName, auth.Priv(stmt.Privilege)); err != nil {
		return nil, fmt.Errorf("revoke: %w", err)
	}

	return &Result{Message: fmt.Sprintf("REVOKE %s ON %s FROM %s", stmt.Privilege, stmt.TableName, stmt.Username)}, nil
}

// handleCreateDatabase handles CREATE DATABASE statement.
func (s *Session) handleCreateDatabase(stmt *CreateDatabaseStmt) (*Result, error) {
	if s.dbMgr == nil {
		return nil, fmt.Errorf("multi-database support not enabled")
	}

	// Check if current user is superuser (if auth is enabled)
	if s.userCat != nil && s.currentUser != "" {
		user, err := s.userCat.GetUser(s.currentUser)
		if err != nil {
			return nil, fmt.Errorf("cannot verify current user: %w", err)
		}
		if !user.Superuser {
			return nil, fmt.Errorf("permission denied: only superusers can create databases")
		}
	}

	owner := stmt.Owner
	if owner == "" {
		owner = s.currentUser
	}

	var err error
	if stmt.IfNotExists {
		_, err = s.dbMgr.CreateDatabaseIfNotExists(stmt.Name, owner)
	} else {
		_, err = s.dbMgr.CreateDatabase(stmt.Name, owner)
	}
	if err != nil {
		return nil, fmt.Errorf("create database: %w", err)
	}

	return &Result{Message: fmt.Sprintf("CREATE DATABASE %s", stmt.Name)}, nil
}

// handleDropDatabase handles DROP DATABASE statement.
func (s *Session) handleDropDatabase(stmt *DropDatabaseStmt) (*Result, error) {
	if s.dbMgr == nil {
		return nil, fmt.Errorf("multi-database support not enabled")
	}

	// Check if current user is superuser (if auth is enabled)
	if s.userCat != nil && s.currentUser != "" {
		user, err := s.userCat.GetUser(s.currentUser)
		if err != nil {
			return nil, fmt.Errorf("cannot verify current user: %w", err)
		}
		if !user.Superuser {
			return nil, fmt.Errorf("permission denied: only superusers can drop databases")
		}
	}

	// Cannot drop current database
	if stmt.Name == s.currentDatabase {
		return nil, fmt.Errorf("cannot drop the currently selected database")
	}

	if err := s.dbMgr.DropDatabase(stmt.Name, stmt.IfExists); err != nil {
		return nil, fmt.Errorf("drop database: %w", err)
	}

	return &Result{Message: fmt.Sprintf("DROP DATABASE %s", stmt.Name)}, nil
}

// handleUseDatabase handles USE database statement.
func (s *Session) handleUseDatabase(stmt *UseDatabaseStmt) (*Result, error) {
	if s.dbMgr == nil {
		// Even without dbMgr, allow switching if we just track the name
		s.currentDatabase = stmt.Name
		return &Result{Message: fmt.Sprintf("Database changed to %s", stmt.Name)}, nil
	}

	if !s.dbMgr.DatabaseExists(stmt.Name) {
		return nil, fmt.Errorf("database %q does not exist", stmt.Name)
	}

	s.currentDatabase = stmt.Name
	return &Result{Message: fmt.Sprintf("Database changed to %s", stmt.Name)}, nil
}

// handleCreateTrigger handles CREATE TRIGGER statement.
func (s *Session) handleCreateTrigger(stmt *CreateTriggerStmt) (*Result, error) {
	if s.triggerCat == nil {
		return nil, fmt.Errorf("trigger support not enabled")
	}

	// Verify the table exists
	if _, err := s.mtm.Catalog().GetTable(stmt.TableName); err != nil {
		return nil, fmt.Errorf("table %q does not exist", stmt.TableName)
	}

	// Check if trigger already exists
	if s.triggerCat.TriggerExists(stmt.Name, stmt.TableName) {
		if stmt.IfNotExists {
			return &Result{Message: fmt.Sprintf("trigger %q already exists (ignored)", stmt.Name)}, nil
		}
		return nil, fmt.Errorf("trigger %q already exists on table %q", stmt.Name, stmt.TableName)
	}

	// Convert AST timing/event to catalog timing/event
	trigger := &catalog.TriggerMeta{
		Name:         stmt.Name,
		TableName:    stmt.TableName,
		Timing:       catalog.TriggerTiming(stmt.Timing),
		Event:        catalog.TriggerEvent(stmt.Event),
		ForEachRow:   stmt.ForEachRow,
		FunctionName: stmt.FunctionName,
		Enabled:      true,
	}

	if err := s.triggerCat.CreateTrigger(trigger); err != nil {
		return nil, fmt.Errorf("create trigger: %w", err)
	}

	return &Result{Message: fmt.Sprintf("CREATE TRIGGER %s", stmt.Name)}, nil
}

// handleDropTrigger handles DROP TRIGGER statement.
func (s *Session) handleDropTrigger(stmt *DropTriggerStmt) (*Result, error) {
	if s.triggerCat == nil {
		return nil, fmt.Errorf("trigger support not enabled")
	}

	// Check if trigger exists
	if !s.triggerCat.TriggerExists(stmt.Name, stmt.TableName) {
		if stmt.IfExists {
			return &Result{Message: fmt.Sprintf("trigger %q does not exist (ignored)", stmt.Name)}, nil
		}
		return nil, fmt.Errorf("trigger %q does not exist on table %q", stmt.Name, stmt.TableName)
	}

	if err := s.triggerCat.DropTrigger(stmt.Name, stmt.TableName); err != nil {
		return nil, fmt.Errorf("drop trigger: %w", err)
	}

	return &Result{Message: fmt.Sprintf("DROP TRIGGER %s", stmt.Name)}, nil
}

// handleCreateProcedure handles CREATE PROCEDURE statement.
func (s *Session) handleCreateProcedure(stmt *CreateProcedureStmt) (*Result, error) {
	if s.procCat == nil {
		return nil, fmt.Errorf("stored procedure support not enabled")
	}

	// Check if procedure already exists
	if s.procCat.ProcedureExists(stmt.Name) {
		if stmt.IfNotExists {
			return &Result{Message: fmt.Sprintf("procedure %q already exists (ignored)", stmt.Name)}, nil
		}
		return nil, fmt.Errorf("procedure %q already exists", stmt.Name)
	}

	// Convert AST params to catalog params
	params := make([]catalog.ProcParamMeta, len(stmt.Params))
	for i, p := range stmt.Params {
		params[i] = catalog.ProcParamMeta{
			Name: p.Name,
			Type: p.Type,
			Mode: catalog.ParamMode(p.Mode),
		}
	}

	meta := &catalog.ProcedureMeta{
		Name:     stmt.Name,
		Type:     catalog.ProcTypeProcedure,
		Params:   params,
		Language: stmt.Language,
		Body:     stmt.BodyText,
	}

	if err := s.procCat.CreateProcedure(meta); err != nil {
		return nil, fmt.Errorf("create procedure: %w", err)
	}

	return &Result{Message: fmt.Sprintf("CREATE PROCEDURE %s", stmt.Name)}, nil
}

// handleDropProcedure handles DROP PROCEDURE statement.
func (s *Session) handleDropProcedure(stmt *DropProcedureStmt) (*Result, error) {
	if s.procCat == nil {
		return nil, fmt.Errorf("stored procedure support not enabled")
	}

	if err := s.procCat.DropProcedure(stmt.Name, stmt.IfExists); err != nil {
		return nil, fmt.Errorf("drop procedure: %w", err)
	}

	return &Result{Message: fmt.Sprintf("DROP PROCEDURE %s", stmt.Name)}, nil
}

// handleCreateFunction handles CREATE FUNCTION statement.
func (s *Session) handleCreateFunction(stmt *CreateFunctionStmt) (*Result, error) {
	if s.procCat == nil {
		return nil, fmt.Errorf("stored function support not enabled")
	}

	// Check if function already exists
	if s.procCat.FunctionExists(stmt.Name) {
		if stmt.IfNotExists {
			return &Result{Message: fmt.Sprintf("function %q already exists (ignored)", stmt.Name)}, nil
		}
		return nil, fmt.Errorf("function %q already exists", stmt.Name)
	}

	// Convert AST params to catalog params
	params := make([]catalog.ProcParamMeta, len(stmt.Params))
	for i, p := range stmt.Params {
		params[i] = catalog.ProcParamMeta{
			Name: p.Name,
			Type: p.Type,
			Mode: catalog.ParamMode(p.Mode),
		}
	}

	meta := &catalog.ProcedureMeta{
		Name:       stmt.Name,
		Type:       catalog.ProcTypeFunction,
		Params:     params,
		ReturnType: stmt.ReturnType,
		Language:   stmt.Language,
		Body:       stmt.BodyText,
	}

	if err := s.procCat.CreateFunction(meta); err != nil {
		return nil, fmt.Errorf("create function: %w", err)
	}

	return &Result{Message: fmt.Sprintf("CREATE FUNCTION %s", stmt.Name)}, nil
}

// handleDropFunction handles DROP FUNCTION statement.
func (s *Session) handleDropFunction(stmt *DropFunctionStmt) (*Result, error) {
	if s.procCat == nil {
		return nil, fmt.Errorf("stored function support not enabled")
	}

	if err := s.procCat.DropFunction(stmt.Name, stmt.IfExists); err != nil {
		return nil, fmt.Errorf("drop function: %w", err)
	}

	return &Result{Message: fmt.Sprintf("DROP FUNCTION %s", stmt.Name)}, nil
}

// handleCall handles CALL procedure_name(args...) statement.
func (s *Session) handleCall(stmt *CallStmt) (*Result, error) {
	if s.procCat == nil {
		return nil, fmt.Errorf("stored procedure support not enabled")
	}

	// Look up procedure
	procMeta, ok := s.procCat.GetProcedure(stmt.Name)
	if !ok {
		return nil, fmt.Errorf("procedure %q not found", stmt.Name)
	}

	// Validate argument count
	if len(stmt.Args) != len(procMeta.Params) {
		return nil, fmt.Errorf("procedure %q expects %d arguments, got %d",
			stmt.Name, len(procMeta.Params), len(stmt.Args))
	}

	// Create interpreter
	pl := NewPLInterpreter(s)

	// Evaluate and bind arguments
	for i, arg := range stmt.Args {
		param := procMeta.Params[i]

		// Evaluate the argument expression
		val, err := pl.evalExpression(arg)
		if err != nil {
			return nil, fmt.Errorf("error evaluating argument %d: %v", i+1, err)
		}

		// Bind based on mode
		switch param.Mode {
		case catalog.ParamModeIn:
			pl.SetParameter(param.Name, val)
		case catalog.ParamModeOut, catalog.ParamModeInOut:
			// For OUT/INOUT, we need a variable to store the output
			outVal := val
			pl.SetOutParameter(param.Name, &outVal)
			if param.Mode == catalog.ParamModeInOut {
				pl.SetParameter(param.Name, val)
			}
		}
	}

	// Parse and execute the procedure body
	body, err := NewParser(procMeta.Body).parsePLBlock()
	if err != nil {
		return nil, fmt.Errorf("error parsing procedure body: %v", err)
	}

	if err := pl.ExecuteBlock(body); err != nil {
		return nil, fmt.Errorf("procedure execution error: %v", err)
	}

	return &Result{Message: fmt.Sprintf("CALL %s", stmt.Name)}, nil
}

// handleShowProcedures handles SHOW PROCEDURES statement.
func (s *Session) handleShowProcedures(_ *ShowProceduresStmt) (*Result, error) {
	if s.procCat == nil {
		return &Result{
			Columns: []string{"procedure_name", "language", "params"},
			Rows:    [][]catalog.Value{},
		}, nil
	}

	procs := s.procCat.ListProcedures()
	rows := make([][]catalog.Value, len(procs))
	for i, p := range procs {
		paramStrs := make([]string, len(p.Params))
		for j, param := range p.Params {
			paramStrs[j] = fmt.Sprintf("%s %s %s", param.Mode.String(), param.Name, param.Type.String())
		}
		rows[i] = []catalog.Value{
			catalog.NewText(p.Name),
			catalog.NewText(p.Language),
			catalog.NewText(strings.Join(paramStrs, ", ")),
		}
	}

	return &Result{
		Columns: []string{"procedure_name", "language", "params"},
		Rows:    rows,
	}, nil
}

// handleShowFunctions handles SHOW FUNCTIONS statement.
func (s *Session) handleShowFunctions(_ *ShowFunctionsStmt) (*Result, error) {
	if s.procCat == nil {
		return &Result{
			Columns: []string{"function_name", "return_type", "language", "params"},
			Rows:    [][]catalog.Value{},
		}, nil
	}

	funcs := s.procCat.ListFunctions()
	rows := make([][]catalog.Value, len(funcs))
	for i, f := range funcs {
		paramStrs := make([]string, len(f.Params))
		for j, param := range f.Params {
			paramStrs[j] = fmt.Sprintf("%s %s %s", param.Mode.String(), param.Name, param.Type.String())
		}
		rows[i] = []catalog.Value{
			catalog.NewText(f.Name),
			catalog.NewText(f.ReturnType.String()),
			catalog.NewText(f.Language),
			catalog.NewText(strings.Join(paramStrs, ", ")),
		}
	}

	return &Result{
		Columns: []string{"function_name", "return_type", "language", "params"},
		Rows:    rows,
	}, nil
}

// handleShow handles SHOW statement (DATABASES, TABLES, etc.)
func (s *Session) handleShow(stmt *ShowStmt) (*Result, error) {
	switch strings.ToUpper(stmt.ShowType) {
	case "DATABASES":
		// Return actual database list from DatabaseManager
		if s.dbMgr == nil {
			// If no DatabaseManager, return just "default"
			return &Result{
				Columns: []string{"database_name"},
				Rows:    [][]catalog.Value{{catalog.NewText("default")}},
			}, nil
		}
		// Get all databases from DatabaseManager
		dbNames := s.dbMgr.ListDatabases()
		rows := make([][]catalog.Value, len(dbNames))
		for i, name := range dbNames {
			rows[i] = []catalog.Value{catalog.NewText(name)}
		}
		return &Result{
			Columns: []string{"database_name"},
			Rows:    rows,
		}, nil
	default:
		// For other SHOW types (TABLES, TRIGGERS, etc.), pass to executor
		tx, shouldCommit, err := s.ensureTransaction()
		if err != nil {
			return nil, err
		}
		result, err := s.executor.Execute(stmt, tx)
		if shouldCommit {
			if s.lockMgr != nil {
				s.lockMgr.ReleaseAll(tx.ID)
			}
			if err == nil {
				_ = s.txnMgr.Commit(tx.ID)
			} else {
				_ = s.txnMgr.Abort(tx.ID)
			}
			s.currentTx = nil
		}
		return result, err
	}
}
