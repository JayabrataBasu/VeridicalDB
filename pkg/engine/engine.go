// Package engine assembles a VeridicalDB database from its parts.
//
// Before this package existed, five call sites (cmd/server, cmd/veridicaldb's
// TUI path, internal/cli twice, and pkg/shard) each hand-built the
// WAL -> txn -> catalog -> session object graph and independently decided which
// of a exec.Session's optional capabilities to wire in. They drifted: the wire
// protocol got none of them. Open builds the graph once and NewSession hands out
// sessions with every available capability attached, so behavior no longer
// depends on how a client connects.
package engine

import (
	"fmt"

	"github.com/JayabrataBasu/VeridicalDB/pkg/auth"
	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/fts"
	"github.com/JayabrataBasu/VeridicalDB/pkg/lock"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/exec"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
	"github.com/JayabrataBasu/VeridicalDB/pkg/wal"
)

const defaultPageSize = 4096

// Logger is the minimal logging surface the engine needs. Both
// *pkg/log.Logger (and any logger with Info/Warn(string, ...any)) satisfy it.
type Logger interface {
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
}

type nopLogger struct{}

func (nopLogger) Info(string, ...any) {}
func (nopLogger) Warn(string, ...any) {}

// Config controls how Open assembles the database.
type Config struct {
	// DataDir is the on-disk root for tables, indexes, WAL, and catalog
	// metadata. Required.
	DataDir string

	// PageSize is the storage page size in bytes. Zero means 4096.
	PageSize int

	// Durable enables the write-ahead log: a WAL file, transaction-boundary
	// logging, crash recovery during Open, and a background checkpointer that
	// Close stops. When false the engine runs without a WAL and without
	// recovery — matching how the embedded REPL, the TUI, and shard nodes have
	// historically run. MVCC snapshot isolation applies either way.
	Durable bool

	// EnableLocking attaches the pessimistic lock manager to every session.
	// Off by default to preserve historical behavior.
	EnableLocking bool

	// Logger receives warnings about optional components that could not be
	// initialized. Nil is fine.
	Logger Logger
}

// DB is an assembled database: one object graph shared by every session it
// hands out.
type DB struct {
	cfg Config
	log Logger

	wal          *wal.WAL
	txnMgr       *txn.Manager
	txnLogger    *wal.TxnLogger
	checkpointer *wal.Checkpointer

	tm  *catalog.TableManager
	mtm *catalog.MVCCTableManager

	idxMgr     *btree.IndexManager
	lockMgr    *lock.Manager
	userCat    *auth.UserCatalog
	dbMgr      *catalog.DatabaseManager
	triggerCat *catalog.TriggerCatalog
	procCat    *catalog.ProcedureCatalog
	ftsMgr     *fts.Manager

	closed bool
}

// Open assembles a database rooted at cfg.DataDir. On any fatal error it
// releases whatever it had already opened.
func Open(cfg Config) (*DB, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("engine.Open: DataDir is required")
	}
	if cfg.PageSize == 0 {
		cfg.PageSize = defaultPageSize
	}
	lg := cfg.Logger
	if lg == nil {
		lg = nopLogger{}
	}

	db := &DB{cfg: cfg, log: lg, txnMgr: txn.NewManager()}

	var walLog *wal.WAL
	if cfg.Durable {
		w, err := wal.Open(cfg.DataDir)
		if err != nil {
			return nil, fmt.Errorf("engine.Open: WAL: %w", err)
		}
		db.wal = w
		walLog = w
		db.txnLogger = wal.NewTxnLogger(w, db.txnMgr)
		db.checkpointer = wal.NewCheckpointer(w, db.txnLogger)
	}

	// TableManager performs crash recovery internally when given a non-nil WAL.
	tm, err := catalog.NewTableManager(cfg.DataDir, cfg.PageSize, walLog)
	if err != nil {
		if db.wal != nil {
			_ = db.wal.Close()
		}
		return nil, fmt.Errorf("engine.Open: table manager: %w", err)
	}
	db.tm = tm
	db.mtm = catalog.NewMVCCTableManager(tm, db.txnMgr, db.txnLogger)

	// Optional capabilities. A failure here is not fatal — the corresponding
	// SQL features are simply unavailable — but it is logged.
	if idxMgr, err := btree.NewIndexManager(cfg.DataDir, cfg.PageSize); err == nil {
		db.idxMgr = idxMgr
	} else {
		lg.Warn("engine: index manager unavailable", "error", err)
	}
	if uc, err := auth.NewUserCatalog(cfg.DataDir); err == nil {
		db.userCat = uc
	} else {
		lg.Warn("engine: user catalog unavailable", "error", err)
	}
	if dm, err := catalog.NewDatabaseManager(cfg.DataDir); err == nil {
		db.dbMgr = dm
	} else {
		lg.Warn("engine: database manager unavailable", "error", err)
	}
	if tc, err := catalog.NewTriggerCatalog(cfg.DataDir); err == nil {
		db.triggerCat = tc
	} else {
		lg.Warn("engine: trigger catalog unavailable", "error", err)
	}
	if pc, err := catalog.NewProcedureCatalog(cfg.DataDir); err == nil {
		db.procCat = pc
	} else {
		lg.Warn("engine: procedure catalog unavailable", "error", err)
	}
	if fm, err := fts.NewManager(cfg.DataDir); err == nil {
		db.ftsMgr = fm
	} else {
		lg.Warn("engine: full-text search unavailable", "error", err)
	}
	if cfg.EnableLocking {
		db.lockMgr = lock.NewManager()
	}

	if db.checkpointer != nil {
		db.checkpointer.SetPageFlusher(tm.Checkpoint)
		db.checkpointer.StartBackground()
	}

	return db, nil
}

// NewSession returns a session with every available capability wired in. Every
// entry point must obtain sessions this way.
func (db *DB) NewSession() *exec.Session {
	s := exec.NewSession(db.mtm)
	if db.idxMgr != nil {
		s.SetIndexManager(db.idxMgr)
	}
	if db.lockMgr != nil {
		s.SetLockManager(db.lockMgr)
	}
	if db.userCat != nil {
		s.SetUserCatalog(db.userCat)
	}
	if db.dbMgr != nil {
		s.SetDatabaseManager(db.dbMgr)
	}
	if db.triggerCat != nil {
		s.SetTriggerCatalog(db.triggerCat)
	}
	if db.procCat != nil {
		s.SetProcedureCatalog(db.procCat)
	}
	if db.ftsMgr != nil {
		s.SetFTSManager(db.ftsMgr)
	}
	return s
}

// Close stops background work and releases resources. Safe to call more than
// once.
func (db *DB) Close() error {
	if db.closed {
		return nil
	}
	db.closed = true
	if db.checkpointer != nil {
		db.checkpointer.StopBackground()
	}
	if db.wal != nil {
		return db.wal.Close()
	}
	return nil
}

// TableManager returns the underlying row/heap table manager.
func (db *DB) TableManager() *catalog.TableManager { return db.tm }

// MVCCTableManager returns the MVCC table manager sessions are built on.
func (db *DB) MVCCTableManager() *catalog.MVCCTableManager { return db.mtm }

// TxnManager returns the shared transaction manager.
func (db *DB) TxnManager() *txn.Manager { return db.txnMgr }

// Catalog returns the schema catalog.
func (db *DB) Catalog() *catalog.Catalog { return db.tm.Catalog() }

// DatabaseManager returns the multi-database manager, or nil if unavailable.
func (db *DB) DatabaseManager() *catalog.DatabaseManager { return db.dbMgr }
