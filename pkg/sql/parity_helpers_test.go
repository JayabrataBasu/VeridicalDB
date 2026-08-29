package sql

import (
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/auth"
	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/fts"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// sqlExec is the common surface of the two SQL execution paths — the pre-MVCC
// *Executor and the *Session (which drives *MVCCExecutor). The shared test
// helpers below accept it so a test migrates from one path to the other by
// changing a single constructor call.
//
// This is the P2 consolidation harness: once every historical *Executor test
// runs green against a *Session, executor.go can be deleted.
type sqlExec interface {
	Execute(stmt Statement) (*Result, error)
	ExecuteSQL(input string) (*Result, error)
}

var (
	_ sqlExec = (*Executor)(nil)
	_ sqlExec = (*Session)(nil)
)

// newParitySession builds a fully-wired Session over the given TableManager:
// index manager, user/trigger/procedure catalogs, and FTS, but deliberately no
// DatabaseManager so DDL works without a preceding USE (matching how bare
// *Executor tests are written). Use this in place of NewExecutor(tm).
func newParitySession(t *testing.T, tm *catalog.TableManager) *Session {
	t.Helper()

	dir := tm.DataDir()
	mtm := catalog.NewMVCCTableManager(tm, txn.NewManager(), nil)
	s := NewSession(mtm)

	if im, err := btree.NewIndexManager(dir, tm.PageSize()); err == nil {
		s.SetIndexManager(im)
	} else {
		t.Fatalf("parity session: index manager: %v", err)
	}
	if uc, err := auth.NewUserCatalog(dir); err == nil {
		s.SetUserCatalog(uc)
	}
	if tc, err := catalog.NewTriggerCatalog(dir); err == nil {
		s.SetTriggerCatalog(tc)
	}
	if pc, err := catalog.NewProcedureCatalog(dir); err == nil {
		s.SetProcedureCatalog(pc)
	}
	if fm, err := fts.NewManager(dir); err == nil {
		s.SetFTSManager(fm)
	}

	t.Cleanup(s.Close)
	return s
}

// numVal returns a catalog.Value's numeric content as a float64, whichever of
// the int/float fields is populated. Aggregate results differ in representation
// between the two executors (the pre-MVCC path does integer division for AVG;
// the MVCC path returns a float), so numeric assertions compare values, not
// types.
func numVal(v catalog.Value) float64 {
	switch v.Type {
	case catalog.TypeInt32:
		return float64(v.Int32)
	case catalog.TypeInt64:
		return float64(v.Int64)
	case catalog.TypeFloat64:
		return v.Float64
	default:
		return 0
	}
}
