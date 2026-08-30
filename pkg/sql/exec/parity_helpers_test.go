package exec

import (
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/auth"
	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/fts"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// sqlExec is the two-method execution surface the shared test helpers accept.
// The pre-MVCC *Executor also satisfied it during the P2 consolidation; now only
// *Session does. Kept as documentation of the surface and for any future
// alternative implementation.
type sqlExec interface {
	Execute(stmt ast.Statement) (*Result, error)
	ExecuteSQL(input string) (*Result, error)
}

var _ sqlExec = (*Session)(nil)

// newParitySession builds a fully-wired Session over the given TableManager:
// index manager, user/trigger/procedure catalogs, and FTS, but deliberately no
// DatabaseManager so DDL works without a preceding USE. This is the standard
// executor fixture for the pkg/sql tests.
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
