package sql

import (
	"path/filepath"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

// TestExecutorSessionParity runs one battery of SQL against both execution
// paths — the legacy *Executor and the shipping *Session — and asserts they
// agree. It is the gate for P2: executor.go can be deleted once every case here
// passes on *Session with no sessionTODO, and the migrated sql_test.go cases do
// too.
//
// A case with sessionTODO is a *verified* Session gap: the *Executor half still
// runs (guarding the reference against regressions), the *Session half is
// skipped, tagged with the owning sub-phase. Clear the tag when it lands.
//
// Remaining P2 backlog. 60 of the 74 sql_test.go tests now run against
// *Session; the 14 below still fail on it and are marked TODO(P2) in
// sql_test.go, still running on *Executor. Each is a real MVCC-executor gap,
// not a test artifact:
//
//	P2.3 joins   TestLateralJoinWithLeftJoin
//	             — LATERAL subquery + LEFT JOIN column resolution.
//	P2.4 CTEs    TestCTEBasic  — a single-column CTE consumed by an aggregate
//	             ("unknown column in CTE"); column count wrong for column-list CTE.
//	             TestRecursiveCTE — the recursive self-reference is not visible
//	             to a JOIN inside the recursive term.
//	P2.5 DML     TestUpdateWithFrom, TestDeleteWithUsing — qualified columns
//	             (emp.dept) unresolved in the multi-table DML path.
//	P2.6 tail    TestGroupingSets / TestCube / TestRollup — the MVCC path emits
//	             only the grand-total grouping, not one row per set.
//	             TestGroupingFunction — GROUPING() returns 0 for a NULL group col.
//	             TestDistinctOnExecution — DISTINCT ON does not deduplicate.
//	             TestWindowFrameExecution — moving-window arithmetic off; MIN/MAX
//	             unsupported as window functions.
//	             TestNthValue — NTH_VALUE window function unsupported.
//	             TestMergeBasic / TestMergeWithSubquery — MERGE result differences.
//
// Fixed on the MVCC path during P2.1/P2.6: constraint enforcement, ON CONFLICT,
// column DEFAULTs, AUTO_INCREMENT, ALTER ADD CONSTRAINT, TEXT->DATE coercion,
// and AVG now returns a float (the pre-MVCC executor did integer division).
func TestExecutorSessionParity(t *testing.T) {
	type kase struct {
		name        string
		setup       []string
		probe       string
		wantErr     bool // probe must error on both paths
		wantRows    int  // -1 = don't check
		sessionTODO string
	}

	cases := []kase{
		// ---- already at parity: regression guard ----
		{
			name: "crud_roundtrip",
			setup: []string{
				`CREATE TABLE u (id INT, name TEXT, age INT)`,
				`INSERT INTO u VALUES (1,'a',25),(2,'b',30),(3,'c',25)`,
			},
			probe: `SELECT * FROM u WHERE age = 25`, wantRows: 2,
		},
		{
			name: "join_full_table_names",
			setup: []string{
				`CREATE TABLE users (id INT PRIMARY KEY, name TEXT)`,
				`INSERT INTO users VALUES (1,'Alice'),(2,'Bob')`,
				`CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)`,
				`INSERT INTO orders VALUES (100,1),(101,1),(102,2)`,
			},
			probe: `SELECT users.name FROM users JOIN orders ON users.id = orders.user_id`, wantRows: 3,
		},
		{
			name: "group_by_having",
			setup: []string{
				`CREATE TABLE s (dept TEXT, amt INT)`,
				`INSERT INTO s VALUES ('eng',1),('eng',2),('ops',3)`,
			},
			probe: `SELECT dept, COUNT(*) FROM s GROUP BY dept HAVING COUNT(*) > 1`, wantRows: 1,
		},
		{
			name: "scalar_subquery_in_where",
			setup: []string{
				`CREATE TABLE p (id INT PRIMARY KEY, price INT)`,
				`INSERT INTO p VALUES (1,100),(2,200),(3,150)`,
			},
			probe: `SELECT id FROM p WHERE price = (SELECT MAX(price) FROM p)`, wantRows: 1,
		},

		// ---- P2.2: information_schema.* via parsed SQL ----
		{
			name: "information_schema_tables_via_sql",
			setup: []string{
				`CREATE TABLE aa (id INT PRIMARY KEY)`,
				`CREATE TABLE bb (id INT PRIMARY KEY)`,
			},
			probe: `SELECT table_name FROM information_schema.tables ORDER BY table_name`, wantRows: 2,
		},
		{
			name: "information_schema_columns_filtered",
			setup: []string{
				`CREATE TABLE aa (id INT PRIMARY KEY, label TEXT)`,
			},
			probe: `SELECT column_name FROM information_schema.columns WHERE table_name = 'aa'`, wantRows: 2,
		},

		// ---- P2.1: constraint enforcement (verified: executor rejects, session does not) ----
		{
			name:    "reject_duplicate_primary_key",
			setup:   []string{`CREATE TABLE t (id INT PRIMARY KEY, v TEXT)`, `INSERT INTO t VALUES (1,'a')`},
			probe:   `INSERT INTO t VALUES (1,'b')`,
			wantErr: true,
		},
		{
			name:    "reject_duplicate_unique",
			setup:   []string{`CREATE TABLE t (id INT, email TEXT UNIQUE)`, `INSERT INTO t VALUES (1,'x@y')`},
			probe:   `INSERT INTO t VALUES (2,'x@y')`,
			wantErr: true,
		},
		{
			name:    "reject_varchar_overflow",
			setup:   []string{`CREATE TABLE t (code VARCHAR(3))`},
			probe:   `INSERT INTO t VALUES ('toolong')`,
			wantErr: true,
		},
		{
			name: "reject_foreign_key_violation",
			setup: []string{
				`CREATE TABLE parent (id INT PRIMARY KEY)`,
				`CREATE TABLE child (id INT, pid INT REFERENCES parent(id))`,
			},
			probe:   `INSERT INTO child VALUES (1, 42)`,
			wantErr: true,
		},
		{
			name: "on_conflict_do_nothing_keeps_one_row",
			setup: []string{
				`CREATE TABLE t (id INT PRIMARY KEY, v TEXT)`,
				`INSERT INTO t VALUES (1,'a')`,
				`INSERT INTO t VALUES (1,'b') ON CONFLICT (id) DO NOTHING`,
			},
			probe: `SELECT v FROM t`, wantRows: 1,
		},
		{
			name: "on_conflict_do_update_no_duplicate",
			setup: []string{
				`CREATE TABLE t (id INT PRIMARY KEY, v TEXT)`,
				`INSERT INTO t VALUES (1,'a')`,
				`INSERT INTO t VALUES (1,'b') ON CONFLICT (id) DO UPDATE SET v = 'updated'`,
			},
			probe: `SELECT v FROM t WHERE id = 1`, wantRows: 1,
		},

		// ---- already at parity: NOT NULL on PK ----
		{
			name:    "reject_null_primary_key",
			setup:   []string{`CREATE TABLE t (id INT PRIMARY KEY, v TEXT)`},
			probe:   `INSERT INTO t (v) VALUES ('x')`,
			wantErr: true,
		},
	}

	runOne := func(t *testing.T, exec sqlExec, c kase) {
		t.Helper()
		for _, s := range c.setup {
			if _, err := exec.ExecuteSQL(s); err != nil {
				t.Fatalf("setup %q: %v", s, err)
			}
		}
		res, err := exec.ExecuteSQL(c.probe)
		if c.wantErr {
			if err == nil {
				t.Fatalf("probe %q: expected an error, got none", c.probe)
			}
			return
		}
		if err != nil {
			t.Fatalf("probe %q: %v", c.probe, err)
		}
		if c.wantRows >= 0 && len(res.Rows) != c.wantRows {
			t.Fatalf("probe %q: want %d rows, got %d", c.probe, c.wantRows, len(res.Rows))
		}
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Run("executor", func(t *testing.T) {
				runOne(t, NewExecutor(newParityTM(t)), c)
			})
			t.Run("session", func(t *testing.T) {
				if c.sessionTODO != "" {
					t.Skipf("verified Session gap — plan phase %s", c.sessionTODO)
				}
				runOne(t, newParitySession(t, newParityTM(t)), c)
			})
		})
	}
}

func newParityTM(t *testing.T) *catalog.TableManager {
	t.Helper()
	tm, err := catalog.NewTableManager(filepath.Join(t.TempDir(), "data"), 4096, nil)
	if err != nil {
		t.Fatalf("NewTableManager: %v", err)
	}
	return tm
}
