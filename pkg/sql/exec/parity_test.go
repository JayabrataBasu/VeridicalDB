package exec

import (
	"path/filepath"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

// TestSessionSQLBattery is a broad regression battery for the single SQL engine
// (*Session over *MVCCExecutor). It began life as the P2 executor-parity gate —
// every case was cross-checked against the pre-MVCC *Executor before that file
// was deleted — and stays as a compact end-to-end guard over the features that
// collapse exposed: constraint enforcement, ON CONFLICT, DEFAULTs, AUTO_INCREMENT,
// information_schema, LATERAL correlation, UPDATE/DELETE ... FROM/USING,
// DISTINCT ON, GROUPING SETS / CUBE / ROLLUP + GROUPING(), window frames + the
// full window-function set, MERGE, and CTEs feeding aggregates / ORDER BY / a
// recursive-term JOIN.
func TestSessionSQLBattery(t *testing.T) {
	type kase struct {
		name     string
		setup    []string
		probe    string
		wantErr  bool // probe must error
		wantRows int  // -1 = don't check
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

		// ---- P2.4: CTE consumed by an aggregate / ORDER BY; recursive CTE + JOIN ----
		{
			name: "cte_feeds_aggregate",
			setup: []string{
				`CREATE TABLE emp (id INT PRIMARY KEY, dept TEXT, sal INT)`,
				`INSERT INTO emp VALUES (1,'eng',100),(2,'eng',90),(3,'sales',80)`,
			},
			probe:    `WITH e AS (SELECT sal FROM emp WHERE dept = 'eng') SELECT SUM(sal) AS total FROM e`,
			wantRows: 1,
		},
		{
			name: "cte_with_order_by_limit",
			setup: []string{
				`CREATE TABLE emp2 (id INT PRIMARY KEY, name TEXT, sal INT)`,
				`INSERT INTO emp2 VALUES (1,'a',100),(2,'b',90),(3,'c',85),(4,'d',70)`,
			},
			probe:    `WITH all_e AS (SELECT name, sal FROM emp2) SELECT name FROM all_e ORDER BY sal DESC LIMIT 2`,
			wantRows: 2,
		},
		{
			name: "recursive_cte_join_in_recursive_term",
			setup: []string{
				`CREATE TABLE org (id INT PRIMARY KEY, name TEXT, mgr INT)`,
				`INSERT INTO org VALUES (1,'ceo',0),(2,'vp',1),(3,'mgr',2),(4,'ic',3)`,
			},
			probe: `WITH RECURSIVE rpt AS (` +
				`SELECT id, name, mgr FROM org WHERE mgr = 1 ` +
				`UNION ALL ` +
				`SELECT o.id, o.name, o.mgr FROM org o JOIN rpt r ON o.mgr = r.id) ` +
				`SELECT * FROM rpt`,
			wantRows: 3,
		},

		// ---- P2.6b: MERGE ----
		{
			name: "merge_matched_update_not_matched_insert",
			setup: []string{
				`CREATE TABLE inv (pid INT, qty INT)`,
				`CREATE TABLE ship (pid INT, qty INT)`,
				`INSERT INTO inv VALUES (1,100),(2,50)`,
				`INSERT INTO ship VALUES (1,25),(3,75)`,
				`MERGE INTO inv AS i USING ship AS s ON i.pid = s.pid ` +
					`WHEN MATCHED THEN UPDATE SET qty = i.qty + s.qty ` +
					`WHEN NOT MATCHED THEN INSERT (pid, qty) VALUES (s.pid, s.qty)`,
			},
			probe:    `SELECT pid, qty FROM inv ORDER BY pid`,
			wantRows: 3,
		},
		{
			name: "merge_from_aggregating_subquery",
			setup: []string{
				`CREATE TABLE acct (aid INT, bal INT)`,
				`CREATE TABLE txns (aid INT, amt INT)`,
				`INSERT INTO acct VALUES (1,1000),(2,2000)`,
				`INSERT INTO txns VALUES (1,100),(1,50),(3,500)`,
				`MERGE INTO acct AS a USING (SELECT aid, SUM(amt) AS total FROM txns GROUP BY aid) AS t ` +
					`ON a.aid = t.aid ` +
					`WHEN MATCHED THEN UPDATE SET bal = a.bal + t.total ` +
					`WHEN NOT MATCHED THEN INSERT (aid, bal) VALUES (t.aid, t.total)`,
			},
			probe:    `SELECT aid, bal FROM acct ORDER BY aid`,
			wantRows: 3,
		},

		// ---- P2.6b: window frames + extended window functions ----
		{
			name: "window_moving_sum_frame",
			setup: []string{
				`CREATE TABLE w (id INT, v INT)`,
				`INSERT INTO w VALUES (1,10),(2,20),(3,30),(4,40),(5,50)`,
			},
			probe:    `SELECT id, SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM w`,
			wantRows: 5,
		},
		{
			name: "window_nth_value",
			setup: []string{
				`CREATE TABLE w2 (dept TEXT, sal INT)`,
				`INSERT INTO w2 VALUES ('e',80),('e',90),('e',70),('s',60),('s',65)`,
			},
			probe: `SELECT dept, NTH_VALUE(sal, 2) OVER (PARTITION BY dept ORDER BY sal DESC ` +
				`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM w2`,
			wantRows: 5,
		},

		// ---- P2.6b: GROUPING SETS / CUBE / ROLLUP + GROUPING() ----
		{
			name: "grouping_sets_row_per_set",
			setup: []string{
				`CREATE TABLE g (region TEXT, product TEXT, amount INT)`,
				`INSERT INTO g VALUES ('E','W',100),('E','G',200),('W','W',300),('W','G',250)`,
			},
			probe:    `SELECT region, product, SUM(amount) FROM g GROUP BY GROUPING SETS ((region),(product),())`,
			wantRows: 5,
		},
		{
			name: "cube_all_combinations",
			setup: []string{
				`CREATE TABLE g2 (a TEXT, b TEXT, n INT)`,
				`INSERT INTO g2 VALUES ('x','p',1),('x','q',2),('y','p',3),('y','q',4)`,
			},
			probe:    `SELECT a, b, SUM(n) FROM g2 GROUP BY CUBE(a, b)`,
			wantRows: 9,
		},
		{
			name: "rollup_hierarchy",
			setup: []string{
				`CREATE TABLE g3 (yr INT, q TEXT, n INT)`,
				`INSERT INTO g3 VALUES (2023,'Q1',100),(2023,'Q2',150),(2024,'Q1',250)`,
			},
			probe:    `SELECT yr, q, SUM(n) FROM g3 GROUP BY ROLLUP(yr, q)`,
			wantRows: 6,
		},

		// ---- P2.6b: DISTINCT ON ----
		{
			name: "distinct_on_keeps_one_per_group",
			setup: []string{
				`CREATE TABLE d (cat TEXT, v INT)`,
				`INSERT INTO d VALUES ('a',1),('a',2),('b',3),('b',4),('c',5)`,
			},
			probe:    `SELECT DISTINCT ON (cat) cat, v FROM d`,
			wantRows: 3,
		},

		// ---- P2.5: UPDATE ... FROM / DELETE ... USING ----
		{
			name: "update_from_qualified_columns",
			setup: []string{
				`CREATE TABLE ord (id INT, cust INT, status TEXT)`,
				`CREATE TABLE cust (id INT, country TEXT)`,
				`INSERT INTO ord VALUES (1,10,'p'),(2,11,'p'),(3,12,'p')`,
				`INSERT INTO cust VALUES (10,'US'),(11,'CA'),(12,'US')`,
			},
			probe:    `UPDATE ord SET status = 'ship' FROM cust WHERE ord.cust = cust.id AND cust.country = 'US'`,
			wantRows: 0,
		},
		{
			name: "delete_using_qualified_columns",
			setup: []string{
				`CREATE TABLE ord2 (id INT, cust INT)`,
				`CREATE TABLE gone (id INT)`,
				`INSERT INTO ord2 VALUES (1,10),(2,11),(3,10)`,
				`INSERT INTO gone VALUES (10)`,
			},
			probe:    `DELETE FROM ord2 USING gone WHERE ord2.cust = gone.id`,
			wantRows: 0,
		},
		{
			name: "update_from_still_enforces_fk",
			setup: []string{
				`CREATE TABLE par (id INT PRIMARY KEY)`,
				`CREATE TABLE chi (id INT, pid INT REFERENCES par(id))`,
				`CREATE TABLE src (bad INT)`,
				`INSERT INTO par VALUES (1)`,
				`INSERT INTO chi VALUES (1,1)`,
				`INSERT INTO src VALUES (99)`,
			},
			probe:   `UPDATE chi SET pid = src.bad FROM src WHERE chi.id = 1`,
			wantErr: true,
		},

		// ---- P2.3: LATERAL subquery, CROSS and LEFT ----
		{
			name: "lateral_left_join_keeps_unmatched",
			setup: []string{
				`CREATE TABLE p (pid INT, pname TEXT)`,
				`CREATE TABLE o (oid INT, pid INT, qty INT)`,
				`INSERT INTO p VALUES (1,'a'),(2,'b'),(3,'c')`,
				`INSERT INTO o VALUES (1,1,5),(2,2,7)`,
			},
			probe: `SELECT p.pname, x.qty FROM p LEFT JOIN LATERAL ` +
				`(SELECT qty FROM o WHERE pid = p.pid LIMIT 1) AS x ON TRUE`,
			wantRows: 3,
		},
		{
			name: "lateral_cross_join_correlated_per_row",
			setup: []string{
				`CREATE TABLE dep (did INT, dname TEXT)`,
				`CREATE TABLE emp (eid INT, did INT, sal INT)`,
				`INSERT INTO dep VALUES (1,'eng'),(2,'sales')`,
				`INSERT INTO emp VALUES (1,1,80),(2,1,90),(3,2,60),(4,2,70)`,
			},
			probe: `SELECT d.dname, t.sal FROM dep d CROSS JOIN LATERAL ` +
				`(SELECT sal FROM emp WHERE did = d.did ORDER BY sal DESC LIMIT 1) AS t`,
			wantRows: 2,
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
			runOne(t, newParitySession(t, newParityTM(t)), c)
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
