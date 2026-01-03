package sql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

func TestParseProcedure_Create(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
		wantLang string
	}{
		{
			name:     "simple procedure",
			sql:      `CREATE PROCEDURE greet() AS $$ BEGIN RAISE NOTICE 'Hello'; END $$ LANGUAGE plpgsql`,
			wantName: "greet",
			wantLang: "plpgsql",
		},
		{
			name:     "procedure with INOUT params",
			sql:      `CREATE PROCEDURE increment(INOUT val INT) AS $$ BEGIN val := val + 1; END $$ LANGUAGE plpgsql`,
			wantName: "increment",
			wantLang: "plpgsql",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parser := NewParser(tc.sql)
			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			createProc, ok := stmt.(*CreateProcedureStmt)
			if !ok {
				t.Fatalf("expected *CreateProcedureStmt, got %T", stmt)
			}

			if createProc.Name != tc.wantName {
				t.Errorf("name = %q, want %q", createProc.Name, tc.wantName)
			}
			if createProc.Language != tc.wantLang {
				t.Errorf("language = %q, want %q", createProc.Language, tc.wantLang)
			}
		})
	}
}

func TestParseProcedure_CreateIfNotExists(t *testing.T) {
	sql := `CREATE PROCEDURE IF NOT EXISTS test_proc() AS $$ BEGIN RETURN; END $$ LANGUAGE plpgsql`

	parser := NewParser(sql)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	createProc, ok := stmt.(*CreateProcedureStmt)
	if !ok {
		t.Fatalf("expected *CreateProcedureStmt, got %T", stmt)
	}

	if !createProc.IfNotExists {
		t.Error("expected IfNotExists = true")
	}
}

func TestParseFunction_Create(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantName   string
		wantReturn catalog.DataType
		wantLang   string
	}{
		{
			name:       "simple function returning INT",
			sql:        `CREATE FUNCTION get_one() RETURNS INT AS $$ BEGIN RETURN 1; END $$ LANGUAGE plpgsql`,
			wantName:   "get_one",
			wantReturn: catalog.TypeInt32,
			wantLang:   "plpgsql",
		},
		{
			name:       "function returning TEXT",
			sql:        `CREATE FUNCTION hello(name TEXT) RETURNS TEXT AS $$ BEGIN RETURN 'Hello'; END $$ LANGUAGE plpgsql`,
			wantName:   "hello",
			wantReturn: catalog.TypeText,
			wantLang:   "plpgsql",
		},
		{
			name:       "function returning BOOLEAN",
			sql:        `CREATE FUNCTION is_even(n INT) RETURNS BOOLEAN AS $$ BEGIN RETURN TRUE; END $$ LANGUAGE plpgsql`,
			wantName:   "is_even",
			wantReturn: catalog.TypeBool,
			wantLang:   "plpgsql",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parser := NewParser(tc.sql)
			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			createFunc, ok := stmt.(*CreateFunctionStmt)
			if !ok {
				t.Fatalf("expected *CreateFunctionStmt, got %T", stmt)
			}

			if createFunc.Name != tc.wantName {
				t.Errorf("name = %q, want %q", createFunc.Name, tc.wantName)
			}
			if createFunc.ReturnType != tc.wantReturn {
				t.Errorf("returnType = %v, want %v", createFunc.ReturnType, tc.wantReturn)
			}
			if createFunc.Language != tc.wantLang {
				t.Errorf("language = %q, want %q", createFunc.Language, tc.wantLang)
			}
		})
	}
}

func TestParseProcedure_Drop(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantName   string
		wantExists bool
	}{
		{
			name:       "simple drop",
			sql:        "DROP PROCEDURE my_proc",
			wantName:   "my_proc",
			wantExists: false,
		},
		{
			name:       "drop if exists",
			sql:        "DROP PROCEDURE IF EXISTS cleanup_proc",
			wantName:   "cleanup_proc",
			wantExists: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parser := NewParser(tc.sql)
			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			dropProc, ok := stmt.(*DropProcedureStmt)
			if !ok {
				t.Fatalf("expected *DropProcedureStmt, got %T", stmt)
			}

			if dropProc.Name != tc.wantName {
				t.Errorf("name = %q, want %q", dropProc.Name, tc.wantName)
			}
			if dropProc.IfExists != tc.wantExists {
				t.Errorf("ifExists = %v, want %v", dropProc.IfExists, tc.wantExists)
			}
		})
	}
}

func TestParseFunction_Drop(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantName   string
		wantExists bool
	}{
		{
			name:       "simple drop",
			sql:        "DROP FUNCTION my_func",
			wantName:   "my_func",
			wantExists: false,
		},
		{
			name:       "drop if exists",
			sql:        "DROP FUNCTION IF EXISTS get_data",
			wantName:   "get_data",
			wantExists: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parser := NewParser(tc.sql)
			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			dropFunc, ok := stmt.(*DropFunctionStmt)
			if !ok {
				t.Fatalf("expected *DropFunctionStmt, got %T", stmt)
			}

			if dropFunc.Name != tc.wantName {
				t.Errorf("name = %q, want %q", dropFunc.Name, tc.wantName)
			}
			if dropFunc.IfExists != tc.wantExists {
				t.Errorf("ifExists = %v, want %v", dropFunc.IfExists, tc.wantExists)
			}
		})
	}
}

func TestParseCall(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantName string
		wantArgs int
	}{
		{
			name:     "call without args",
			sql:      "CALL cleanup()",
			wantName: "cleanup",
			wantArgs: 0,
		},
		{
			name:     "call with one arg",
			sql:      "CALL greet('Alice')",
			wantName: "greet",
			wantArgs: 1,
		},
		{
			name:     "call with multiple args",
			sql:      "CALL insert_user('Bob', 30, 'bob@example.com')",
			wantName: "insert_user",
			wantArgs: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parser := NewParser(tc.sql)
			stmt, err := parser.Parse()
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			call, ok := stmt.(*CallStmt)
			if !ok {
				t.Fatalf("expected *CallStmt, got %T", stmt)
			}

			if call.Name != tc.wantName {
				t.Errorf("name = %q, want %q", call.Name, tc.wantName)
			}
			if len(call.Args) != tc.wantArgs {
				t.Errorf("args count = %d, want %d", len(call.Args), tc.wantArgs)
			}
		})
	}
}

func TestParseShowProcedures(t *testing.T) {
	sql := "SHOW PROCEDURES"

	parser := NewParser(sql)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	_, ok := stmt.(*ShowProceduresStmt)
	if !ok {
		t.Fatalf("expected *ShowProceduresStmt, got %T", stmt)
	}
}

func TestParseShowFunctions(t *testing.T) {
	sql := "SHOW FUNCTIONS"

	parser := NewParser(sql)
	stmt, err := parser.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	_, ok := stmt.(*ShowFunctionsStmt)
	if !ok {
		t.Fatalf("expected *ShowFunctionsStmt, got %T", stmt)
	}
}

func TestProcedureCatalog(t *testing.T) {
	// Create temp directory for test
	tmpDir, err := os.MkdirTemp("", "proc_catalog_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dataDir := filepath.Join(tmpDir, "data")
	_ = os.MkdirAll(dataDir, 0755)

	pc, err := catalog.NewProcedureCatalog(dataDir)
	if err != nil {
		t.Fatalf("NewProcedureCatalog failed: %v", err)
	}

	// Test CreateProcedure
	t.Run("CreateProcedure", func(t *testing.T) {
		err := pc.CreateProcedure(&catalog.ProcedureMeta{
			Name: "greet",
			Params: []catalog.ProcParamMeta{
				{Name: "name", Type: catalog.TypeText, Mode: catalog.ParamModeIn},
			},
			Body:     "BEGIN RAISE NOTICE name; END",
			Language: "plpgsql",
		})
		if err != nil {
			t.Fatalf("CreateProcedure failed: %v", err)
		}

		proc, ok := pc.GetProcedure("greet")
		if !ok {
			t.Fatalf("GetProcedure failed: procedure not found")
		}

		if proc.Name != "greet" {
			t.Errorf("name = %q, want %q", proc.Name, "greet")
		}
		if len(proc.Params) != 1 {
			t.Errorf("params count = %d, want 1", len(proc.Params))
		}
	})

	// Test CreateFunction
	t.Run("CreateFunction", func(t *testing.T) {
		err := pc.CreateFunction(&catalog.ProcedureMeta{
			Name: "add",
			Params: []catalog.ProcParamMeta{
				{Name: "a", Type: catalog.TypeInt32, Mode: catalog.ParamModeIn},
				{Name: "b", Type: catalog.TypeInt32, Mode: catalog.ParamModeIn},
			},
			ReturnType: catalog.TypeInt32,
			Body:       "BEGIN RETURN a + b; END",
			Language:   "plpgsql",
		})
		if err != nil {
			t.Fatalf("CreateFunction failed: %v", err)
		}

		fn, ok := pc.GetFunction("add")
		if !ok {
			t.Fatalf("GetFunction failed: function not found")
		}

		if fn.Name != "add" {
			t.Errorf("name = %q, want %q", fn.Name, "add")
		}
		if fn.ReturnType != catalog.TypeInt32 {
			t.Errorf("returnType = %v, want %v", fn.ReturnType, catalog.TypeInt32)
		}
	})

	// Test ListProcedures
	t.Run("ListProcedures", func(t *testing.T) {
		procs := pc.ListProcedures()
		if len(procs) != 1 {
			t.Errorf("procedures count = %d, want 1", len(procs))
		}
	})

	// Test ListFunctions
	t.Run("ListFunctions", func(t *testing.T) {
		fns := pc.ListFunctions()
		if len(fns) != 1 {
			t.Errorf("functions count = %d, want 1", len(fns))
		}
	})

	// Test DropProcedure
	t.Run("DropProcedure", func(t *testing.T) {
		err := pc.DropProcedure("greet", false)
		if err != nil {
			t.Fatalf("DropProcedure failed: %v", err)
		}

		_, ok := pc.GetProcedure("greet")
		if ok {
			t.Error("expected procedure to be dropped")
		}
	})

	// Test DropFunction
	t.Run("DropFunction", func(t *testing.T) {
		err := pc.DropFunction("add", false)
		if err != nil {
			t.Fatalf("DropFunction failed: %v", err)
		}

		_, ok := pc.GetFunction("add")
		if ok {
			t.Error("expected function to be dropped")
		}
	})

	// Test persistence
	t.Run("Persistence", func(t *testing.T) {
		// Create a new procedure
		pc.CreateProcedure(&catalog.ProcedureMeta{
			Name:     "persisted_proc",
			Body:     "BEGIN END",
			Language: "plpgsql",
		})

		// Create a new catalog instance (simulating restart)
		pc2, err := catalog.NewProcedureCatalog(dataDir)
		if err != nil {
			t.Fatalf("NewProcedureCatalog failed: %v", err)
		}

		// Verify procedure was persisted
		proc, ok := pc2.GetProcedure("persisted_proc")
		if !ok {
			t.Fatalf("GetProcedure after reload failed: procedure not found")
		}
		if proc.Name != "persisted_proc" {
			t.Errorf("name = %q, want %q", proc.Name, "persisted_proc")
		}
	})
}

func TestParsePLBlock(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{
			name:    "empty block",
			body:    "BEGIN END",
			wantErr: false,
		},
		{
			name:    "block with declare",
			body:    "DECLARE x INT; BEGIN x := 1; END",
			wantErr: false,
		},
		{
			name:    "block with if",
			body:    "BEGIN IF 1 = 1 THEN RETURN; END IF; END",
			wantErr: false,
		},
		{
			name:    "block with while",
			body:    "DECLARE i INT; BEGIN i := 0; WHILE i < 10 LOOP i := i + 1; END LOOP; END",
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Wrap in CREATE PROCEDURE for parsing
			sql := "CREATE PROCEDURE test() AS $$ " + tc.body + " $$ LANGUAGE plpgsql"

			parser := NewParser(sql)
			stmt, err := parser.Parse()

			if tc.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			createProc, ok := stmt.(*CreateProcedureStmt)
			if !ok {
				t.Fatalf("expected *CreateProcedureStmt, got %T", stmt)
			}

			if createProc.Body == nil {
				t.Error("expected parsed body, got nil")
			}
		})
	}
}
