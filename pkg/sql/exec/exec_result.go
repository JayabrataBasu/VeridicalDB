package exec

import (
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
)

// ViewDef stores a view definition.
type ViewDef struct {
	Name    string
	Columns []string        // optional column aliases
	Query   *ast.SelectStmt // the SELECT that defines the view
}

// Result represents the result of executing a SQL statement.
type Result struct {
	Message      string
	Columns      []string
	Rows         [][]catalog.Value
	RowsAffected int
}
