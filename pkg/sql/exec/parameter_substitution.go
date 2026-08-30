package exec

import (
	"fmt"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
)

// SubstituteParams replaces PlaceholderExpr with LiteralExpr in the statement.
func SubstituteParams(stmt ast.Statement, params []catalog.Value) (ast.Statement, error) {
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		return substituteParamsSelect(s, params)
	case *ast.InsertStmt:
		return substituteParamsInsert(s, params)
	case *ast.UpdateStmt:
		return substituteParamsUpdate(s, params)
	case *ast.DeleteStmt:
		return substituteParamsDelete(s, params)
	default:
		return nil, fmt.Errorf("parameter substitution not supported for statement type: %T", stmt)
	}
}

func substituteParamsSelect(stmt *ast.SelectStmt, params []catalog.Value) (*ast.SelectStmt, error) {
	newStmt := *stmt // Shallow copy

	// Substitute in WHERE
	if stmt.Where != nil {
		var err error
		newStmt.Where, err = substituteParamsExpr(stmt.Where, params)
		if err != nil {
			return nil, err
		}
	}

	// Substitute in HAVING
	if stmt.Having != nil {
		var err error
		newStmt.Having, err = substituteParamsExpr(stmt.Having, params)
		if err != nil {
			return nil, err
		}
	}

	// Substitute in Columns (e.g. SELECT )
	newCols := make([]ast.SelectColumn, len(stmt.Columns))
	for i, col := range stmt.Columns {
		newCols[i] = col
		if col.Expression != nil {
			var err error
			newCols[i].Expression, err = substituteParamsExpr(col.Expression, params)
			if err != nil {
				return nil, err
			}
		}
	}
	newStmt.Columns = newCols

	return &newStmt, nil
}

func substituteParamsInsert(stmt *ast.InsertStmt, params []catalog.Value) (*ast.InsertStmt, error) {
	newStmt := *stmt // Shallow copy

	// Substitute in Values
	newValues := make([][]ast.Expression, len(stmt.ValuesList))
	for i, row := range stmt.ValuesList {
		newRow := make([]ast.Expression, len(row))
		for j, expr := range row {
			var err error
			newRow[j], err = substituteParamsExpr(expr, params)
			if err != nil {
				return nil, err
			}
		}
		newValues[i] = newRow
	}
	newStmt.ValuesList = newValues

	return &newStmt, nil
}

func substituteParamsUpdate(stmt *ast.UpdateStmt, params []catalog.Value) (*ast.UpdateStmt, error) {
	newStmt := *stmt // Shallow copy

	// Substitute in Assignments
	newAssignments := make([]ast.Assignment, len(stmt.Assignments))
	for i, assign := range stmt.Assignments {
		newAssign := assign
		var err error
		newAssign.Value, err = substituteParamsExpr(assign.Value, params)
		if err != nil {
			return nil, err
		}
		newAssignments[i] = newAssign
	}
	newStmt.Assignments = newAssignments

	// Substitute in WHERE
	if stmt.Where != nil {
		var err error
		newStmt.Where, err = substituteParamsExpr(stmt.Where, params)
		if err != nil {
			return nil, err
		}
	}

	return &newStmt, nil
}

func substituteParamsDelete(stmt *ast.DeleteStmt, params []catalog.Value) (*ast.DeleteStmt, error) {
	newStmt := *stmt // Shallow copy

	// Substitute in WHERE
	if stmt.Where != nil {
		var err error
		newStmt.Where, err = substituteParamsExpr(stmt.Where, params)
		if err != nil {
			return nil, err
		}
	}

	return &newStmt, nil
}

func substituteParamsExpr(expr ast.Expression, params []catalog.Value) (ast.Expression, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *ast.PlaceholderExpr:
		if e.Index > len(params) {
			return nil, fmt.Errorf("parameter index $%d out of range (count: %d)", e.Index, len(params))
		}
		return &ast.LiteralExpr{Value: params[e.Index-1]}, nil

	case *ast.BinaryExpr:
		left, err := substituteParamsExpr(e.Left, params)
		if err != nil {
			return nil, err
		}
		right, err := substituteParamsExpr(e.Right, params)
		if err != nil {
			return nil, err
		}
		return &ast.BinaryExpr{Left: left, Op: e.Op, Right: right}, nil

	case *ast.UnaryExpr:
		sub, err := substituteParamsExpr(e.Expr, params)
		if err != nil {
			return nil, err
		}
		return &ast.UnaryExpr{Op: e.Op, Expr: sub}, nil

	case *ast.FunctionExpr:
		newArgs := make([]ast.Expression, len(e.Args))
		for i, arg := range e.Args {
			var err error
			newArgs[i], err = substituteParamsExpr(arg, params)
			if err != nil {
				return nil, err
			}
		}
		return &ast.FunctionExpr{Name: e.Name, Args: newArgs}, nil

	case *ast.BetweenExpr:
		subExpr, err := substituteParamsExpr(e.Expr, params)
		if err != nil {
			return nil, err
		}
		subLow, err := substituteParamsExpr(e.Low, params)
		if err != nil {
			return nil, err
		}
		subHigh, err := substituteParamsExpr(e.High, params)
		if err != nil {
			return nil, err
		}
		return &ast.BetweenExpr{Expr: subExpr, Low: subLow, High: subHigh, Not: e.Not}, nil

	case *ast.LikeExpr:
		subExpr, err := substituteParamsExpr(e.Expr, params)
		if err != nil {
			return nil, err
		}
		subPattern, err := substituteParamsExpr(e.Pattern, params)
		if err != nil {
			return nil, err
		}
		return &ast.LikeExpr{Expr: subExpr, Pattern: subPattern, CaseInsensitive: e.CaseInsensitive, Not: e.Not}, nil

	case *ast.IsNullExpr:
		subExpr, err := substituteParamsExpr(e.Expr, params)
		if err != nil {
			return nil, err
		}
		return &ast.IsNullExpr{Expr: subExpr, Not: e.Not}, nil

	case *ast.CastExpr:
		subExpr, err := substituteParamsExpr(e.Expr, params)
		if err != nil {
			return nil, err
		}
		return &ast.CastExpr{Expr: subExpr, TargetType: e.TargetType}, nil

	case *ast.LiteralExpr, *ast.ColumnRef:
		return e, nil

	default:
		return e, nil
	}
}
