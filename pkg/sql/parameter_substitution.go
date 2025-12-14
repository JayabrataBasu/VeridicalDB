package sql

import (
"fmt"

"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

// SubstituteParams replaces PlaceholderExpr with LiteralExpr in the statement.
func SubstituteParams(stmt Statement, params []catalog.Value) (Statement, error) {
switch s := stmt.(type) {
case *SelectStmt:
return substituteParamsSelect(s, params)
case *InsertStmt:
return substituteParamsInsert(s, params)
case *UpdateStmt:
return substituteParamsUpdate(s, params)
case *DeleteStmt:
return substituteParamsDelete(s, params)
default:
return nil, fmt.Errorf("parameter substitution not supported for statement type: %T", stmt)
}
}

func substituteParamsSelect(stmt *SelectStmt, params []catalog.Value) (*SelectStmt, error) {
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
newCols := make([]SelectColumn, len(stmt.Columns))
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

func substituteParamsInsert(stmt *InsertStmt, params []catalog.Value) (*InsertStmt, error) {
newStmt := *stmt // Shallow copy

// Substitute in Values
newValues := make([][]Expression, len(stmt.ValuesList))
for i, row := range stmt.ValuesList {
newRow := make([]Expression, len(row))
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

func substituteParamsUpdate(stmt *UpdateStmt, params []catalog.Value) (*UpdateStmt, error) {
newStmt := *stmt // Shallow copy

// Substitute in Assignments
newAssignments := make([]Assignment, len(stmt.Assignments))
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

func substituteParamsDelete(stmt *DeleteStmt, params []catalog.Value) (*DeleteStmt, error) {
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

func substituteParamsExpr(expr Expression, params []catalog.Value) (Expression, error) {
if expr == nil {
return nil, nil
}

switch e := expr.(type) {
case *PlaceholderExpr:
if e.Index > len(params) {
return nil, fmt.Errorf("parameter index $%d out of range (count: %d)", e.Index, len(params))
}
return &LiteralExpr{Value: params[e.Index-1]}, nil

case *BinaryExpr:
left, err := substituteParamsExpr(e.Left, params)
if err != nil {
return nil, err
}
right, err := substituteParamsExpr(e.Right, params)
if err != nil {
return nil, err
}
return &BinaryExpr{Left: left, Op: e.Op, Right: right}, nil

case *UnaryExpr:
sub, err := substituteParamsExpr(e.Expr, params)
if err != nil {
return nil, err
}
return &UnaryExpr{Op: e.Op, Expr: sub}, nil

case *FunctionExpr:
newArgs := make([]Expression, len(e.Args))
for i, arg := range e.Args {
var err error
newArgs[i], err = substituteParamsExpr(arg, params)
if err != nil {
return nil, err
}
}
return &FunctionExpr{Name: e.Name, Args: newArgs}, nil

case *BetweenExpr:
subExpr, err := substituteParamsExpr(e.Expr, params)
if err != nil { return nil, err }
subLow, err := substituteParamsExpr(e.Low, params)
if err != nil { return nil, err }
subHigh, err := substituteParamsExpr(e.High, params)
if err != nil { return nil, err }
return &BetweenExpr{Expr: subExpr, Low: subLow, High: subHigh, Not: e.Not}, nil

case *LikeExpr:
subExpr, err := substituteParamsExpr(e.Expr, params)
if err != nil { return nil, err }
subPattern, err := substituteParamsExpr(e.Pattern, params)
if err != nil { return nil, err }
return &LikeExpr{Expr: subExpr, Pattern: subPattern, CaseInsensitive: e.CaseInsensitive, Not: e.Not}, nil

case *IsNullExpr:
subExpr, err := substituteParamsExpr(e.Expr, params)
if err != nil { return nil, err }
return &IsNullExpr{Expr: subExpr, Not: e.Not}, nil

case *CastExpr:
subExpr, err := substituteParamsExpr(e.Expr, params)
if err != nil { return nil, err }
return &CastExpr{Expr: subExpr, TargetType: e.TargetType}, nil

case *LiteralExpr, *ColumnRef:
return e, nil

default:
return e, nil
}
}
