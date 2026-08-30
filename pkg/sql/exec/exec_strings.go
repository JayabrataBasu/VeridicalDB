package exec

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/token"
)

// exprToString converts an Expression AST node to a SQL string representation.
// This is used to serialize CHECK constraints for storage.
func exprToString(expr ast.Expression) string {
	if expr == nil {
		return ""
	}

	switch e := expr.(type) {
	case *ast.LiteralExpr:
		switch e.Value.Type {
		case catalog.TypeText:
			return fmt.Sprintf("'%s'", e.Value.Text)
		case catalog.TypeInt32:
			return fmt.Sprintf("%d", e.Value.Int32)
		case catalog.TypeInt64:
			return fmt.Sprintf("%d", e.Value.Int64)
		case catalog.TypeBool:
			if e.Value.Bool {
				return "TRUE"
			}
			return "FALSE"
		default:
			if e.Value.IsNull {
				return "NULL"
			}
			return fmt.Sprintf("%v", e.Value)
		}

	case *ast.ColumnRef:
		return e.Name

	case *ast.BinaryExpr:
		left := exprToString(e.Left)
		right := exprToString(e.Right)
		op := tokenToOperator(e.Op)
		return fmt.Sprintf("(%s %s %s)", left, op, right)

	case *ast.UnaryExpr:
		operand := exprToString(e.Expr)
		if e.Op == token.TOKEN_NOT {
			return fmt.Sprintf("NOT %s", operand)
		}
		return fmt.Sprintf("-%s", operand)

	case *ast.InExpr:
		left := exprToString(e.Left)
		values := make([]string, len(e.Values))
		for i, v := range e.Values {
			values[i] = exprToString(v)
		}
		not := ""
		if e.Not {
			not = "NOT "
		}
		return fmt.Sprintf("%s %sIN (%s)", left, not, strings.Join(values, ", "))

	case *ast.BetweenExpr:
		val := exprToString(e.Expr)
		low := exprToString(e.Low)
		high := exprToString(e.High)
		not := ""
		if e.Not {
			not = "NOT "
		}
		return fmt.Sprintf("%s %sBETWEEN %s AND %s", val, not, low, high)

	case *ast.LikeExpr:
		left := exprToString(e.Expr)
		pattern := exprToString(e.Pattern)
		op := "LIKE"
		if e.CaseInsensitive {
			op = "ILIKE"
		}
		not := ""
		if e.Not {
			not = "NOT "
		}
		return fmt.Sprintf("%s %s%s %s", left, not, op, pattern)

	case *ast.FunctionExpr:
		args := make([]string, len(e.Args))
		for i, arg := range e.Args {
			args[i] = exprToString(arg)
		}
		return fmt.Sprintf("%s(%s)", e.Name, strings.Join(args, ", "))

	case *ast.CaseExpr:
		var sb strings.Builder
		sb.WriteString("CASE")
		if e.Operand != nil {
			sb.WriteString(" ")
			sb.WriteString(exprToString(e.Operand))
		}
		for _, when := range e.Whens {
			sb.WriteString(" WHEN ")
			sb.WriteString(exprToString(when.Condition))
			sb.WriteString(" THEN ")
			sb.WriteString(exprToString(when.Result))
		}
		if e.Else != nil {
			sb.WriteString(" ELSE ")
			sb.WriteString(exprToString(e.Else))
		}
		sb.WriteString(" END")
		return sb.String()

	default:
		return "?"
	}
}

// tokenToOperator converts a token type to its SQL string representation.
func tokenToOperator(op token.TokenType) string {
	switch op {
	case token.TOKEN_EQ:
		return "="
	case token.TOKEN_NE:
		return "<>"
	case token.TOKEN_LT:
		return "<"
	case token.TOKEN_LE:
		return "<="
	case token.TOKEN_GT:
		return ">"
	case token.TOKEN_GE:
		return ">="
	case token.TOKEN_AND:
		return "AND"
	case token.TOKEN_OR:
		return "OR"
	case token.TOKEN_PLUS:
		return "+"
	case token.TOKEN_MINUS:
		return "-"
	case token.TOKEN_STAR:
		return "*"
	case token.TOKEN_SLASH:
		return "/"
	default:
		return "?"
	}
}
