package sql

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

// PLInterpreter executes PL/pgSQL code within a procedure or function.
type PLInterpreter struct {
	session   *Session
	variables map[string]catalog.Value  // local variables
	params    map[string]catalog.Value  // input parameters
	outParams map[string]*catalog.Value // OUT/INOUT parameters (pointers for modification)
	returnVal *catalog.Value            // return value for functions
	returned  bool                      // flag indicating RETURN was executed
	exited    bool                      // flag indicating EXIT was executed
}

// NewPLInterpreter creates a new PL/pgSQL interpreter.
func NewPLInterpreter(session *Session) *PLInterpreter {
	return &PLInterpreter{
		session:   session,
		variables: make(map[string]catalog.Value),
		params:    make(map[string]catalog.Value),
		outParams: make(map[string]*catalog.Value),
	}
}

// SetParameter sets an input parameter value.
func (pl *PLInterpreter) SetParameter(name string, value catalog.Value) {
	pl.params[strings.ToLower(name)] = value
}

// SetOutParameter sets a reference to an OUT/INOUT parameter.
func (pl *PLInterpreter) SetOutParameter(name string, ref *catalog.Value) {
	pl.outParams[strings.ToLower(name)] = ref
}

// GetVariable retrieves a variable or parameter value.
func (pl *PLInterpreter) GetVariable(name string) (catalog.Value, bool) {
	lname := strings.ToLower(name)
	if v, ok := pl.variables[lname]; ok {
		return v, true
	}
	if v, ok := pl.params[lname]; ok {
		return v, true
	}
	if ref, ok := pl.outParams[lname]; ok {
		return *ref, true
	}
	return catalog.Value{}, false
}

// SetVariable sets a variable value.
func (pl *PLInterpreter) SetVariable(name string, value catalog.Value) error {
	lname := strings.ToLower(name)

	// Check if it's an OUT/INOUT parameter
	if ref, ok := pl.outParams[lname]; ok {
		*ref = value
		return nil
	}

	// Check if it's a local variable
	if _, ok := pl.variables[lname]; ok {
		pl.variables[lname] = value
		return nil
	}

	// Check if it's an IN parameter (read-only)
	if _, ok := pl.params[lname]; ok {
		return fmt.Errorf("cannot assign to IN parameter %q", name)
	}

	return fmt.Errorf("variable %q not declared", name)
}

// DeclareVariable declares a new local variable.
func (pl *PLInterpreter) DeclareVariable(name string, dataType catalog.DataType, defaultVal *catalog.Value) {
	lname := strings.ToLower(name)
	if defaultVal != nil {
		pl.variables[lname] = *defaultVal
	} else {
		// Initialize with NULL of the appropriate type
		pl.variables[lname] = catalog.Null(dataType)
	}
}

// ReturnValue gets the return value (for functions).
func (pl *PLInterpreter) ReturnValue() *catalog.Value {
	return pl.returnVal
}

// HasReturned checks if RETURN was executed.
func (pl *PLInterpreter) HasReturned() bool {
	return pl.returned
}

// ExecuteBlock executes a PL/pgSQL block.
func (pl *PLInterpreter) ExecuteBlock(block *PLBlock) error {
	// Process DECLARE section
	for _, decl := range block.Declarations {
		var defaultVal *catalog.Value
		if decl.Default != nil {
			val, err := pl.evalExpression(decl.Default)
			if err != nil {
				return fmt.Errorf("error evaluating default for %s: %v", decl.Name, err)
			}
			defaultVal = &val
		}
		pl.DeclareVariable(decl.Name, decl.Type, defaultVal)
	}

	// Execute statements
	err := pl.executeStatements(block.Statements)
	if err != nil {
		// Check for exception handlers
		if len(block.ExceptionHandlers) > 0 {
			handled := false
			for _, handler := range block.ExceptionHandlers {
				for _, excName := range handler.Exceptions {
					if strings.ToUpper(excName) == "OTHERS" {
						// OTHERS catches all exceptions
						err = pl.executeStatements(handler.Statements)
						handled = true
						break
					}
				}
				if handled {
					break
				}
			}
			if handled {
				return err // return error from handler (nil if successful)
			}
		}
		return err
	}

	return nil
}

// executeStatements executes a list of PL statements.
func (pl *PLInterpreter) executeStatements(stmts []PLStatement) error {
	for _, stmt := range stmts {
		if pl.returned || pl.exited {
			break
		}
		if err := pl.executeStatement(stmt); err != nil {
			return err
		}
	}
	return nil
}

// executeStatement executes a single PL statement.
func (pl *PLInterpreter) executeStatement(stmt PLStatement) error {
	switch s := stmt.(type) {
	case *PLAssign:
		return pl.executeAssign(s)
	case *PLIf:
		return pl.executeIf(s)
	case *PLWhile:
		return pl.executeWhile(s)
	case *PLLoop:
		return pl.executeLoop(s)
	case *PLFor:
		return pl.executeFor(s)
	case *PLExit:
		return pl.executeExit(s)
	case *PLContinue:
		return pl.executeContinue(s)
	case *PLReturn:
		return pl.executeReturn(s)
	case *PLRaise:
		return pl.executeRaise(s)
	case *PLPerform:
		return pl.executePerform(s)
	case *PLSQL:
		return pl.executePLSQL(s)
	default:
		return fmt.Errorf("unknown PL statement type: %T", stmt)
	}
}

// executeAssign executes a variable assignment.
func (pl *PLInterpreter) executeAssign(stmt *PLAssign) error {
	val, err := pl.evalExpression(stmt.Value)
	if err != nil {
		return fmt.Errorf("error evaluating assignment: %v", err)
	}
	return pl.SetVariable(stmt.Variable, val)
}

// executeIf executes an IF statement.
func (pl *PLInterpreter) executeIf(stmt *PLIf) error {
	cond, err := pl.evalCondition(stmt.Condition)
	if err != nil {
		return err
	}

	if cond {
		return pl.executeStatements(stmt.Then)
	}

	// Check ELSIF branches
	for _, elsif := range stmt.ElsIfs {
		cond, err := pl.evalCondition(elsif.Condition)
		if err != nil {
			return err
		}
		if cond {
			return pl.executeStatements(elsif.Then)
		}
	}

	// ELSE branch
	if len(stmt.Else) > 0 {
		return pl.executeStatements(stmt.Else)
	}

	return nil
}

// executeWhile executes a WHILE loop.
func (pl *PLInterpreter) executeWhile(stmt *PLWhile) error {
	for {
		cond, err := pl.evalCondition(stmt.Condition)
		if err != nil {
			return err
		}
		if !cond {
			break
		}

		pl.exited = false
		if err := pl.executeStatements(stmt.Body); err != nil {
			return err
		}
		if pl.returned {
			break
		}
		if pl.exited {
			pl.exited = false
			break
		}
	}
	return nil
}

// executeLoop executes an infinite LOOP (until EXIT).
func (pl *PLInterpreter) executeLoop(stmt *PLLoop) error {
	for {
		pl.exited = false
		if err := pl.executeStatements(stmt.Body); err != nil {
			return err
		}
		if pl.returned || pl.exited {
			pl.exited = false
			break
		}
	}
	return nil
}

// executeFor executes a FOR loop.
func (pl *PLInterpreter) executeFor(stmt *PLFor) error {
	if stmt.Query != nil {
		// FOR rec IN (SELECT ...) LOOP
		return pl.executeForQuery(stmt)
	}
	return pl.executeForNumeric(stmt)
}

// executeForNumeric executes a numeric FOR loop.
func (pl *PLInterpreter) executeForNumeric(stmt *PLFor) error {
	lower, err := pl.evalExpression(stmt.LowerBound)
	if err != nil {
		return fmt.Errorf("error evaluating FOR lower bound: %v", err)
	}
	upper, err := pl.evalExpression(stmt.UpperBound)
	if err != nil {
		return fmt.Errorf("error evaluating FOR upper bound: %v", err)
	}

	lowerInt, ok := pl.valueToInt64(lower)
	if !ok {
		return fmt.Errorf("FOR lower bound must be integer")
	}
	upperInt, ok := pl.valueToInt64(upper)
	if !ok {
		return fmt.Errorf("FOR upper bound must be integer")
	}

	// Declare loop variable
	pl.variables[strings.ToLower(stmt.Variable)] = catalog.NewInt32(int32(lowerInt))

	if stmt.Reverse {
		for i := upperInt; i >= lowerInt; i-- {
			pl.variables[strings.ToLower(stmt.Variable)] = catalog.NewInt32(int32(i))
			pl.exited = false
			if err := pl.executeStatements(stmt.Body); err != nil {
				return err
			}
			if pl.returned || pl.exited {
				pl.exited = false
				break
			}
		}
	} else {
		for i := lowerInt; i <= upperInt; i++ {
			pl.variables[strings.ToLower(stmt.Variable)] = catalog.NewInt32(int32(i))
			pl.exited = false
			if err := pl.executeStatements(stmt.Body); err != nil {
				return err
			}
			if pl.returned || pl.exited {
				pl.exited = false
				break
			}
		}
	}

	return nil
}

// executeForQuery executes a FOR query loop.
func (pl *PLInterpreter) executeForQuery(stmt *PLFor) error {
	// Execute the query
	result, err := pl.session.Execute(stmt.Query)
	if err != nil {
		return fmt.Errorf("error executing FOR query: %v", err)
	}

	// Iterate over rows
	for _, row := range result.Rows {
		// For simplicity, if single column, assign directly; otherwise use record
		if len(row) == 1 {
			pl.variables[strings.ToLower(stmt.Variable)] = row[0]
		} else {
			// Store as first value for now (record types would need more work)
			if len(row) > 0 {
				pl.variables[strings.ToLower(stmt.Variable)] = row[0]
			}
		}

		pl.exited = false
		if err := pl.executeStatements(stmt.Body); err != nil {
			return err
		}
		if pl.returned || pl.exited {
			pl.exited = false
			break
		}
	}

	return nil
}

// executeExit executes an EXIT statement.
func (pl *PLInterpreter) executeExit(stmt *PLExit) error {
	if stmt.Condition != nil {
		cond, err := pl.evalCondition(stmt.Condition)
		if err != nil {
			return err
		}
		if !cond {
			return nil
		}
	}
	pl.exited = true
	return nil
}

// executeContinue executes a CONTINUE statement.
func (pl *PLInterpreter) executeContinue(stmt *PLContinue) error {
	if stmt.Condition != nil {
		cond, err := pl.evalCondition(stmt.Condition)
		if err != nil {
			return err
		}
		if !cond {
			return nil
		}
	}
	// For CONTINUE, we just return nil to skip remaining statements
	// The loop will continue naturally
	return nil
}

// executeReturn executes a RETURN statement.
func (pl *PLInterpreter) executeReturn(stmt *PLReturn) error {
	if stmt.Value != nil {
		val, err := pl.evalExpression(stmt.Value)
		if err != nil {
			return fmt.Errorf("error evaluating RETURN value: %v", err)
		}
		pl.returnVal = &val
	}
	pl.returned = true
	return nil
}

// executeRaise executes a RAISE statement.
func (pl *PLInterpreter) executeRaise(stmt *PLRaise) error {
	msg := stmt.Message

	// Format message with arguments
	for i, arg := range stmt.Args {
		val, err := pl.evalExpression(arg)
		if err != nil {
			return err
		}
		placeholder := fmt.Sprintf("%%%d", i+1)
		msg = strings.Replace(msg, placeholder, val.String(), 1)
		// Also replace % for positional args
		msg = strings.Replace(msg, "%", val.String(), 1)
	}

	switch strings.ToUpper(stmt.Level) {
	case "NOTICE", "INFO", "DEBUG", "WARNING":
		// These are just informational, don't stop execution
		// In a real implementation, these would be logged or sent to client
		fmt.Printf("[%s] %s\n", stmt.Level, msg)
		return nil
	case "EXCEPTION":
		return fmt.Errorf("RAISE EXCEPTION: %s", msg)
	default:
		return fmt.Errorf("RAISE %s: %s", stmt.Level, msg)
	}
}

// executePerform executes a PERFORM statement (SELECT without returning).
func (pl *PLInterpreter) executePerform(stmt *PLPerform) error {
	_, err := pl.session.Execute(stmt.Query)
	return err
}

// executePLSQL executes an embedded SQL statement.
func (pl *PLInterpreter) executePLSQL(stmt *PLSQL) error {
	result, err := pl.session.Execute(stmt.Statement)
	if err != nil {
		return err
	}

	// Handle SELECT INTO
	if len(stmt.Into) > 0 && len(result.Rows) > 0 {
		row := result.Rows[0]
		for i, varName := range stmt.Into {
			if i < len(row) {
				if err := pl.SetVariable(varName, row[i]); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// evalExpression evaluates an expression in the PL context.
func (pl *PLInterpreter) evalExpression(expr Expression) (catalog.Value, error) {
	switch e := expr.(type) {
	case *LiteralExpr:
		return e.Value, nil

	case *ColumnRef:
		// Treat as variable reference
		if val, ok := pl.GetVariable(e.Name); ok {
			return val, nil
		}
		return catalog.Value{}, fmt.Errorf("unknown variable: %s", e.Name)

	case *BinaryExpr:
		return pl.evalBinaryExpr(e)

	case *UnaryExpr:
		return pl.evalUnaryExpr(e)

	case *FunctionExpr:
		return pl.evalFunctionExpr(e)

	default:
		// For complex expressions, use the session's executor
		// This is a simplified approach
		return catalog.Value{}, fmt.Errorf("unsupported expression type in PL: %T", expr)
	}
}

// evalBinaryExpr evaluates a binary expression.
func (pl *PLInterpreter) evalBinaryExpr(expr *BinaryExpr) (catalog.Value, error) {
	left, err := pl.evalExpression(expr.Left)
	if err != nil {
		return catalog.Value{}, err
	}
	right, err := pl.evalExpression(expr.Right)
	if err != nil {
		return catalog.Value{}, err
	}

	switch expr.Op {
	case TOKEN_PLUS:
		return pl.addValues(left, right)
	case TOKEN_MINUS:
		return pl.subtractValues(left, right)
	case TOKEN_STAR:
		return pl.multiplyValues(left, right)
	case TOKEN_SLASH:
		return pl.divideValues(left, right)
	case TOKEN_EQ:
		return catalog.NewBool(left.Compare(right) == 0), nil
	case TOKEN_NE:
		return catalog.NewBool(left.Compare(right) != 0), nil
	case TOKEN_LT:
		cmp := left.Compare(right)
		return catalog.NewBool(cmp < 0), nil
	case TOKEN_LE:
		cmp := left.Compare(right)
		return catalog.NewBool(cmp <= 0), nil
	case TOKEN_GT:
		cmp := left.Compare(right)
		return catalog.NewBool(cmp > 0), nil
	case TOKEN_GE:
		cmp := left.Compare(right)
		return catalog.NewBool(cmp >= 0), nil
	case TOKEN_AND:
		lb, _ := pl.valueToBool(left)
		rb, _ := pl.valueToBool(right)
		return catalog.NewBool(lb && rb), nil
	case TOKEN_OR:
		lb, _ := pl.valueToBool(left)
		rb, _ := pl.valueToBool(right)
		return catalog.NewBool(lb || rb), nil
	case TOKEN_CONCAT:
		return catalog.NewText(left.String() + right.String()), nil
	default:
		return catalog.Value{}, fmt.Errorf("unsupported binary operator: %v", expr.Op)
	}
}

// evalUnaryExpr evaluates a unary expression.
func (pl *PLInterpreter) evalUnaryExpr(expr *UnaryExpr) (catalog.Value, error) {
	val, err := pl.evalExpression(expr.Expr)
	if err != nil {
		return catalog.Value{}, err
	}

	switch expr.Op {
	case TOKEN_NOT:
		b, _ := pl.valueToBool(val)
		return catalog.NewBool(!b), nil
	case TOKEN_MINUS:
		if i, ok := pl.valueToInt64(val); ok {
			return catalog.NewInt32(int32(-i)), nil
		}
		return catalog.Value{}, fmt.Errorf("cannot negate non-numeric value")
	default:
		return catalog.Value{}, fmt.Errorf("unsupported unary operator: %v", expr.Op)
	}
}

// evalFunctionExpr evaluates a function call.
func (pl *PLInterpreter) evalFunctionExpr(expr *FunctionExpr) (catalog.Value, error) {
	// Evaluate arguments
	args := make([]catalog.Value, len(expr.Args))
	for i, arg := range expr.Args {
		val, err := pl.evalExpression(arg)
		if err != nil {
			return catalog.Value{}, err
		}
		args[i] = val
	}

	// Handle common functions
	fname := strings.ToUpper(expr.Name)
	switch fname {
	case "COALESCE":
		for _, arg := range args {
			if !arg.IsNull {
				return arg, nil
			}
		}
		return catalog.Null(catalog.TypeUnknown), nil

	case "NULLIF":
		if len(args) != 2 {
			return catalog.Value{}, fmt.Errorf("NULLIF requires 2 arguments")
		}
		if args[0].Compare(args[1]) == 0 {
			return catalog.Null(args[0].Type), nil
		}
		return args[0], nil

	case "ABS":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("ABS requires 1 argument")
		}
		if i, ok := pl.valueToInt64(args[0]); ok {
			if i < 0 {
				return catalog.NewInt32(int32(-i)), nil
			}
			return catalog.NewInt32(int32(i)), nil
		}
		return catalog.Value{}, fmt.Errorf("ABS requires numeric argument")

	case "UPPER":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("UPPER requires 1 argument")
		}
		return catalog.NewText(strings.ToUpper(args[0].String())), nil

	case "LOWER":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("LOWER requires 1 argument")
		}
		return catalog.NewText(strings.ToLower(args[0].String())), nil

	case "LENGTH":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("LENGTH requires 1 argument")
		}
		return catalog.NewInt32(int32(len(args[0].String()))), nil

	default:
		// Try to call a user-defined function
		return pl.callUserFunction(expr.Name, args)
	}
}

// callUserFunction calls a user-defined function.
func (pl *PLInterpreter) callUserFunction(name string, _ []catalog.Value) (catalog.Value, error) {
	// This would look up the function in the procedure catalog and execute it
	// For now, return an error
	return catalog.Value{}, fmt.Errorf("unknown function: %s", name)
}

// evalCondition evaluates a boolean condition.
func (pl *PLInterpreter) evalCondition(expr Expression) (bool, error) {
	val, err := pl.evalExpression(expr)
	if err != nil {
		return false, err
	}
	b, ok := pl.valueToBool(val)
	if !ok {
		// Treat non-null values as true
		return !val.IsNull, nil
	}
	return b, nil
}

// Helper method to convert Value to int64
func (pl *PLInterpreter) valueToInt64(v catalog.Value) (int64, bool) {
	if v.IsNull {
		return 0, false
	}
	switch v.Type {
	case catalog.TypeInt32:
		return int64(v.Int32), true
	case catalog.TypeInt64:
		return v.Int64, true
	default:
		return 0, false
	}
}

// Helper method to convert Value to bool
func (pl *PLInterpreter) valueToBool(v catalog.Value) (bool, bool) {
	if v.IsNull {
		return false, false
	}
	if v.Type == catalog.TypeBool {
		return v.Bool, true
	}
	return false, false
}

// Arithmetic helper methods
func (pl *PLInterpreter) addValues(left, right catalog.Value) (catalog.Value, error) {
	if li, ok := pl.valueToInt64(left); ok {
		if ri, ok := pl.valueToInt64(right); ok {
			return catalog.NewInt32(int32(li + ri)), nil
		}
	}
	// String concatenation fallback
	return catalog.NewText(left.String() + right.String()), nil
}

func (pl *PLInterpreter) subtractValues(left, right catalog.Value) (catalog.Value, error) {
	li, lok := pl.valueToInt64(left)
	ri, rok := pl.valueToInt64(right)
	if lok && rok {
		return catalog.NewInt32(int32(li - ri)), nil
	}
	return catalog.Value{}, fmt.Errorf("cannot subtract non-numeric values")
}

func (pl *PLInterpreter) multiplyValues(left, right catalog.Value) (catalog.Value, error) {
	li, lok := pl.valueToInt64(left)
	ri, rok := pl.valueToInt64(right)
	if lok && rok {
		return catalog.NewInt32(int32(li * ri)), nil
	}
	return catalog.Value{}, fmt.Errorf("cannot multiply non-numeric values")
}

func (pl *PLInterpreter) divideValues(left, right catalog.Value) (catalog.Value, error) {
	li, lok := pl.valueToInt64(left)
	ri, rok := pl.valueToInt64(right)
	if lok && rok {
		if ri == 0 {
			return catalog.Value{}, fmt.Errorf("division by zero")
		}
		return catalog.NewInt32(int32(li / ri)), nil
	}
	return catalog.Value{}, fmt.Errorf("cannot divide non-numeric values")
}
