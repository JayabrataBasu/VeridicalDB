package sql

import (
	"fmt"
	"strings"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/token"
)

// valuesEqual compares two catalog values for equality.
func valuesEqual(a, b catalog.Value) bool {
	if a.IsNull || b.IsNull {
		return false // NULL != NULL for uniqueness
	}
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case catalog.TypeInt32:
		return a.Int32 == b.Int32
	case catalog.TypeInt64:
		return a.Int64 == b.Int64
	case catalog.TypeText:
		return a.Text == b.Text
	case catalog.TypeBool:
		return a.Bool == b.Bool
	case catalog.TypeTimestamp:
		return a.Timestamp.Equal(b.Timestamp)
	default:
		return false
	}
}

// compareValues compares two values with the given operator.
func compareValues(left, right catalog.Value, op token.TokenType) (bool, error) {
	// Handle NULL comparisons
	if left.IsNull || right.IsNull {
		// NULL compared to anything is always false (use IS NULL for null checks)
		return false, nil
	}

	// Type coercion for comparison
	if left.Type != right.Type {
		// Try to coerce right to left's type
		var err error
		right, err = coerceValue(right, left.Type)
		if err != nil {
			return false, err
		}
	}

	var cmp int // -1 = less, 0 = equal, 1 = greater

	switch left.Type {
	case catalog.TypeInt32:
		if left.Int32 < right.Int32 {
			cmp = -1
		} else if left.Int32 > right.Int32 {
			cmp = 1
		}
	case catalog.TypeInt64:
		if left.Int64 < right.Int64 {
			cmp = -1
		} else if left.Int64 > right.Int64 {
			cmp = 1
		}
	case catalog.TypeText:
		cmp = strings.Compare(left.Text, right.Text)
	case catalog.TypeBool:
		if left.Bool == right.Bool {
			cmp = 0
		} else if !left.Bool {
			cmp = -1
		} else {
			cmp = 1
		}
	case catalog.TypeTimestamp:
		if left.Timestamp.Before(right.Timestamp) {
			cmp = -1
		} else if left.Timestamp.After(right.Timestamp) {
			cmp = 1
		}
	default:
		return false, fmt.Errorf("cannot compare type %v", left.Type)
	}

	switch op {
	case token.TOKEN_EQ:
		return cmp == 0, nil
	case token.TOKEN_NE:
		return cmp != 0, nil
	case token.TOKEN_LT:
		return cmp < 0, nil
	case token.TOKEN_LE:
		return cmp <= 0, nil
	case token.TOKEN_GT:
		return cmp > 0, nil
	case token.TOKEN_GE:
		return cmp >= 0, nil
	default:
		return false, fmt.Errorf("unknown comparison operator: %v", op)
	}
}

// coerceValue attempts to coerce a value to the target type.
func coerceValue(val catalog.Value, targetType catalog.DataType) (catalog.Value, error) {
	if val.IsNull {
		return catalog.Null(targetType), nil
	}

	if val.Type == targetType {
		return val, nil
	}

	// Int32 to Int64
	if val.Type == catalog.TypeInt32 && targetType == catalog.TypeInt64 {
		return catalog.NewInt64(int64(val.Int32)), nil
	}

	// Int64 to Int32 (if in range)
	if val.Type == catalog.TypeInt64 && targetType == catalog.TypeInt32 {
		if val.Int64 >= -2147483648 && val.Int64 <= 2147483647 {
			return catalog.NewInt32(int32(val.Int64)), nil
		}
		return catalog.Value{}, fmt.Errorf("value %d out of range for INT", val.Int64)
	}

	// String to DATE conversion
	if val.Type == catalog.TypeText && targetType == catalog.TypeDate {
		// Try to parse as YYYY-MM-DD
		parsedDate, err := time.Parse("2006-01-02", val.Text)
		if err != nil {
			return catalog.Value{}, fmt.Errorf("invalid date format: expected YYYY-MM-DD, got %q", val.Text)
		}
		return catalog.NewDate(parsedDate), nil
	}

	// String to TIMESTAMP conversion (if not already handled)
	if val.Type == catalog.TypeText && targetType == catalog.TypeTimestamp {
		// Try to parse as RFC3339
		parsedTime, err := time.Parse(time.RFC3339, val.Text)
		if err != nil {
			// Try other common formats
			if parsedTime, err = time.Parse("2006-01-02 15:04:05", val.Text); err != nil {
				return catalog.Value{}, fmt.Errorf("invalid timestamp format: %w", err)
			}
		}
		return catalog.NewTimestamp(parsedTime), nil
	}

	return catalog.Value{}, fmt.Errorf("cannot coerce %v to %v", val.Type, targetType)
}

// compareValuesForSort compares two values for sorting.
// Returns -1 if left < right, 0 if equal, 1 if left > right.
// NULLs are sorted last (greater than any non-NULL value).
func compareValuesForSort(left, right catalog.Value) int {
	// Handle NULLs - NULLs sort last
	if left.IsNull && right.IsNull {
		return 0
	}
	if left.IsNull {
		return 1 // NULL is greater (sorted last)
	}
	if right.IsNull {
		return -1
	}

	// Compare based on type
	switch left.Type {
	case catalog.TypeInt32:
		r := right
		if right.Type == catalog.TypeInt64 {
			// Compare as int64
			if int64(left.Int32) < right.Int64 {
				return -1
			} else if int64(left.Int32) > right.Int64 {
				return 1
			}
			return 0
		}
		if left.Int32 < r.Int32 {
			return -1
		} else if left.Int32 > r.Int32 {
			return 1
		}
		return 0

	case catalog.TypeInt64:
		r := right
		if right.Type == catalog.TypeInt32 {
			// Compare as int64
			if left.Int64 < int64(right.Int32) {
				return -1
			} else if left.Int64 > int64(right.Int32) {
				return 1
			}
			return 0
		}
		if left.Int64 < r.Int64 {
			return -1
		} else if left.Int64 > r.Int64 {
			return 1
		}
		return 0

	case catalog.TypeText:
		return strings.Compare(left.Text, right.Text)

	case catalog.TypeBool:
		if left.Bool == right.Bool {
			return 0
		}
		if !left.Bool {
			return -1
		}
		return 1

	case catalog.TypeTimestamp:
		if left.Timestamp.Before(right.Timestamp) {
			return -1
		} else if left.Timestamp.After(right.Timestamp) {
			return 1
		}
		return 0
	}

	return 0
}

// matchLikePattern matches a string against a SQL LIKE pattern.
// % matches zero or more characters, _ matches exactly one character.
// If caseInsensitive is true, the match is case-insensitive (ILIKE).
func matchLikePattern(s, pattern string, caseInsensitive bool) bool {
	if caseInsensitive {
		s = strings.ToLower(s)
		pattern = strings.ToLower(pattern)
	}

	// Convert SQL LIKE pattern to a simple state machine match
	// % = match any sequence of characters (including empty)
	// _ = match exactly one character
	return matchLikeHelper(s, pattern)
}

// matchLikeHelper performs the actual LIKE pattern matching.
func matchLikeHelper(s, pattern string) bool {
	si, pi := 0, 0
	starIdx, matchIdx := -1, 0

	for si < len(s) {
		if pi < len(pattern) && (pattern[pi] == '_' || pattern[pi] == s[si]) {
			// Single character match or _ wildcard
			si++
			pi++
		} else if pi < len(pattern) && pattern[pi] == '%' {
			// % wildcard - remember position and try to match 0 characters first
			starIdx = pi
			matchIdx = si
			pi++
		} else if starIdx != -1 {
			// No match, but we have a previous % - backtrack
			pi = starIdx + 1
			matchIdx++
			si = matchIdx
		} else {
			// No match and no % to backtrack to
			return false
		}
	}

	// Check remaining pattern (should only be % characters)
	for pi < len(pattern) && pattern[pi] == '%' {
		pi++
	}

	return pi == len(pattern)
}

// evalArithmetic evaluates an arithmetic operation on two values.
func evalArithmetic(left, right catalog.Value, op token.TokenType) (catalog.Value, error) {
	if left.IsNull || right.IsNull {
		return catalog.Null(catalog.TypeInt64), nil // NULL arithmetic returns NULL
	}

	// Coerce to the larger numeric type
	var leftInt, rightInt int64
	var isInt bool

	switch left.Type {
	case catalog.TypeInt32:
		leftInt = int64(left.Int32)
		isInt = true
	case catalog.TypeInt64:
		leftInt = left.Int64
		isInt = true
	default:
		return catalog.Value{}, fmt.Errorf("arithmetic not supported for type: %v", left.Type)
	}

	switch right.Type {
	case catalog.TypeInt32:
		rightInt = int64(right.Int32)
	case catalog.TypeInt64:
		rightInt = right.Int64
	default:
		return catalog.Value{}, fmt.Errorf("arithmetic not supported for type: %v", right.Type)
	}

	if !isInt {
		return catalog.Value{}, fmt.Errorf("arithmetic requires numeric operands")
	}

	var result int64
	switch op {
	case token.TOKEN_PLUS:
		result = leftInt + rightInt
	case token.TOKEN_MINUS:
		result = leftInt - rightInt
	case token.TOKEN_STAR:
		result = leftInt * rightInt
	case token.TOKEN_SLASH:
		if rightInt == 0 {
			return catalog.Value{}, fmt.Errorf("division by zero")
		}
		result = leftInt / rightInt
	default:
		return catalog.Value{}, fmt.Errorf("unknown arithmetic operator: %v", op)
	}

	return catalog.NewInt64(result), nil
}
