package sql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/token"
)

// evalFunction evaluates a function call expression.
func evalFunction(name string, args []catalog.Value) (catalog.Value, error) {
	switch strings.ToUpper(name) {
	case "COALESCE":
		// COALESCE returns the first non-NULL argument
		for _, arg := range args {
			if !arg.IsNull {
				return arg, nil
			}
		}
		return catalog.Null(catalog.TypeUnknown), nil

	case "NULLIF":
		// NULLIF(a, b) returns NULL if a = b, otherwise returns a
		if len(args) != 2 {
			return catalog.Value{}, fmt.Errorf("NULLIF requires exactly 2 arguments")
		}
		if args[0].IsNull || args[1].IsNull {
			return args[0], nil
		}
		// Compare the values
		eq, _ := compareValues(args[0], args[1], token.TOKEN_EQ)
		if eq {
			return catalog.Null(args[0].Type), nil
		}
		return args[0], nil

	case "UPPER":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("UPPER requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("UPPER requires text argument")
		}
		return catalog.NewText(strings.ToUpper(args[0].Text)), nil

	case "LOWER":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("LOWER requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("LOWER requires text argument")
		}
		return catalog.NewText(strings.ToLower(args[0].Text)), nil

	case "LENGTH", "LEN":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("LENGTH requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt64), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("LENGTH requires text argument")
		}
		return catalog.NewInt64(int64(len(args[0].Text))), nil

	case "CONCAT":
		var result strings.Builder
		for _, arg := range args {
			if arg.IsNull {
				continue // CONCAT skips NULLs (like MySQL, unlike standard SQL)
			}
			if arg.Type != catalog.TypeText {
				return catalog.Value{}, fmt.Errorf("CONCAT requires text arguments")
			}
			result.WriteString(arg.Text)
		}
		return catalog.NewText(result.String()), nil

	case "SUBSTR", "SUBSTRING":
		// SUBSTR(str, start) or SUBSTR(str, start, length)
		if len(args) < 2 || len(args) > 3 {
			return catalog.Value{}, fmt.Errorf("SUBSTR requires 2 or 3 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("SUBSTR first argument must be text")
		}

		str := args[0].Text
		var start int64
		switch args[1].Type {
		case catalog.TypeInt32:
			start = int64(args[1].Int32)
		case catalog.TypeInt64:
			start = args[1].Int64
		default:
			return catalog.Value{}, fmt.Errorf("SUBSTR start must be integer")
		}

		// SQL uses 1-based indexing
		start-- // convert to 0-based
		if start < 0 {
			start = 0
		}
		if start >= int64(len(str)) {
			return catalog.NewText(""), nil
		}

		if len(args) == 3 {
			var length int64
			switch args[2].Type {
			case catalog.TypeInt32:
				length = int64(args[2].Int32)
			case catalog.TypeInt64:
				length = args[2].Int64
			default:
				return catalog.Value{}, fmt.Errorf("SUBSTR length must be integer")
			}
			if length < 0 {
				length = 0
			}
			end := start + length
			if end > int64(len(str)) {
				end = int64(len(str))
			}
			return catalog.NewText(str[start:end]), nil
		}

		return catalog.NewText(str[start:]), nil

	case "NOW", "CURRENT_TIMESTAMP":
		// Return current timestamp
		now := time.Now()
		return catalog.NewTimestamp(now), nil

	case "CURRENT_DATE":
		// Return current date (timestamp with time set to midnight)
		now := time.Now()
		date := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		return catalog.NewTimestamp(date), nil

	case "YEAR":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("YEAR requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("YEAR requires timestamp argument")
		}
		return catalog.NewInt32(int32(args[0].Timestamp.Year())), nil

	case "MONTH":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("MONTH requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("MONTH requires timestamp argument")
		}
		return catalog.NewInt32(int32(args[0].Timestamp.Month())), nil

	case "DAY":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("DAY requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("DAY requires timestamp argument")
		}
		return catalog.NewInt32(int32(args[0].Timestamp.Day())), nil

	case "HOUR":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("HOUR requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("HOUR requires timestamp argument")
		}
		return catalog.NewInt32(int32(args[0].Timestamp.Hour())), nil

	case "MINUTE":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("MINUTE requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("MINUTE requires timestamp argument")
		}
		return catalog.NewInt32(int32(args[0].Timestamp.Minute())), nil

	case "SECOND":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("SECOND requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("SECOND requires timestamp argument")
		}
		return catalog.NewInt32(int32(args[0].Timestamp.Second())), nil

	case "DATE_ADD":
		// DATE_ADD(date, interval_value, interval_unit)
		if len(args) != 3 {
			return catalog.Value{}, fmt.Errorf("DATE_ADD requires 3 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeTimestamp), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("DATE_ADD first argument must be timestamp")
		}
		if args[1].Type != catalog.TypeText || args[2].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("DATE_ADD interval arguments must be text")
		}
		interval, err := strconv.ParseInt(args[1].Text, 10, 64)
		if err != nil {
			return catalog.Value{}, fmt.Errorf("DATE_ADD interval value must be integer: %w", err)
		}
		ts := args[0].Timestamp
		switch strings.ToUpper(args[2].Text) {
		case "YEAR":
			ts = ts.AddDate(int(interval), 0, 0)
		case "MONTH":
			ts = ts.AddDate(0, int(interval), 0)
		case "DAY":
			ts = ts.AddDate(0, 0, int(interval))
		case "HOUR":
			ts = ts.Add(time.Duration(interval) * time.Hour)
		case "MINUTE":
			ts = ts.Add(time.Duration(interval) * time.Minute)
		case "SECOND":
			ts = ts.Add(time.Duration(interval) * time.Second)
		default:
			return catalog.Value{}, fmt.Errorf("DATE_ADD unknown interval unit: %s", args[2].Text)
		}
		return catalog.NewTimestamp(ts), nil

	case "DATE_SUB":
		// DATE_SUB(date, interval_value, interval_unit) - same as DATE_ADD but subtract
		if len(args) != 3 {
			return catalog.Value{}, fmt.Errorf("DATE_SUB requires 3 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeTimestamp), nil
		}
		if args[0].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("DATE_SUB first argument must be timestamp")
		}
		if args[1].Type != catalog.TypeText || args[2].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("DATE_SUB interval arguments must be text")
		}
		interval, err := strconv.ParseInt(args[1].Text, 10, 64)
		if err != nil {
			return catalog.Value{}, fmt.Errorf("DATE_SUB interval value must be integer: %w", err)
		}
		ts := args[0].Timestamp
		switch strings.ToUpper(args[2].Text) {
		case "YEAR":
			ts = ts.AddDate(-int(interval), 0, 0)
		case "MONTH":
			ts = ts.AddDate(0, -int(interval), 0)
		case "DAY":
			ts = ts.AddDate(0, 0, -int(interval))
		case "HOUR":
			ts = ts.Add(-time.Duration(interval) * time.Hour)
		case "MINUTE":
			ts = ts.Add(-time.Duration(interval) * time.Minute)
		case "SECOND":
			ts = ts.Add(-time.Duration(interval) * time.Second)
		default:
			return catalog.Value{}, fmt.Errorf("DATE_SUB unknown interval unit: %s", args[2].Text)
		}
		return catalog.NewTimestamp(ts), nil

	case "EXTRACT":
		// EXTRACT(part, date) - part is a string like "YEAR", "MONTH", etc.
		if len(args) != 2 {
			return catalog.Value{}, fmt.Errorf("EXTRACT requires 2 arguments")
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("EXTRACT first argument must be text (part name)")
		}
		if args[1].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[1].Type != catalog.TypeTimestamp {
			return catalog.Value{}, fmt.Errorf("EXTRACT second argument must be timestamp")
		}
		ts := args[1].Timestamp
		switch strings.ToUpper(args[0].Text) {
		case "YEAR":
			return catalog.NewInt32(int32(ts.Year())), nil
		case "MONTH":
			return catalog.NewInt32(int32(ts.Month())), nil
		case "DAY":
			return catalog.NewInt32(int32(ts.Day())), nil
		case "HOUR":
			return catalog.NewInt32(int32(ts.Hour())), nil
		case "MINUTE":
			return catalog.NewInt32(int32(ts.Minute())), nil
		case "SECOND":
			return catalog.NewInt32(int32(ts.Second())), nil
		default:
			return catalog.Value{}, fmt.Errorf("EXTRACT unknown part: %s", args[0].Text)
		}

	// Math functions
	case "ABS":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("ABS requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(args[0].Type), nil
		}
		switch args[0].Type {
		case catalog.TypeInt32:
			v := args[0].Int32
			if v < 0 {
				v = -v
			}
			return catalog.NewInt32(v), nil
		case catalog.TypeInt64:
			v := args[0].Int64
			if v < 0 {
				v = -v
			}
			return catalog.NewInt64(v), nil
		default:
			return catalog.Value{}, fmt.Errorf("ABS requires numeric argument")
		}

	case "ROUND":
		if len(args) < 1 || len(args) > 2 {
			return catalog.Value{}, fmt.Errorf("ROUND requires 1 or 2 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(args[0].Type), nil
		}
		// For integers, ROUND just returns the value
		switch args[0].Type {
		case catalog.TypeInt32:
			return args[0], nil
		case catalog.TypeInt64:
			return args[0], nil
		default:
			return catalog.Value{}, fmt.Errorf("ROUND requires numeric argument")
		}

	case "FLOOR":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("FLOOR requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(args[0].Type), nil
		}
		// For integers, FLOOR just returns the value
		switch args[0].Type {
		case catalog.TypeInt32:
			return args[0], nil
		case catalog.TypeInt64:
			return args[0], nil
		default:
			return catalog.Value{}, fmt.Errorf("FLOOR requires numeric argument")
		}

	case "CEIL", "CEILING":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("CEIL requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(args[0].Type), nil
		}
		// For integers, CEIL just returns the value
		switch args[0].Type {
		case catalog.TypeInt32:
			return args[0], nil
		case catalog.TypeInt64:
			return args[0], nil
		default:
			return catalog.Value{}, fmt.Errorf("CEIL requires numeric argument")
		}

	case "MOD":
		if len(args) != 2 {
			return catalog.Value{}, fmt.Errorf("MOD requires exactly 2 arguments")
		}
		if args[0].IsNull || args[1].IsNull {
			return catalog.Null(catalog.TypeInt64), nil
		}
		var a, b int64
		switch args[0].Type {
		case catalog.TypeInt32:
			a = int64(args[0].Int32)
		case catalog.TypeInt64:
			a = args[0].Int64
		default:
			return catalog.Value{}, fmt.Errorf("MOD requires numeric arguments")
		}
		switch args[1].Type {
		case catalog.TypeInt32:
			b = int64(args[1].Int32)
		case catalog.TypeInt64:
			b = args[1].Int64
		default:
			return catalog.Value{}, fmt.Errorf("MOD requires numeric arguments")
		}
		if b == 0 {
			return catalog.Value{}, fmt.Errorf("MOD division by zero")
		}
		return catalog.NewInt64(a % b), nil

	case "POWER", "POW":
		if len(args) != 2 {
			return catalog.Value{}, fmt.Errorf("POWER requires exactly 2 arguments")
		}
		if args[0].IsNull || args[1].IsNull {
			return catalog.Null(catalog.TypeInt64), nil
		}
		var base, exp int64
		switch args[0].Type {
		case catalog.TypeInt32:
			base = int64(args[0].Int32)
		case catalog.TypeInt64:
			base = args[0].Int64
		default:
			return catalog.Value{}, fmt.Errorf("POWER requires numeric arguments")
		}
		switch args[1].Type {
		case catalog.TypeInt32:
			exp = int64(args[1].Int32)
		case catalog.TypeInt64:
			exp = args[1].Int64
		default:
			return catalog.Value{}, fmt.Errorf("POWER requires numeric arguments")
		}
		result := int64(1)
		for i := int64(0); i < exp; i++ {
			result *= base
		}
		return catalog.NewInt64(result), nil

	case "SQRT":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("SQRT requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeInt64), nil
		}
		var val int64
		switch args[0].Type {
		case catalog.TypeInt32:
			val = int64(args[0].Int32)
		case catalog.TypeInt64:
			val = args[0].Int64
		default:
			return catalog.Value{}, fmt.Errorf("SQRT requires numeric argument")
		}
		if val < 0 {
			return catalog.Value{}, fmt.Errorf("SQRT of negative number")
		}
		// Integer square root
		result := int64(0)
		for result*result <= val {
			result++
		}
		return catalog.NewInt64(result - 1), nil

	// String functions
	case "TRIM":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("TRIM requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("TRIM requires text argument")
		}
		return catalog.NewText(strings.TrimSpace(args[0].Text)), nil

	case "LTRIM":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("LTRIM requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("LTRIM requires text argument")
		}
		return catalog.NewText(strings.TrimLeft(args[0].Text, " \t\n\r")), nil

	case "RTRIM":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("RTRIM requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("RTRIM requires text argument")
		}
		return catalog.NewText(strings.TrimRight(args[0].Text, " \t\n\r")), nil

	case "REPLACE":
		if len(args) != 3 {
			return catalog.Value{}, fmt.Errorf("REPLACE requires exactly 3 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText || args[1].Type != catalog.TypeText || args[2].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("REPLACE requires text arguments")
		}
		return catalog.NewText(strings.ReplaceAll(args[0].Text, args[1].Text, args[2].Text)), nil

	case "POSITION":
		// POSITION(substr, str) returns 1-based position, 0 if not found
		if len(args) != 2 {
			return catalog.Value{}, fmt.Errorf("POSITION requires exactly 2 arguments")
		}
		if args[0].IsNull || args[1].IsNull {
			return catalog.Null(catalog.TypeInt32), nil
		}
		if args[0].Type != catalog.TypeText || args[1].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("POSITION requires text arguments")
		}
		idx := strings.Index(args[1].Text, args[0].Text)
		if idx == -1 {
			return catalog.NewInt32(0), nil
		}
		return catalog.NewInt32(int32(idx + 1)), nil

	case "REVERSE":
		if len(args) != 1 {
			return catalog.Value{}, fmt.Errorf("REVERSE requires exactly 1 argument")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("REVERSE requires text argument")
		}
		runes := []rune(args[0].Text)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return catalog.NewText(string(runes)), nil

	case "REPEAT":
		if len(args) != 2 {
			return catalog.Value{}, fmt.Errorf("REPEAT requires exactly 2 arguments")
		}
		if args[0].IsNull || args[1].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("REPEAT first argument must be text")
		}
		var count int64
		switch args[1].Type {
		case catalog.TypeInt32:
			count = int64(args[1].Int32)
		case catalog.TypeInt64:
			count = args[1].Int64
		default:
			return catalog.Value{}, fmt.Errorf("REPEAT second argument must be integer")
		}
		if count < 0 {
			count = 0
		}
		return catalog.NewText(strings.Repeat(args[0].Text, int(count))), nil

	case "LPAD":
		if len(args) != 3 {
			return catalog.Value{}, fmt.Errorf("LPAD requires exactly 3 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText || args[2].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("LPAD requires text arguments")
		}
		var length int64
		switch args[1].Type {
		case catalog.TypeInt32:
			length = int64(args[1].Int32)
		case catalog.TypeInt64:
			length = args[1].Int64
		default:
			return catalog.Value{}, fmt.Errorf("LPAD length must be integer")
		}
		str := args[0].Text
		pad := args[2].Text
		if len(pad) == 0 {
			return catalog.NewText(str), nil
		}
		for int64(len(str)) < length {
			str = pad + str
		}
		if int64(len(str)) > length {
			str = str[int64(len(str))-length:]
		}
		return catalog.NewText(str), nil

	case "RPAD":
		if len(args) != 3 {
			return catalog.Value{}, fmt.Errorf("RPAD requires exactly 3 arguments")
		}
		if args[0].IsNull {
			return catalog.Null(catalog.TypeText), nil
		}
		if args[0].Type != catalog.TypeText || args[2].Type != catalog.TypeText {
			return catalog.Value{}, fmt.Errorf("RPAD requires text arguments")
		}
		var length int64
		switch args[1].Type {
		case catalog.TypeInt32:
			length = int64(args[1].Int32)
		case catalog.TypeInt64:
			length = args[1].Int64
		default:
			return catalog.Value{}, fmt.Errorf("RPAD length must be integer")
		}
		str := args[0].Text
		pad := args[2].Text
		if len(pad) == 0 {
			return catalog.NewText(str), nil
		}
		for int64(len(str)) < length {
			str = str + pad
		}
		if int64(len(str)) > length {
			str = str[:length]
		}
		return catalog.NewText(str), nil

	default:
		return catalog.Value{}, fmt.Errorf("unknown function: %s", name)
	}
}
