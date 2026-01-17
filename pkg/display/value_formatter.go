package display

import (
	"strconv"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

// ValueFormatter formats individual database values for display.
type ValueFormatter struct {
	NullString      string // How to display NULL values (default: "NULL")
	FloatFormat     byte   // 'g', 'f', or 'e' (default: 'g')
	TimestampLayout string // Time format layout (default: "2006-01-02 15:04:05")
	JSONPretty      bool   // Pretty-print JSON
}

// NewValueFormatter creates a new ValueFormatter with default settings.
func NewValueFormatter() *ValueFormatter {
	return &ValueFormatter{
		NullString:      "NULL",
		FloatFormat:     'g',
		TimestampLayout: "2006-01-02 15:04:05",
		JSONPretty:      false,
	}
}

// Format converts a database value to its string representation for display.
func (vf *ValueFormatter) Format(v catalog.Value) string {
	if v.IsNull {
		return vf.NullString
	}

	switch v.Type {
	case catalog.TypeInt32:
		return strconv.FormatInt(int64(v.Int32), 10)
	case catalog.TypeInt64:
		return strconv.FormatInt(v.Int64, 10)
	case catalog.TypeFloat64:
		return strconv.FormatFloat(v.Float64, byte(vf.FloatFormat), -1, 64)
	case catalog.TypeText:
		return v.Text
	case catalog.TypeBool:
		if v.Bool {
			return "true"
		}
		return "false"
	case catalog.TypeTimestamp:
		return v.Timestamp.Format(vf.TimestampLayout)
	case catalog.TypeJSON:
		return v.JSON
	default:
		return "?"
	}
}

// FormatForCSV returns a CSV-safe representation of the value.
// Quotes values containing commas or quotes, and escapes internal quotes.
func (vf *ValueFormatter) FormatForCSV(v catalog.Value) string {
	str := vf.Format(v)
	if strings.ContainsAny(str, `,"`) {
		// Escape quotes by doubling them
		escaped := strings.ReplaceAll(str, `"`, `""`)
		return `"` + escaped + `"`
	}
	return str
}

// FormatForJSON returns a JSON-compatible representation of the value.
// Null values return nil, numbers return numeric types, strings return quoted strings.
func (vf *ValueFormatter) FormatForJSON(v catalog.Value) interface{} {
	if v.IsNull {
		return nil
	}

	switch v.Type {
	case catalog.TypeInt32:
		return v.Int32
	case catalog.TypeInt64:
		return v.Int64
	case catalog.TypeFloat64:
		return v.Float64
	case catalog.TypeBool:
		return v.Bool
	case catalog.TypeTimestamp:
		// Return Unix timestamp for JSON compatibility
		return v.Timestamp.Unix()
	case catalog.TypeText:
		return v.Text
	case catalog.TypeJSON:
		// Return the JSON string as-is (caller should parse if needed)
		return v.JSON
	default:
		return nil
	}
}

// EstimatedWidth returns an estimated display width for a value.
// Used for column width calculation.
func (vf *ValueFormatter) EstimatedWidth(v catalog.Value) int {
	return len(vf.Format(v))
}
