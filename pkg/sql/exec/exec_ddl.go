package exec

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

// generateCreateTableDDL generates a CREATE TABLE statement from table metadata.
func generateCreateTableDDL(meta *catalog.TableMeta) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(meta.Name)
	sb.WriteString(" (\n")

	for i, col := range meta.Schema.Columns {
		sb.WriteString("  ")
		sb.WriteString(col.Name)
		sb.WriteString(" ")
		sb.WriteString(col.Type.String())
		if col.NotNull {
			sb.WriteString(" NOT NULL")
		}
		if col.HasDefault && col.DefaultValue != nil {
			sb.WriteString(" DEFAULT ")
			if col.DefaultValue.IsNull {
				sb.WriteString("NULL")
			} else {
				switch col.DefaultValue.Type {
				case catalog.TypeText:
					sb.WriteString("'")
					sb.WriteString(col.DefaultValue.Text)
					sb.WriteString("'")
				case catalog.TypeInt32:
					sb.WriteString(fmt.Sprintf("%d", col.DefaultValue.Int32))
				case catalog.TypeInt64:
					sb.WriteString(fmt.Sprintf("%d", col.DefaultValue.Int64))
				case catalog.TypeBool:
					if col.DefaultValue.Bool {
						sb.WriteString("TRUE")
					} else {
						sb.WriteString("FALSE")
					}
				default:
					sb.WriteString(fmt.Sprintf("%v", col.DefaultValue))
				}
			}
		}
		if col.AutoIncrement {
			sb.WriteString(" AUTO_INCREMENT")
		}
		if col.PrimaryKey {
			sb.WriteString(" PRIMARY KEY")
		}
		if i < len(meta.Schema.Columns)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}

	sb.WriteString(")")
	if meta.StorageType == "COLUMN" || meta.StorageType == "column" {
		sb.WriteString(" USING COLUMN")
	}
	sb.WriteString(";")
	return sb.String()
}
