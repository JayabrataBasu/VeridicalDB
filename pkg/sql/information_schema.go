package sql

import (
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

// Supported information_schema virtual tables. Queries against these names are
// answered from catalog metadata rather than stored rows, on both execution
// paths.
const (
	InfoSchemaTables                 = "information_schema.tables"
	InfoSchemaColumns                = "information_schema.columns"
	InfoSchemaTableConstraints       = "information_schema.table_constraints"
	InfoSchemaSchemata               = "information_schema.schemata"
	InfoSchemaViews                  = "information_schema.views"
	InfoSchemaKeyColumnUsage         = "information_schema.key_column_usage"
	InfoSchemaReferentialConstraints = "information_schema.referential_constraints"
)

const (
	infoCatalog = "veridicaldb"
	infoSchema  = "public"
)

// isInformationSchemaTable reports whether name refers to the information_schema
// namespace.
func isInformationSchemaTable(name string) bool {
	return strings.HasPrefix(strings.ToLower(name), "information_schema.")
}

func textCol(name string) catalog.Column { return catalog.Column{Name: name, Type: catalog.TypeText} }
func intCol(name string) catalog.Column  { return catalog.Column{Name: name, Type: catalog.TypeInt32} }

// informationSchemaTable returns a synthetic TableMeta describing one of the
// supported information_schema virtual tables, or (nil, false) for an unknown
// name.
func informationSchemaTable(tableName string) (*catalog.TableMeta, bool) {
	var cols []catalog.Column
	switch strings.ToLower(tableName) {
	case InfoSchemaTables:
		cols = []catalog.Column{
			textCol("table_catalog"), textCol("table_schema"),
			textCol("table_name"), textCol("table_type"),
		}
	case InfoSchemaColumns:
		cols = []catalog.Column{
			textCol("table_catalog"), textCol("table_schema"), textCol("table_name"),
			textCol("column_name"), intCol("ordinal_position"), textCol("column_default"),
			textCol("is_nullable"), textCol("data_type"),
		}
	case InfoSchemaTableConstraints:
		cols = []catalog.Column{
			textCol("constraint_catalog"), textCol("constraint_schema"), textCol("constraint_name"),
			textCol("table_schema"), textCol("table_name"), textCol("constraint_type"),
		}
	case InfoSchemaSchemata:
		cols = []catalog.Column{
			textCol("catalog_name"), textCol("schema_name"), textCol("schema_owner"),
		}
	case InfoSchemaViews:
		cols = []catalog.Column{
			textCol("table_catalog"), textCol("table_schema"), textCol("table_name"),
			textCol("view_definition"),
		}
	case InfoSchemaKeyColumnUsage:
		cols = []catalog.Column{
			textCol("constraint_catalog"), textCol("constraint_schema"), textCol("constraint_name"),
			textCol("table_catalog"), textCol("table_schema"), textCol("table_name"),
			textCol("column_name"), intCol("ordinal_position"),
		}
	case InfoSchemaReferentialConstraints:
		cols = []catalog.Column{
			textCol("constraint_catalog"), textCol("constraint_schema"), textCol("constraint_name"),
			textCol("unique_constraint_catalog"), textCol("unique_constraint_schema"),
			textCol("unique_constraint_name"), textCol("match_option"),
			textCol("update_rule"), textCol("delete_rule"),
		}
	default:
		return nil, false
	}
	meta := &catalog.TableMeta{Name: strings.ToLower(tableName), Columns: cols}
	meta.Schema = catalog.NewSchema(cols)
	return meta, true
}

// scanInformationSchema synthesises the rows of an information_schema virtual
// table from catalog metadata (cat) and the executor's view definitions
// (views), invoking fn for each row. The caller is responsible for holding any
// lock guarding views.
func scanInformationSchema(tableName string, cat *catalog.Catalog, views map[string]*ViewDef, fn func(rid storage.RID, row []catalog.Value) (bool, error)) error {
	tables := cat.ListTables()
	slot := 0
	emit := func(vals ...catalog.Value) (bool, error) {
		cont, err := fn(storage.RID{Table: tableName, Slot: uint16(slot)}, vals)
		slot++
		return cont, err
	}

	switch strings.ToLower(tableName) {
	case InfoSchemaTables:
		for _, tName := range tables {
			if cont, err := emit(
				catalog.NewText(infoCatalog), catalog.NewText(infoSchema),
				catalog.NewText(tName), catalog.NewText("BASE TABLE"),
			); err != nil || !cont {
				return err
			}
		}
		for vName := range views {
			if cont, err := emit(
				catalog.NewText(infoCatalog), catalog.NewText(infoSchema),
				catalog.NewText(vName), catalog.NewText("VIEW"),
			); err != nil || !cont {
				return err
			}
		}

	case InfoSchemaColumns:
		for _, tName := range tables {
			table, err := cat.GetTable(tName)
			if err != nil {
				continue
			}
			for i, col := range table.Schema.Columns {
				isNullable := "YES"
				if col.NotNull || col.PrimaryKey {
					isNullable = "NO"
				}
				defaultVal := catalog.Null(catalog.TypeText)
				if col.HasDefault && col.DefaultValue != nil {
					defaultVal = catalog.NewText(col.DefaultValue.String())
				}
				if cont, err := emit(
					catalog.NewText(infoCatalog), catalog.NewText(infoSchema), catalog.NewText(tName),
					catalog.NewText(col.Name), catalog.NewInt32(int32(i+1)), defaultVal,
					catalog.NewText(isNullable), catalog.NewText(col.Type.String()),
				); err != nil || !cont {
					return err
				}
			}
		}

	case InfoSchemaTableConstraints:
		for _, tName := range tables {
			table, err := cat.GetTable(tName)
			if err != nil {
				continue
			}
			if hasPrimaryKey(table.Schema) {
				if cont, err := emit(
					catalog.NewText(infoCatalog), catalog.NewText(infoSchema),
					catalog.NewText(tName+"_pkey"), catalog.NewText(infoSchema),
					catalog.NewText(tName), catalog.NewText("PRIMARY KEY"),
				); err != nil || !cont {
					return err
				}
			}
			for _, fk := range table.Schema.ForeignKeys {
				if cont, err := emit(
					catalog.NewText(infoCatalog), catalog.NewText(infoSchema),
					catalog.NewText(fkName(tName, fk)), catalog.NewText(infoSchema),
					catalog.NewText(tName), catalog.NewText("FOREIGN KEY"),
				); err != nil || !cont {
					return err
				}
			}
			for _, col := range table.Schema.Columns {
				if col.Unique && !col.PrimaryKey {
					if cont, err := emit(
						catalog.NewText(infoCatalog), catalog.NewText(infoSchema),
						catalog.NewText(tName+"_"+col.Name+"_key"), catalog.NewText(infoSchema),
						catalog.NewText(tName), catalog.NewText("UNIQUE"),
					); err != nil || !cont {
						return err
					}
				}
			}
		}

	case InfoSchemaSchemata:
		if cont, err := emit(
			catalog.NewText(infoCatalog), catalog.NewText(infoSchema), catalog.NewText(infoCatalog),
		); err != nil || !cont {
			return err
		}

	case InfoSchemaViews:
		for vName, vd := range views {
			def := catalog.Null(catalog.TypeText)
			if vd != nil && vd.Query != nil {
				def = catalog.NewText(exprToString(vd.Query.Where))
			}
			if cont, err := emit(
				catalog.NewText(infoCatalog), catalog.NewText(infoSchema),
				catalog.NewText(vName), def,
			); err != nil || !cont {
				return err
			}
		}

	case InfoSchemaKeyColumnUsage:
		for _, tName := range tables {
			table, err := cat.GetTable(tName)
			if err != nil {
				continue
			}
			pos := 1
			for _, col := range table.Schema.Columns {
				if !col.PrimaryKey {
					continue
				}
				if cont, err := emit(
					catalog.NewText(infoCatalog), catalog.NewText(infoSchema), catalog.NewText(tName+"_pkey"),
					catalog.NewText(infoCatalog), catalog.NewText(infoSchema), catalog.NewText(tName),
					catalog.NewText(col.Name), catalog.NewInt32(int32(pos)),
				); err != nil || !cont {
					return err
				}
				pos++
			}
			for _, fk := range table.Schema.ForeignKeys {
				for i, colName := range fk.Columns {
					if cont, err := emit(
						catalog.NewText(infoCatalog), catalog.NewText(infoSchema), catalog.NewText(fkName(tName, fk)),
						catalog.NewText(infoCatalog), catalog.NewText(infoSchema), catalog.NewText(tName),
						catalog.NewText(colName), catalog.NewInt32(int32(i+1)),
					); err != nil || !cont {
						return err
					}
				}
			}
		}

	case InfoSchemaReferentialConstraints:
		for _, tName := range tables {
			table, err := cat.GetTable(tName)
			if err != nil {
				continue
			}
			for _, fk := range table.Schema.ForeignKeys {
				if cont, err := emit(
					catalog.NewText(infoCatalog), catalog.NewText(infoSchema), catalog.NewText(fkName(tName, fk)),
					catalog.NewText(infoCatalog), catalog.NewText(infoSchema), catalog.NewText(fk.RefTable+"_pkey"),
					catalog.NewText("NONE"), catalog.NewText("NO ACTION"), catalog.NewText("NO ACTION"),
				); err != nil || !cont {
					return err
				}
			}
		}
	}
	return nil
}

func hasPrimaryKey(s *catalog.Schema) bool {
	for _, col := range s.Columns {
		if col.PrimaryKey {
			return true
		}
	}
	return false
}

func fkName(table string, fk catalog.ForeignKey) string {
	if fk.Name != "" {
		return fk.Name
	}
	return table + "_" + strings.Join(fk.Columns, "_") + "_fkey"
}
