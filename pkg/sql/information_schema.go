package sql

import (
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
)

const (
	InfoSchemaTables           = "information_schema.tables"
	InfoSchemaColumns          = "information_schema.columns"
	InfoSchemaTableConstraints = "information_schema.table_constraints"
)

func (e *Executor) getInformationSchemaTable(tableName string) (*catalog.TableMeta, bool) {
	switch strings.ToLower(tableName) {
	case InfoSchemaTables:
		meta := &catalog.TableMeta{
			Name: InfoSchemaTables,
			Columns: []catalog.Column{
				{Name: "table_catalog", Type: catalog.TypeText},
				{Name: "table_schema", Type: catalog.TypeText},
				{Name: "table_name", Type: catalog.TypeText},
				{Name: "table_type", Type: catalog.TypeText},
			},
		}
		meta.Schema = catalog.NewSchema(meta.Columns)
		return meta, true
	case InfoSchemaColumns:
		meta := &catalog.TableMeta{
			Name: InfoSchemaColumns,
			Columns: []catalog.Column{
				{Name: "table_catalog", Type: catalog.TypeText},
				{Name: "table_schema", Type: catalog.TypeText},
				{Name: "table_name", Type: catalog.TypeText},
				{Name: "column_name", Type: catalog.TypeText},
				{Name: "ordinal_position", Type: catalog.TypeInt32},
				{Name: "column_default", Type: catalog.TypeText},
				{Name: "is_nullable", Type: catalog.TypeText},
				{Name: "data_type", Type: catalog.TypeText},
			},
		}
		meta.Schema = catalog.NewSchema(meta.Columns)
		return meta, true
	case InfoSchemaTableConstraints:
		meta := &catalog.TableMeta{
			Name: InfoSchemaTableConstraints,
			Columns: []catalog.Column{
				{Name: "constraint_catalog", Type: catalog.TypeText},
				{Name: "constraint_schema", Type: catalog.TypeText},
				{Name: "constraint_name", Type: catalog.TypeText},
				{Name: "table_schema", Type: catalog.TypeText},
				{Name: "table_name", Type: catalog.TypeText},
				{Name: "constraint_type", Type: catalog.TypeText},
			},
		}
		meta.Schema = catalog.NewSchema(meta.Columns)
		return meta, true
	}
	return nil, false
}

func (e *Executor) scanInformationSchema(tableName string, fn func(rid storage.RID, row []catalog.Value) (bool, error)) error {
	tables := e.tm.Catalog().ListTables()

	switch strings.ToLower(tableName) {
	case InfoSchemaTables:
		for i, tName := range tables {
			row := []catalog.Value{
				catalog.NewText("veridicaldb"),
				catalog.NewText("public"),
				catalog.NewText(tName),
				catalog.NewText("BASE TABLE"),
			}
			if cont, err := fn(storage.RID{Table: tableName, Slot: uint16(i)}, row); err != nil || !cont {
				return err
			}
		}
		// Add views
		e.viewsMu.RLock()
		defer e.viewsMu.RUnlock()
		i := len(tables)
		for vName := range e.views {
			row := []catalog.Value{
				catalog.NewText("veridicaldb"),
				catalog.NewText("public"),
				catalog.NewText(vName),
				catalog.NewText("VIEW"),
			}
			if cont, err := fn(storage.RID{Table: tableName, Slot: uint16(i)}, row); err != nil || !cont {
				return err
			}
			i++
		}

	case InfoSchemaColumns:
		rowIdx := 0
		for _, tName := range tables {
			table, err := e.tm.Catalog().GetTable(tName)
			if err != nil {
				continue
			}
			for i, col := range table.Schema.Columns {
				isNullable := "NO"
				if !col.NotNull {
					isNullable = "YES"
				}
				defaultVal := "NULL"
				if col.HasDefault && col.DefaultValue != nil {
					defaultVal = col.DefaultValue.String()
				}

				row := []catalog.Value{
					catalog.NewText("veridicaldb"),
					catalog.NewText("public"),
					catalog.NewText(tName),
					catalog.NewText(col.Name),
					catalog.NewInt32(int32(i + 1)),
					catalog.NewText(defaultVal),
					catalog.NewText(isNullable),
					catalog.NewText(col.Type.String()),
				}
				if cont, err := fn(storage.RID{Table: tableName, Slot: uint16(rowIdx)}, row); err != nil || !cont {
					return err
				}
				rowIdx++
			}
		}

	case InfoSchemaTableConstraints:
		rowIdx := 0
		for _, tName := range tables {
			table, err := e.tm.Catalog().GetTable(tName)
			if err != nil {
				continue
			}
			// Primary Key
			pkCols := []string{}
			for _, col := range table.Schema.Columns {
				if col.PrimaryKey {
					pkCols = append(pkCols, col.Name)
				}
			}
			if len(pkCols) > 0 {
				row := []catalog.Value{
					catalog.NewText("veridicaldb"),
					catalog.NewText("public"),
					catalog.NewText(tName + "_pkey"), // Synthetic name
					catalog.NewText("public"),
					catalog.NewText(tName),
					catalog.NewText("PRIMARY KEY"),
				}
				if cont, err := fn(storage.RID{Table: tableName, Slot: uint16(rowIdx)}, row); err != nil || !cont {
					return err
				}
				rowIdx++
			}

			// Foreign Keys
			for _, fk := range table.Schema.ForeignKeys {
				row := []catalog.Value{
					catalog.NewText("veridicaldb"),
					catalog.NewText("public"),
					catalog.NewText(fk.Name),
					catalog.NewText("public"),
					catalog.NewText(tName),
					catalog.NewText("FOREIGN KEY"),
				}
				if cont, err := fn(storage.RID{Table: tableName, Slot: uint16(rowIdx)}, row); err != nil || !cont {
					return err
				}
				rowIdx++
			}
		}
	}
	return nil
}
