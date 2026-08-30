package exec

import (
	"fmt"
	"strings"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/sql/ast"
	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// Constraint enforcement for the MVCC insert path. Ported from the pre-MVCC
// executor.go (validateVarcharLength / validateUniqueConstraints /
// checkForeignKeys / findConflictingRow) so the shipping path rejects the same
// violations. All scans go through the MVCC table manager and therefore respect
// the transaction's snapshot: an in-flight or rolled-back row is not a conflict.
//
// Plan phase P2.1.

// checkVarcharLengths rejects text values longer than a column's VARCHAR(n).
func (e *MVCCExecutor) checkVarcharLengths(schema *catalog.Schema, values []catalog.Value) error {
	for i, col := range schema.Columns {
		if col.Type != catalog.TypeText || col.Length == 0 {
			continue
		}
		if i < len(values) && !values[i].IsNull && len(values[i].Text) > col.Length {
			return fmt.Errorf("value too long for type VARCHAR(%d): got %d characters", col.Length, len(values[i].Text))
		}
	}
	return nil
}

// checkUniqueness rejects a row that would duplicate an existing primary key or
// any column-level UNIQUE value. NULLs never conflict. Rows at excludeRID (the
// pre-image of an UPDATE) are skipped so a row does not collide with itself;
// pass the zero RID for INSERT.
func (e *MVCCExecutor) checkUniqueness(tableName string, schema *catalog.Schema, values []catalog.Value, excludeRID storage.RID, tx *txn.Transaction) error {
	var pkIdx []int
	for i, col := range schema.Columns {
		if col.PrimaryKey {
			pkIdx = append(pkIdx, i)
		}
	}

	type check struct {
		cols []int
		desc string
	}
	var checks []check
	if len(pkIdx) > 0 {
		anyNull := false
		for _, i := range pkIdx {
			if i >= len(values) || values[i].IsNull {
				anyNull = true
			}
		}
		if !anyNull {
			checks = append(checks, check{pkIdx, "primary key"})
		}
	}
	for i, col := range schema.Columns {
		if col.Unique && !col.PrimaryKey && i < len(values) && !values[i].IsNull {
			checks = append(checks, check{[]int{i}, fmt.Sprintf("unique constraint on column %q", col.Name)})
		}
	}
	if len(checks) == 0 {
		return nil
	}

	var violation string
	err := e.mtm.Scan(tableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		if row.RID == excludeRID && excludeRID != (storage.RID{}) {
			return true, nil
		}
		for _, c := range checks {
			match := true
			for _, i := range c.cols {
				if i >= len(row.Values) || row.Values[i].Compare(values[i]) != 0 {
					match = false
					break
				}
			}
			if match {
				violation = c.desc
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	if violation != "" {
		return fmt.Errorf("duplicate key value violates %s", violation)
	}
	return nil
}

// checkForeignKeys rejects a row whose foreign-key values have no matching row
// in the referenced table. A FK column that is NULL is not checked.
func (e *MVCCExecutor) checkForeignKeys(meta *catalog.TableMeta, values []catalog.Value, tx *txn.Transaction) error {
	for _, fk := range meta.Schema.ForeignKeys {
		fkVals := make([]catalog.Value, 0, len(fk.Columns))
		hasNull := false
		for _, colName := range fk.Columns {
			_, idx := meta.Schema.ColumnByName(colName)
			if idx < 0 {
				return fmt.Errorf("foreign key column %q not found in table %q", colName, meta.Name)
			}
			if idx >= len(values) || values[idx].IsNull {
				hasNull = true
				break
			}
			fkVals = append(fkVals, values[idx])
		}
		if hasNull {
			continue
		}

		refMeta, err := e.mtm.Catalog().GetTable(fk.RefTable)
		if err != nil {
			return fmt.Errorf("referenced table %q not found", fk.RefTable)
		}
		refIdx := make([]int, len(fk.RefColumns))
		for i, rc := range fk.RefColumns {
			_, idx := refMeta.Schema.ColumnByName(rc)
			if idx < 0 {
				return fmt.Errorf("referenced column %q not found in table %q", rc, fk.RefTable)
			}
			refIdx[i] = idx
		}

		found := false
		err = e.mtm.Scan(fk.RefTable, tx, func(row *catalog.MVCCRow) (bool, error) {
			for i, ri := range refIdx {
				if ri >= len(row.Values) || row.Values[ri].Compare(fkVals[i]) != 0 {
					return true, nil
				}
			}
			found = true
			return false, nil
		})
		if err != nil {
			return err
		}
		if !found {
			name := fk.Name
			if name == "" {
				name = fmt.Sprintf("%s -> %s", meta.Name, fk.RefTable)
			}
			return fmt.Errorf("insert or update on table %q violates foreign key constraint %q", meta.Name, name)
		}
	}
	return nil
}

// findConflictRID locates an existing row that conflicts with values on the
// statement's ON CONFLICT columns, or on the primary key when none are named.
func (e *MVCCExecutor) findConflictRID(stmt *ast.InsertStmt, schema *catalog.Schema, values []catalog.Value, tx *txn.Transaction) (storage.RID, []catalog.Value, bool, error) {
	var idxs []int
	if stmt.OnConflict != nil && len(stmt.OnConflict.ConflictColumns) > 0 {
		for _, name := range stmt.OnConflict.ConflictColumns {
			if _, idx := schema.ColumnByName(name); idx >= 0 {
				idxs = append(idxs, idx)
			}
		}
	} else {
		for i, col := range schema.Columns {
			if col.PrimaryKey {
				idxs = append(idxs, i)
			}
		}
	}
	if len(idxs) == 0 {
		return storage.RID{}, nil, false, nil
	}

	var (
		hitRID storage.RID
		hitRow []catalog.Value
		hit    bool
	)
	err := e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		for _, i := range idxs {
			if i >= len(values) || i >= len(row.Values) || row.Values[i].Compare(values[i]) != 0 {
				return true, nil
			}
		}
		hitRID = row.RID
		hitRow = append([]catalog.Value(nil), row.Values...)
		hit = true
		return false, nil
	})
	return hitRID, hitRow, hit, err
}

// applyOnConflictUpdate implements INSERT ... ON CONFLICT ... DO UPDATE for the
// MVCC path: it rewrites the conflicting row (MVCC delete + insert of the merged
// version) with the assignments from the DO UPDATE clause. Assignment values are
// evaluated against the existing row; a bare `excluded.<col>` reference resolves
// to the row that was being inserted.
func (e *MVCCExecutor) applyOnConflictUpdate(
	stmt *ast.InsertStmt,
	meta *catalog.TableMeta,
	rid storage.RID,
	existing []catalog.Value,
	proposed []catalog.Value,
	tx *txn.Transaction,
	rowIdx int,
) error {
	newRow := append([]catalog.Value(nil), existing...)

	for _, assign := range stmt.OnConflict.UpdateSet {
		col, idx := meta.Schema.ColumnByName(assign.Column)
		if col == nil {
			return fmt.Errorf("row %d: unknown column in ON CONFLICT DO UPDATE: %s", rowIdx+1, assign.Column)
		}

		var val catalog.Value
		if ref, ok := assign.Value.(*ast.ColumnRef); ok && strings.HasPrefix(strings.ToLower(ref.Name), "excluded.") {
			exCol := ref.Name[len("excluded."):]
			_, exIdx := meta.Schema.ColumnByName(exCol)
			if exIdx < 0 {
				return fmt.Errorf("row %d: unknown column in excluded.%s", rowIdx+1, exCol)
			}
			val = proposed[exIdx]
		} else {
			v, err := e.evalExpr(assign.Value, meta.Schema, existing, tx)
			if err != nil {
				return fmt.Errorf("row %d: %w", rowIdx+1, err)
			}
			val = v
		}

		coerced, err := coerceValueMVCC(val, col.Type)
		if err != nil {
			return fmt.Errorf("row %d, column %s: %w", rowIdx+1, assign.Column, err)
		}
		newRow[idx] = coerced
	}

	if err := e.checkVarcharLengths(meta.Schema, newRow); err != nil {
		return fmt.Errorf("row %d: %w", rowIdx+1, err)
	}
	if err := e.checkForeignKeys(meta, newRow, tx); err != nil {
		return fmt.Errorf("row %d: %w", rowIdx+1, err)
	}

	if err := e.updateIndexesOnDelete(stmt.TableName, rid, existing, meta.Schema); err != nil {
		return fmt.Errorf("row %d: %w", rowIdx+1, err)
	}
	if err := e.mtm.MarkDeleted(rid, tx); err != nil {
		return fmt.Errorf("row %d: %w", rowIdx+1, err)
	}
	newRID, err := e.mtm.Insert(stmt.TableName, newRow, tx)
	if err != nil {
		return fmt.Errorf("row %d: %w", rowIdx+1, err)
	}
	if err := e.updateIndexesOnInsert(stmt.TableName, newRID, newRow, meta.Schema); err != nil {
		return fmt.Errorf("row %d: %w", rowIdx+1, err)
	}
	return nil
}

// checkReferencingForeignKeys rejects deleting or updating a row that is still
// referenced by a foreign key in another table (RESTRICT semantics). oldRow is
// the row's pre-image.
func (e *MVCCExecutor) checkReferencingForeignKeys(meta *catalog.TableMeta, oldRow []catalog.Value, tx *txn.Transaction) error {
	cat := e.mtm.Catalog()
	for _, tableName := range cat.ListTables() {
		table, err := cat.GetTable(tableName)
		if err != nil {
			continue
		}
		for _, fk := range table.Schema.ForeignKeys {
			if fk.RefTable != meta.Name {
				continue
			}
			refVals := make([]catalog.Value, 0, len(fk.RefColumns))
			for _, rc := range fk.RefColumns {
				_, idx := meta.Schema.ColumnByName(rc)
				if idx < 0 || idx >= len(oldRow) {
					return fmt.Errorf("referenced column %q not found in %q", rc, meta.Name)
				}
				refVals = append(refVals, oldRow[idx])
			}
			childIdx := make([]int, len(fk.Columns))
			for i, cc := range fk.Columns {
				_, idx := table.Schema.ColumnByName(cc)
				if idx < 0 {
					return fmt.Errorf("foreign key column %q not found in %q", cc, tableName)
				}
				childIdx[i] = idx
			}

			var referenced bool
			err = e.mtm.Scan(tableName, tx, func(row *catalog.MVCCRow) (bool, error) {
				for i, ci := range childIdx {
					if ci >= len(row.Values) || row.Values[ci].Compare(refVals[i]) != 0 {
						return true, nil
					}
				}
				referenced = true
				return false, nil
			})
			if err != nil {
				return err
			}
			if referenced {
				name := fk.Name
				if name == "" {
					name = fmt.Sprintf("%s -> %s", tableName, meta.Name)
				}
				return fmt.Errorf("update or delete on table %q violates foreign key constraint %q on table %q", meta.Name, name, tableName)
			}
		}
	}
	return nil
}

// applyAutoIncrement fills any AUTO_INCREMENT column that was left NULL with
// max(existing) + 1. It scans MVCC-visible rows each call rather than keeping an
// in-memory counter, so an explicit value inserted earlier is respected and
// there is no state to get out of sync.
func (e *MVCCExecutor) applyAutoIncrement(tableName string, schema *catalog.Schema, values []catalog.Value, tx *txn.Transaction) error {
	for i, col := range schema.Columns {
		if !col.AutoIncrement || i >= len(values) || !values[i].IsNull {
			continue
		}

		var maxVal int64
		err := e.mtm.Scan(tableName, tx, func(row *catalog.MVCCRow) (bool, error) {
			if i >= len(row.Values) || row.Values[i].IsNull {
				return true, nil
			}
			var v int64
			switch row.Values[i].Type {
			case catalog.TypeInt32:
				v = int64(row.Values[i].Int32)
			case catalog.TypeInt64:
				v = row.Values[i].Int64
			}
			if v > maxVal {
				maxVal = v
			}
			return true, nil
		})
		if err != nil {
			return err
		}

		next := maxVal + 1
		if col.Type == catalog.TypeInt32 {
			values[i] = catalog.NewInt32(int32(next))
		} else {
			values[i] = catalog.NewInt64(next)
		}
	}
	return nil
}
