package sql

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/btree"
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
	"github.com/JayabrataBasu/VeridicalDB/pkg/storage"
	"github.com/JayabrataBasu/VeridicalDB/pkg/txn"
)

// convertPartitionSpec converts an AST PartitionSpec to a catalog PartitionSpec.
func convertPartitionSpec(astSpec *PartitionSpec) *catalog.PartitionSpec {
	if astSpec == nil {
		return nil
	}

	// Convert partition type
	var partType catalog.PartitionType
	switch astSpec.Type {
	case PartitionRange:
		partType = catalog.PartitionTypeRange
	case PartitionList:
		partType = catalog.PartitionTypeList
	case PartitionHash:
		partType = catalog.PartitionTypeHash
	}

	// Convert partitions
	partitions := make([]catalog.PartitionInfo, len(astSpec.Partitions))
	for i, p := range astSpec.Partitions {
		partitions[i] = catalog.PartitionInfo{
			Name:  p.Name,
			Bound: convertPartitionBound(partType, &p.Bound),
		}
	}

	return &catalog.PartitionSpec{
		Type:       partType,
		Columns:    astSpec.Columns,
		Partitions: partitions,
		NumBuckets: astSpec.NumBuckets,
	}
}

// convertPartitionBound converts AST partition bound to catalog PartitionBound.
func convertPartitionBound(partType catalog.PartitionType, bound *PartitionBoundDef) catalog.PartitionBound {
	result := catalog.PartitionBound{
		IsMaxValue: bound.IsMaxValue,
	}

	switch partType {
	case catalog.PartitionTypeRange:
		// For RANGE, store the LessThan value
		if bound.LessThan != nil {
			result.LessThan = exprToCatalogValue(bound.LessThan)
		}
	case catalog.PartitionTypeList:
		// For LIST, store the list of values
		result.Values = convertBoundValues(bound)
	}

	return result
}

// convertBoundValues converts AST partition bound to catalog values.
func convertBoundValues(bound *PartitionBoundDef) []catalog.Value {
	if bound == nil || len(bound.Values) == 0 {
		return nil
	}

	values := make([]catalog.Value, len(bound.Values))
	for i, v := range bound.Values {
		values[i] = exprToCatalogValue(v)
	}
	return values
}

// exprToCatalogValue converts an AST expression to a catalog Value.
func exprToCatalogValue(expr Expression) catalog.Value {
	switch e := expr.(type) {
	case *LiteralExpr:
		// LiteralExpr already contains a catalog.Value
		return e.Value
	case *ColumnRef:
		// Special case for MAXVALUE identifier
		if strings.ToUpper(e.Name) == "MAXVALUE" {
			return catalog.Value{IsNull: true} // MAXVALUE represented as null
		}
		return catalog.NewText(e.Name)
	default:
		return catalog.Value{IsNull: true}
	}
}

// partitionRouter routes rows to the correct partition.
type partitionRouter struct {
	spec       *catalog.PartitionSpec
	colIndexes []int // indexes of partition columns in the schema
}

// newPartitionRouter creates a partition router for a partitioned table.
func newPartitionRouter(meta *catalog.TableMeta) (*partitionRouter, error) {
	if meta.PartitionSpec == nil {
		return nil, fmt.Errorf("table is not partitioned")
	}

	// Map partition column names to indexes
	colIndexes := make([]int, len(meta.PartitionSpec.Columns))
	for i, colName := range meta.PartitionSpec.Columns {
		found := false
		for j, col := range meta.Columns {
			if strings.EqualFold(col.Name, colName) {
				colIndexes[i] = j
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("partition column %q not found in table", colName)
		}
	}

	return &partitionRouter{
		spec:       meta.PartitionSpec,
		colIndexes: colIndexes,
	}, nil
}

// route determines which partition a row belongs to.
func (r *partitionRouter) route(values []catalog.Value) (string, error) {
	if len(r.colIndexes) == 0 {
		return "", fmt.Errorf("no partition columns defined")
	}

	// Get the partition key value (for single-column partitioning)
	keyIdx := r.colIndexes[0]
	keyVal := values[keyIdx]

	switch r.spec.Type {
	case catalog.PartitionTypeRange:
		return r.routeRange(keyVal)
	case catalog.PartitionTypeList:
		return r.routeList(keyVal)
	case catalog.PartitionTypeHash:
		return r.routeHash(keyVal)
	default:
		return "", fmt.Errorf("unknown partition type")
	}
}

// routeRange routes a value to a RANGE partition.
func (r *partitionRouter) routeRange(val catalog.Value) (string, error) {
	for _, part := range r.spec.Partitions {
		if part.Bound.IsMaxValue {
			return part.Name, nil
		}
		// Handle unset bounds
		bound := part.Bound.LessThan
		if bound.Type == catalog.TypeUnknown && !bound.IsNull {
			// Bound not properly set, skip this partition
			continue
		}
		if bound.IsNull && bound.Type == catalog.TypeUnknown {
			continue
		}
		cmp := comparePartitionValues(val, bound)
		if cmp < 0 {
			return part.Name, nil
		}
	}
	return "", fmt.Errorf("no partition found for value %v", val)
}

// comparePartitionValues compares two values, coercing numeric types if needed.
func comparePartitionValues(a, b catalog.Value) int {
	// Handle nulls
	if a.IsNull && b.IsNull {
		return 0
	}
	if a.IsNull {
		return -1
	}
	if b.IsNull {
		return 1
	}

	// If types match, use standard comparison
	if a.Type == b.Type {
		return a.Compare(b)
	}

	// Coerce numeric types for comparison
	aNum, aIsNum := toNumeric(a)
	bNum, bIsNum := toNumeric(b)

	if aIsNum && bIsNum {
		if aNum < bNum {
			return -1
		} else if aNum > bNum {
			return 1
		}
		return 0
	}

	// For text comparison
	if a.Type == catalog.TypeText && b.Type == catalog.TypeText {
		if a.Text < b.Text {
			return -1
		} else if a.Text > b.Text {
			return 1
		}
		return 0
	}

	// Fallback to standard compare
	return a.Compare(b)
}

// toNumeric converts a value to float64 for numeric comparison.
func toNumeric(v catalog.Value) (float64, bool) {
	switch v.Type {
	case catalog.TypeInt32:
		return float64(v.Int32), true
	case catalog.TypeInt64:
		return float64(v.Int64), true
	case catalog.TypeFloat64:
		return v.Float64, true
	default:
		return 0, false
	}
}

// routeList routes a value to a LIST partition.
func (r *partitionRouter) routeList(val catalog.Value) (string, error) {
	for _, part := range r.spec.Partitions {
		for _, boundVal := range part.Bound.Values {
			if comparePartitionValues(val, boundVal) == 0 {
				return part.Name, nil
			}
		}
	}
	return "", fmt.Errorf("no partition found for value %v", val)
}

// routeHash routes a value to a HASH partition.
func (r *partitionRouter) routeHash(val catalog.Value) (string, error) {
	if r.spec.NumBuckets <= 0 {
		return "", fmt.Errorf("invalid number of hash buckets")
	}

	// Simple hash: convert value to a hash
	var hash uint64
	switch {
	case val.Type == catalog.TypeInt32:
		hash = uint64(val.Int32)
	case val.Type == catalog.TypeInt64:
		hash = uint64(val.Int64)
	case val.Type == catalog.TypeText:
		for _, c := range val.Text {
			hash = hash*31 + uint64(c)
		}
	default:
		hash = 0
	}

	bucketIdx := int(hash % uint64(r.spec.NumBuckets))

	// Find the partition for this bucket
	if bucketIdx < len(r.spec.Partitions) {
		return r.spec.Partitions[bucketIdx].Name, nil
	}

	// Generate partition name for hash buckets
	return fmt.Sprintf("p%d", bucketIdx), nil
}

// MVCCExecutor executes SQL statements with MVCC transaction support.
type MVCCExecutor struct {
	mtm        *catalog.MVCCTableManager
	indexMgr   *btree.IndexManager
	triggerCat *catalog.TriggerCatalog
	procCat    *catalog.ProcedureCatalog
	session    *Session
	// Views defined in this executor (CREATE VIEW)
	views   map[string]*ViewDef
	viewsMu sync.RWMutex
}

// NewMVCCExecutor creates a new MVCC-aware executor.
func NewMVCCExecutor(mtm *catalog.MVCCTableManager) *MVCCExecutor {
	return &MVCCExecutor{mtm: mtm, views: make(map[string]*ViewDef)}
}

// SetIndexManager sets the index manager for index maintenance during DML operations.
func (e *MVCCExecutor) SetIndexManager(mgr *btree.IndexManager) {
	e.indexMgr = mgr
}

// SetTriggerCatalog sets the trigger catalog for trigger firing during DML operations.
func (e *MVCCExecutor) SetTriggerCatalog(cat *catalog.TriggerCatalog) {
	e.triggerCat = cat
}

// SetProcedureCatalog sets the procedure catalog for trigger function execution.
func (e *MVCCExecutor) SetProcedureCatalog(cat *catalog.ProcedureCatalog) {
	e.procCat = cat
}

// SetSession sets the session for PL/pgSQL execution within triggers.
func (e *MVCCExecutor) SetSession(s *Session) {
	e.session = s
}

// Execute executes a SQL statement within a transaction context.
// For BEGIN/COMMIT/ROLLBACK, tx can be nil and session handles them.
func (e *MVCCExecutor) Execute(stmt Statement, tx *txn.Transaction) (*Result, error) {
	switch s := stmt.(type) {
	case *CreateTableStmt:
		return e.executeCreate(s)
	case *CreateViewStmt:
		if tx != nil {
			return e.executeCreateViewMVCC(s, tx)
		}
		return e.executeCreateView(s)
	case *DropTableStmt:
		return e.executeDrop(s)
	case *DropViewStmt:
		return e.executeDropView(s)
	case *InsertStmt:
		return e.executeInsert(s, tx)
	case *SelectStmt:
		return e.executeSelect(s, tx)
	case *UnionStmt:
		return e.executeUnion(s, tx)
	case *UpdateStmt:
		return e.executeUpdate(s, tx)
	case *DeleteStmt:
		return e.executeDelete(s, tx)
	case *AlterTableStmt:
		return e.executeAlter(s)
	case *TruncateTableStmt:
		return e.executeTruncate(s)
	case *ShowStmt:
		return e.executeShow(s)
	case *ExplainStmt:
		return e.executeExplain(s, tx)
	case *MergeStmt:
		return e.executeMerge(s, tx)
	case *BeginStmt, *CommitStmt, *RollbackStmt:
		// These are handled by the session, not the executor
		return nil, fmt.Errorf("transaction statements should be handled by session")
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func (e *MVCCExecutor) executeCreate(stmt *CreateTableStmt) (*Result, error) {
	cols := make([]catalog.Column, len(stmt.Columns))
	for i, def := range stmt.Columns {
		col := catalog.Column{
			ID:            i,
			Name:          def.Name,
			Type:          def.Type,
			NotNull:       def.NotNull,
			PrimaryKey:    def.PrimaryKey,
			HasDefault:    def.HasDefault,
			AutoIncrement: def.AutoIncrement,
		}

		// If there's a default value, evaluate it and store it
		if def.HasDefault && def.Default != nil {
			defaultVal, err := e.evalExpr(def.Default, nil, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("invalid default value for column %s: %w", def.Name, err)
			}
			col.DefaultValue = &defaultVal
		}

		// If there's a CHECK constraint, serialize it to string for storage
		if def.Check != nil {
			col.CheckExpr = exprToString(def.Check)
		}

		cols[i] = col
	}

	var foreignKeys []catalog.ForeignKey

	// Process table-level FKs
	for _, fkDef := range stmt.ForeignKeys {
		fk := catalog.ForeignKey{
			Name:       fkDef.ConstraintName,
			Columns:    fkDef.Columns,
			RefTable:   fkDef.RefTable,
			RefColumns: fkDef.RefColumns,
		}
		foreignKeys = append(foreignKeys, fk)
	}

	// Process inline FKs
	for _, c := range stmt.Columns {
		if c.ReferencesTable != "" {
			refCols := []string{}
			if c.ReferencesColumn != "" {
				refCols = append(refCols, c.ReferencesColumn)
			} else {
				// Resolve PK of referenced table
				refTable, err := e.mtm.Catalog().GetTable(c.ReferencesTable)
				if err != nil {
					return nil, fmt.Errorf("referenced table %q not found", c.ReferencesTable)
				}
				var pkColName string
				for _, rc := range refTable.Columns {
					if rc.PrimaryKey {
						if pkColName != "" {
							return nil, fmt.Errorf("referenced table %q has composite primary key, must specify column", c.ReferencesTable)
						}
						pkColName = rc.Name
					}
				}
				if pkColName == "" {
					return nil, fmt.Errorf("referenced table %q has no primary key", c.ReferencesTable)
				}
				refCols = append(refCols, pkColName)
			}

			fk := catalog.ForeignKey{
				Name:       fmt.Sprintf("fk_%s_%s_%d", stmt.TableName, c.Name, time.Now().UnixNano()),
				Columns:    []string{c.Name},
				RefTable:   c.ReferencesTable,
				RefColumns: refCols,
			}
			foreignKeys = append(foreignKeys, fk)
		}
	}

	// Use storage type from statement (default is "ROW")
	storageType := strings.ToLower(stmt.StorageType)
	if storageType == "" {
		storageType = "row"
	}

	// Convert partition spec from AST to catalog format
	partSpec := convertPartitionSpec(stmt.PartitionSpec)

	if err := e.mtm.CreateTableWithStorage(stmt.TableName, cols, foreignKeys, storageType, partSpec); err != nil {
		return nil, err
	}

	msg := fmt.Sprintf("Table '%s' created", stmt.TableName)
	if storageType == "column" {
		msg += " (columnar storage)"
	}
	if partSpec != nil {
		msg += fmt.Sprintf(" (partitioned by %s)", partSpec.Type)
	}
	return &Result{
		Message: msg + ".",
	}, nil
}

func (e *MVCCExecutor) executeDrop(stmt *DropTableStmt) (*Result, error) {
	if err := e.mtm.DropTable(stmt.TableName); err != nil {
		return nil, err
	}
	return &Result{
		Message: fmt.Sprintf("Table '%s' dropped.", stmt.TableName),
	}, nil
}

func (e *MVCCExecutor) executeInsert(stmt *InsertStmt, tx *txn.Transaction) (*Result, error) {
	if tx == nil {
		return nil, fmt.Errorf("INSERT requires an active transaction")
	}

	cat := e.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		// Table not found. In future, we may support INSERT INTO view, but currently that's unsupported.
		return nil, err
	}

	totalInserted := 0

	// Helper to process and insert a single row of values
	processAndInsert := func(rowValues []catalog.Value, rowIdx int) error {
		// Build values array
		values := make([]catalog.Value, len(meta.Columns))

		// Initialize all values to NULL
		for i := range values {
			values[i] = catalog.Null(meta.Columns[i].Type)
		}

		// If column list specified, map values to columns
		if len(stmt.Columns) > 0 {
			if len(stmt.Columns) != len(rowValues) {
				return fmt.Errorf("row %d: column count (%d) doesn't match value count (%d)",
					rowIdx+1, len(stmt.Columns), len(rowValues))
			}
			for i, colName := range stmt.Columns {
				col, idx := meta.Schema.ColumnByName(colName)
				if col == nil {
					return fmt.Errorf("unknown column: %s", colName)
				}
				val, err := coerceValueMVCC(rowValues[i], col.Type)
				if err != nil {
					return fmt.Errorf("row %d, column %s: %w", rowIdx+1, colName, err)
				}
				values[idx] = val
			}
		} else {
			// Positional values
			if len(rowValues) != len(meta.Columns) {
				return fmt.Errorf("row %d: expected %d values, got %d", rowIdx+1, len(meta.Columns), len(rowValues))
			}
			for i, val := range rowValues {
				coerced, err := coerceValueMVCC(val, meta.Columns[i].Type)
				if err != nil {
					return fmt.Errorf("row %d, column %s: %w", rowIdx+1, meta.Columns[i].Name, err)
				}
				values[i] = coerced
			}
		}

		// Validate CHECK constraints
		if err := e.validateCheckConstraints(meta.Schema, values); err != nil {
			return fmt.Errorf("row %d: %w", rowIdx+1, err)
		}

		targetTable := stmt.TableName

		// Fire BEFORE INSERT triggers
		if err := e.fireBeforeInsertTriggers(targetTable, values, meta.Schema); err != nil {
			return fmt.Errorf("row %d: BEFORE INSERT trigger failed: %w", rowIdx+1, err)
		}

		// Insert with MVCC
		rid, err := e.mtm.Insert(targetTable, values, tx)
		if err != nil {
			return fmt.Errorf("row %d: %w", rowIdx+1, err)
		}

		// Update indexes for the new row
		if err := e.updateIndexesOnInsert(targetTable, rid, values, meta.Schema); err != nil {
			return fmt.Errorf("row %d: index update failed: %w", rowIdx+1, err)
		}

		// Fire AFTER INSERT triggers
		if err := e.fireAfterInsertTriggers(targetTable, values, meta.Schema); err != nil {
			return fmt.Errorf("row %d: AFTER INSERT trigger failed: %w", rowIdx+1, err)
		}

		totalInserted++
		return nil
	}

	if stmt.Select != nil {
		// INSERT INTO ... SELECT ...
		selectResult, err := e.executeSelect(stmt.Select, tx)
		if err != nil {
			return nil, fmt.Errorf("INSERT SELECT: %w", err)
		}
		for i, row := range selectResult.Rows {
			if err := processAndInsert(row, i); err != nil {
				return nil, err
			}
		}
	} else {
		// INSERT INTO ... VALUES (...)
		for rowIdx, rowExprs := range stmt.ValuesList {
			rowValues := make([]catalog.Value, len(rowExprs))
			for i, expr := range rowExprs {
				val, err := e.evalExpr(expr, meta.Schema, nil, nil)
				if err != nil {
					return nil, fmt.Errorf("row %d: %w", rowIdx+1, err)
				}
				rowValues[i] = val
			}
			if err := processAndInsert(rowValues, rowIdx); err != nil {
				return nil, err
			}
		}
	}

	if totalInserted == 1 {
		return &Result{Message: "1 row inserted.", RowsAffected: 1}, nil
	}
	return &Result{
		Message:      fmt.Sprintf("%d rows inserted.", totalInserted),
		RowsAffected: totalInserted,
	}, nil
}

func (e *MVCCExecutor) executeSelect(stmt *SelectStmt, tx *txn.Transaction) (*Result, error) {
	if tx == nil {
		return nil, fmt.Errorf("SELECT requires an active transaction")
	}

	// If the FROM refers to a view instead of a table, expand it early.
	if _, err := e.mtm.Catalog().GetTable(stmt.TableName); err != nil {
		e.viewsMu.RLock()
		viewDef, exists := e.views[stmt.TableName]
		e.viewsMu.RUnlock()
		if exists {
			return e.executeSelectFromView(stmt, viewDef, tx)
		}
		// proceed and let later logic return the original error
	}

	// Handle JOINs separately
	if len(stmt.Joins) > 0 {
		return e.executeSelectWithJoins(stmt, tx)
	}

	// Check if query contains window functions
	hasWindowFunctions := false
	for _, col := range stmt.Columns {
		if col.Expression != nil {
			if _, ok := col.Expression.(*WindowFuncExpr); ok {
				hasWindowFunctions = true
				break
			}
		}
	}

	if hasWindowFunctions {
		return e.executeSelectWithWindowFunctions(stmt, tx)
	}

	// Check for aggregates or GROUP BY
	hasAggregates := false
	for _, sc := range stmt.Columns {
		if sc.Aggregate != nil {
			hasAggregates = true
			break
		}
	}
	if hasAggregates || len(stmt.GroupBy) > 0 {
		return e.executeSelectWithAggregatesMVCC(stmt, tx)
	}

	// Try to use an index scan first
	result, used, err := e.executeSelectWithIndex(stmt, tx)
	if err != nil {
		return nil, err
	}
	if used {
		return result, nil // Index scan succeeded
	}

	// Fall back to full table scan
	cat := e.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Determine output columns
	var outCols []string
	var colIndices []int
	var colExpressions []Expression // For expressions like CASE

	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		// SELECT *
		for i, col := range meta.Columns {
			outCols = append(outCols, col.Name)
			colIndices = append(colIndices, i) // Use slice index, not col.ID
			colExpressions = append(colExpressions, nil)
		}
	} else {
		for _, sc := range stmt.Columns {
			// Handle expressions (like CASE)
			if sc.Expression != nil {
				alias := sc.Alias
				if alias == "" {
					alias = "expr" // Default alias for expressions
				}
				outCols = append(outCols, alias)
				colIndices = append(colIndices, -1) // -1 means this is an expression
				colExpressions = append(colExpressions, sc.Expression)
			} else {
				col, idx := meta.Schema.ColumnByName(sc.Name)
				if col == nil {
					return nil, fmt.Errorf("unknown column: %s", sc.Name)
				}
				// Use alias if specified, otherwise use column name
				if sc.Alias != "" {
					outCols = append(outCols, sc.Alias)
				} else {
					outCols = append(outCols, sc.Name)
				}
				colIndices = append(colIndices, idx)
				colExpressions = append(colExpressions, nil)
			}
		}
	}

	// Scan and filter using MVCC visibility
	var rows [][]catalog.Value

	err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, err := e.evalCondition(stmt.Where, meta.Schema, row.Values, tx)
			if err != nil {
				return false, err
			}
			if !match {
				return true, nil // Continue scanning
			}
		}

		// Project columns (including expressions)
		outRow := make([]catalog.Value, len(colIndices))
		for i, idx := range colIndices {
			if idx >= 0 {
				// Regular column
				outRow[i] = row.Values[idx]
			} else {
				// Expression (like CASE)
				val, err := e.evalExpr(colExpressions[i], meta.Schema, row.Values, tx)
				if err != nil {
					return false, fmt.Errorf("error evaluating expression: %w", err)
				}
				outRow[i] = val
			}
		}
		rows = append(rows, outRow)
		return true, nil
	})

	if err != nil {
		return nil, err
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		orderByIndices := make([]int, len(stmt.OrderBy))
		for i, ob := range stmt.OrderBy {
			_, idx := meta.Schema.ColumnByName(ob.Column)
			if idx < 0 {
				// Try to find in output columns
				found := false
				for j, col := range outCols {
					if strings.EqualFold(col, ob.Column) {
						orderByIndices[i] = j
						found = true
						break
					}
				}
				if !found {
					return nil, fmt.Errorf("unknown column in ORDER BY: %s", ob.Column)
				}
			} else {
				orderByIndices[i] = idx
			}
		}
		sortRowsMVCC(rows, stmt.OrderBy, orderByIndices)
	}

	// Apply DISTINCT
	if stmt.Distinct {
		uniqueRows := make(map[string]bool)
		var distinctRows [][]catalog.Value
		for _, row := range rows {
			key := groupKeyString(row)
			if !uniqueRows[key] {
				uniqueRows[key] = true
				distinctRows = append(distinctRows, row)
			}
		}
		rows = distinctRows
	}

	// Apply OFFSET
	if stmt.Offset != nil && *stmt.Offset > 0 {
		offset := int(*stmt.Offset)
		if offset >= len(rows) {
			rows = nil
		} else {
			rows = rows[offset:]
		}
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		limit := int(*stmt.Limit)
		if limit >= 0 && limit < len(rows) {
			rows = rows[:limit]
		}
	}

	return &Result{
		Columns: outCols,
		Rows:    rows,
	}, nil
}

// executeSelectWithAggregatesMVCC handles simple aggregate queries (no GROUP BY or global aggregates)
func (e *MVCCExecutor) executeSelectWithAggregatesMVCC(stmt *SelectStmt, tx *txn.Transaction) (*Result, error) {
	if tx == nil {
		return nil, fmt.Errorf("SELECT with aggregates requires an active transaction")
	}

	cat := e.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// If GROUP BY is not present, keep the old global-aggregate behavior
	if len(stmt.GroupBy) == 0 {
		// Prepare aggregator states per column
		type aggState struct {
			count    int64
			sum      int64
			sumFloat float64
			hasValue bool
			max      catalog.Value
		}

		states := make([]aggState, len(stmt.Columns))

		// Scan rows and update aggregators
		err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
			// WHERE filter
			if stmt.Where != nil {
				match, err := e.evalCondition(stmt.Where, meta.Schema, row.Values, tx)
				if err != nil {
					return false, err
				}
				if !match {
					return true, nil
				}
			}

			for i, sc := range stmt.Columns {
				if sc.Aggregate == nil {
					continue
				}
				agg := sc.Aggregate
				switch agg.Function {
				case "COUNT":
					if agg.Arg == "*" || agg.Arg == "" {
						states[i].count++
						states[i].hasValue = true
					} else {
						col, idx := meta.Schema.ColumnByName(agg.Arg)
						if col == nil {
							return false, fmt.Errorf("unknown column: %s", agg.Arg)
						}
						val := row.Values[idx]
						if !val.IsNull {
							states[i].count++
							states[i].hasValue = true
						}
					}
				case "SUM":
					col, idx := meta.Schema.ColumnByName(agg.Arg)
					if col == nil {
						return false, fmt.Errorf("unknown column: %s", agg.Arg)
					}
					val := row.Values[idx]
					if val.IsNull {
						continue
					}
					switch val.Type {
					case catalog.TypeInt32:
						states[i].sum += int64(val.Int32)
						states[i].hasValue = true
					case catalog.TypeInt64:
						states[i].sum += val.Int64
						states[i].hasValue = true
					default:
						// treat as float
						states[i].sumFloat += val.Float64
						states[i].hasValue = true
					}
				case "MAX":
					col, idx := meta.Schema.ColumnByName(agg.Arg)
					if col == nil {
						return false, fmt.Errorf("unknown column: %s", agg.Arg)
					}
					val := row.Values[idx]
					if val.IsNull {
						continue
					}
					if !states[i].hasValue {
						states[i].max = val
						states[i].hasValue = true
					} else {
						if compareValuesForSort(val, states[i].max) > 0 {
							states[i].max = val
						}
					}
				case "AVG":
					col, idx := meta.Schema.ColumnByName(agg.Arg)
					if col == nil {
						return false, fmt.Errorf("unknown column: %s", agg.Arg)
					}
					val := row.Values[idx]
					if val.IsNull {
						continue
					}
					switch val.Type {
					case catalog.TypeInt32:
						states[i].sumFloat += float64(val.Int32)
					case catalog.TypeInt64:
						states[i].sumFloat += float64(val.Int64)
					default:
						states[i].sumFloat += val.Float64
					}
					states[i].count++
					states[i].hasValue = true
				default:
					return false, fmt.Errorf("unsupported aggregate: %s", agg.Function)
				}
			}

			return true, nil
		})
		if err != nil {
			return nil, err
		}

		// Build result row (single row for global aggregates)
		outCols := make([]string, len(stmt.Columns))
		row := make([]catalog.Value, len(stmt.Columns))
		for i, sc := range stmt.Columns {
			if sc.Aggregate == nil {
				// Non-aggregate without GROUP BY: undefined, return NULL
				outCols[i] = sc.Alias
				row[i] = catalog.Null(catalog.TypeUnknown)
				continue
			}
			agg := sc.Aggregate
			outCols[i] = sc.Alias
			st := states[i]
			switch agg.Function {
			case "COUNT":
				row[i] = catalog.NewInt64(st.count)
			case "SUM":
				if st.hasValue {
					row[i] = catalog.NewInt64(st.sum)
				} else {
					row[i] = catalog.Null(catalog.TypeInt64)
				}
			case "MAX":
				if st.hasValue {
					row[i] = st.max
				} else {
					row[i] = catalog.Null(catalog.TypeUnknown)
				}
			case "AVG":
				if st.count == 0 {
					row[i] = catalog.Null(catalog.TypeFloat64)
				} else {
					row[i] = catalog.NewFloat64(st.sumFloat / float64(st.count))
				}
			default:
				row[i] = catalog.Null(catalog.TypeUnknown)
			}
		}

		return &Result{Columns: outCols, Rows: [][]catalog.Value{row}}, nil
	}

	// GROUP BY handling
	// Resolve GROUP BY column indices
	groupByIndices := make([]int, len(stmt.GroupBy))
	for i, colName := range stmt.GroupBy {
		_, idx := meta.Schema.ColumnByName(colName)
		if idx < 0 {
			return nil, fmt.Errorf("unknown column in GROUP BY: %s", colName)
		}
		groupByIndices[i] = idx
	}

	// Validate that non-aggregate columns appear in GROUP BY
	type columnInfo struct {
		isAggregate bool
		aggregate   *AggregateFunc
		colName     string
		colIdx      int // schema column index for regular columns
	}
	columnInfos := make([]columnInfo, len(stmt.Columns))
	outputCols := make([]string, len(stmt.Columns))
	for i, col := range stmt.Columns {
		if col.Aggregate != nil {
			agg := col.Aggregate
			columnInfos[i].isAggregate = true
			columnInfos[i].aggregate = agg
			if col.Alias != "" {
				outputCols[i] = col.Alias
			} else {
				outputCols[i] = fmt.Sprintf("%s(%s)", agg.Function, agg.Arg)
			}

			if agg.Arg != "*" {
				_, idx := meta.Schema.ColumnByName(agg.Arg)
				if idx < 0 {
					return nil, fmt.Errorf("unknown column: %s", agg.Arg)
				}
			}
		} else if col.Name != "" {
			columnInfos[i].colName = col.Name
			if col.Alias != "" {
				outputCols[i] = col.Alias
			} else {
				outputCols[i] = col.Name
			}

			// Verify column is in GROUP BY
			found := false
			for j, gbCol := range stmt.GroupBy {
				if gbCol == col.Name {
					found = true
					columnInfos[i].colIdx = groupByIndices[j]
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column %s must appear in GROUP BY clause or be used in an aggregate function", col.Name)
			}
		}
	}

	// Use a map with string key for grouping
	groups := make(map[string]*groupState)
	var groupOrder []string

	// Scan rows
	err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, err := e.evalCondition(stmt.Where, meta.Schema, row.Values, tx)
			if err != nil {
				return false, err
			}
			if !match {
				return true, nil
			}
		}

		// Build group key
		var groupKey []catalog.Value
		var keyStr string
		if len(groupByIndices) > 0 {
			groupKey = make([]catalog.Value, len(groupByIndices))
			for i, idx := range groupByIndices {
				groupKey[i] = row.Values[idx]
			}
			keyStr = groupKeyString(groupKey)
		} else {
			keyStr = ""
		}

		// Get or create group
		grp, exists := groups[keyStr]
		if !exists {
			grp = &groupState{groupKey: groupKey, aggregators: make([]aggregatorState, len(stmt.Columns))}
			groups[keyStr] = grp
			groupOrder = append(groupOrder, keyStr)
		}

		// Update aggregators
		for i, colInfo := range columnInfos {
			if !colInfo.isAggregate {
				continue
			}
			agg := colInfo.aggregate
			aggState := &grp.aggregators[i]

			switch agg.Function {
			case "COUNT":
				if agg.Arg == "*" {
					aggState.count++
				} else {
					_, idx := meta.Schema.ColumnByName(agg.Arg)
					if idx >= 0 && !row.Values[idx].IsNull {
						aggState.count++
					}
				}
			case "SUM":
				_, idx := meta.Schema.ColumnByName(agg.Arg)
				if idx >= 0 {
					val := row.Values[idx]
					if !val.IsNull {
						aggState.hasValue = true
						switch val.Type {
						case catalog.TypeInt32:
							aggState.sum += int64(val.Int32)
						case catalog.TypeInt64:
							aggState.sum += val.Int64
						default:
							// ignore floats for now
						}
					}
				}
			case "AVG":
				_, idx := meta.Schema.ColumnByName(agg.Arg)
				if idx >= 0 {
					val := row.Values[idx]
					if !val.IsNull {
						aggState.count++
						aggState.hasValue = true
						switch val.Type {
						case catalog.TypeInt32:
							aggState.sumFloat += float64(val.Int32)
						case catalog.TypeInt64:
							aggState.sumFloat += float64(val.Int64)
						default:
							aggState.sumFloat += val.Float64
						}
					}
				}
			case "MIN":
				_, idx := meta.Schema.ColumnByName(agg.Arg)
				if idx >= 0 {
					val := row.Values[idx]
					if !val.IsNull {
						if !aggState.hasValue {
							aggState.min = val
							aggState.hasValue = true
						} else if compareValuesForSort(val, aggState.min) < 0 {
							aggState.min = val
						}
					}
				}
			case "MAX":
				_, idx := meta.Schema.ColumnByName(agg.Arg)
				if idx >= 0 {
					val := row.Values[idx]
					if !val.IsNull {
						if !aggState.hasValue {
							aggState.max = val
							aggState.hasValue = true
						} else if compareValuesForSort(val, aggState.max) > 0 {
							aggState.max = val
						}
					}
				}
			}
		}

		return true, nil
	})
	if err != nil {
		return nil, err
	}

	// Build result rows from groups
	var resultRows [][]catalog.Value
	for _, keyStr := range groupOrder {
		grp := groups[keyStr]

		// Apply HAVING filter if present
		if stmt.Having != nil {
			match, err := e.evalHavingConditionMVCC(stmt.Having, grp, stmt.Columns, meta.Schema)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}

		// Build result row
		resultRow := make([]catalog.Value, len(stmt.Columns))
		for i, colInfo := range columnInfos {
			if colInfo.isAggregate {
				aggState := grp.aggregators[i]
				switch colInfo.aggregate.Function {
				case "COUNT":
					resultRow[i] = catalog.NewInt64(aggState.count)
				case "SUM":
					if !aggState.hasValue {
						resultRow[i] = catalog.Null(catalog.TypeInt64)
					} else {
						resultRow[i] = catalog.NewInt64(aggState.sum)
					}
				case "AVG":
					if !aggState.hasValue || aggState.count == 0 {
						resultRow[i] = catalog.Null(catalog.TypeInt64)
					} else {
						avg := aggState.sumFloat / float64(aggState.count)
						resultRow[i] = catalog.NewInt64(int64(avg))
					}
				case "MIN":
					if !aggState.hasValue {
						resultRow[i] = catalog.Null(catalog.TypeUnknown)
					} else {
						resultRow[i] = aggState.min
					}
				case "MAX":
					if !aggState.hasValue {
						resultRow[i] = catalog.Null(catalog.TypeUnknown)
					} else {
						resultRow[i] = aggState.max
					}
				}
			} else {
				// Regular column from GROUP BY
				for j, gbCol := range stmt.GroupBy {
					if gbCol == colInfo.colName {
						resultRow[i] = grp.groupKey[j]
						break
					}
				}
			}
		}
		resultRows = append(resultRows, resultRow)
	}

	return &Result{Columns: outputCols, Rows: resultRows}, nil
}

func (e *MVCCExecutor) executeUpdate(stmt *UpdateStmt, tx *txn.Transaction) (*Result, error) {
	if tx == nil {
		return nil, fmt.Errorf("UPDATE requires an active transaction")
	}

	cat := e.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Collect rows to update
	var toUpdate []struct {
		rid    storage.RID
		oldRow []catalog.Value
		newRow []catalog.Value
	}

	err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, err := e.evalCondition(stmt.Where, meta.Schema, row.Values, tx)
			if err != nil {
				return false, err
			}
			if !match {
				return true, nil
			}
		}

		// Keep old row for index cleanup
		oldRow := make([]catalog.Value, len(row.Values))
		copy(oldRow, row.Values)

		// Build new row with updates
		newRow := make([]catalog.Value, len(row.Values))
		copy(newRow, row.Values)

		for _, assign := range stmt.Assignments {
			col, idx := meta.Schema.ColumnByName(assign.Column)
			if col == nil {
				return false, fmt.Errorf("unknown column: %s", assign.Column)
			}
			val, err := e.evalExpr(assign.Value, meta.Schema, row.Values, tx)
			if err != nil {
				return false, err
			}
			val, err = coerceValueMVCC(val, col.Type)
			if err != nil {
				return false, fmt.Errorf("column %s: %w", assign.Column, err)
			}
			newRow[idx] = val
		}

		// Validate CHECK constraints on the updated row
		if err := e.validateCheckConstraints(meta.Schema, newRow); err != nil {
			return false, err
		}

		toUpdate = append(toUpdate, struct {
			rid    storage.RID
			oldRow []catalog.Value
			newRow []catalog.Value
		}{rid: row.RID, oldRow: oldRow, newRow: newRow})

		return true, nil
	})

	if err != nil {
		return nil, err
	}

	// Perform updates: mark old tuple deleted, insert new tuple
	for _, u := range toUpdate {
		// Fire BEFORE UPDATE triggers
		if err := e.fireBeforeUpdateTriggers(stmt.TableName, u.oldRow, u.newRow, meta.Schema); err != nil {
			return nil, fmt.Errorf("BEFORE UPDATE trigger failed: %w", err)
		}

		// Remove old index entries
		if err := e.updateIndexesOnDelete(stmt.TableName, u.rid, u.oldRow, meta.Schema); err != nil {
			return nil, fmt.Errorf("update index remove failed: %w", err)
		}

		// Mark old tuple as deleted
		if err := e.mtm.MarkDeleted(u.rid, tx); err != nil {
			return nil, fmt.Errorf("update failed: %w", err)
		}

		// Insert new version
		newRID, err := e.mtm.Insert(stmt.TableName, u.newRow, tx)
		if err != nil {
			return nil, fmt.Errorf("update insert failed: %w", err)
		}

		// Add new index entries
		if err := e.updateIndexesOnInsert(stmt.TableName, newRID, u.newRow, meta.Schema); err != nil {
			return nil, fmt.Errorf("update index insert failed: %w", err)
		}

		// Fire AFTER UPDATE triggers
		if err := e.fireAfterUpdateTriggers(stmt.TableName, u.oldRow, u.newRow, meta.Schema); err != nil {
			return nil, fmt.Errorf("AFTER UPDATE trigger failed: %w", err)
		}
	}

	return &Result{
		Message:      fmt.Sprintf("%d row(s) updated.", len(toUpdate)),
		RowsAffected: len(toUpdate),
	}, nil
}

func (e *MVCCExecutor) executeDelete(stmt *DeleteStmt, tx *txn.Transaction) (*Result, error) {
	if tx == nil {
		return nil, fmt.Errorf("DELETE requires an active transaction")
	}

	cat := e.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Collect rows to delete (need values for index cleanup)
	var toDelete []struct {
		rid    storage.RID
		values []catalog.Value
	}

	err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, err := e.evalCondition(stmt.Where, meta.Schema, row.Values, tx)
			if err != nil {
				return false, err
			}
			if !match {
				return true, nil
			}
		}

		// Copy row values for index cleanup
		values := make([]catalog.Value, len(row.Values))
		copy(values, row.Values)
		toDelete = append(toDelete, struct {
			rid    storage.RID
			values []catalog.Value
		}{rid: row.RID, values: values})
		return true, nil
	})

	if err != nil {
		return nil, err
	}

	// Mark tuples as deleted and clean up indexes
	for _, d := range toDelete {
		// Fire BEFORE DELETE triggers
		if err := e.fireBeforeDeleteTriggers(stmt.TableName, d.values, meta.Schema); err != nil {
			return nil, fmt.Errorf("BEFORE DELETE trigger failed: %w", err)
		}

		// Remove index entries
		if err := e.updateIndexesOnDelete(stmt.TableName, d.rid, d.values, meta.Schema); err != nil {
			return nil, fmt.Errorf("delete index cleanup failed: %w", err)
		}

		if err := e.mtm.MarkDeleted(d.rid, tx); err != nil {
			return nil, fmt.Errorf("delete failed: %w", err)
		}

		// Fire AFTER DELETE triggers
		if err := e.fireAfterDeleteTriggers(stmt.TableName, d.values, meta.Schema); err != nil {
			return nil, fmt.Errorf("AFTER DELETE trigger failed: %w", err)
		}
	}

	return &Result{
		Message:      fmt.Sprintf("%d row(s) deleted.", len(toDelete)),
		RowsAffected: len(toDelete),
	}, nil
}

// evalExpr evaluates an expression to a value.
// tx may be nil for contexts without an active transaction; subqueries require a non-nil tx.
func (e *MVCCExecutor) evalExpr(expr Expression, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value, nil

	case *ColumnRef:
		if row == nil {
			return catalog.Value{}, fmt.Errorf("cannot reference column %s without row context", ex.Name)
		}
		col, idx := schema.ColumnByName(ex.Name)
		if col == nil {
			return catalog.Value{}, fmt.Errorf("unknown column: %s", ex.Name)
		}
		return row[idx], nil

	case *BinaryExpr:
		// Handle arithmetic operations
		switch ex.Op {
		case TOKEN_PLUS, TOKEN_MINUS, TOKEN_STAR, TOKEN_SLASH:
			left, err := e.evalExpr(ex.Left, schema, row, tx)
			if err != nil {
				return catalog.Value{}, err
			}
			right, err := e.evalExpr(ex.Right, schema, row, tx)
			if err != nil {
				return catalog.Value{}, err
			}
			return evalArithmetic(left, right, ex.Op)
		}
		return catalog.Value{}, fmt.Errorf("unsupported binary operator in expression: %v", ex.Op)

	case *FunctionExpr:
		// Evaluate function arguments
		args := make([]catalog.Value, len(ex.Args))
		for i, arg := range ex.Args {
			val, err := e.evalExpr(arg, schema, row, tx)
			if err != nil {
				return catalog.Value{}, err
			}
			args[i] = val
		}
		return evalFunction(ex.Name, args)

	case *CaseExpr:
		return e.evalCaseExpr(ex, schema, row, tx)

	case *CastExpr:
		return e.evalCastExpr(ex, schema, row, tx)

	case *IsNullExpr:
		// IS NULL in value context returns boolean
		val, err := e.evalExpr(ex.Expr, schema, row, tx)
		if err != nil {
			return catalog.Value{}, err
		}
		result := val.IsNull
		if ex.Not {
			return catalog.NewBool(!result), nil
		}
		return catalog.NewBool(result), nil

	case *JSONAccessExpr:
		return e.evalJSONAccess(ex, schema, row, tx)

	case *JSONPathExpr:
		return e.evalJSONPath(ex, schema, row, tx)

	case *JSONContainsExpr:
		return e.evalJSONContains(ex, schema, row, tx)

	case *JSONExistsExpr:
		return e.evalJSONExists(ex, schema, row, tx)

	// Full-Text Search expressions
	case *TSVectorExpr:
		return e.evalTSVector(ex, schema, row, tx)

	case *TSQueryExpr:
		return e.evalTSQuery(ex, schema, row, tx)

	case *TSMatchExpr:
		return e.evalTSMatch(ex, schema, row, tx)

	case *TSRankExpr:
		return e.evalTSRank(ex, schema, row, tx)

	case *TSHeadlineExpr:
		return e.evalTSHeadline(ex, schema, row, tx)

	case *MatchAgainstExpr:
		return e.evalMatchAgainst(ex, schema, row, tx)

	case *SubqueryExpr:
		// Scalar subquery: execute and return the single-column single-row value
		if tx == nil {
			return catalog.Value{}, fmt.Errorf("subquery requires an active transaction")
		}

		// Support correlated subqueries by substituting outer column references
		query := ex.Query
		if schema != nil && row != nil {
			// Perform best-effort substitution of unqualified column refs matching outer schema
			query = e.substituteCorrelatedSelectUsingSchema(ex.Query, schema, row)
		}

		res, err := e.executeSelect(query, tx)
		if err != nil {
			return catalog.Value{}, err
		}
		if len(res.Rows) == 0 {
			return catalog.Value{IsNull: true}, nil
		}
		if len(res.Rows) > 1 {
			return catalog.Value{}, fmt.Errorf("subquery returned more than one row")
		}
		if len(res.Rows[0]) == 0 {
			return catalog.Value{}, fmt.Errorf("subquery returned no columns")
		}
		return res.Rows[0][0], nil

	default:
		return catalog.Value{}, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// evalCondition evaluates a boolean expression.
func (e *MVCCExecutor) evalCondition(expr Expression, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (bool, error) {
	switch ex := expr.(type) {
	case *BinaryExpr:
		switch ex.Op {
		case TOKEN_AND:
			left, err := e.evalCondition(ex.Left, schema, row, tx)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil
			}
			return e.evalCondition(ex.Right, schema, row, tx)

		case TOKEN_OR:
			left, err := e.evalCondition(ex.Left, schema, row, tx)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil
			}
			return e.evalCondition(ex.Right, schema, row, tx)

		case TOKEN_EQ, TOKEN_NE, TOKEN_LT, TOKEN_LE, TOKEN_GT, TOKEN_GE:
			leftVal, err := e.evalExpr(ex.Left, schema, row, tx)
			if err != nil {
				return false, err
			}
			rightVal, err := e.evalExpr(ex.Right, schema, row, tx)
			if err != nil {
				return false, err
			}
			return compareValuesMVCC(leftVal, rightVal, ex.Op)

		default:
			return false, fmt.Errorf("unsupported operator in condition: %v", ex.Op)
		}

	case *UnaryExpr:
		if ex.Op == TOKEN_NOT {
			result, err := e.evalCondition(ex.Expr, schema, row, tx)
			if err != nil {
				return false, err
			}
			return !result, nil
		}
		return false, fmt.Errorf("unsupported unary operator: %v", ex.Op)

	case *InExpr:
		// Evaluate IN expression
		leftVal, err := e.evalExpr(ex.Left, schema, row, tx)
		if err != nil {
			return false, err
		}
		if leftVal.IsNull {
			return false, nil // NULL IN (...) is always false
		}

		found := false

		// Check if this is a subquery IN
		if ex.Subquery != nil {
			if tx == nil {
				return false, fmt.Errorf("IN subquery requires an active transaction")
			}

			// Support correlated subqueries by substituting outer column references
			query := ex.Subquery
			if schema != nil && row != nil {
				query = e.substituteCorrelatedSelectUsingSchema(ex.Subquery, schema, row)
			}

			res, err := e.executeSelect(query, tx)
			if err != nil {
				return false, err
			}
			found = false
			for _, r := range res.Rows {
				if len(r) == 0 {
					continue
				}
				eq, err := compareValuesMVCC(leftVal, r[0], TOKEN_EQ)
				if err != nil {
					return false, err
				}
				if eq {
					found = true
					break
				}
			}
			if ex.Not {
				return !found, nil
			}
			return found, nil
		}

		// Value list IN
		for _, valExpr := range ex.Values {
			rightVal, err := e.evalExpr(valExpr, schema, row, tx)
			if err != nil {
				return false, err
			}
			if rightVal.IsNull {
				continue // Skip NULL values in the list
			}
			eq, err := compareValuesMVCC(leftVal, rightVal, TOKEN_EQ)
			if err != nil {
				return false, err
			}
			if eq {
				found = true
				break
			}
		}

		if ex.Not {
			return !found, nil
		}
		return found, nil

	case *BetweenExpr:
		// Evaluate BETWEEN expression
		val, err := e.evalExpr(ex.Expr, schema, row, tx)
		if err != nil {
			return false, err
		}
		if val.IsNull {
			return false, nil // NULL BETWEEN ... is always false
		}

		lowVal, err := e.evalExpr(ex.Low, schema, row, tx)
		if err != nil {
			return false, err
		}
		highVal, err := e.evalExpr(ex.High, schema, row, tx)
		if err != nil {
			return false, err
		}

		// val >= low AND val <= high
		geLow, err := compareValuesMVCC(val, lowVal, TOKEN_GE)
		if err != nil {
			return false, err
		}
		leHigh, err := compareValuesMVCC(val, highVal, TOKEN_LE)
		if err != nil {
			return false, err
		}

		result := geLow && leHigh
		if ex.Not {
			return !result, nil
		}
		return result, nil

	case *LikeExpr:
		// Evaluate LIKE expression
		val, err := e.evalExpr(ex.Expr, schema, row, tx)
		if err != nil {
			return false, err
		}
		if val.IsNull {
			return false, nil // NULL LIKE ... is always false
		}

		patternVal, err := e.evalExpr(ex.Pattern, schema, row, tx)
		if err != nil {
			return false, err
		}
		if patternVal.IsNull {
			return false, nil
		}

		// Both must be text for LIKE
		if val.Type != catalog.TypeText || patternVal.Type != catalog.TypeText {
			return false, fmt.Errorf("LIKE requires text operands")
		}

		result := matchLikePattern(val.Text, patternVal.Text, ex.CaseInsensitive)
		if ex.Not {
			return !result, nil
		}
		return result, nil

	case *IsNullExpr:
		// Evaluate IS NULL / IS NOT NULL
		val, err := e.evalExpr(ex.Expr, schema, row, tx)
		if err != nil {
			return false, err
		}
		result := val.IsNull
		if ex.Not {
			return !result, nil // IS NOT NULL
		}
		return result, nil // IS NULL

	case *SubqueryExpr:
		// Evaluate scalar subquery used as a boolean expression
		v, err := e.evalExpr(ex, schema, row, tx)
		if err != nil {
			return false, err
		}
		if v.IsNull {
			return false, nil
		}
		if v.Type != catalog.TypeBool {
			return false, fmt.Errorf("subquery did not return boolean")
		}
		return v.Bool, nil

	case *ExistsExpr:
		if tx == nil {
			return false, fmt.Errorf("EXISTS subquery requires an active transaction")
		}

		// Support correlated subqueries by substituting outer column references
		query := ex.Query
		if schema != nil && row != nil {
			query = e.substituteCorrelatedSelectUsingSchema(ex.Query, schema, row)
		}

		res, err := e.executeSelect(query, tx)
		if err != nil {
			return false, err
		}
		exists := len(res.Rows) > 0
		if ex.Not {
			return !exists, nil
		}
		return exists, nil

	case *LiteralExpr:
		if ex.Value.Type == catalog.TypeBool {
			return ex.Value.Bool, nil
		}
		return false, fmt.Errorf("expected boolean expression")

	case *TSMatchExpr:
		// Evaluate Full-Text Search match expression
		result, err := e.evalTSMatch(ex, schema, row, tx)
		if err != nil {
			return false, err
		}
		if result.Type == catalog.TypeBool {
			return result.Bool, nil
		}
		return false, fmt.Errorf("FTS match expression did not return boolean")

	default:
		return false, fmt.Errorf("unsupported condition type: %T", expr)
	}
}

// evalHavingConditionMVCC evaluates a HAVING condition against group aggregates for MVCC executor.
func (e *MVCCExecutor) evalHavingConditionMVCC(expr Expression, grp *groupState, columns []SelectColumn, schema *catalog.Schema) (bool, error) {
	switch ex := expr.(type) {
	case *BinaryExpr:
		switch ex.Op {
		case TOKEN_AND:
			left, err := e.evalHavingConditionMVCC(ex.Left, grp, columns, schema)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil
			}
			return e.evalHavingConditionMVCC(ex.Right, grp, columns, schema)

		case TOKEN_OR:
			left, err := e.evalHavingConditionMVCC(ex.Left, grp, columns, schema)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil
			}
			return e.evalHavingConditionMVCC(ex.Right, grp, columns, schema)

		case TOKEN_EQ, TOKEN_NE, TOKEN_LT, TOKEN_LE, TOKEN_GT, TOKEN_GE:
			left, err := e.evalHavingExprMVCC(ex.Left, grp, columns, schema)
			if err != nil {
				return false, err
			}
			right, err := e.evalHavingExprMVCC(ex.Right, grp, columns, schema)
			if err != nil {
				return false, err
			}
			return compareValuesMVCC(left, right, ex.Op)
		}

	case *LiteralExpr:
		if ex.Value.Type == catalog.TypeBool {
			return ex.Value.Bool, nil
		}
	}

	return false, fmt.Errorf("cannot evaluate HAVING expression: %T", expr)
}

// evalHavingExprMVCC evaluates an expression in HAVING context (can reference aggregates).
func (e *MVCCExecutor) evalHavingExprMVCC(expr Expression, grp *groupState, columns []SelectColumn, _ *catalog.Schema) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value, nil

	case *ColumnRef:
		// Look in SELECT columns for non-aggregate columns
		for i, col := range columns {
			if col.Name == ex.Name && col.Aggregate == nil {
				if i < len(grp.groupKey) {
					return grp.groupKey[i], nil
				}
			}
		}
		return catalog.Value{}, fmt.Errorf("column %s not found in GROUP BY", ex.Name)

	case *FunctionExpr:
		funcName := strings.ToUpper(ex.Name)

		// Try to extract simple first argument name (ColumnRef)
		var argName string
		if len(ex.Args) > 0 {
			if cref, ok := ex.Args[0].(*ColumnRef); ok {
				argName = cref.Name
			}
		}

		// Find matching aggregate in SELECT columns (match function and optionally argument)
		for i, col := range columns {
			if col.Aggregate == nil {
				continue
			}
			if strings.ToUpper(col.Aggregate.Function) != funcName {
				continue
			}
			if argName != "" && col.Aggregate.Arg != argName {
				continue
			}
			if i < len(grp.aggregators) {
				aggState := grp.aggregators[i]
				switch funcName {
				case "COUNT":
					return catalog.NewInt64(aggState.count), nil
				case "SUM":
					if !aggState.hasValue {
						return catalog.Null(catalog.TypeInt64), nil
					}
					return catalog.NewInt64(aggState.sum), nil
				case "AVG":
					if aggState.count == 0 {
						return catalog.Null(catalog.TypeInt64), nil
					}
					avg := int64(aggState.sumFloat / float64(aggState.count))
					return catalog.NewInt64(avg), nil
				case "MIN":
					if !aggState.hasValue {
						return catalog.Null(catalog.TypeUnknown), nil
					}
					return aggState.min, nil
				case "MAX":
					if !aggState.hasValue {
						return catalog.Null(catalog.TypeUnknown), nil
					}
					return aggState.max, nil
				}
			}
		}
		return catalog.Value{}, fmt.Errorf("aggregate %s not found in SELECT columns", funcName)
	}

	return catalog.Value{}, fmt.Errorf("unsupported HAVING expression: %T", expr)
}

// compareValues compares two values with the given operator.
// Note: This function is shared with executor.go, using the same name
// compareValuesMVCC is local to avoid redeclaration
func compareValuesMVCC(left, right catalog.Value, op TokenType) (bool, error) {
	// Handle NULL comparisons
	if left.IsNull || right.IsNull {
		return false, nil // NULL comparisons are always false
	}

	// If both are numeric (int/float), compare as floats for cross-type comparisons
	if lnum, lok := toNumeric(left); lok {
		if rnum, rok := toNumeric(right); rok {
			switch op {
			case TOKEN_EQ:
				return lnum == rnum, nil
			case TOKEN_NE:
				return lnum != rnum, nil
			case TOKEN_LT:
				return lnum < rnum, nil
			case TOKEN_LE:
				return lnum <= rnum, nil
			case TOKEN_GT:
				return lnum > rnum, nil
			case TOKEN_GE:
				return lnum >= rnum, nil
			}
		}
	}

	// Type coercion for comparison (fall back)
	if left.Type != right.Type {
		// Try to coerce right to left's type
		coerced, err := coerceValueMVCC(right, left.Type)
		if err != nil {
			return false, nil // Incompatible types
		}
		right = coerced
	}

	var cmp int
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
		return false, fmt.Errorf("unsupported type for comparison: %v", left.Type)
	}

	switch op {
	case TOKEN_EQ:
		return cmp == 0, nil
	case TOKEN_NE:
		return cmp != 0, nil
	case TOKEN_LT:
		return cmp < 0, nil
	case TOKEN_LE:
		return cmp <= 0, nil
	case TOKEN_GT:
		return cmp > 0, nil
	case TOKEN_GE:
		return cmp >= 0, nil
	default:
		return false, fmt.Errorf("unsupported comparison operator: %v", op)
	}
}

// coerceValueMVCC attempts to convert a value to the target type.
func coerceValueMVCC(v catalog.Value, targetType catalog.DataType) (catalog.Value, error) {
	if v.IsNull {
		return catalog.Null(targetType), nil
	}

	if v.Type == targetType {
		return v, nil
	}

	// INT32 -> INT64
	if v.Type == catalog.TypeInt32 && targetType == catalog.TypeInt64 {
		return catalog.NewInt64(int64(v.Int32)), nil
	}

	// INT64 -> INT32 (with range check)
	if v.Type == catalog.TypeInt64 && targetType == catalog.TypeInt32 {
		if v.Int64 < -2147483648 || v.Int64 > 2147483647 {
			return catalog.Value{}, fmt.Errorf("value %d out of INT32 range", v.Int64)
		}
		return catalog.NewInt32(int32(v.Int64)), nil
	}

	return catalog.Value{}, fmt.Errorf("cannot coerce %v to %v", v.Type, targetType)
}

// updateIndexesOnInsert updates all indexes on a table after an INSERT.
func (e *MVCCExecutor) updateIndexesOnInsert(tableName string, rid storage.RID, values []catalog.Value, schema *catalog.Schema) error {
	if e.indexMgr == nil {
		return nil // No index manager, nothing to do
	}

	indexes := e.indexMgr.ListIndexes(tableName)
	for _, meta := range indexes {
		// Build key from column values
		key, err := e.buildIndexKey(meta.Columns, values, schema)
		if err != nil {
			return fmt.Errorf("build key for index %s: %w", meta.Name, err)
		}

		// Insert into index: key -> RID
		if err := e.indexMgr.Insert(meta.Name, key, rid); err != nil {
			return fmt.Errorf("insert into index %s: %w", meta.Name, err)
		}
	}

	return nil
}

// updateIndexesOnDelete updates all indexes on a table after a DELETE.
// Note: For MVCC, we don't actually delete from indexes immediately.
// The index will contain stale entries that are filtered during query.
// A proper implementation would need garbage collection.
func (e *MVCCExecutor) updateIndexesOnDelete(tableName string, _ storage.RID, values []catalog.Value, schema *catalog.Schema) error {
	if e.indexMgr == nil {
		return nil
	}

	indexes := e.indexMgr.ListIndexes(tableName)
	for _, meta := range indexes {
		// Build key from column values
		key, err := e.buildIndexKey(meta.Columns, values, schema)
		if err != nil {
			return fmt.Errorf("build key for index %s: %w", meta.Name, err)
		}

		// Delete from index
		if err := e.indexMgr.Delete(meta.Name, key); err != nil {
			return fmt.Errorf("delete from index %s: %w", meta.Name, err)
		}
	}

	return nil
}

// buildIndexKey builds an index key from column values.
func (e *MVCCExecutor) buildIndexKey(columns []string, values []catalog.Value, schema *catalog.Schema) ([]byte, error) {
	if len(columns) == 1 {
		// Single column index
		col, idx := schema.ColumnByName(columns[0])
		if col == nil {
			return nil, fmt.Errorf("unknown column: %s", columns[0])
		}
		return encodeIndexValue(values[idx])
	}

	// Composite index - collect all parts and combine
	var parts [][]byte
	for _, colName := range columns {
		col, idx := schema.ColumnByName(colName)
		if col == nil {
			return nil, fmt.Errorf("unknown column: %s", colName)
		}
		part, err := encodeIndexValue(values[idx])
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	// Call EncodeCompositeKey with variadic expansion
	return btree.EncodeCompositeKey(parts...), nil
}

// encodeIndexValue encodes a catalog.Value for use as an index key.
func encodeIndexValue(v catalog.Value) ([]byte, error) {
	if v.IsNull {
		// NULL values get a special encoding (0x00 prefix)
		return []byte{0x00}, nil
	}

	switch v.Type {
	case catalog.TypeInt32:
		return btree.EncodeIntKey(int64(v.Int32)), nil
	case catalog.TypeInt64:
		return btree.EncodeIntKey(v.Int64), nil
	case catalog.TypeBool:
		if v.Bool {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case catalog.TypeText:
		// Prefix with 0x01 to distinguish from NULL
		return append([]byte{0x01}, []byte(v.Text)...), nil
	default:
		return nil, fmt.Errorf("unsupported type for index: %v", v.Type)
	}
}

// Note: encodeRID and decodeRID are reserved for future use when implementing
// proper index-based RID encoding/decoding for garbage collection.
// They are commented out to avoid unused function warnings.
/*
// encodeRID encodes a RID as a value for index entries.
func encodeRID(rid storage.RID) []byte {
	// Encode as: [Page:4][Slot:2]
	buf := make([]byte, 6)
	buf[0] = byte(rid.Page >> 24)
	buf[1] = byte(rid.Page >> 16)
	buf[2] = byte(rid.Page >> 8)
	buf[3] = byte(rid.Page)
	buf[4] = byte(rid.Slot >> 8)
	buf[5] = byte(rid.Slot)
	return buf
}

// decodeRID decodes a RID from index entry value.
func decodeRID(tableName string, data []byte) (storage.RID, error) {
	if len(data) < 6 {
		return storage.RID{}, fmt.Errorf("invalid RID data: too short")
	}
	page := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
	slot := uint16(data[4])<<8 | uint16(data[5])
	return storage.RID{Table: tableName, Page: page, Slot: slot}, nil
}
*/

// IndexScanInfo holds information about an index scan that can be used.
type IndexScanInfo struct {
	IndexName string
	Key       []byte
	Op        TokenType // The comparison operator (=, <, >, <=, >=)
	Column    string
}

// findUsableIndex checks if an index can be used for a WHERE clause.
// Returns nil if no suitable index is found.
func (e *MVCCExecutor) findUsableIndex(tableName string, where Expression, schema *catalog.Schema) *IndexScanInfo {
	if e.indexMgr == nil || where == nil {
		return nil
	}

	// Handle AND expressions recursively
	if binExpr, ok := where.(*BinaryExpr); ok && binExpr.Op == TOKEN_AND {
		if info := e.findUsableIndex(tableName, binExpr.Left, schema); info != nil {
			return info
		}
		return e.findUsableIndex(tableName, binExpr.Right, schema)
	}

	// Look for simple equality or range conditions: column <op> literal
	switch ex := where.(type) {
	case *BinaryExpr:
		// Check for equality and range operators
		if ex.Op == TOKEN_EQ || ex.Op == TOKEN_LT || ex.Op == TOKEN_GT ||
			ex.Op == TOKEN_LE || ex.Op == TOKEN_GE {
			// Check if left is column and right is literal (or vice versa)
			var colName string
			var literal *LiteralExpr
			op := ex.Op

			if col, ok := ex.Left.(*ColumnRef); ok {
				if lit, ok := ex.Right.(*LiteralExpr); ok {
					colName = col.Name
					literal = lit
					// op stays the same: column < 10 means op is <
				}
			} else if col, ok := ex.Right.(*ColumnRef); ok {
				if lit, ok := ex.Left.(*LiteralExpr); ok {
					colName = col.Name
					literal = lit
					// Flip the operator: 10 < column means column > 10
					op = flipComparisonOperator(op)
				}
			}

			if colName != "" && literal != nil {
				// Look for an index on this column
				indexes := e.indexMgr.ListIndexes(tableName)
				for _, meta := range indexes {
					if len(meta.Columns) == 1 && strings.EqualFold(meta.Columns[0], colName) {
						// Found a matching single-column index
						key, err := encodeIndexValue(literal.Value)
						if err == nil {
							return &IndexScanInfo{
								IndexName: meta.Name,
								Key:       key,
								Op:        op,
								Column:    colName,
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// flipComparisonOperator flips a comparison operator (for when literal is on left side).
func flipComparisonOperator(op TokenType) TokenType {
	switch op {
	case TOKEN_LT:
		return TOKEN_GT
	case TOKEN_GT:
		return TOKEN_LT
	case TOKEN_LE:
		return TOKEN_GE
	case TOKEN_GE:
		return TOKEN_LE
	default:
		return op
	}
}

// executeIndexScan performs an index scan using the given IndexScanInfo.
func (e *MVCCExecutor) executeIndexScan(_ string, scanInfo *IndexScanInfo, _ *txn.Transaction) ([]storage.RID, error) {
	switch scanInfo.Op {
	case TOKEN_EQ:
		// For equality, use SearchAll to get all matching RIDs (handles non-unique indexes)
		rids, err := e.indexMgr.SearchAll(scanInfo.IndexName, scanInfo.Key)
		if err != nil {
			if err == btree.ErrKeyNotFound {
				return nil, nil // No matches
			}
			return nil, err
		}
		return rids, nil

	case TOKEN_LT:
		// column < value: scan from minimum to value (exclusive)
		rids, err := e.indexMgr.SearchRange(scanInfo.IndexName, nil, scanInfo.Key)
		if err != nil {
			if err == btree.ErrKeyNotFound {
				return nil, nil
			}
			return nil, err
		}
		// Exclude the boundary key (less than, not less than or equal)
		return e.filterExcludeBoundary(rids, scanInfo.Key, false), nil

	case TOKEN_LE:
		// column <= value: scan from minimum to value (inclusive)
		rids, err := e.indexMgr.SearchRange(scanInfo.IndexName, nil, scanInfo.Key)
		if err != nil {
			if err == btree.ErrKeyNotFound {
				return nil, nil
			}
			return nil, err
		}
		// Include the boundary (SearchRange is inclusive)
		return rids, nil

	case TOKEN_GT:
		// column > value: scan from value to maximum (exclusive)
		rids, err := e.indexMgr.SearchRange(scanInfo.IndexName, scanInfo.Key, nil)
		if err != nil {
			if err == btree.ErrKeyNotFound {
				return nil, nil
			}
			return nil, err
		}
		// Exclude the boundary key (greater than, not greater than or equal)
		return e.filterExcludeBoundary(rids, scanInfo.Key, true), nil

	case TOKEN_GE:
		// column >= value: scan from value to maximum (inclusive)
		rids, err := e.indexMgr.SearchRange(scanInfo.IndexName, scanInfo.Key, nil)
		if err != nil {
			if err == btree.ErrKeyNotFound {
				return nil, nil
			}
			return nil, err
		}
		// Include the boundary (SearchRange is inclusive)
		return rids, nil

	default:
		return nil, fmt.Errorf("unsupported index scan operator: %v", scanInfo.Op)
	}
}

// filterExcludeBoundary removes entries that exactly match the boundary key.
// For < operator, we want to exclude entries at the upper bound.
// For > operator, we want to exclude entries at the lower bound.
// The excludeStart parameter indicates if we're filtering the start (>) or end (<).
func (e *MVCCExecutor) filterExcludeBoundary(rids []storage.RID, _ []byte, _ bool) []storage.RID {
	// For now, we rely on the index to return keys in order.
	// If we need to filter exact matches, we would need the key values.
	// The B-tree SearchRange returns inclusive results, so we need post-filtering.
	// Since we don't have access to the keys here, we'll accept that SearchRange
	// returns inclusive results and the WHERE clause evaluation will filter correctly.
	// This is a simplification - a more complete implementation would track keys.
	return rids
}

// executeSelectWithJoins handles SELECT with JOIN clauses for MVCC executor.
func (e *MVCCExecutor) executeSelectWithJoins(stmt *SelectStmt, tx *txn.Transaction) (*Result, error) {
	if len(stmt.Joins) != 1 {
		return nil, fmt.Errorf("only single JOIN is currently supported")
	}

	join := stmt.Joins[0]
	if join.JoinType != "INNER" && join.JoinType != "LEFT" && join.JoinType != "RIGHT" && join.JoinType != "FULL" && join.JoinType != "CROSS" {
		return nil, fmt.Errorf("unsupported JOIN type: %s", join.JoinType)
	}

	cat := e.mtm.Catalog()
	leftMeta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// Handle LATERAL joins with subquery
	if join.Lateral && join.Subquery != nil {
		return e.executeSelectWithLateralJoin(stmt, tx, leftMeta, join)
	}

	rightMeta, err := cat.GetTable(join.TableName)
	if err != nil {
		return nil, err
	}

	// Build combined schema
	combinedSchema := &catalog.Schema{
		Columns: make([]catalog.Column, 0, len(leftMeta.Schema.Columns)+len(rightMeta.Schema.Columns)),
	}
	colTableMap := make(map[string]int)

	for i, col := range leftMeta.Schema.Columns {
		combinedSchema.Columns = append(combinedSchema.Columns, col)
		colTableMap[stmt.TableName+"."+col.Name] = i
		colTableMap[col.Name] = i
	}

	leftLen := len(leftMeta.Schema.Columns)
	rightLen := len(rightMeta.Schema.Columns)
	for i, col := range rightMeta.Schema.Columns {
		combinedSchema.Columns = append(combinedSchema.Columns, col)
		colTableMap[join.TableName+"."+col.Name] = leftLen + i
		if _, exists := colTableMap[col.Name]; !exists {
			colTableMap[col.Name] = leftLen + i
		}
	}

	// Create NULL rows for outer joins
	nullRightRow := make([]catalog.Value, rightLen)
	for i, col := range rightMeta.Schema.Columns {
		nullRightRow[i] = catalog.Null(col.Type)
	}
	nullLeftRow := make([]catalog.Value, leftLen)
	for i, col := range leftMeta.Schema.Columns {
		nullLeftRow[i] = catalog.Null(col.Type)
	}

	// Collect rows from both tables
	var leftRows [][]catalog.Value
	err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		rowCopy := make([]catalog.Value, len(row.Values))
		copy(rowCopy, row.Values)
		leftRows = append(leftRows, rowCopy)
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	var rightRows [][]catalog.Value
	err = e.mtm.Scan(join.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		rowCopy := make([]catalog.Value, len(row.Values))
		copy(rowCopy, row.Values)
		rightRows = append(rightRows, rowCopy)
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	// Perform join based on type
	var joinedRows [][]catalog.Value

	switch join.JoinType {
	case "INNER":
		for _, leftRow := range leftRows {
			for _, rightRow := range rightRows {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)

				match, err := e.evalJoinConditionMVCC(join.Condition, combinedSchema, combinedRow, colTableMap)
				if err != nil {
					return nil, err
				}
				if match {
					joinedRows = append(joinedRows, combinedRow)
				}
			}
		}

	case "LEFT":
		for _, leftRow := range leftRows {
			foundMatch := false
			for _, rightRow := range rightRows {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)

				match, err := e.evalJoinConditionMVCC(join.Condition, combinedSchema, combinedRow, colTableMap)
				if err != nil {
					return nil, err
				}
				if match {
					foundMatch = true
					joinedRows = append(joinedRows, combinedRow)
				}
			}
			if !foundMatch {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], nullRightRow)
				joinedRows = append(joinedRows, combinedRow)
			}
		}

	case "RIGHT":
		rightRowMatches := make(map[int]bool)
		for _, leftRow := range leftRows {
			for i, rightRow := range rightRows {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)

				match, err := e.evalJoinConditionMVCC(join.Condition, combinedSchema, combinedRow, colTableMap)
				if err != nil {
					return nil, err
				}
				if match {
					rightRowMatches[i] = true
					joinedRows = append(joinedRows, combinedRow)
				}
			}
		}
		// Add unmatched right rows
		for i, rightRow := range rightRows {
			if !rightRowMatches[i] {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, nullLeftRow)
				copy(combinedRow[leftLen:], rightRow)
				joinedRows = append(joinedRows, combinedRow)
			}
		}

	case "FULL":
		// Track which left and right rows have been matched
		leftRowMatches := make(map[int]bool)
		rightRowMatches := make(map[int]bool)

		// Match left with right
		for i, leftRow := range leftRows {
			for j, rightRow := range rightRows {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)

				match, err := e.evalJoinConditionMVCC(join.Condition, combinedSchema, combinedRow, colTableMap)
				if err != nil {
					return nil, err
				}
				if match {
					leftRowMatches[i] = true
					rightRowMatches[j] = true
					joinedRows = append(joinedRows, combinedRow)
				}
			}
		}

		// Add unmatched left rows with NULLs for right side
		for i, leftRow := range leftRows {
			if !leftRowMatches[i] {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], nullRightRow)
				joinedRows = append(joinedRows, combinedRow)
			}
		}

		// Add unmatched right rows with NULLs for left side
		for j, rightRow := range rightRows {
			if !rightRowMatches[j] {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, nullLeftRow)
				copy(combinedRow[leftLen:], rightRow)
				joinedRows = append(joinedRows, combinedRow)
			}
		}

	case "CROSS":
		// CROSS JOIN: Cartesian product of both tables (no condition)
		for _, leftRow := range leftRows {
			for _, rightRow := range rightRows {
				combinedRow := make([]catalog.Value, leftLen+rightLen)
				copy(combinedRow, leftRow)
				copy(combinedRow[leftLen:], rightRow)
				joinedRows = append(joinedRows, combinedRow)
			}
		}
	}

	// Apply WHERE filter
	var filteredRows [][]catalog.Value
	for _, row := range joinedRows {
		if stmt.Where != nil {
			match, err := e.evalJoinConditionMVCC(stmt.Where, combinedSchema, row, colTableMap)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		filteredRows = append(filteredRows, row)
	}

	// Determine output columns
	var outputCols []string
	var colIndices []int

	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		for i, c := range combinedSchema.Columns {
			outputCols = append(outputCols, c.Name)
			colIndices = append(colIndices, i)
		}
	} else {
		for _, sc := range stmt.Columns {
			idx, ok := colTableMap[sc.Name]
			if !ok {
				return nil, fmt.Errorf("unknown column: %s", sc.Name)
			}
			if sc.Alias != "" {
				outputCols = append(outputCols, sc.Alias)
			} else {
				outputCols = append(outputCols, sc.Name)
			}
			colIndices = append(colIndices, idx)
		}
	}

	// Project columns
	resultRows := make([][]catalog.Value, len(filteredRows))
	for i, row := range filteredRows {
		projectedRow := make([]catalog.Value, len(colIndices))
		for j, idx := range colIndices {
			projectedRow[j] = row[idx]
		}
		resultRows[i] = projectedRow
	}

	// Apply ORDER BY
	if len(stmt.OrderBy) > 0 {
		orderByIndices := make([]int, len(stmt.OrderBy))
		for i, ob := range stmt.OrderBy {
			idx, ok := colTableMap[ob.Column]
			if !ok {
				return nil, fmt.Errorf("unknown column in ORDER BY: %s", ob.Column)
			}
			orderByIndices[i] = idx
		}
		sortRowsMVCC(filteredRows, stmt.OrderBy, orderByIndices)
		for i, row := range filteredRows {
			projectedRow := make([]catalog.Value, len(colIndices))
			for j, idx := range colIndices {
				projectedRow[j] = row[idx]
			}
			resultRows[i] = projectedRow
		}
	}

	// Apply LIMIT/OFFSET
	if stmt.Offset != nil && *stmt.Offset > 0 {
		offset := int(*stmt.Offset)
		if offset >= len(resultRows) {
			resultRows = nil
		} else {
			resultRows = resultRows[offset:]
		}
	}
	if stmt.Limit != nil {
		limit := int(*stmt.Limit)
		if limit < len(resultRows) {
			resultRows = resultRows[:limit]
		}
	}

	// Apply DISTINCT
	if stmt.Distinct {
		resultRows = deduplicateRows(resultRows)
	}

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

// evalJoinConditionMVCC evaluates a join condition for MVCC executor.
func (e *MVCCExecutor) evalJoinConditionMVCC(expr Expression, schema *catalog.Schema, row []catalog.Value, colMap map[string]int) (bool, error) {
	switch ex := expr.(type) {
	case *BinaryExpr:
		switch ex.Op {
		case TOKEN_AND:
			left, err := e.evalJoinConditionMVCC(ex.Left, schema, row, colMap)
			if err != nil {
				return false, err
			}
			if !left {
				return false, nil
			}
			return e.evalJoinConditionMVCC(ex.Right, schema, row, colMap)

		case TOKEN_OR:
			left, err := e.evalJoinConditionMVCC(ex.Left, schema, row, colMap)
			if err != nil {
				return false, err
			}
			if left {
				return true, nil
			}
			return e.evalJoinConditionMVCC(ex.Right, schema, row, colMap)

		default:
			// Comparison operators
			leftVal, err := e.evalJoinExprMVCC(ex.Left, schema, row, colMap)
			if err != nil {
				return false, err
			}
			rightVal, err := e.evalJoinExprMVCC(ex.Right, schema, row, colMap)
			if err != nil {
				return false, err
			}
			return compareValuesMVCC(leftVal, rightVal, ex.Op)
		}
	case *UnaryExpr:
		if ex.Op == TOKEN_NOT {
			result, err := e.evalJoinConditionMVCC(ex.Expr, schema, row, colMap)
			if err != nil {
				return false, err
			}
			return !result, nil
		}
	}
	return false, fmt.Errorf("unsupported join condition type: %T", expr)
}

// evalJoinExprMVCC evaluates an expression in a join context for MVCC executor.
func (e *MVCCExecutor) evalJoinExprMVCC(expr Expression, _ *catalog.Schema, row []catalog.Value, colMap map[string]int) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *ColumnRef:
		// ColumnRef.Name may be "table.column" or just "column"
		idx, ok := colMap[ex.Name]
		if !ok {
			return catalog.Value{}, fmt.Errorf("unknown column in join: %s", ex.Name)
		}
		return row[idx], nil
	case *LiteralExpr:
		return ex.Value, nil
	}
	return catalog.Value{}, fmt.Errorf("unsupported expression type in join: %T", expr)
}

// executeSelectWithIndex performs a SELECT using an index scan when possible.
func (e *MVCCExecutor) executeSelectWithIndex(stmt *SelectStmt, tx *txn.Transaction) (*Result, bool, error) {
	cat := e.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, false, err
	}

	// Check if we can use an index
	scanInfo := e.findUsableIndex(stmt.TableName, stmt.Where, meta.Schema)
	if scanInfo == nil {
		return nil, false, nil // No usable index, fall back to table scan
	}

	// Perform index scan
	rids, err := e.executeIndexScan(stmt.TableName, scanInfo, tx)
	if err != nil {
		return nil, false, err
	}

	// Determine output columns
	var outCols []string
	var colIndices []int

	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		for i, col := range meta.Columns {
			outCols = append(outCols, col.Name)
			colIndices = append(colIndices, i)
		}
	} else {
		for _, sc := range stmt.Columns {
			col, idx := meta.Schema.ColumnByName(sc.Name)
			if col == nil {
				return nil, false, fmt.Errorf("unknown column: %s", sc.Name)
			}
			outCols = append(outCols, sc.Name)
			colIndices = append(colIndices, idx)
		}
	}

	// Fetch and filter rows by RID
	var rows [][]catalog.Value
	for _, rid := range rids {
		// Fetch the row using MVCC visibility
		row, err := e.mtm.FetchWithMVCC(stmt.TableName, rid)
		if err != nil {
			continue // Row may have been deleted
		}

		// Check MVCC visibility
		if !txn.IsVisible(row.Header, tx.Snapshot, e.mtm.TxnManager(), tx.ID) {
			continue
		}

		// Apply any remaining WHERE conditions
		// (the index only covers part of the WHERE clause potentially)
		if stmt.Where != nil {
			match, err := e.evalCondition(stmt.Where, meta.Schema, row.Values, tx)
			if err != nil {
				return nil, false, err
			}
			if !match {
				continue
			}
		}

		// Project columns
		outRow := make([]catalog.Value, len(colIndices))
		for i, idx := range colIndices {
			outRow[i] = row.Values[idx]
		}
		rows = append(rows, outRow)
	}

	return &Result{
		Columns: outCols,
		Rows:    rows,
	}, true, nil
}

// executeAlter handles ALTER TABLE statements.
func (e *MVCCExecutor) executeAlter(stmt *AlterTableStmt) (*Result, error) {
	meta, err := e.mtm.GetTableMeta(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %q does not exist", stmt.TableName)
	}

	switch stmt.Action {
	case "ADD COLUMN":
		// Check if column already exists
		for _, col := range meta.Schema.Columns {
			if strings.EqualFold(col.Name, stmt.ColumnDef.Name) {
				return nil, fmt.Errorf("column %q already exists in table %q", stmt.ColumnDef.Name, stmt.TableName)
			}
		}
		// Add the new column to schema
		newCol := catalog.Column{
			Name:       stmt.ColumnDef.Name,
			Type:       stmt.ColumnDef.Type,
			NotNull:    stmt.ColumnDef.NotNull,
			HasDefault: stmt.ColumnDef.HasDefault,
		}
		// Handle default value
		if stmt.ColumnDef.HasDefault && stmt.ColumnDef.Default != nil {
			if lit, ok := stmt.ColumnDef.Default.(*LiteralExpr); ok {
				val := lit.Value
				newCol.DefaultValue = &val
			}
		}
		meta.Schema.Columns = append(meta.Schema.Columns, newCol)
		meta.Columns = meta.Schema.Columns
		// Persist updated metadata
		if err := e.mtm.UpdateTableMeta(meta); err != nil {
			return nil, fmt.Errorf("failed to update table metadata: %w", err)
		}
		return &Result{Message: fmt.Sprintf("Column %q added to table %q.", stmt.ColumnDef.Name, stmt.TableName)}, nil

	case "DROP COLUMN":
		// Find and remove the column
		idx := -1
		for i, col := range meta.Schema.Columns {
			if strings.EqualFold(col.Name, stmt.ColumnName) {
				idx = i
				break
			}
		}
		if idx == -1 {
			return nil, fmt.Errorf("column %q does not exist in table %q", stmt.ColumnName, stmt.TableName)
		}
		// Remove column from schema
		meta.Schema.Columns = append(meta.Schema.Columns[:idx], meta.Schema.Columns[idx+1:]...)
		meta.Columns = meta.Schema.Columns
		// Persist updated metadata
		if err := e.mtm.UpdateTableMeta(meta); err != nil {
			return nil, fmt.Errorf("failed to update table metadata: %w", err)
		}
		return &Result{Message: fmt.Sprintf("Column %q dropped from table %q.", stmt.ColumnName, stmt.TableName)}, nil

	case "RENAME TO":
		// Rename the table
		if err := e.mtm.RenameTable(stmt.TableName, stmt.NewName); err != nil {
			return nil, fmt.Errorf("failed to rename table: %w", err)
		}
		return &Result{Message: fmt.Sprintf("Table %q renamed to %q.", stmt.TableName, stmt.NewName)}, nil

	case "RENAME COLUMN":
		// Find and rename the column
		found := false
		for i, col := range meta.Schema.Columns {
			if strings.EqualFold(col.Name, stmt.ColumnName) {
				meta.Schema.Columns[i].Name = stmt.NewName
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column %q does not exist in table %q", stmt.ColumnName, stmt.TableName)
		}
		meta.Columns = meta.Schema.Columns
		// Persist updated metadata
		if err := e.mtm.UpdateTableMeta(meta); err != nil {
			return nil, fmt.Errorf("failed to update table metadata: %w", err)
		}
		return &Result{Message: fmt.Sprintf("Column %q renamed to %q in table %q.", stmt.ColumnName, stmt.NewName, stmt.TableName)}, nil

	default:
		return nil, fmt.Errorf("unsupported ALTER TABLE action: %s", stmt.Action)
	}
}

// executeTruncate handles TRUNCATE TABLE statements.
func (e *MVCCExecutor) executeTruncate(stmt *TruncateTableStmt) (*Result, error) {
	// Verify table exists
	_, err := e.mtm.GetTableMeta(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("table %q does not exist", stmt.TableName)
	}

	// Truncate the table (delete all rows)
	count, err := e.mtm.TruncateTable(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("failed to truncate table: %w", err)
	}

	return &Result{
		Message:      fmt.Sprintf("Table %q truncated. %d row(s) deleted.", stmt.TableName, count),
		RowsAffected: count,
	}, nil
}

// executeShow handles SHOW statements.
func (e *MVCCExecutor) executeShow(stmt *ShowStmt) (*Result, error) {
	switch stmt.ShowType {
	case "TABLES":
		// List all tables
		tables := e.mtm.ListTables()
		result := &Result{
			Columns: []string{"table_name"},
			Rows:    make([][]catalog.Value, 0, len(tables)),
		}
		for _, t := range tables {
			result.Rows = append(result.Rows, []catalog.Value{catalog.NewText(t)})
		}
		return result, nil

	case "CREATE TABLE":
		// Show CREATE TABLE statement
		meta, err := e.mtm.GetTableMeta(stmt.TableName)
		if err != nil {
			return nil, fmt.Errorf("table %q does not exist", stmt.TableName)
		}
		ddl := generateCreateTableDDL(meta)
		return &Result{
			Columns: []string{"Create Table"},
			Rows:    [][]catalog.Value{{catalog.NewText(ddl)}},
		}, nil

	case "DATABASES":
		// SHOW DATABASES is handled at session level where DatabaseManager is available
		// This case should not be reached, but return a fallback just in case
		return &Result{
			Columns: []string{"database_name"},
			Rows:    [][]catalog.Value{{catalog.NewText("default")}},
		}, nil

	case "TRIGGERS":
		// List triggers (optionally filtered by table)
		if e.triggerCat == nil {
			return &Result{
				Columns: []string{"trigger_name", "table_name", "timing", "event", "function"},
				Rows:    [][]catalog.Value{},
				Message: "Trigger support not enabled",
			}, nil
		}
		var triggers []*catalog.TriggerMeta
		if stmt.TableName != "" {
			triggers = e.triggerCat.GetTriggersForTable(stmt.TableName)
		} else {
			triggers = e.triggerCat.ListAllTriggers()
		}
		result := &Result{
			Columns: []string{"trigger_name", "table_name", "timing", "event", "for_each", "function", "enabled"},
			Rows:    make([][]catalog.Value, 0, len(triggers)),
		}
		for _, t := range triggers {
			forEach := "STATEMENT"
			if t.ForEachRow {
				forEach = "ROW"
			}
			enabled := "YES"
			if !t.Enabled {
				enabled = "NO"
			}
			result.Rows = append(result.Rows, []catalog.Value{
				catalog.NewText(t.Name),
				catalog.NewText(t.TableName),
				catalog.NewText(t.Timing.String()),
				catalog.NewText(t.Event.String()),
				catalog.NewText(forEach),
				catalog.NewText(t.FunctionName),
				catalog.NewText(enabled),
			})
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported SHOW type: %s", stmt.ShowType)
	}
}

// executeExplain executes an EXPLAIN statement
func (e *MVCCExecutor) executeExplain(stmt *ExplainStmt, tx *txn.Transaction) (*Result, error) {
	selectStmt, ok := stmt.Statement.(*SelectStmt)
	if !ok {
		return nil, fmt.Errorf("EXPLAIN only supports SELECT statements")
	}

	// Get table metadata to validate the table exists
	meta, err := e.mtm.GetTableMeta(selectStmt.TableName)
	if err != nil {
		return nil, err
	}

	// Create a planner and generate the plan
	planner := NewPlanner(e.indexMgr)
	plan := planner.Plan(selectStmt, meta)

	// Build the explanation
	var details []string

	// Basic plan type
	planExplain := plan.Explain()
	details = append(details, planExplain)

	if plan.Type == PlanIndexScan {
		details = append(details, "  Using index: "+plan.IndexName)
	}

	if plan.RemainingWhere != nil {
		details = append(details, "  Filter: <where clause>")
	}

	if selectStmt.OrderBy != nil {
		details = append(details, "  Sort: ORDER BY clause")
	}

	if selectStmt.Limit != nil {
		details = append(details, "  Limit: LIMIT clause")
	}

	// If ANALYZE is specified, also run the query and show row count
	if stmt.Analyze {
		result, err := e.executeSelect(selectStmt, tx)
		if err != nil {
			return nil, fmt.Errorf("EXPLAIN ANALYZE failed: %w", err)
		}
		details = append(details, fmt.Sprintf("  Actual rows: %d", len(result.Rows)))
	}

	// Build result rows
	var explanation [][]catalog.Value
	for _, detail := range details {
		explanation = append(explanation, []catalog.Value{catalog.NewText(detail)})
	}

	return &Result{
		Columns: []string{"QUERY PLAN"},
		Rows:    explanation,
	}, nil
}

// evalCaseExpr evaluates a CASE WHEN expression.
func (e *MVCCExecutor) evalCaseExpr(caseExpr *CaseExpr, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	// Simple CASE: CASE operand WHEN val1 THEN res1 ...
	// Searched CASE: CASE WHEN cond1 THEN res1 ...

	if caseExpr.Operand != nil {
		// Simple CASE - compare operand with each WHEN value
		operandVal, err := e.evalExpr(caseExpr.Operand, schema, row, tx)
		if err != nil {
			return catalog.Value{}, err
		}

		for _, when := range caseExpr.Whens {
			whenVal, err := e.evalExpr(when.Condition, schema, row, tx)
			if err != nil {
				return catalog.Value{}, err
			}

			// Compare operand with WHEN value
			equal, err := compareValuesMVCC(operandVal, whenVal, TOKEN_EQ)
			if err != nil {
				return catalog.Value{}, err
			}

			if equal {
				return e.evalExpr(when.Result, schema, row, tx)
			}
		}
	} else {
		// Searched CASE - evaluate each WHEN condition as boolean
		for _, when := range caseExpr.Whens {
			condResult, err := e.evalCondition(when.Condition, schema, row, nil)
			if err != nil {
				return catalog.Value{}, err
			}

			if condResult {
				return e.evalExpr(when.Result, schema, row, tx)
			}
		}
	}

	// No WHEN matched, return ELSE or NULL
	if caseExpr.Else != nil {
		return e.evalExpr(caseExpr.Else, schema, row, tx)
	}

	// No ELSE clause - return NULL
	return catalog.Null(catalog.TypeUnknown), nil
}

// validateCheckConstraints validates all CHECK constraints for a row.
func (e *MVCCExecutor) validateCheckConstraints(schema *catalog.Schema, values []catalog.Value) error {
	for i, col := range schema.Columns {
		if col.CheckExpr == "" {
			continue
		}

		// Parse the CHECK expression directly
		parser := NewParser(col.CheckExpr)
		expr, err := parser.parseExpression()
		if err != nil {
			return fmt.Errorf("invalid CHECK expression for column %s: %w", col.Name, err)
		}

		// Evaluate the CHECK expression with the row values
		result, err := e.evalCondition(expr, schema, values, nil)
		if err != nil {
			return fmt.Errorf("error evaluating CHECK constraint for column %s: %w", col.Name, err)
		}

		if !result {
			return fmt.Errorf("CHECK constraint violated for column %s (value: %v)", col.Name, values[i])
		}
	}

	return nil
}

// evalCastExpr evaluates a CAST expression for MVCC executor.
func (e *MVCCExecutor) evalCastExpr(cast *CastExpr, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	val, err := e.evalExpr(cast.Expr, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}

	if val.IsNull {
		return catalog.Null(cast.TargetType), nil
	}

	// Handle CAST type conversions
	switch cast.TargetType {
	case catalog.TypeInt32:
		switch val.Type {
		case catalog.TypeInt32:
			return val, nil
		case catalog.TypeInt64:
			if val.Int64 >= -2147483648 && val.Int64 <= 2147483647 {
				return catalog.NewInt32(int32(val.Int64)), nil
			}
			return catalog.Value{}, fmt.Errorf("value %d out of range for INT", val.Int64)
		case catalog.TypeText:
			i, err := strconv.ParseInt(val.Text, 10, 32)
			if err != nil {
				return catalog.Value{}, fmt.Errorf("cannot cast '%s' to INT", val.Text)
			}
			return catalog.NewInt32(int32(i)), nil
		case catalog.TypeBool:
			if val.Bool {
				return catalog.NewInt32(1), nil
			}
			return catalog.NewInt32(0), nil
		}

	case catalog.TypeInt64:
		switch val.Type {
		case catalog.TypeInt32:
			return catalog.NewInt64(int64(val.Int32)), nil
		case catalog.TypeInt64:
			return val, nil
		case catalog.TypeText:
			i, err := strconv.ParseInt(val.Text, 10, 64)
			if err != nil {
				return catalog.Value{}, fmt.Errorf("cannot cast '%s' to BIGINT", val.Text)
			}
			return catalog.NewInt64(i), nil
		case catalog.TypeBool:
			if val.Bool {
				return catalog.NewInt64(1), nil
			}
			return catalog.NewInt64(0), nil
		}

	case catalog.TypeText:
		switch val.Type {
		case catalog.TypeInt32:
			return catalog.NewText(strconv.FormatInt(int64(val.Int32), 10)), nil
		case catalog.TypeInt64:
			return catalog.NewText(strconv.FormatInt(val.Int64, 10)), nil
		case catalog.TypeText:
			return val, nil
		case catalog.TypeBool:
			if val.Bool {
				return catalog.NewText("true"), nil
			}
			return catalog.NewText("false"), nil
		case catalog.TypeTimestamp:
			return catalog.NewText(val.Timestamp.Format(time.RFC3339)), nil
		}

	case catalog.TypeBool:
		switch val.Type {
		case catalog.TypeInt32:
			return catalog.NewBool(val.Int32 != 0), nil
		case catalog.TypeInt64:
			return catalog.NewBool(val.Int64 != 0), nil
		case catalog.TypeText:
			lower := strings.ToLower(val.Text)
			if lower == "true" || lower == "1" || lower == "yes" {
				return catalog.NewBool(true), nil
			}
			if lower == "false" || lower == "0" || lower == "no" {
				return catalog.NewBool(false), nil
			}
			return catalog.Value{}, fmt.Errorf("cannot cast '%s' to BOOL", val.Text)
		case catalog.TypeBool:
			return val, nil
		}

	case catalog.TypeTimestamp:
		switch val.Type {
		case catalog.TypeText:
			formats := []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"}
			for _, format := range formats {
				if t, err := time.Parse(format, val.Text); err == nil {
					return catalog.NewTimestamp(t), nil
				}
			}
			return catalog.Value{}, fmt.Errorf("cannot cast '%s' to TIMESTAMP", val.Text)
		case catalog.TypeTimestamp:
			return val, nil
		}

	case catalog.TypeJSON:
		switch val.Type {
		case catalog.TypeText:
			// Validate JSON by attempting to parse it
			if !isValidJSON(val.Text) {
				return catalog.Value{}, fmt.Errorf("invalid JSON: %s", val.Text)
			}
			return catalog.NewJSON(val.Text), nil
		case catalog.TypeJSON:
			return val, nil
		}
	}

	return catalog.Value{}, fmt.Errorf("cannot cast %v to %v", val.Type, cast.TargetType)
}

// isValidJSON checks if a string is valid JSON
func isValidJSON(s string) bool {
	var js interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}

// evalJSONAccess evaluates -> and ->> operators
func (e *MVCCExecutor) evalJSONAccess(expr *JSONAccessExpr, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	objVal, err := e.evalExpr(expr.Object, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}

	keyVal, err := e.evalExpr(expr.Key, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}

	if objVal.IsNull {
		return catalog.Null(catalog.TypeJSON), nil
	}

	// Get JSON string
	var jsonStr string
	switch objVal.Type {
	case catalog.TypeJSON:
		jsonStr = objVal.JSON
	case catalog.TypeText:
		jsonStr = objVal.Text
	default:
		return catalog.Value{}, fmt.Errorf("-> operator requires JSON or TEXT, got %v", objVal.Type)
	}

	// Parse JSON
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return catalog.Value{}, fmt.Errorf("invalid JSON: %v", err)
	}

	// Access by key or index
	var result interface{}
	switch keyVal.Type {
	case catalog.TypeText:
		// Object field access
		obj, ok := data.(map[string]interface{})
		if !ok {
			return catalog.Null(catalog.TypeJSON), nil
		}
		result = obj[keyVal.Text]
	case catalog.TypeInt32, catalog.TypeInt64:
		// Array index access
		arr, ok := data.([]interface{})
		if !ok {
			return catalog.Null(catalog.TypeJSON), nil
		}
		var idx int
		if keyVal.Type == catalog.TypeInt32 {
			idx = int(keyVal.Int32)
		} else {
			idx = int(keyVal.Int64)
		}
		if idx < 0 || idx >= len(arr) {
			return catalog.Null(catalog.TypeJSON), nil
		}
		result = arr[idx]
	default:
		return catalog.Value{}, fmt.Errorf("JSON key must be TEXT or INT, got %v", keyVal.Type)
	}

	if result == nil {
		return catalog.Null(catalog.TypeJSON), nil
	}

	if expr.AsText {
		// ->> returns text
		switch v := result.(type) {
		case string:
			return catalog.NewText(v), nil
		case float64:
			return catalog.NewText(strconv.FormatFloat(v, 'f', -1, 64)), nil
		case bool:
			return catalog.NewText(strconv.FormatBool(v)), nil
		default:
			// For objects/arrays, serialize to JSON string
			bytes, _ := json.Marshal(v)
			return catalog.NewText(string(bytes)), nil
		}
	} else {
		// -> returns JSON
		bytes, err := json.Marshal(result)
		if err != nil {
			return catalog.Value{}, err
		}
		return catalog.NewJSON(string(bytes)), nil
	}
}

// evalJSONPath evaluates #> and #>> operators
func (e *MVCCExecutor) evalJSONPath(expr *JSONPathExpr, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	objVal, err := e.evalExpr(expr.Object, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}

	if objVal.IsNull {
		return catalog.Null(catalog.TypeJSON), nil
	}

	// Get JSON string
	var jsonStr string
	switch objVal.Type {
	case catalog.TypeJSON:
		jsonStr = objVal.JSON
	case catalog.TypeText:
		jsonStr = objVal.Text
	default:
		return catalog.Value{}, fmt.Errorf("#> operator requires JSON or TEXT, got %v", objVal.Type)
	}

	// Parse JSON
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return catalog.Value{}, fmt.Errorf("invalid JSON: %v", err)
	}

	// Navigate the path
	current := data
	for _, pathExpr := range expr.Path {
		pathVal, err := e.evalExpr(pathExpr, schema, row, tx)
		if err != nil {
			return catalog.Value{}, err
		}

		if current == nil {
			return catalog.Null(catalog.TypeJSON), nil
		}

		switch pathVal.Type {
		case catalog.TypeText:
			obj, ok := current.(map[string]interface{})
			if !ok {
				return catalog.Null(catalog.TypeJSON), nil
			}
			current = obj[pathVal.Text]
		case catalog.TypeInt32, catalog.TypeInt64:
			arr, ok := current.([]interface{})
			if !ok {
				return catalog.Null(catalog.TypeJSON), nil
			}
			var idx int
			if pathVal.Type == catalog.TypeInt32 {
				idx = int(pathVal.Int32)
			} else {
				idx = int(pathVal.Int64)
			}
			if idx < 0 || idx >= len(arr) {
				return catalog.Null(catalog.TypeJSON), nil
			}
			current = arr[idx]
		default:
			return catalog.Value{}, fmt.Errorf("JSON path element must be TEXT or INT")
		}
	}

	if current == nil {
		return catalog.Null(catalog.TypeJSON), nil
	}

	if expr.AsText {
		// #>> returns text
		switch v := current.(type) {
		case string:
			return catalog.NewText(v), nil
		case float64:
			return catalog.NewText(strconv.FormatFloat(v, 'f', -1, 64)), nil
		case bool:
			return catalog.NewText(strconv.FormatBool(v)), nil
		default:
			bytes, _ := json.Marshal(v)
			return catalog.NewText(string(bytes)), nil
		}
	} else {
		// #> returns JSON
		bytes, err := json.Marshal(current)
		if err != nil {
			return catalog.Value{}, err
		}
		return catalog.NewJSON(string(bytes)), nil
	}
}

// evalJSONContains evaluates @> and <@ operators
func (e *MVCCExecutor) evalJSONContains(expr *JSONContainsExpr, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	leftVal, err := e.evalExpr(expr.Left, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}

	rightVal, err := e.evalExpr(expr.Right, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}

	if leftVal.IsNull || rightVal.IsNull {
		return catalog.Null(catalog.TypeBool), nil
	}

	// Get JSON strings
	var leftJSON, rightJSON string
	switch leftVal.Type {
	case catalog.TypeJSON:
		leftJSON = leftVal.JSON
	case catalog.TypeText:
		leftJSON = leftVal.Text
	default:
		return catalog.Value{}, fmt.Errorf("@> operator requires JSON or TEXT")
	}

	switch rightVal.Type {
	case catalog.TypeJSON:
		rightJSON = rightVal.JSON
	case catalog.TypeText:
		rightJSON = rightVal.Text
	default:
		return catalog.Value{}, fmt.Errorf("@> operator requires JSON or TEXT")
	}

	// Parse both JSON values
	var left, right interface{}
	if err := json.Unmarshal([]byte(leftJSON), &left); err != nil {
		return catalog.Value{}, fmt.Errorf("invalid JSON on left: %v", err)
	}
	if err := json.Unmarshal([]byte(rightJSON), &right); err != nil {
		return catalog.Value{}, fmt.Errorf("invalid JSON on right: %v", err)
	}

	// Check containment
	var contains bool
	if expr.Contains {
		// @> : left contains right
		contains = jsonContains(left, right)
	} else {
		// <@ : left is contained by right
		contains = jsonContains(right, left)
	}

	return catalog.NewBool(contains), nil
}

// jsonContains checks if container contains contained
func jsonContains(container, contained interface{}) bool {
	switch c := contained.(type) {
	case map[string]interface{}:
		containerObj, ok := container.(map[string]interface{})
		if !ok {
			return false
		}
		for k, v := range c {
			containerVal, exists := containerObj[k]
			if !exists {
				return false
			}
			if !jsonContains(containerVal, v) {
				return false
			}
		}
		return true
	case []interface{}:
		containerArr, ok := container.([]interface{})
		if !ok {
			return false
		}
		// All elements in contained must be in container
		for _, cv := range c {
			found := false
			for _, containerVal := range containerArr {
				if jsonEqual(containerVal, cv) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	default:
		return jsonEqual(container, contained)
	}
}

// jsonEqual checks if two JSON values are equal
func jsonEqual(a, b interface{}) bool {
	// Simple equality check using JSON serialization
	aBytes, _ := json.Marshal(a)
	bBytes, _ := json.Marshal(b)
	return string(aBytes) == string(bBytes)
}

// evalJSONExists evaluates ?, ?|, ?& operators
func (e *MVCCExecutor) evalJSONExists(expr *JSONExistsExpr, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	objVal, err := e.evalExpr(expr.Object, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}

	if objVal.IsNull {
		return catalog.Null(catalog.TypeBool), nil
	}

	// Get JSON string
	var jsonStr string
	switch objVal.Type {
	case catalog.TypeJSON:
		jsonStr = objVal.JSON
	case catalog.TypeText:
		jsonStr = objVal.Text
	default:
		return catalog.Value{}, fmt.Errorf("? operator requires JSON or TEXT")
	}

	// Parse JSON
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return catalog.Value{}, fmt.Errorf("invalid JSON: %v", err)
	}

	// Get object keys
	obj, ok := data.(map[string]interface{})
	if !ok {
		return catalog.NewBool(false), nil
	}

	// Evaluate key expressions
	var keys []string
	for _, keyExpr := range expr.Keys {
		keyVal, err := e.evalExpr(keyExpr, schema, row, tx)
		if err != nil {
			return catalog.Value{}, err
		}
		if keyVal.Type == catalog.TypeText {
			keys = append(keys, keyVal.Text)
		} else {
			return catalog.Value{}, fmt.Errorf("JSON key must be TEXT")
		}
	}

	switch expr.Mode {
	case "any": // ? : single key exists
		if len(keys) == 0 {
			return catalog.NewBool(false), nil
		}
		_, exists := obj[keys[0]]
		return catalog.NewBool(exists), nil
	case "or": // ?| : any key exists
		for _, k := range keys {
			if _, exists := obj[k]; exists {
				return catalog.NewBool(true), nil
			}
		}
		return catalog.NewBool(false), nil
	case "and": // ?& : all keys exist
		for _, k := range keys {
			if _, exists := obj[k]; !exists {
				return catalog.NewBool(false), nil
			}
		}
		return catalog.NewBool(true), nil
	default:
		return catalog.Value{}, fmt.Errorf("unknown JSON exists mode: %s", expr.Mode)
	}
}

// executeCreateView creates a new view (MVCC version).
func (e *MVCCExecutor) executeCreateView(_ *CreateViewStmt) (*Result, error) {
	return nil, fmt.Errorf("CREATE VIEW should be created via Session/Executor; use CREATE VIEW via Session")
}

// executeDropView drops a view (MVCC version).
func (e *MVCCExecutor) executeDropView(stmt *DropViewStmt) (*Result, error) {
	if stmt.IfExists {
		return &Result{Message: fmt.Sprintf("View '%s' does not exist (IF EXISTS specified).", stmt.ViewName)}, nil
	}
	e.viewsMu.Lock()
	defer e.viewsMu.Unlock()
	if _, exists := e.views[stmt.ViewName]; !exists {
		return nil, fmt.Errorf("view '%s' does not exist", stmt.ViewName)
	}
	delete(e.views, stmt.ViewName)
	return &Result{Message: fmt.Sprintf("View '%s' dropped.", stmt.ViewName)}, nil
}

// executeCreateViewMVCC stores view definitions for later expansion during SELECT.
func (e *MVCCExecutor) executeCreateViewMVCC(stmt *CreateViewStmt, tx *txn.Transaction) (*Result, error) {
	e.viewsMu.Lock()
	defer e.viewsMu.Unlock()

	if _, exists := e.views[stmt.ViewName]; exists {
		if stmt.OrReplace {
			delete(e.views, stmt.ViewName)
		} else {
			return nil, fmt.Errorf("view '%s' already exists", stmt.ViewName)
		}
	}

	// Validate the view query by executing it under a temp/read tx
	if tx == nil {
		// start a short-lived transaction for validation
		tmpTx := e.mtm.TxnManager().Begin()
		defer func() { _ = tmpTx }()
		if _, err := e.executeSelect(stmt.Query, tmpTx); err != nil {
			return nil, fmt.Errorf("invalid view query: %w", err)
		}
	} else {
		if _, err := e.executeSelect(stmt.Query, tx); err != nil {
			return nil, fmt.Errorf("invalid view query: %w", err)
		}
	}

	e.views[stmt.ViewName] = &ViewDef{
		Name:    stmt.ViewName,
		Query:   stmt.Query,
		Columns: append([]string{}, stmt.Columns...),
	}

	return &Result{Message: fmt.Sprintf("View '%s' created.", stmt.ViewName)}, nil
}

// executeSelectFromView expands and executes a SELECT from a view definition under a transaction.
func (e *MVCCExecutor) executeSelectFromView(outerStmt *SelectStmt, viewDef *ViewDef, tx *txn.Transaction) (*Result, error) {
	// Execute the view's underlying query
	viewResult, err := e.executeSelect(viewDef.Query, tx)
	if err != nil {
		return nil, fmt.Errorf("error executing view '%s': %w", viewDef.Name, err)
	}

	// Apply column aliases if provided
	if len(viewDef.Columns) > 0 {
		if len(viewDef.Columns) != len(viewResult.Columns) {
			return nil, fmt.Errorf("view '%s' column count mismatch: %d aliases for %d columns", viewDef.Name, len(viewDef.Columns), len(viewResult.Columns))
		}
		copy(viewResult.Columns, viewDef.Columns)
	}

	// If outer query is SELECT * FROM view, return view result directly
	if len(outerStmt.Columns) == 1 && outerStmt.Columns[0].Star {
		return viewResult, nil
	}

	// Build a schema for the view result to evaluate WHERE/projections
	viewSchema := &catalog.Schema{Columns: make([]catalog.Column, len(viewResult.Columns))}
	for i, colName := range viewResult.Columns {
		colType := catalog.TypeUnknown
		if len(viewResult.Rows) > 0 {
			colType = viewResult.Rows[0][i].Type
		}
		viewSchema.Columns[i] = catalog.Column{Name: colName, Type: colType}
	}

	// Filter view rows using outer WHERE
	var filtered [][]catalog.Value
	for _, row := range viewResult.Rows {
		if outerStmt.Where != nil {
			match, err := e.evalCondition(outerStmt.Where, viewSchema, row, tx)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		filtered = append(filtered, row)
	}

	// Build output column names
	var outCols []string
	for _, sc := range outerStmt.Columns {
		if sc.Star {
			outCols = append(outCols, viewResult.Columns...)
			continue
		}
		if sc.Expression != nil {
			alias := sc.Alias
			if alias == "" {
				alias = "expr"
			}
			outCols = append(outCols, alias)
			continue
		}
		if sc.Alias != "" {
			outCols = append(outCols, sc.Alias)
		} else {
			outCols = append(outCols, sc.Name)
		}
	}

	// Build output rows
	var outRows [][]catalog.Value
	hasStar := false
	for _, sc := range outerStmt.Columns {
		if sc.Star {
			hasStar = true
			break
		}
	}

	for _, row := range filtered {
		if !hasStar {
			outRow := make([]catalog.Value, len(outerStmt.Columns))
			for i, sc := range outerStmt.Columns {
				if sc.Expression != nil {
					val, err := e.evalExpr(sc.Expression, viewSchema, row, tx)
					if err != nil {
						return nil, err
					}
					outRow[i] = val
					continue
				}
				col, idx := viewSchema.ColumnByName(sc.Name)
				if col == nil {
					return nil, fmt.Errorf("unknown column '%s' in view '%s'", sc.Name, viewDef.Name)
				}
				outRow[i] = row[idx]
			}
			outRows = append(outRows, outRow)
			continue
		}

		// STAR present - expand into the outCols layout
		outRow := make([]catalog.Value, len(outCols))
		pos := 0
		for _, sc := range outerStmt.Columns {
			if sc.Star {
				for i := 0; i < len(viewResult.Columns); i++ {
					if i < len(row) {
						outRow[pos] = row[i]
					} else {
						outRow[pos] = catalog.Null(catalog.TypeUnknown)
					}
					pos++
				}
				continue
			}
			if sc.Expression != nil {
				val, err := e.evalExpr(sc.Expression, viewSchema, row, tx)
				if err != nil {
					return nil, err
				}
				outRow[pos] = val
				pos++
				continue
			}
			col, idx := viewSchema.ColumnByName(sc.Name)
			if col == nil {
				return nil, fmt.Errorf("unknown column '%s' in view '%s'", sc.Name, viewDef.Name)
			}
			outRow[pos] = row[idx]
			pos++
		}
		outRows = append(outRows, outRow)
	}

	return &Result{Columns: outCols, Rows: outRows}, nil
}

// executeUnion executes a UNION/INTERSECT/EXCEPT operation (MVCC version).
func (e *MVCCExecutor) executeUnion(stmt *UnionStmt, tx *txn.Transaction) (*Result, error) {
	// Execute left SELECT
	leftResult, err := e.executeSelect(stmt.Left, tx)
	if err != nil {
		return nil, fmt.Errorf("error executing left side of %s: %w", stmt.Op, err)
	}

	// Execute right SELECT
	rightResult, err := e.executeSelect(stmt.Right, tx)
	if err != nil {
		return nil, fmt.Errorf("error executing right side of %s: %w", stmt.Op, err)
	}

	// Verify column counts match
	if len(leftResult.Columns) != len(rightResult.Columns) {
		return nil, fmt.Errorf("%s requires same number of columns on both sides (got %d and %d)",
			stmt.Op, len(leftResult.Columns), len(rightResult.Columns))
	}

	// Use left side's column names
	result := &Result{Columns: leftResult.Columns}

	switch stmt.Op {
	case "UNION":
		if stmt.All {
			// UNION ALL - just concatenate
			result.Rows = append(leftResult.Rows, rightResult.Rows...)
		} else {
			// UNION - concatenate and remove duplicates
			seen := make(map[string]bool)
			for _, row := range leftResult.Rows {
				key := rowKey(row)
				if !seen[key] {
					seen[key] = true
					result.Rows = append(result.Rows, row)
				}
			}
			for _, row := range rightResult.Rows {
				key := rowKey(row)
				if !seen[key] {
					seen[key] = true
					result.Rows = append(result.Rows, row)
				}
			}
		}

	case "INTERSECT":
		// Find rows that appear in both
		rightSet := make(map[string]bool)
		for _, row := range rightResult.Rows {
			rightSet[rowKey(row)] = true
		}
		seen := make(map[string]bool)
		for _, row := range leftResult.Rows {
			key := rowKey(row)
			if rightSet[key] && (stmt.All || !seen[key]) {
				seen[key] = true
				result.Rows = append(result.Rows, row)
			}
		}

	case "EXCEPT":
		// Find rows in left that don't appear in right
		rightSet := make(map[string]bool)
		for _, row := range rightResult.Rows {
			rightSet[rowKey(row)] = true
		}
		seen := make(map[string]bool)
		for _, row := range leftResult.Rows {
			key := rowKey(row)
			if !rightSet[key] && (stmt.All || !seen[key]) {
				seen[key] = true
				result.Rows = append(result.Rows, row)
			}
		}
	}

	// Apply ORDER BY if specified
	if len(stmt.OrderBy) > 0 {
		orderByIndices := make([]int, len(stmt.OrderBy))
		for i, ob := range stmt.OrderBy {
			found := false
			for j, colName := range result.Columns {
				if strings.EqualFold(colName, ob.Column) {
					orderByIndices[i] = j
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("ORDER BY column %s not found in result", ob.Column)
			}
		}
		sortRowsMVCC(result.Rows, stmt.OrderBy, orderByIndices)
	}

	// Apply LIMIT/OFFSET
	if stmt.Offset != nil && *stmt.Offset > 0 {
		if int(*stmt.Offset) >= len(result.Rows) {
			result.Rows = nil
		} else {
			result.Rows = result.Rows[*stmt.Offset:]
		}
	}
	if stmt.Limit != nil {
		if int(*stmt.Limit) < len(result.Rows) {
			result.Rows = result.Rows[:*stmt.Limit]
		}
	}

	return result, nil
}

// sortRowsMVCC sorts rows for MVCC executor based on ORDER BY clauses.
func sortRowsMVCC(rows [][]catalog.Value, orderBy []OrderByClause, orderByIndices []int) {
	if len(rows) <= 1 || len(orderBy) == 0 {
		return
	}

	sort.SliceStable(rows, func(i, j int) bool {
		for k, ob := range orderBy {
			idx := orderByIndices[k]
			left := rows[i][idx]
			right := rows[j][idx]

			cmp := compareValuesForSortMVCC(left, right)
			if cmp == 0 {
				continue
			}

			if ob.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

// compareValuesForSortMVCC compares two values for sorting.
func compareValuesForSortMVCC(left, right catalog.Value) int {
	if left.IsNull && right.IsNull {
		return 0
	}
	if left.IsNull {
		return 1
	}
	if right.IsNull {
		return -1
	}

	switch left.Type {
	case catalog.TypeInt32:
		if right.Type == catalog.TypeInt64 {
			if int64(left.Int32) < right.Int64 {
				return -1
			} else if int64(left.Int32) > right.Int64 {
				return 1
			}
			return 0
		}
		if left.Int32 < right.Int32 {
			return -1
		} else if left.Int32 > right.Int32 {
			return 1
		}
		return 0

	case catalog.TypeInt64:
		if right.Type == catalog.TypeInt32 {
			if left.Int64 < int64(right.Int32) {
				return -1
			} else if left.Int64 > int64(right.Int32) {
				return 1
			}
			return 0
		}
		if left.Int64 < right.Int64 {
			return -1
		} else if left.Int64 > right.Int64 {
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

// executeSelectWithWindowFunctions handles SELECT with window functions using MVCC.
func (e *MVCCExecutor) executeSelectWithWindowFunctions(stmt *SelectStmt, tx *txn.Transaction) (*Result, error) {
	cat := e.mtm.Catalog()
	meta, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, err
	}

	// First, scan all rows that match WHERE clause
	var allRows [][]catalog.Value

	err = e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		// Apply WHERE filter
		if stmt.Where != nil {
			match, evalErr := e.evalCondition(stmt.Where, meta.Schema, row.Values, tx)
			if evalErr != nil {
				return false, evalErr
			}
			if !match {
				return true, nil // continue scanning
			}
		}

		rowCopy := make([]catalog.Value, len(row.Values))
		copy(rowCopy, row.Values)
		allRows = append(allRows, rowCopy)
		return true, nil // continue
	})
	if err != nil {
		return nil, err
	}

	// Determine output columns
	var outputCols []string
	for _, col := range stmt.Columns {
		if col.Star {
			for _, c := range meta.Schema.Columns {
				outputCols = append(outputCols, c.Name)
			}
		} else if col.Alias != "" {
			outputCols = append(outputCols, col.Alias)
		} else if col.Name != "" {
			outputCols = append(outputCols, col.Name)
		} else if col.Expression != nil {
			if wf, ok := col.Expression.(*WindowFuncExpr); ok {
				outputCols = append(outputCols, wf.Function)
			} else {
				outputCols = append(outputCols, "expr")
			}
		}
	}

	// Build output rows with window function results
	outputRows := make([][]catalog.Value, len(allRows))
	for i := range outputRows {
		outputRows[i] = make([]catalog.Value, len(stmt.Columns))
	}

	// Process each column
	for colIdx, col := range stmt.Columns {
		if col.Star {
			continue // handled separately
		}

		if wf, ok := col.Expression.(*WindowFuncExpr); ok {
			// This is a window function - compute values for all rows
			windowValues, err := e.computeWindowFunction(wf, allRows, meta.Schema)
			if err != nil {
				return nil, err
			}
			for rowIdx, val := range windowValues {
				outputRows[rowIdx][colIdx] = val
			}
		} else if col.Name != "" {
			// Regular column
			_, idx := meta.Schema.ColumnByName(col.Name)
			if idx < 0 {
				return nil, fmt.Errorf("unknown column: %s", col.Name)
			}
			for rowIdx, row := range allRows {
				outputRows[rowIdx][colIdx] = row[idx]
			}
		} else if col.Expression != nil {
			// Other expression
			for rowIdx, row := range allRows {
				val, err := e.evalExpr(col.Expression, meta.Schema, row, tx)
				if err != nil {
					return nil, err
				}
				outputRows[rowIdx][colIdx] = val
			}
		}
	}

	// Handle SELECT * expansion
	starOffset := 0
	for colIdx, col := range stmt.Columns {
		if col.Star {
			for rowIdx, row := range allRows {
				for i := range meta.Schema.Columns {
					if colIdx+starOffset+i < len(outputRows[rowIdx]) {
						outputRows[rowIdx][colIdx+starOffset+i] = row[i]
					}
				}
			}
			starOffset += len(meta.Schema.Columns) - 1
		}
	}

	// Apply ORDER BY if present
	if len(stmt.OrderBy) > 0 {
		orderByIndices := make([]int, len(stmt.OrderBy))
		for i, ob := range stmt.OrderBy {
			_, idx := meta.Schema.ColumnByName(ob.Column)
			if idx < 0 {
				return nil, fmt.Errorf("unknown column in ORDER BY: %s", ob.Column)
			}
			orderByIndices[i] = idx
		}

		type indexedRow struct {
			origIdx   int
			outputRow []catalog.Value
			sortRow   []catalog.Value
		}
		indexed := make([]indexedRow, len(allRows))
		for i := range allRows {
			indexed[i] = indexedRow{origIdx: i, outputRow: outputRows[i], sortRow: allRows[i]}
		}

		sort.SliceStable(indexed, func(i, j int) bool {
			for k, ob := range stmt.OrderBy {
				idx := orderByIndices[k]
				cmp := compareValuesForSortMVCC(indexed[i].sortRow[idx], indexed[j].sortRow[idx])
				if cmp != 0 {
					if ob.Desc {
						return cmp > 0
					}
					return cmp < 0
				}
			}
			return false
		})

		for i, ir := range indexed {
			outputRows[i] = ir.outputRow
		}
	}

	// Apply LIMIT/OFFSET
	if stmt.Offset != nil && *stmt.Offset > 0 {
		if int(*stmt.Offset) >= len(outputRows) {
			outputRows = nil
		} else {
			outputRows = outputRows[*stmt.Offset:]
		}
	}
	if stmt.Limit != nil && *stmt.Limit >= 0 {
		if int(*stmt.Limit) < len(outputRows) {
			outputRows = outputRows[:*stmt.Limit]
		}
	}

	return &Result{
		Columns: outputCols,
		Rows:    outputRows,
	}, nil
}

// computeWindowFunction computes window function values for all rows (MVCC version).
func (e *MVCCExecutor) computeWindowFunction(wf *WindowFuncExpr, rows [][]catalog.Value, schema *catalog.Schema) ([]catalog.Value, error) {
	result := make([]catalog.Value, len(rows))

	// Get partition column indices
	partitionIndices := make([]int, len(wf.Over.PartitionBy))
	for i, colName := range wf.Over.PartitionBy {
		_, idx := schema.ColumnByName(colName)
		if idx < 0 {
			return nil, fmt.Errorf("unknown column in PARTITION BY: %s", colName)
		}
		partitionIndices[i] = idx
	}

	// Get order by column indices
	orderByIndices := make([]int, len(wf.Over.OrderBy))
	for i, ob := range wf.Over.OrderBy {
		_, idx := schema.ColumnByName(ob.Column)
		if idx < 0 {
			return nil, fmt.Errorf("unknown column in window ORDER BY: %s", ob.Column)
		}
		orderByIndices[i] = idx
	}

	// Group rows by partition
	partitions := make(map[string][]int)
	partitionOrder := []string{}

	for i, row := range rows {
		key := mvccMakePartitionKey(row, partitionIndices)
		if _, exists := partitions[key]; !exists {
			partitionOrder = append(partitionOrder, key)
		}
		partitions[key] = append(partitions[key], i)
	}

	// Process each partition
	for _, partKey := range partitionOrder {
		rowIndices := partitions[partKey]

		// Sort within partition
		if len(wf.Over.OrderBy) > 0 {
			sort.SliceStable(rowIndices, func(i, j int) bool {
				for k, ob := range wf.Over.OrderBy {
					idx := orderByIndices[k]
					cmp := compareValuesForSortMVCC(rows[rowIndices[i]][idx], rows[rowIndices[j]][idx])
					if cmp != 0 {
						if ob.Desc {
							return cmp > 0
						}
						return cmp < 0
					}
				}
				return false
			})
		}

		// Compute function
		switch strings.ToUpper(wf.Function) {
		case "ROW_NUMBER":
			for rank, rowIdx := range rowIndices {
				result[rowIdx] = catalog.NewInt64(int64(rank + 1))
			}

		case "RANK":
			var prevVals []catalog.Value
			rank := 1
			for i, rowIdx := range rowIndices {
				currVals := mvccGetOrderByValues(rows[rowIdx], orderByIndices)
				if i == 0 || !mvccWindowValuesEqual(currVals, prevVals) {
					rank = i + 1
				}
				result[rowIdx] = catalog.NewInt64(int64(rank))
				prevVals = currVals
			}

		case "DENSE_RANK":
			var prevVals []catalog.Value
			rank := 0
			for i, rowIdx := range rowIndices {
				currVals := mvccGetOrderByValues(rows[rowIdx], orderByIndices)
				if i == 0 || !mvccWindowValuesEqual(currVals, prevVals) {
					rank++
				}
				result[rowIdx] = catalog.NewInt64(int64(rank))
				prevVals = currVals
			}

		case "SUM":
			if len(wf.Args) != 1 {
				return nil, fmt.Errorf("SUM requires exactly 1 argument")
			}
			colRef, ok := wf.Args[0].(*ColumnRef)
			if !ok {
				return nil, fmt.Errorf("SUM argument must be a column reference")
			}
			_, colIdx := schema.ColumnByName(colRef.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("unknown column: %s", colRef.Name)
			}

			var runningSum int64
			for _, rowIdx := range rowIndices {
				val := rows[rowIdx][colIdx]
				if !val.IsNull {
					switch val.Type {
					case catalog.TypeInt32:
						runningSum += int64(val.Int32)
					case catalog.TypeInt64:
						runningSum += val.Int64
					}
				}
				result[rowIdx] = catalog.NewInt64(runningSum)
			}

		case "COUNT":
			for i, rowIdx := range rowIndices {
				result[rowIdx] = catalog.NewInt64(int64(i + 1))
			}

		case "AVG":
			if len(wf.Args) != 1 {
				return nil, fmt.Errorf("AVG requires exactly 1 argument")
			}
			colRef, ok := wf.Args[0].(*ColumnRef)
			if !ok {
				return nil, fmt.Errorf("AVG argument must be a column reference")
			}
			_, colIdx := schema.ColumnByName(colRef.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("unknown column: %s", colRef.Name)
			}

			var runningSum int64
			count := 0
			for _, rowIdx := range rowIndices {
				val := rows[rowIdx][colIdx]
				if !val.IsNull {
					switch val.Type {
					case catalog.TypeInt32:
						runningSum += int64(val.Int32)
					case catalog.TypeInt64:
						runningSum += val.Int64
					}
					count++
				}
				if count > 0 {
					result[rowIdx] = catalog.NewInt64(runningSum / int64(count))
				} else {
					result[rowIdx] = catalog.Null(catalog.TypeInt64)
				}
			}

		default:
			return nil, fmt.Errorf("unsupported window function: %s", wf.Function)
		}
	}

	return result, nil
}

// Helper functions for MVCC window function processing
func mvccMakePartitionKey(row []catalog.Value, indices []int) string {
	if len(indices) == 0 {
		return ""
	}
	var parts []string
	for _, idx := range indices {
		parts = append(parts, mvccValueToString(row[idx]))
	}
	return strings.Join(parts, "|")
}

func mvccGetOrderByValues(row []catalog.Value, indices []int) []catalog.Value {
	vals := make([]catalog.Value, len(indices))
	for i, idx := range indices {
		vals[i] = row[idx]
	}
	return vals
}

func mvccWindowValuesEqual(a, b []catalog.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if compareValuesForSortMVCC(a[i], b[i]) != 0 {
			return false
		}
	}
	return true
}

func mvccValueToString(v catalog.Value) string {
	if v.IsNull {
		return "NULL"
	}
	switch v.Type {
	case catalog.TypeInt32:
		return fmt.Sprintf("%d", v.Int32)
	case catalog.TypeInt64:
		return fmt.Sprintf("%d", v.Int64)
	case catalog.TypeText:
		return v.Text
	case catalog.TypeBool:
		return fmt.Sprintf("%t", v.Bool)
	case catalog.TypeTimestamp:
		return v.Timestamp.String()
	default:
		return "?"
	}
}

// executeSelectWithLateralJoin handles SELECT with a LATERAL join for MVCC.
func (e *MVCCExecutor) executeSelectWithLateralJoin(stmt *SelectStmt, tx *txn.Transaction, leftMeta *catalog.TableMeta, join JoinClause) (*Result, error) {
	var joinedRows [][]catalog.Value
	var combinedSchema *catalog.Schema
	var colTableMap map[string]int
	leftLen := len(leftMeta.Schema.Columns)
	var rightLen int
	schemaInitialized := false

	// Collect all left rows first
	var leftRows [][]catalog.Value
	err := e.mtm.Scan(stmt.TableName, tx, func(row *catalog.MVCCRow) (bool, error) {
		rowCopy := make([]catalog.Value, len(row.Values))
		copy(rowCopy, row.Values)
		leftRows = append(leftRows, rowCopy)
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	// Process each left row with the lateral subquery
	for _, leftRow := range leftRows {
		// Create a correlated subquery with substituted values
		correlatedQuery := e.substituteCorrelatedColumnsMVCC(join.Subquery, leftMeta, leftRow, stmt.TableName, stmt.TableAlias)

		// Execute the lateral subquery
		subResult, err := e.Execute(correlatedQuery, tx)
		if err != nil {
			return nil, fmt.Errorf("error executing LATERAL subquery: %w", err)
		}

		// Initialize schema on first successful subquery execution
		if !schemaInitialized && subResult != nil {
			rightLen = len(subResult.Columns)

			combinedSchema = &catalog.Schema{
				Columns: make([]catalog.Column, 0, leftLen+rightLen),
			}
			colTableMap = make(map[string]int)

			// Add left table columns
			for i, col := range leftMeta.Schema.Columns {
				combinedSchema.Columns = append(combinedSchema.Columns, col)
				colTableMap[stmt.TableName+"."+col.Name] = i
				if stmt.TableAlias != "" {
					colTableMap[stmt.TableAlias+"."+col.Name] = i
				}
				colTableMap[col.Name] = i
			}

			// Add right (subquery) columns
			alias := join.TableAlias
			for i, colName := range subResult.Columns {
				newCol := catalog.Column{Name: colName, Type: catalog.TypeText}
				combinedSchema.Columns = append(combinedSchema.Columns, newCol)
				if alias != "" {
					colTableMap[alias+"."+colName] = leftLen + i
				}
				if _, exists := colTableMap[colName]; !exists {
					colTableMap[colName] = leftLen + i
				}
			}

			schemaInitialized = true
		}

		if subResult == nil || len(subResult.Rows) == 0 {
			if join.JoinType == "LEFT" || join.JoinType == "CROSS" {
				if schemaInitialized {
					nullRightRow := make([]catalog.Value, rightLen)
					for i := 0; i < rightLen; i++ {
						nullRightRow[i] = catalog.Null(catalog.TypeText)
					}
					combinedRow := make([]catalog.Value, leftLen+rightLen)
					copy(combinedRow, leftRow)
					copy(combinedRow[leftLen:], nullRightRow)
					joinedRows = append(joinedRows, combinedRow)
				}
			}
			continue
		}

		for _, rightRow := range subResult.Rows {
			combinedRow := make([]catalog.Value, leftLen+rightLen)
			copy(combinedRow, leftRow)
			copy(combinedRow[leftLen:], rightRow)

			if join.Condition != nil {
				match, err := e.evalJoinConditionMVCC(join.Condition, combinedSchema, combinedRow, colTableMap)
				if err != nil {
					return nil, err
				}
				if !match {
					continue
				}
			}
			joinedRows = append(joinedRows, combinedRow)
		}
	}

	if combinedSchema == nil {
		combinedSchema = &catalog.Schema{
			Columns: make([]catalog.Column, 0, leftLen),
		}
		colTableMap = make(map[string]int)
		for i, col := range leftMeta.Schema.Columns {
			combinedSchema.Columns = append(combinedSchema.Columns, col)
			colTableMap[col.Name] = i
		}
	}

	// Apply WHERE filter
	var filteredRows [][]catalog.Value
	for _, row := range joinedRows {
		if stmt.Where != nil {
			match, err := e.evalJoinConditionMVCC(stmt.Where, combinedSchema, row, colTableMap)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		filteredRows = append(filteredRows, row)
	}

	// Determine output columns
	var outputCols []string
	var colIndices []int

	if len(stmt.Columns) == 1 && stmt.Columns[0].Star {
		for i, c := range combinedSchema.Columns {
			outputCols = append(outputCols, c.Name)
			colIndices = append(colIndices, i)
		}
	} else {
		for _, sc := range stmt.Columns {
			idx, ok := colTableMap[sc.Name]
			if !ok {
				return nil, fmt.Errorf("unknown column: %s", sc.Name)
			}
			if sc.Alias != "" {
				outputCols = append(outputCols, sc.Alias)
			} else {
				outputCols = append(outputCols, sc.Name)
			}
			colIndices = append(colIndices, idx)
		}
	}

	// Project columns
	resultRows := make([][]catalog.Value, len(filteredRows))
	for i, row := range filteredRows {
		projectedRow := make([]catalog.Value, len(colIndices))
		for j, idx := range colIndices {
			projectedRow[j] = row[idx]
		}
		resultRows[i] = projectedRow
	}

	// Apply ORDER BY, LIMIT, OFFSET, DISTINCT as in regular executor
	if stmt.Limit != nil {
		limit := int(*stmt.Limit)
		if limit < len(resultRows) {
			resultRows = resultRows[:limit]
		}
	}

	if stmt.Distinct {
		resultRows = deduplicateRows(resultRows)
	}

	return &Result{
		Columns: outputCols,
		Rows:    resultRows,
	}, nil
}

// substituteCorrelatedColumnsMVCC replaces column references from the left table with actual values.
func (e *MVCCExecutor) substituteCorrelatedColumnsMVCC(subquery *SelectStmt, leftMeta *catalog.TableMeta, leftRow []catalog.Value, leftTableName string, leftTableAlias string) *SelectStmt {
	newQuery := &SelectStmt{
		Distinct:   subquery.Distinct,
		DistinctOn: append([]string{}, subquery.DistinctOn...),
		TableName:  subquery.TableName,
		TableAlias: subquery.TableAlias,
		GroupBy:    append([]string{}, subquery.GroupBy...),
		Limit:      subquery.Limit,
		Offset:     subquery.Offset,
	}

	newQuery.Columns = make([]SelectColumn, len(subquery.Columns))
	copy(newQuery.Columns, subquery.Columns)

	newQuery.OrderBy = make([]OrderByClause, len(subquery.OrderBy))
	copy(newQuery.OrderBy, subquery.OrderBy)

	if subquery.Where != nil {
		newQuery.Where = e.substituteExpressionValuesMVCC(subquery.Where, leftMeta, leftRow, leftTableName, leftTableAlias)
	}

	if subquery.Having != nil {
		newQuery.Having = e.substituteExpressionValuesMVCC(subquery.Having, leftMeta, leftRow, leftTableName, leftTableAlias)
	}

	return newQuery
}

// substituteExpressionValuesMVCC substitutes column references from the left table with actual values.
func (e *MVCCExecutor) substituteExpressionValuesMVCC(expr Expression, leftMeta *catalog.TableMeta, leftRow []catalog.Value, leftTableName string, leftTableAlias string) Expression {
	switch ex := expr.(type) {
	case *ColumnRef:
		colName := ex.Name
		parts := splitQualifiedName(colName)
		var lookupName string
		if len(parts) == 2 {
			tablePart := parts[0]
			columnPart := parts[1]
			if tablePart == leftTableName || tablePart == leftTableAlias {
				lookupName = columnPart
			}
		} else {
			lookupName = colName
		}

		for i, col := range leftMeta.Schema.Columns {
			if col.Name == lookupName {
				return &LiteralExpr{Value: leftRow[i]}
			}
		}
		return ex

	case *BinaryExpr:
		return &BinaryExpr{
			Left:  e.substituteExpressionValuesMVCC(ex.Left, leftMeta, leftRow, leftTableName, leftTableAlias),
			Op:    ex.Op,
			Right: e.substituteExpressionValuesMVCC(ex.Right, leftMeta, leftRow, leftTableName, leftTableAlias),
		}

	case *UnaryExpr:
		return &UnaryExpr{
			Op:   ex.Op,
			Expr: e.substituteExpressionValuesMVCC(ex.Expr, leftMeta, leftRow, leftTableName, leftTableAlias),
		}

	case *InExpr:
		newValues := make([]Expression, len(ex.Values))
		for i, v := range ex.Values {
			newValues[i] = e.substituteExpressionValuesMVCC(v, leftMeta, leftRow, leftTableName, leftTableAlias)
		}
		return &InExpr{
			Left:   e.substituteExpressionValuesMVCC(ex.Left, leftMeta, leftRow, leftTableName, leftTableAlias),
			Values: newValues,
			Not:    ex.Not,
		}

	default:
		return expr
	}
}

// substituteExpressionValuesFromSchema replaces unqualified ColumnRef nodes with literal values
// when the column name exists in the provided schema. This is a best-effort helper for correlated
// subqueries where we only have an outer schema and current row values.
func (e *MVCCExecutor) substituteExpressionValuesFromSchema(expr Expression, schema *catalog.Schema, row []catalog.Value) Expression {
	switch ex := expr.(type) {
	case *ColumnRef:
		if schema == nil || row == nil {
			return ex
		}
		// Handle qualified references like alias.col by checking the last part
		name := ex.Name
		if strings.Contains(name, ".") {
			parts := splitQualifiedName(name)
			name = parts[len(parts)-1]
		}
		col, idx := schema.ColumnByName(name)
		if col == nil || idx < 0 || idx >= len(row) {
			return ex
		}
		return &LiteralExpr{Value: row[idx]}

	case *LiteralExpr:
		return ex

	case *BinaryExpr:
		return &BinaryExpr{Left: e.substituteExpressionValuesFromSchema(ex.Left, schema, row), Op: ex.Op, Right: e.substituteExpressionValuesFromSchema(ex.Right, schema, row)}

	case *UnaryExpr:
		return &UnaryExpr{Op: ex.Op, Expr: e.substituteExpressionValuesFromSchema(ex.Expr, schema, row)}

	case *InExpr:
		var vals []Expression
		for _, v := range ex.Values {
			vals = append(vals, e.substituteExpressionValuesFromSchema(v, schema, row))
		}
		return &InExpr{Left: e.substituteExpressionValuesFromSchema(ex.Left, schema, row), Values: vals, Subquery: ex.Subquery, Not: ex.Not}

	case *CaseExpr:
		newWhens := make([]WhenClause, len(ex.Whens))
		for i, w := range ex.Whens {
			newWhens[i] = WhenClause{Condition: e.substituteExpressionValuesFromSchema(w.Condition, schema, row), Result: e.substituteExpressionValuesFromSchema(w.Result, schema, row)}
		}
		var newElse Expression
		if ex.Else != nil {
			newElse = e.substituteExpressionValuesFromSchema(ex.Else, schema, row)
		}
		return &CaseExpr{Operand: ex.Operand, Whens: newWhens, Else: newElse}

	case *FunctionExpr:
		newArgs := make([]Expression, len(ex.Args))
		for i, a := range ex.Args {
			newArgs[i] = e.substituteExpressionValuesFromSchema(a, schema, row)
		}
		return &FunctionExpr{Name: ex.Name, Args: newArgs}

	case *ExistsExpr:
		// For nested EXISTS inside subqueries, substitute inside the inner query
		if ex.Query != nil {
			newQuery := e.substituteCorrelatedSelectUsingSchema(ex.Query, schema, row)
			return &ExistsExpr{Query: newQuery, Not: ex.Not}
		}
		return ex

	case *SubqueryExpr:
		if ex.Query != nil {
			newQuery := e.substituteCorrelatedSelectUsingSchema(ex.Query, schema, row)
			return &SubqueryExpr{Query: newQuery}
		}
		return ex

	default:
		return expr
	}
}

// substituteExpressionValuesCorrelated substitutes outer values into expressions, but avoids
// replacing columns that clearly belong to the inner (subquery) table by using innerSchema.
func (e *MVCCExecutor) substituteExpressionValuesCorrelated(expr Expression, outerSchema *catalog.Schema, outerRow []catalog.Value, innerSchema *catalog.Schema, innerTableName string) Expression {
	switch ex := expr.(type) {
	case *ColumnRef:
		// Determine column name and optional qualifier
		name := ex.Name
		qualifier := ""
		if strings.Contains(name, ".") {
			parts := splitQualifiedName(name)
			if len(parts) == 2 {
				qualifier = parts[0]
				name = parts[1]
			}
		}

		// If unqualified and innerSchema has this column, treat as inner column (do not substitute)
		if qualifier == "" && innerSchema != nil {
			if col, _ := innerSchema.ColumnByName(name); col != nil {
				return ex
			}
		}

		// If qualified with inner table name/alias that matches innerTableName, do not substitute
		if qualifier != "" && innerSchema != nil {
			if qualifier == innerTableName {
				return ex
			}
			// If inner schema has this column and qualifier is absent or unknown, avoid substituting
			if qualifier == "" {
				if col, _ := innerSchema.ColumnByName(name); col != nil {
					return ex
				}
			}
		}

		// Otherwise, substitute from outer schema if available
		if outerSchema == nil || outerRow == nil {
			return ex
		}
		col, idx := outerSchema.ColumnByName(name)
		if col == nil || idx < 0 || idx >= len(outerRow) {
			return ex
		}
		return &LiteralExpr{Value: outerRow[idx]}

	case *LiteralExpr:
		return ex

	case *BinaryExpr:
		return &BinaryExpr{Left: e.substituteExpressionValuesCorrelated(ex.Left, outerSchema, outerRow, innerSchema, innerTableName), Op: ex.Op, Right: e.substituteExpressionValuesCorrelated(ex.Right, outerSchema, outerRow, innerSchema, innerTableName)}

	case *UnaryExpr:
		return &UnaryExpr{Op: ex.Op, Expr: e.substituteExpressionValuesCorrelated(ex.Expr, outerSchema, outerRow, innerSchema, innerTableName)}

	case *InExpr:
		var vals []Expression
		for _, v := range ex.Values {
			vals = append(vals, e.substituteExpressionValuesCorrelated(v, outerSchema, outerRow, innerSchema, innerTableName))
		}
		var subq *SelectStmt
		if ex.Subquery != nil {
			subq = e.substituteCorrelatedSelectUsingSchema(ex.Subquery, outerSchema, outerRow)
		}
		return &InExpr{Left: e.substituteExpressionValuesCorrelated(ex.Left, outerSchema, outerRow, innerSchema, innerTableName), Values: vals, Subquery: subq, Not: ex.Not}

	case *CaseExpr:
		newWhens := make([]WhenClause, len(ex.Whens))
		for i, w := range ex.Whens {
			newWhens[i] = WhenClause{Condition: e.substituteExpressionValuesCorrelated(w.Condition, outerSchema, outerRow, innerSchema, innerTableName), Result: e.substituteExpressionValuesCorrelated(w.Result, outerSchema, outerRow, innerSchema, innerTableName)}
		}
		var newElse Expression
		if ex.Else != nil {
			newElse = e.substituteExpressionValuesCorrelated(ex.Else, outerSchema, outerRow, innerSchema, innerTableName)
		}
		return &CaseExpr{Operand: ex.Operand, Whens: newWhens, Else: newElse}

	case *FunctionExpr:
		newArgs := make([]Expression, len(ex.Args))
		for i, a := range ex.Args {
			newArgs[i] = e.substituteExpressionValuesCorrelated(a, outerSchema, outerRow, innerSchema, innerTableName)
		}
		return &FunctionExpr{Name: ex.Name, Args: newArgs}

	case *ExistsExpr:
		if ex.Query != nil {
			newQuery := e.substituteCorrelatedSelectUsingSchema(ex.Query, outerSchema, outerRow)
			return &ExistsExpr{Query: newQuery, Not: ex.Not}
		}
		return ex

	case *SubqueryExpr:
		if ex.Query != nil {
			newQuery := e.substituteCorrelatedSelectUsingSchema(ex.Query, outerSchema, outerRow)
			return &SubqueryExpr{Query: newQuery}
		}
		return ex

	default:
		return expr
	}
}

// substituteCorrelatedSelectUsingSchema returns a shallow copy of the SelectStmt with WHERE/HAVING
// and column expressions substituted using the provided outer schema/row.
func (e *MVCCExecutor) substituteCorrelatedSelectUsingSchema(sub *SelectStmt, schema *catalog.Schema, row []catalog.Value) *SelectStmt {
	if sub == nil {
		return nil
	}
	newQ := &SelectStmt{
		With:         sub.With,
		Distinct:     sub.Distinct,
		DistinctOn:   append([]string{}, sub.DistinctOn...),
		TableName:    sub.TableName,
		TableAlias:   sub.TableAlias,
		GroupBy:      append([]string{}, sub.GroupBy...),
		GroupingSets: append([]GroupingSet{}, sub.GroupingSets...),
		Limit:        sub.Limit,
		LimitExpr:    sub.LimitExpr,
		Offset:       sub.Offset,
	}

	// Attempt to fetch the inner table schema for cautious substitution
	var innerSchema *catalog.Schema
	if sub.TableName != "" {
		if meta, err := e.mtm.Catalog().GetTable(sub.TableName); err == nil {
			innerSchema = meta.Schema
		}
	}

	// Copy columns and substitute expressions inside any Expression fields
	newQ.Columns = make([]SelectColumn, len(sub.Columns))
	for i, sc := range sub.Columns {
		nc := sc
		if nc.Expression != nil {
			nc.Expression = e.substituteExpressionValuesCorrelated(nc.Expression, schema, row, innerSchema, sub.TableName)
		}
		newQ.Columns[i] = nc
	}

	// Substitute WHERE and HAVING cautiously (avoid replacing inner table columns)
	if sub.Where != nil {
		newQ.Where = e.substituteExpressionValuesCorrelated(sub.Where, schema, row, innerSchema, sub.TableName)
	}
	if sub.Having != nil {
		newQ.Having = e.substituteExpressionValuesCorrelated(sub.Having, schema, row, innerSchema, sub.TableName)
	}

	// Note: We intentionally do not attempt to rewrite JOINs or FROM clauses here.
	// This helper is a best-effort substitution for simple correlated subqueries that
	// reference outer columns via unqualified names.

	return newQ
}

// executeMerge handles MERGE statements for MVCC executor.
func (e *MVCCExecutor) executeMerge(stmt *MergeStmt, tx *txn.Transaction) (*Result, error) {
	cat := e.mtm.Catalog()

	targetMeta, err := cat.GetTable(stmt.TargetTable)
	if err != nil {
		return nil, fmt.Errorf("target table %q does not exist: %w", stmt.TargetTable, err)
	}

	// Get source data
	var sourceRows [][]catalog.Value
	var sourceCols []string
	var sourceSchema *catalog.Schema

	if stmt.SourceQuery != nil {
		result, err := e.Execute(stmt.SourceQuery, tx)
		if err != nil {
			return nil, fmt.Errorf("error executing source query: %w", err)
		}
		sourceRows = result.Rows
		sourceCols = result.Columns
		sourceSchema = &catalog.Schema{
			Columns: make([]catalog.Column, len(sourceCols)),
		}
		for i, col := range sourceCols {
			sourceSchema.Columns[i] = catalog.Column{Name: col, Type: catalog.TypeText}
		}
	} else {
		sourceMeta, err := cat.GetTable(stmt.SourceTable)
		if err != nil {
			return nil, fmt.Errorf("source table %q does not exist: %w", stmt.SourceTable, err)
		}
		sourceSchema = sourceMeta.Schema
		for _, col := range sourceSchema.Columns {
			sourceCols = append(sourceCols, col.Name)
		}
		_ = sourceCols // sourceCols collected for potential future use
		err = e.mtm.Scan(stmt.SourceTable, tx, func(row *catalog.MVCCRow) (bool, error) {
			rowCopy := make([]catalog.Value, len(row.Values))
			copy(rowCopy, row.Values)
			sourceRows = append(sourceRows, rowCopy)
			return true, nil
		})
		if err != nil {
			return nil, err
		}
	}

	// Build combined schema
	combinedSchema := &catalog.Schema{
		Columns: make([]catalog.Column, 0, len(targetMeta.Schema.Columns)+len(sourceSchema.Columns)),
	}
	colMap := make(map[string]int)

	targetAlias := stmt.TargetAlias
	if targetAlias == "" {
		targetAlias = stmt.TargetTable
	}
	for i, col := range targetMeta.Schema.Columns {
		combinedSchema.Columns = append(combinedSchema.Columns, col)
		colMap[targetAlias+"."+col.Name] = i
		colMap[col.Name] = i
	}

	targetLen := len(targetMeta.Schema.Columns)
	sourceAlias := stmt.SourceAlias
	if sourceAlias == "" {
		sourceAlias = stmt.SourceTable
	}
	for i, col := range sourceSchema.Columns {
		combinedSchema.Columns = append(combinedSchema.Columns, col)
		colMap[sourceAlias+"."+col.Name] = targetLen + i
		if _, exists := colMap[col.Name]; !exists {
			colMap[col.Name] = targetLen + i
		}
	}

	var rowsInserted, rowsUpdated, rowsDeleted int

	for _, sourceRow := range sourceRows {
		// Collect target rows with their RIDs
		type targetRowInfo struct {
			rid    storage.RID
			values []catalog.Value
		}
		var targetRowsInfo []targetRowInfo

		err := e.mtm.Scan(stmt.TargetTable, tx, func(row *catalog.MVCCRow) (bool, error) {
			rowCopy := make([]catalog.Value, len(row.Values))
			copy(rowCopy, row.Values)
			targetRowsInfo = append(targetRowsInfo, targetRowInfo{rid: row.RID, values: rowCopy})
			return true, nil
		})
		if err != nil {
			return nil, err
		}

		foundMatch := false
		for _, targetInfo := range targetRowsInfo {
			combinedRow := make([]catalog.Value, targetLen+len(sourceRow))
			copy(combinedRow, targetInfo.values)
			copy(combinedRow[targetLen:], sourceRow)

			match, err := e.evalJoinConditionMVCC(stmt.Condition, combinedSchema, combinedRow, colMap)
			if err != nil {
				return nil, fmt.Errorf("error evaluating MERGE condition: %w", err)
			}

			if match {
				foundMatch = true
				for _, when := range stmt.WhenClauses {
					if !when.Matched {
						continue
					}

					if when.Condition != nil {
						condMatch, err := e.evalJoinConditionMVCC(when.Condition, combinedSchema, combinedRow, colMap)
						if err != nil {
							return nil, err
						}
						if !condMatch {
							continue
						}
					}

					switch when.Action.ActionType {
					case "UPDATE":
						newRow := make([]catalog.Value, len(targetInfo.values))
						copy(newRow, targetInfo.values)

						for _, assign := range when.Action.Assignments {
							colIdx := -1
							for j, col := range targetMeta.Schema.Columns {
								if strings.EqualFold(col.Name, assign.Column) {
									colIdx = j
									break
								}
							}
							if colIdx == -1 {
								return nil, fmt.Errorf("unknown column in UPDATE: %s", assign.Column)
							}

							val, err := e.evalMergeExprMVCC(assign.Value, combinedSchema, combinedRow, colMap)
							if err != nil {
								return nil, err
							}
							newRow[colIdx] = val
						}

						if err := e.mtm.MarkDeleted(targetInfo.rid, tx); err != nil {
							return nil, err
						}
						if _, err := e.mtm.Insert(stmt.TargetTable, newRow, tx); err != nil {
							return nil, err
						}
						rowsUpdated++

					case "DELETE":
						if err := e.mtm.MarkDeleted(targetInfo.rid, tx); err != nil {
							return nil, err
						}
						rowsDeleted++

					case "DO NOTHING":
						// Do nothing
					}
					break
				}
				break
			}
		}

		if !foundMatch {
			for _, when := range stmt.WhenClauses {
				if when.Matched {
					continue
				}

				if when.Condition != nil {
					combinedRow := make([]catalog.Value, targetLen+len(sourceRow))
					for j := 0; j < targetLen; j++ {
						combinedRow[j] = catalog.Null(targetMeta.Schema.Columns[j].Type)
					}
					copy(combinedRow[targetLen:], sourceRow)

					condMatch, err := e.evalJoinConditionMVCC(when.Condition, combinedSchema, combinedRow, colMap)
					if err != nil {
						return nil, err
					}
					if !condMatch {
						continue
					}
				}

				switch when.Action.ActionType {
				case "INSERT":
					newRow := make([]catalog.Value, len(targetMeta.Schema.Columns))

					evalRow := make([]catalog.Value, targetLen+len(sourceRow))
					for j := 0; j < targetLen; j++ {
						evalRow[j] = catalog.Null(targetMeta.Schema.Columns[j].Type)
					}
					copy(evalRow[targetLen:], sourceRow)

					if len(when.Action.Columns) > 0 {
						if len(when.Action.Columns) != len(when.Action.Values) {
							return nil, fmt.Errorf("INSERT column count does not match value count")
						}

						for j, col := range targetMeta.Schema.Columns {
							if col.HasDefault && col.DefaultValue != nil {
								newRow[j] = *col.DefaultValue
							} else {
								newRow[j] = catalog.Null(col.Type)
							}
						}

						for j, colName := range when.Action.Columns {
							colIdx := -1
							for k, col := range targetMeta.Schema.Columns {
								if strings.EqualFold(col.Name, colName) {
									colIdx = k
									break
								}
							}
							if colIdx == -1 {
								return nil, fmt.Errorf("unknown column in INSERT: %s", colName)
							}

							val, err := e.evalMergeExprMVCC(when.Action.Values[j], combinedSchema, evalRow, colMap)
							if err != nil {
								return nil, err
							}
							newRow[colIdx] = val
						}
					} else {
						if len(when.Action.Values) != len(targetMeta.Schema.Columns) {
							return nil, fmt.Errorf("INSERT value count does not match target column count")
						}

						for j, valExpr := range when.Action.Values {
							val, err := e.evalMergeExprMVCC(valExpr, combinedSchema, evalRow, colMap)
							if err != nil {
								return nil, err
							}
							newRow[j] = val
						}
					}

					if _, err := e.mtm.Insert(stmt.TargetTable, newRow, tx); err != nil {
						return nil, err
					}
					rowsInserted++

				case "DO NOTHING":
					// Do nothing
				}
				break
			}
		}
	}

	return &Result{
		Message:      fmt.Sprintf("MERGE: %d row(s) inserted, %d row(s) updated, %d row(s) deleted.", rowsInserted, rowsUpdated, rowsDeleted),
		RowsAffected: rowsInserted + rowsUpdated + rowsDeleted,
	}, nil
}

// evalMergeExprMVCC evaluates an expression in MERGE context for MVCC.
func (e *MVCCExecutor) evalMergeExprMVCC(expr Expression, schema *catalog.Schema, row []catalog.Value, colMap map[string]int) (catalog.Value, error) {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value, nil

	case *ColumnRef:
		idx, ok := colMap[ex.Name]
		if !ok {
			return catalog.Value{}, fmt.Errorf("unknown column: %s", ex.Name)
		}
		return row[idx], nil

	case *BinaryExpr:
		left, err := e.evalMergeExprMVCC(ex.Left, schema, row, colMap)
		if err != nil {
			return catalog.Value{}, err
		}
		right, err := e.evalMergeExprMVCC(ex.Right, schema, row, colMap)
		if err != nil {
			return catalog.Value{}, err
		}
		return evalBinaryExprValueMVCC(left, ex.Op, right)

	default:
		return catalog.Value{}, fmt.Errorf("unsupported expression type in MERGE: %T", expr)
	}
}

func evalBinaryExprValueMVCC(left catalog.Value, op TokenType, right catalog.Value) (catalog.Value, error) {
	leftNum := valueToInt64MVCC(left)
	rightNum := valueToInt64MVCC(right)

	switch op {
	case TOKEN_PLUS:
		return catalog.NewInt64(leftNum + rightNum), nil
	case TOKEN_MINUS:
		return catalog.NewInt64(leftNum - rightNum), nil
	case TOKEN_STAR:
		return catalog.NewInt64(leftNum * rightNum), nil
	case TOKEN_SLASH:
		if rightNum == 0 {
			return catalog.Value{}, fmt.Errorf("division by zero")
		}
		return catalog.NewInt64(leftNum / rightNum), nil
	default:
		return catalog.Value{}, fmt.Errorf("unsupported operator: %v", op)
	}
}

func valueToInt64MVCC(v catalog.Value) int64 {
	if v.IsNull {
		return 0
	}
	switch v.Type {
	case catalog.TypeInt32:
		return int64(v.Int32)
	case catalog.TypeInt64:
		return v.Int64
	default:
		return 0
	}
}

// EvaluateExpression evaluates an expression with no row context.
// This is useful for evaluating parameters in EXECUTE statements.
func (e *MVCCExecutor) EvaluateExpression(expr Expression) (catalog.Value, error) {
	return e.evalExpr(expr, nil, nil, nil)
}

// TriggerContext holds context information for trigger execution.
type TriggerContext struct {
	TableName string
	OldRow    []catalog.Value // For UPDATE/DELETE triggers
	NewRow    []catalog.Value // For INSERT/UPDATE triggers
	Schema    *catalog.Schema
}

// fireTriggers fires all triggers matching the given timing and event.
// It executes the trigger function body with OLD/NEW row bindings.
func (e *MVCCExecutor) fireTriggers(tableName string, timing catalog.TriggerTiming, event catalog.TriggerEvent, ctx *TriggerContext) error {
	if e.triggerCat == nil {
		return nil // Triggers not enabled
	}

	triggers := e.triggerCat.GetTriggersForTableEvent(tableName, timing, event)
	for _, trigger := range triggers {
		if !trigger.Enabled {
			continue
		}

		// Execute the trigger function
		if err := e.executeTriggerFunction(trigger, ctx); err != nil {
			return fmt.Errorf("trigger %q execution failed: %v", trigger.Name, err)
		}
	}
	return nil
}

// executeTriggerFunction executes a single trigger's function body.
func (e *MVCCExecutor) executeTriggerFunction(trigger *catalog.TriggerMeta, ctx *TriggerContext) error {
	// Require procedure catalog for function lookup
	if e.procCat == nil {
		return fmt.Errorf("procedure catalog not available for trigger execution")
	}

	// Require session for PL interpreter
	if e.session == nil {
		return fmt.Errorf("session not available for trigger execution")
	}

	// Look up the trigger function
	funcMeta, ok := e.procCat.GetFunction(trigger.FunctionName)
	if !ok {
		return fmt.Errorf("trigger function %q not found", trigger.FunctionName)
	}

	// Create a PL interpreter for this trigger execution
	pl := NewPLInterpreter(e.session)

	// Bind OLD and NEW row variables based on the trigger event
	if ctx != nil && ctx.Schema != nil {
		if err := e.bindTriggerRowVariables(pl, ctx); err != nil {
			return err
		}
	}

	// Parse the function body
	body, err := NewParser(funcMeta.Body).parsePLBlock()
	if err != nil {
		return fmt.Errorf("error parsing trigger function body: %v", err)
	}

	// Execute the function
	if err := pl.ExecuteBlock(body); err != nil {
		return fmt.Errorf("trigger function error: %v", err)
	}

	return nil
}

// bindTriggerRowVariables binds OLD and NEW record variables in the PL interpreter.
func (e *MVCCExecutor) bindTriggerRowVariables(pl *PLInterpreter, ctx *TriggerContext) error {
	// For row-level triggers, we bind OLD and NEW as composite record types.
	// For simplicity, we bind each column as OLD_colname and NEW_colname.
	if ctx.Schema == nil {
		return nil
	}

	// Bind OLD row columns (for UPDATE/DELETE)
	if ctx.OldRow != nil {
		for i, col := range ctx.Schema.Columns {
			if i < len(ctx.OldRow) {
				varName := "old_" + col.Name
				pl.DeclareVariable(varName, col.Type, nil)
				_ = pl.SetVariable(varName, ctx.OldRow[i])
			}
		}
	}

	// Bind NEW row columns (for INSERT/UPDATE)
	if ctx.NewRow != nil {
		for i, col := range ctx.Schema.Columns {
			if i < len(ctx.NewRow) {
				varName := "new_" + col.Name
				pl.DeclareVariable(varName, col.Type, nil)
				_ = pl.SetVariable(varName, ctx.NewRow[i])
			}
		}
	}

	return nil
}

// fireBeforeInsertTriggers fires BEFORE INSERT triggers.
func (e *MVCCExecutor) fireBeforeInsertTriggers(tableName string, newRow []catalog.Value, schema *catalog.Schema) error {
	return e.fireTriggers(tableName, catalog.TriggerBefore, catalog.TriggerInsert, &TriggerContext{
		TableName: tableName,
		NewRow:    newRow,
		Schema:    schema,
	})
}

// fireAfterInsertTriggers fires AFTER INSERT triggers.
func (e *MVCCExecutor) fireAfterInsertTriggers(tableName string, newRow []catalog.Value, schema *catalog.Schema) error {
	return e.fireTriggers(tableName, catalog.TriggerAfter, catalog.TriggerInsert, &TriggerContext{
		TableName: tableName,
		NewRow:    newRow,
		Schema:    schema,
	})
}

// fireBeforeUpdateTriggers fires BEFORE UPDATE triggers.
func (e *MVCCExecutor) fireBeforeUpdateTriggers(tableName string, oldRow, newRow []catalog.Value, schema *catalog.Schema) error {
	return e.fireTriggers(tableName, catalog.TriggerBefore, catalog.TriggerUpdate, &TriggerContext{
		TableName: tableName,
		OldRow:    oldRow,
		NewRow:    newRow,
		Schema:    schema,
	})
}

// fireAfterUpdateTriggers fires AFTER UPDATE triggers.
func (e *MVCCExecutor) fireAfterUpdateTriggers(tableName string, oldRow, newRow []catalog.Value, schema *catalog.Schema) error {
	return e.fireTriggers(tableName, catalog.TriggerAfter, catalog.TriggerUpdate, &TriggerContext{
		TableName: tableName,
		OldRow:    oldRow,
		NewRow:    newRow,
		Schema:    schema,
	})
}

// fireBeforeDeleteTriggers fires BEFORE DELETE triggers.
func (e *MVCCExecutor) fireBeforeDeleteTriggers(tableName string, oldRow []catalog.Value, schema *catalog.Schema) error {
	return e.fireTriggers(tableName, catalog.TriggerBefore, catalog.TriggerDelete, &TriggerContext{
		TableName: tableName,
		OldRow:    oldRow,
		Schema:    schema,
	})
}

// fireAfterDeleteTriggers fires AFTER DELETE triggers.
func (e *MVCCExecutor) fireAfterDeleteTriggers(tableName string, oldRow []catalog.Value, schema *catalog.Schema) error {
	return e.fireTriggers(tableName, catalog.TriggerAfter, catalog.TriggerDelete, &TriggerContext{
		TableName: tableName,
		OldRow:    oldRow,
		Schema:    schema,
	})
}

// ==================== Full-Text Search Evaluation Functions ====================

// evalTSVector evaluates to_tsvector() - converts text to a text search vector.
func (e *MVCCExecutor) evalTSVector(expr *TSVectorExpr, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	// Evaluate the text expression
	textVal, err := e.evalExpr(expr.Text, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}

	// Get the text content
	var text string
	if textVal.IsNull {
		return catalog.Null(catalog.TypeText), nil
	}

	switch textVal.Type {
	case catalog.TypeText:
		text = textVal.Text
	case catalog.TypeJSON:
		text = textVal.JSON
	default:
		return catalog.Value{}, fmt.Errorf("to_tsvector requires text argument, got %v", textVal.Type)
	}

	// Create TSVector representation
	tsv := ftsNewTSVector(text)
	return catalog.NewText(tsv), nil
}

// evalTSQuery evaluates to_tsquery(), plainto_tsquery(), websearch_to_tsquery().
func (e *MVCCExecutor) evalTSQuery(expr *TSQueryExpr, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	// Evaluate the query expression
	queryVal, err := e.evalExpr(expr.Query, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}

	if queryVal.IsNull {
		return catalog.Null(catalog.TypeText), nil
	}

	var query string
	if queryVal.Type == catalog.TypeText {
		query = queryVal.Text
	} else {
		return catalog.Value{}, fmt.Errorf("tsquery requires text argument")
	}

	// Create TSQuery representation
	tsq := ftsNewTSQuery(query, expr.PlainText)
	return catalog.NewText(tsq), nil
}

// evalTSMatch evaluates the @@ text search match operator.
func (e *MVCCExecutor) evalTSMatch(expr *TSMatchExpr, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	// Evaluate left (vector/text) and right (query/text)
	leftVal, err := e.evalExpr(expr.Left, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}
	rightVal, err := e.evalExpr(expr.Right, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}

	if leftVal.IsNull || rightVal.IsNull {
		return catalog.Null(catalog.TypeBool), nil
	}

	// Get text from both sides
	var leftText, rightText string
	if leftVal.Type == catalog.TypeText {
		leftText = leftVal.Text
	} else {
		leftText = leftVal.String()
	}
	if rightVal.Type == catalog.TypeText {
		rightText = rightVal.Text
	} else {
		rightText = rightVal.String()
	}

	// Perform text search match
	matches := ftsMatch(leftText, rightText)
	return catalog.NewBool(matches), nil
}

// evalTSRank evaluates ts_rank() - calculates relevance ranking.
func (e *MVCCExecutor) evalTSRank(expr *TSRankExpr, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	// Evaluate vector and query
	vectorVal, err := e.evalExpr(expr.Vector, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}
	queryVal, err := e.evalExpr(expr.Query, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}

	if vectorVal.IsNull || queryVal.IsNull {
		return catalog.Null(catalog.TypeFloat64), nil
	}

	// Get text representations
	var vectorText, queryText string
	if vectorVal.Type == catalog.TypeText {
		vectorText = vectorVal.Text
	} else {
		vectorText = vectorVal.String()
	}
	if queryVal.Type == catalog.TypeText {
		queryText = queryVal.Text
	} else {
		queryText = queryVal.String()
	}

	// Calculate rank
	rank := ftsRank(vectorText, queryText)
	return catalog.NewFloat64(rank), nil
}

// evalTSHeadline evaluates ts_headline() - generates result headlines with highlighted terms.
func (e *MVCCExecutor) evalTSHeadline(expr *TSHeadlineExpr, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	// Evaluate text and query
	textVal, err := e.evalExpr(expr.Text, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}
	queryVal, err := e.evalExpr(expr.Query, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}

	if textVal.IsNull || queryVal.IsNull {
		return catalog.Null(catalog.TypeText), nil
	}

	var text, query string
	if textVal.Type == catalog.TypeText {
		text = textVal.Text
	} else {
		text = textVal.String()
	}
	if queryVal.Type == catalog.TypeText {
		query = queryVal.Text
	} else {
		query = queryVal.String()
	}

	// Generate headline with highlighted terms
	headline := ftsHeadline(text, query)
	return catalog.NewText(headline), nil
}

// evalMatchAgainst evaluates MySQL-style MATCH...AGAINST expression.
func (e *MVCCExecutor) evalMatchAgainst(expr *MatchAgainstExpr, schema *catalog.Schema, row []catalog.Value, tx *txn.Transaction) (catalog.Value, error) {
	// Get query text
	queryVal, err := e.evalExpr(expr.Query, schema, row, tx)
	if err != nil {
		return catalog.Value{}, err
	}

	if queryVal.IsNull {
		return catalog.NewFloat64(0), nil
	}

	var queryText string
	if queryVal.Type == catalog.TypeText {
		queryText = queryVal.Text
	} else {
		queryText = queryVal.String()
	}

	// Concatenate all specified columns
	var combinedText strings.Builder
	for i, colName := range expr.Columns {
		colIdx := -1
		for j, col := range schema.Columns {
			if col.Name == colName {
				colIdx = j
				break
			}
		}
		if colIdx == -1 {
			return catalog.Value{}, fmt.Errorf("column %q not found", colName)
		}
		if colIdx < len(row) {
			if i > 0 {
				combinedText.WriteString(" ")
			}
			if row[colIdx].Type == catalog.TypeText {
				combinedText.WriteString(row[colIdx].Text)
			}
		}
	}

	// Calculate relevance score
	score := ftsRank(combinedText.String(), queryText)

	// In boolean mode, return 0 or 1
	if expr.InBoolMode {
		if ftsMatch(combinedText.String(), queryText) {
			return catalog.NewFloat64(1.0), nil
		}
		return catalog.NewFloat64(0.0), nil
	}

	return catalog.NewFloat64(score), nil
}

// ==================== FTS Helper Functions ====================

// ftsNewTSVector creates a tsvector string representation from text.
func ftsNewTSVector(text string) string {
	// Simple tokenization and normalization
	words := strings.Fields(strings.ToLower(text))
	seen := make(map[string][]int)

	for pos, word := range words {
		// Basic normalization - remove non-alphanumeric
		normalized := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
				return r
			}
			return -1
		}, word)

		if len(normalized) > 1 && !isStopWord(normalized) {
			// Simple stemming (just remove common suffixes)
			stemmed := simpleStem(normalized)
			seen[stemmed] = append(seen[stemmed], pos+1)
		}
	}

	// Build tsvector string
	var parts []string
	for term, positions := range seen {
		posStr := make([]string, len(positions))
		for i, p := range positions {
			posStr[i] = fmt.Sprintf("%d", p)
		}
		parts = append(parts, fmt.Sprintf("'%s':%s", term, strings.Join(posStr, ",")))
	}

	return strings.Join(parts, " ")
}

// ftsNewTSQuery creates a tsquery string representation.
func ftsNewTSQuery(query string, plainText bool) string {
	words := strings.Fields(strings.ToLower(query))
	var terms []string

	for _, word := range words {
		// Skip operators in boolean mode
		if word == "&" || word == "|" || word == "!" {
			if !plainText {
				terms = append(terms, word)
			}
			continue
		}

		// Normalize
		normalized := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
				return r
			}
			return -1
		}, word)

		if len(normalized) > 0 && !isStopWord(normalized) {
			stemmed := simpleStem(normalized)
			terms = append(terms, fmt.Sprintf("'%s'", stemmed))
		}
	}

	if plainText {
		return strings.Join(terms, " & ")
	}
	return strings.Join(terms, " ")
}

// ftsMatch performs a full-text search match.
func ftsMatch(document, query string) bool {
	// Tokenize document
	docWords := make(map[string]bool)
	for _, word := range strings.Fields(strings.ToLower(document)) {
		normalized := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
				return r
			}
			return -1
		}, word)
		if len(normalized) > 1 {
			docWords[simpleStem(normalized)] = true
		}
	}

	// Check if all query terms are present
	queryTerms := strings.Fields(strings.ToLower(query))
	matchCount := 0
	totalTerms := 0

	for _, term := range queryTerms {
		// Skip operators
		if term == "&" || term == "|" || term == "!" {
			continue
		}

		normalized := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
				return r
			}
			return -1
		}, term)

		if len(normalized) > 0 && !isStopWord(normalized) {
			totalTerms++
			stemmed := simpleStem(normalized)
			if docWords[stemmed] {
				matchCount++
			}
		}
	}

	// Match if at least half of terms found (or all for strict matching)
	return totalTerms > 0 && matchCount > 0
}

// ftsRank calculates a relevance score (simplified TF-IDF).
func ftsRank(document, query string) float64 {
	// Count term frequencies in document
	docWords := make(map[string]int)
	totalWords := 0
	for _, word := range strings.Fields(strings.ToLower(document)) {
		normalized := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
				return r
			}
			return -1
		}, word)
		if len(normalized) > 1 {
			stemmed := simpleStem(normalized)
			docWords[stemmed]++
			totalWords++
		}
	}

	if totalWords == 0 {
		return 0
	}

	// Calculate score based on query term frequencies
	queryTerms := strings.Fields(strings.ToLower(query))
	score := 0.0

	for _, term := range queryTerms {
		normalized := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
				return r
			}
			return -1
		}, term)

		if len(normalized) > 0 && !isStopWord(normalized) {
			stemmed := simpleStem(normalized)
			if count, exists := docWords[stemmed]; exists {
				// TF component
				tf := float64(count) / float64(totalWords)
				score += tf
			}
		}
	}

	return score
}

// ftsHeadline generates a headline with search terms highlighted.
func ftsHeadline(document, query string) string {
	// Extract query terms
	queryTerms := make(map[string]bool)
	for _, term := range strings.Fields(strings.ToLower(query)) {
		normalized := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
				return r
			}
			return -1
		}, term)
		if len(normalized) > 0 {
			queryTerms[simpleStem(normalized)] = true
		}
	}

	// Find sentences containing query terms
	words := strings.Fields(document)
	if len(words) == 0 {
		return ""
	}

	// Find windows around matching terms
	var highlights []string
	windowSize := 10

	for i, word := range words {
		normalized := strings.Map(func(r rune) rune {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
				return r
			}
			return -1
		}, strings.ToLower(word))

		if len(normalized) > 0 && queryTerms[simpleStem(normalized)] {
			// Found a match - extract window
			start := i - windowSize/2
			if start < 0 {
				start = 0
			}
			end := i + windowSize/2
			if end > len(words) {
				end = len(words)
			}

			snippet := words[start:end]
			// Highlight the matching word
			for j, w := range snippet {
				wNorm := strings.Map(func(r rune) rune {
					if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
						return r
					}
					return -1
				}, strings.ToLower(w))
				if queryTerms[simpleStem(wNorm)] {
					snippet[j] = "<b>" + w + "</b>"
				}
			}

			prefix := ""
			suffix := ""
			if start > 0 {
				prefix = "..."
			}
			if end < len(words) {
				suffix = "..."
			}

			highlights = append(highlights, prefix+strings.Join(snippet, " ")+suffix)

			// Skip ahead to avoid overlapping highlights
			if len(highlights) >= 3 {
				break
			}
		}
	}

	if len(highlights) == 0 {
		// No matches - return beginning of document
		end := windowSize
		if end > len(words) {
			end = len(words)
		}
		return strings.Join(words[:end], " ") + "..."
	}

	return strings.Join(highlights, " ... ")
}

// simpleStem performs basic suffix removal for stemming.
func simpleStem(word string) string {
	// Very basic Porter-style stemming
	suffixes := []string{"ing", "ed", "es", "er", "ly", "tion", "ness", "ment", "ful", "less", "able", "ible", "ous", "ive", "al"}

	for _, suffix := range suffixes {
		if len(word) > len(suffix)+2 && strings.HasSuffix(word, suffix) {
			return word[:len(word)-len(suffix)]
		}
	}

	// Handle plural 's'
	if len(word) > 3 && strings.HasSuffix(word, "s") && !strings.HasSuffix(word, "ss") {
		return word[:len(word)-1]
	}

	return word
}

// isStopWord checks if a word is a common stop word.
func isStopWord(word string) bool {
	stopWords := map[string]bool{
		"a": true, "an": true, "and": true, "are": true, "as": true, "at": true,
		"be": true, "by": true, "for": true, "from": true, "has": true, "he": true,
		"in": true, "is": true, "it": true, "its": true, "of": true, "on": true,
		"or": true, "that": true, "the": true, "to": true, "was": true, "were": true,
		"will": true, "with": true, "this": true, "but": true, "they": true,
		"have": true, "had": true, "what": true, "when": true, "where": true,
		"who": true, "which": true, "why": true, "how": true, "all": true,
		"each": true, "every": true, "both": true, "few": true, "more": true,
		"most": true, "other": true, "some": true, "such": true, "no": true,
		"nor": true, "not": true, "only": true, "own": true, "same": true,
		"so": true, "than": true, "too": true, "very": true, "can": true,
		"just": true, "should": true, "now": true, "i": true, "me": true,
		"my": true, "we": true, "our": true, "you": true, "your": true,
	}
	return stopWords[word]
}
