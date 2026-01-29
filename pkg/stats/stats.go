package stats

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

// StatsManager manages table and column statistics for query optimization.
type StatsManager struct {
	tables map[string]*TableStats
	mu     sync.RWMutex
}

// NewStatsManager creates a new statistics manager.
func NewStatsManager() *StatsManager {
	return &StatsManager{
		tables: make(map[string]*TableStats),
	}
}

// TableStats contains statistics for a table.
type TableStats struct {
	TableName    string
	RowCount     int64
	PageCount    int32
	AvgRowSize   int32 // Average row size in bytes
	LastAnalyzed time.Time
	Columns      map[string]*ColumnStats
}

// ColumnStats contains statistics for a single column.
type ColumnStats struct {
	ColumnName      string
	DataType        catalog.DataType
	NullCount       int64      // Number of NULL values
	DistinctCount   int64      // Number of distinct values (n_distinct)
	MostCommonVals  []Value    // Most common values
	MostCommonFreqs []float64  // Frequencies of most common values
	Histogram       *Histogram // Distribution histogram for range queries
	MinValue        Value      // Minimum value in column
	MaxValue        Value      // Maximum value in column
	AvgWidth        int32      // Average width in bytes (for strings)
}

// Value represents a typed value for statistics.
type Value struct {
	Type      catalog.DataType
	IntVal    int64
	FloatVal  float64
	StringVal string
	BoolVal   bool
	IsNull    bool
}

// Histogram represents data distribution for a column.
type Histogram struct {
	Bounds         []Value // Upper bounds of histogram buckets
	Frequencies    []int64 // Number of rows in each bucket
	DistinctCounts []int64 // Distinct values per bucket
	NumBuckets     int
}

// GetTableStats returns statistics for a table.
func (sm *StatsManager) GetTableStats(tableName string) (*TableStats, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stats, exists := sm.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("no statistics for table %s", tableName)
	}

	return stats, nil
}

// SetTableStats stores statistics for a table.
func (sm *StatsManager) SetTableStats(stats *TableStats) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	stats.LastAnalyzed = time.Now()
	sm.tables[stats.TableName] = stats

	return nil
}

// DeleteTableStats removes statistics for a table.
func (sm *StatsManager) DeleteTableStats(tableName string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	delete(sm.tables, tableName)
}

// EstimateSelectivity estimates the selectivity of a predicate on a column.
// Returns a value between 0.0 and 1.0 representing the fraction of rows that match.
func (cs *ColumnStats) EstimateSelectivity(op string, value Value) float64 {
	if cs == nil {
		return 0.1 // Default selectivity if no stats
	}

	totalRows := float64(cs.DistinctCount)
	if totalRows == 0 {
		return 1.0
	}

	switch op {
	case "=":
		// Equality: 1 / n_distinct (uniform distribution assumption)
		if cs.DistinctCount > 0 {
			// Check most common values
			for i, mcv := range cs.MostCommonVals {
				if valuesEqual(mcv, value) {
					return cs.MostCommonFreqs[i]
				}
			}
			// Not in MCV, assume uniform distribution for remaining
			return 1.0 / float64(cs.DistinctCount)
		}
		return 0.1

	case "<", "<=", ">", ">=":
		// Range query: use histogram if available
		if cs.Histogram != nil {
			return cs.estimateRangeSelectivity(op, value)
		}
		// Fallback: assume 33% for < or >, 50% for <= or >=
		if op == "<" || op == ">" {
			return 0.33
		}
		return 0.5

	case "!=":
		// Not equal: 1 - selectivity(=)
		return 1.0 - cs.EstimateSelectivity("=", value)

	case "IS NULL":
		// NULL check
		if cs.NullCount == 0 {
			return 0.0
		}
		totalCount := cs.NullCount + (cs.DistinctCount * 10) // Rough estimate
		return float64(cs.NullCount) / float64(totalCount)

	case "IS NOT NULL":
		return 1.0 - cs.EstimateSelectivity("IS NULL", value)

	default:
		return 0.1 // Unknown operator
	}
}

// estimateRangeSelectivity estimates selectivity for range predicates using histogram.
func (cs *ColumnStats) estimateRangeSelectivity(op string, value Value) float64 {
	if cs.Histogram == nil || len(cs.Histogram.Bounds) == 0 {
		return 0.33 // Default fallback
	}

	h := cs.Histogram
	totalRows := int64(0)
	for _, freq := range h.Frequencies {
		totalRows += freq
	}

	if totalRows == 0 {
		return 0.33
	}

	// Find which bucket the value falls into
	bucketIdx := -1
	for i, bound := range h.Bounds {
		if compareValues(value, bound) <= 0 {
			bucketIdx = i
			break
		}
	}

	if bucketIdx == -1 {
		// Value is beyond all bounds
		if op == ">" || op == ">=" {
			return 0.0
		}
		return 1.0
	}

	// Calculate selectivity based on bucket
	matchingRows := int64(0)

	switch op {
	case "<", "<=":
		// Sum all rows up to and including this bucket
		for i := 0; i <= bucketIdx; i++ {
			matchingRows += h.Frequencies[i]
		}
		if op == "<" && bucketIdx >= 0 {
			// Subtract half of the boundary bucket for strict <
			matchingRows -= h.Frequencies[bucketIdx] / 2
		}

	case ">", ">=":
		// Sum all rows after this bucket
		for i := bucketIdx + 1; i < len(h.Frequencies); i++ {
			matchingRows += h.Frequencies[i]
		}
		if op == ">=" && bucketIdx >= 0 {
			// Add back the boundary bucket
			matchingRows += h.Frequencies[bucketIdx]
		}
	}

	return float64(matchingRows) / float64(totalRows)
}

// EstimateCardinality estimates the number of rows returned by a query.
func (ts *TableStats) EstimateCardinality(predicates []Predicate) int64 {
	if ts.RowCount == 0 {
		return 0
	}

	// Start with total row count
	cardinality := float64(ts.RowCount)

	// Apply each predicate's selectivity (assume independence)
	for _, pred := range predicates {
		colStats, exists := ts.Columns[pred.Column]
		if exists {
			selectivity := colStats.EstimateSelectivity(pred.Operator, pred.Value)
			cardinality *= selectivity
		} else {
			// No stats for this column, use default selectivity
			cardinality *= 0.1
		}
	}

	return int64(math.Max(1, cardinality))
}

// Predicate represents a query predicate for cardinality estimation.
type Predicate struct {
	Column   string
	Operator string // =, <, >, <=, >=, !=, IS NULL, IS NOT NULL
	Value    Value
}

// compareValues compares two values. Returns -1, 0, or 1.
func compareValues(a, b Value) int {
	if a.IsNull || b.IsNull {
		return 0
	}

	switch a.Type {
	case catalog.TypeInt32, catalog.TypeInt64:
		if a.IntVal < b.IntVal {
			return -1
		} else if a.IntVal > b.IntVal {
			return 1
		}
		return 0

	case catalog.TypeFloat64:
		if a.FloatVal < b.FloatVal {
			return -1
		} else if a.FloatVal > b.FloatVal {
			return 1
		}
		return 0

	case catalog.TypeText:
		if a.StringVal < b.StringVal {
			return -1
		} else if a.StringVal > b.StringVal {
			return 1
		}
		return 0

	case catalog.TypeBool:
		if a.BoolVal == b.BoolVal {
			return 0
		} else if !a.BoolVal && b.BoolVal {
			return -1
		}
		return 1

	default:
		return 0
	}
}

// valuesEqual checks if two values are equal.
func valuesEqual(a, b Value) bool {
	if a.IsNull && b.IsNull {
		return true
	}
	if a.IsNull || b.IsNull {
		return false
	}
	if a.Type != b.Type {
		return false
	}

	switch a.Type {
	case catalog.TypeInt32, catalog.TypeInt64:
		return a.IntVal == b.IntVal
	case catalog.TypeFloat64:
		return a.FloatVal == b.FloatVal
	case catalog.TypeText:
		return a.StringVal == b.StringVal
	case catalog.TypeBool:
		return a.BoolVal == b.BoolVal
	default:
		return false
	}
}

// NewHistogram creates a histogram with specified number of buckets.
func NewHistogram(numBuckets int) *Histogram {
	return &Histogram{
		Bounds:         make([]Value, 0, numBuckets),
		Frequencies:    make([]int64, 0, numBuckets),
		DistinctCounts: make([]int64, 0, numBuckets),
		NumBuckets:     numBuckets,
	}
}

// AddBucket adds a bucket to the histogram.
func (h *Histogram) AddBucket(upperBound Value, frequency int64, distinctCount int64) {
	h.Bounds = append(h.Bounds, upperBound)
	h.Frequencies = append(h.Frequencies, frequency)
	h.DistinctCounts = append(h.DistinctCounts, distinctCount)
}
