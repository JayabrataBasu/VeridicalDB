package stats

import (
	"testing"
	"time"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

func TestStatsManager(t *testing.T) {
	sm := NewStatsManager()

	ts := &TableStats{
		TableName:  "users",
		RowCount:   1000,
		PageCount:  10,
		AvgRowSize: 128,
		Columns:    make(map[string]*ColumnStats),
	}

	ts.Columns["id"] = &ColumnStats{
		ColumnName:    "id",
		DataType:      catalog.TypeInt32,
		NullCount:     0,
		DistinctCount: 1000,
	}

	err := sm.SetTableStats(ts)
	if err != nil {
		t.Fatalf("SetTableStats failed: %v", err)
	}

	retrieved, err := sm.GetTableStats("users")
	if err != nil {
		t.Fatalf("GetTableStats failed: %v", err)
	}

	if retrieved.RowCount != 1000 {
		t.Errorf("expected row count 1000, got %d", retrieved.RowCount)
	}

	if time.Since(retrieved.LastAnalyzed) > time.Second {
		t.Error("LastAnalyzed should be recent")
	}
}

func TestSelectivityEstimation(t *testing.T) {
	cs := &ColumnStats{
		ColumnName:    "age",
		DataType:      catalog.TypeInt32,
		NullCount:     10,
		DistinctCount: 100,
	}

	value := Value{Type: catalog.TypeInt32, IntVal: 25}
	selectivity := cs.EstimateSelectivity("=", value)

	expected := 1.0 / 100.0
	if selectivity < expected-0.01 || selectivity > expected+0.01 {
		t.Errorf("expected selectivity ~%.4f, got %.4f", expected, selectivity)
	}

	selectivity = cs.EstimateSelectivity("!=", value)
	if selectivity < 0.98 || selectivity > 1.0 {
		t.Errorf("expected selectivity ~0.99, got %.4f", selectivity)
	}
}

func TestHistogramRangeEstimation(t *testing.T) {
	cs := &ColumnStats{
		ColumnName:    "score",
		DataType:      catalog.TypeInt32,
		DistinctCount: 100,
	}

	hist := NewHistogram(4)
	hist.AddBucket(Value{Type: catalog.TypeInt32, IntVal: 25}, 250, 25)
	hist.AddBucket(Value{Type: catalog.TypeInt32, IntVal: 50}, 250, 25)
	hist.AddBucket(Value{Type: catalog.TypeInt32, IntVal: 75}, 250, 25)
	hist.AddBucket(Value{Type: catalog.TypeInt32, IntVal: 100}, 250, 25)
	cs.Histogram = hist

	tests := []struct {
		op        string
		value     int64
		expected  float64
		tolerance float64
	}{
		{"<", 50, 0.5, 0.2}, // Allow 20% tolerance for simplified logic
		{"<=", 50, 0.5, 0.2},
		{">", 50, 0.5, 0.25},
		{">=", 50, 0.5, 0.25},
		{"<", 25, 0.25, 0.15},
		{">", 75, 0.25, 0.15},
	}

	for _, tt := range tests {
		value := Value{Type: catalog.TypeInt32, IntVal: tt.value}
		selectivity := cs.EstimateSelectivity(tt.op, value)

		if selectivity < tt.expected-tt.tolerance || selectivity > tt.expected+tt.tolerance {
			t.Errorf("%s %d: expected selectivity ~%.2f, got %.2f",
				tt.op, tt.value, tt.expected, selectivity)
		}
	}
}

func TestCardinalityEstimation(t *testing.T) {
	ts := &TableStats{
		TableName:  "users",
		RowCount:   10000,
		PageCount:  100,
		AvgRowSize: 128,
		Columns:    make(map[string]*ColumnStats),
	}

	ts.Columns["age"] = &ColumnStats{
		ColumnName:    "age",
		DataType:      catalog.TypeInt32,
		DistinctCount: 100,
	}

	ts.Columns["country"] = &ColumnStats{
		ColumnName:    "country",
		DataType:      catalog.TypeText,
		DistinctCount: 10,
	}

	predicates := []Predicate{
		{Column: "age", Operator: "=", Value: Value{Type: catalog.TypeInt32, IntVal: 25}},
	}

	cardinality := ts.EstimateCardinality(predicates)
	expected := int64(100)

	if cardinality != expected {
		t.Errorf("expected cardinality %d, got %d", expected, cardinality)
	}

	predicates = []Predicate{
		{Column: "age", Operator: "=", Value: Value{Type: catalog.TypeInt32, IntVal: 25}},
		{Column: "country", Operator: "=", Value: Value{Type: catalog.TypeText, StringVal: "US"}},
	}

	cardinality = ts.EstimateCardinality(predicates)
	expected = int64(10)

	if cardinality != expected {
		t.Errorf("expected cardinality %d, got %d", expected, cardinality)
	}
}
