package partition

import (
	"testing"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

func TestPartitionType(t *testing.T) {
	tests := []struct {
		pt       PartitionType
		expected string
	}{
		{PartitionTypeNone, "NONE"},
		{PartitionTypeRange, "RANGE"},
		{PartitionTypeList, "LIST"},
		{PartitionTypeHash, "HASH"},
	}

	for _, tt := range tests {
		if got := tt.pt.String(); got != tt.expected {
			t.Errorf("PartitionType.String() = %v, want %v", got, tt.expected)
		}
	}
}

func TestParsePartitionType(t *testing.T) {
	tests := []struct {
		input    string
		expected PartitionType
	}{
		{"RANGE", PartitionTypeRange},
		{"LIST", PartitionTypeList},
		{"HASH", PartitionTypeHash},
		{"NONE", PartitionTypeNone},
		{"invalid", PartitionTypeNone},
	}

	for _, tt := range tests {
		if got := ParsePartitionType(tt.input); got != tt.expected {
			t.Errorf("ParsePartitionType(%q) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

func TestRangePartitionRouting(t *testing.T) {
	// Create a RANGE partition spec with 3 partitions
	spec := &PartitionSpec{
		Type:    PartitionTypeRange,
		Columns: []string{"year"},
		Partitions: []PartitionDef{
			{
				Name: "p_2020",
				Bound: PartitionBound{
					LessThan: catalog.NewInt32(2021),
				},
			},
			{
				Name: "p_2021",
				Bound: PartitionBound{
					LessThan: catalog.NewInt32(2022),
				},
			},
			{
				Name: "p_future",
				Bound: PartitionBound{
					IsMaxValue: true,
				},
			},
		},
	}

	router := NewRouter(spec)

	tests := []struct {
		value     catalog.Value
		partition string
	}{
		{catalog.NewInt32(2019), "p_2020"},
		{catalog.NewInt32(2020), "p_2020"},
		{catalog.NewInt32(2021), "p_2021"},
		{catalog.NewInt32(2022), "p_future"},
		{catalog.NewInt32(2030), "p_future"},
	}

	for _, tt := range tests {
		got, err := router.Route(tt.value)
		if err != nil {
			t.Errorf("Route(%v) error: %v", tt.value, err)
			continue
		}
		if got != tt.partition {
			t.Errorf("Route(%v) = %v, want %v", tt.value, got, tt.partition)
		}
	}
}

func TestListPartitionRouting(t *testing.T) {
	// Create a LIST partition spec
	spec := &PartitionSpec{
		Type:    PartitionTypeList,
		Columns: []string{"region"},
		Partitions: []PartitionDef{
			{
				Name: "p_us",
				Bound: PartitionBound{
					Values: []catalog.Value{
						catalog.NewText("US"),
						catalog.NewText("USA"),
					},
				},
			},
			{
				Name: "p_eu",
				Bound: PartitionBound{
					Values: []catalog.Value{
						catalog.NewText("UK"),
						catalog.NewText("DE"),
						catalog.NewText("FR"),
					},
				},
			},
			{
				Name: "p_asia",
				Bound: PartitionBound{
					Values: []catalog.Value{
						catalog.NewText("JP"),
						catalog.NewText("CN"),
						catalog.NewText("IN"),
					},
				},
			},
		},
	}

	router := NewRouter(spec)

	tests := []struct {
		value     catalog.Value
		partition string
		shouldErr bool
	}{
		{catalog.NewText("US"), "p_us", false},
		{catalog.NewText("USA"), "p_us", false},
		{catalog.NewText("UK"), "p_eu", false},
		{catalog.NewText("DE"), "p_eu", false},
		{catalog.NewText("JP"), "p_asia", false},
		{catalog.NewText("BR"), "", true}, // Not in any partition
	}

	for _, tt := range tests {
		got, err := router.Route(tt.value)
		if tt.shouldErr {
			if err == nil {
				t.Errorf("Route(%v) expected error, got %v", tt.value, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Route(%v) error: %v", tt.value, err)
			continue
		}
		if got != tt.partition {
			t.Errorf("Route(%v) = %v, want %v", tt.value, got, tt.partition)
		}
	}
}

func TestHashPartitionRouting(t *testing.T) {
	// Create a HASH partition spec with 4 partitions
	spec := CreateHashPartitionSpec([]string{"id"}, 4)

	router := NewRouter(spec)

	// Test that routing is deterministic
	value1 := catalog.NewInt32(100)
	p1, err := router.Route(value1)
	if err != nil {
		t.Fatalf("Route error: %v", err)
	}

	// Same value should always go to same partition
	for i := 0; i < 10; i++ {
		p, err := router.Route(value1)
		if err != nil {
			t.Fatalf("Route error: %v", err)
		}
		if p != p1 {
			t.Errorf("Hash routing not deterministic: got %v, expected %v", p, p1)
		}
	}

	// Test distribution across partitions
	partitionCounts := make(map[string]int)
	for i := 0; i < 1000; i++ {
		p, err := router.Route(catalog.NewInt32(int32(i)))
		if err != nil {
			t.Fatalf("Route error for %d: %v", i, err)
		}
		partitionCounts[p]++
	}

	// Should have all 4 partitions used
	if len(partitionCounts) != 4 {
		t.Errorf("Expected 4 partitions used, got %d: %v", len(partitionCounts), partitionCounts)
	}

	// Each partition should have at least some entries (reasonably balanced)
	for p, count := range partitionCounts {
		if count < 100 { // At least 100 out of 1000
			t.Logf("Partition %s has %d entries (possibly unbalanced)", p, count)
		}
	}
}

func TestRouteMulti(t *testing.T) {
	spec := &PartitionSpec{
		Type:    PartitionTypeList,
		Columns: []string{"status"},
		Partitions: []PartitionDef{
			{
				Name: "p_active",
				Bound: PartitionBound{
					Values: []catalog.Value{catalog.NewText("active"), catalog.NewText("pending")},
				},
			},
			{
				Name: "p_inactive",
				Bound: PartitionBound{
					Values: []catalog.Value{catalog.NewText("inactive"), catalog.NewText("archived")},
				},
			},
		},
	}

	router := NewRouter(spec)

	values := []catalog.Value{
		catalog.NewText("active"),
		catalog.NewText("inactive"),
		catalog.NewText("active"), // duplicate
	}

	partitions, err := router.RouteMulti(values)
	if err != nil {
		t.Fatalf("RouteMulti error: %v", err)
	}

	if len(partitions) != 2 {
		t.Errorf("Expected 2 unique partitions, got %d: %v", len(partitions), partitions)
	}
}

func TestPrunerEquality(t *testing.T) {
	spec := &PartitionSpec{
		Type:    PartitionTypeRange,
		Columns: []string{"id"},
		Partitions: []PartitionDef{
			{Name: "p0", Bound: PartitionBound{LessThan: catalog.NewInt32(100)}},
			{Name: "p1", Bound: PartitionBound{LessThan: catalog.NewInt32(200)}},
			{Name: "p2", Bound: PartitionBound{IsMaxValue: true}},
		},
	}

	pruner := NewPruner(spec)

	tests := []struct {
		value      catalog.Value
		partitions []string
	}{
		{catalog.NewInt32(50), []string{"p0"}},
		{catalog.NewInt32(150), []string{"p1"}},
		{catalog.NewInt32(300), []string{"p2"}},
	}

	for _, tt := range tests {
		got := pruner.PruneForEquality(tt.value)
		if len(got) != len(tt.partitions) {
			t.Errorf("PruneForEquality(%v) = %v, want %v", tt.value, got, tt.partitions)
			continue
		}
		for i, p := range got {
			if p != tt.partitions[i] {
				t.Errorf("PruneForEquality(%v)[%d] = %v, want %v", tt.value, i, p, tt.partitions[i])
			}
		}
	}
}

func TestPrunerRange(t *testing.T) {
	spec := &PartitionSpec{
		Type:    PartitionTypeRange,
		Columns: []string{"id"},
		Partitions: []PartitionDef{
			{Name: "p0", Bound: PartitionBound{LessThan: catalog.NewInt32(100)}},
			{Name: "p1", Bound: PartitionBound{LessThan: catalog.NewInt32(200)}},
			{Name: "p2", Bound: PartitionBound{LessThan: catalog.NewInt32(300)}},
			{Name: "p3", Bound: PartitionBound{IsMaxValue: true}},
		},
	}

	pruner := NewPruner(spec)

	// Range 50-150 should touch p0 and p1
	low := catalog.NewInt32(50)
	high := catalog.NewInt32(150)
	partitions := pruner.PruneForRange(low, high, true, true)

	// Should not return empty
	if len(partitions) == 0 {
		t.Error("PruneForRange returned empty")
	}

	// Should include p0 and p1
	hasP0 := false
	hasP1 := false
	for _, p := range partitions {
		if p == "p0" {
			hasP0 = true
		}
		if p == "p1" {
			hasP1 = true
		}
	}
	if !hasP0 || !hasP1 {
		t.Errorf("Expected p0 and p1 in result, got %v", partitions)
	}
}

func TestValidatorRangePartitions(t *testing.T) {
	validator := NewValidator()
	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32},
			{Name: "year", Type: catalog.TypeInt32},
		},
	}

	// Valid RANGE partition
	validSpec := &PartitionSpec{
		Type:    PartitionTypeRange,
		Columns: []string{"year"},
		Partitions: []PartitionDef{
			{Name: "p1", Bound: PartitionBound{LessThan: catalog.NewInt32(2020)}},
			{Name: "p2", Bound: PartitionBound{LessThan: catalog.NewInt32(2021)}},
			{Name: "p3", Bound: PartitionBound{IsMaxValue: true}},
		},
	}
	if err := validator.ValidateSpec(validSpec, schema); err != nil {
		t.Errorf("ValidateSpec failed for valid spec: %v", err)
	}

	// Invalid: MAXVALUE not last
	invalidSpec1 := &PartitionSpec{
		Type:    PartitionTypeRange,
		Columns: []string{"year"},
		Partitions: []PartitionDef{
			{Name: "p1", Bound: PartitionBound{IsMaxValue: true}},
			{Name: "p2", Bound: PartitionBound{LessThan: catalog.NewInt32(2021)}},
		},
	}
	if err := validator.ValidateSpec(invalidSpec1, schema); err == nil {
		t.Error("ValidateSpec should fail when MAXVALUE is not last")
	}

	// Invalid: bounds not in ascending order
	invalidSpec2 := &PartitionSpec{
		Type:    PartitionTypeRange,
		Columns: []string{"year"},
		Partitions: []PartitionDef{
			{Name: "p1", Bound: PartitionBound{LessThan: catalog.NewInt32(2021)}},
			{Name: "p2", Bound: PartitionBound{LessThan: catalog.NewInt32(2020)}},
		},
	}
	if err := validator.ValidateSpec(invalidSpec2, schema); err == nil {
		t.Error("ValidateSpec should fail when bounds not in ascending order")
	}

	// Invalid: column not in schema
	invalidSpec3 := &PartitionSpec{
		Type:    PartitionTypeRange,
		Columns: []string{"nonexistent"},
		Partitions: []PartitionDef{
			{Name: "p1", Bound: PartitionBound{LessThan: catalog.NewInt32(2020)}},
		},
	}
	if err := validator.ValidateSpec(invalidSpec3, schema); err == nil {
		t.Error("ValidateSpec should fail when column doesn't exist")
	}
}

func TestValidatorListPartitions(t *testing.T) {
	validator := NewValidator()
	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "region", Type: catalog.TypeText},
		},
	}

	// Valid LIST partition
	validSpec := &PartitionSpec{
		Type:    PartitionTypeList,
		Columns: []string{"region"},
		Partitions: []PartitionDef{
			{Name: "p_us", Bound: PartitionBound{Values: []catalog.Value{catalog.NewText("US")}}},
			{Name: "p_eu", Bound: PartitionBound{Values: []catalog.Value{catalog.NewText("UK"), catalog.NewText("DE")}}},
		},
	}
	if err := validator.ValidateSpec(validSpec, schema); err != nil {
		t.Errorf("ValidateSpec failed for valid spec: %v", err)
	}

	// Invalid: duplicate values
	invalidSpec := &PartitionSpec{
		Type:    PartitionTypeList,
		Columns: []string{"region"},
		Partitions: []PartitionDef{
			{Name: "p1", Bound: PartitionBound{Values: []catalog.Value{catalog.NewText("US")}}},
			{Name: "p2", Bound: PartitionBound{Values: []catalog.Value{catalog.NewText("US")}}}, // Duplicate!
		},
	}
	if err := validator.ValidateSpec(invalidSpec, schema); err == nil {
		t.Error("ValidateSpec should fail for duplicate values")
	}

	// Invalid: empty values
	invalidSpec2 := &PartitionSpec{
		Type:    PartitionTypeList,
		Columns: []string{"region"},
		Partitions: []PartitionDef{
			{Name: "p1", Bound: PartitionBound{Values: []catalog.Value{}}},
		},
	}
	if err := validator.ValidateSpec(invalidSpec2, schema); err == nil {
		t.Error("ValidateSpec should fail for empty values list")
	}
}

func TestValidatorHashPartitions(t *testing.T) {
	validator := NewValidator()
	schema := &catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.TypeInt32},
		},
	}

	// Valid HASH partition
	validSpec := CreateHashPartitionSpec([]string{"id"}, 4)
	if err := validator.ValidateSpec(validSpec, schema); err != nil {
		t.Errorf("ValidateSpec failed for valid spec: %v", err)
	}

	// Invalid: zero buckets
	invalidSpec := &PartitionSpec{
		Type:       PartitionTypeHash,
		Columns:    []string{"id"},
		NumBuckets: 0,
	}
	if err := validator.ValidateSpec(invalidSpec, schema); err == nil {
		t.Error("ValidateSpec should fail for zero buckets")
	}
}

func TestCreateRangePartitionSpec(t *testing.T) {
	partitions := []PartitionDef{
		{Name: "p1", Bound: PartitionBound{LessThan: catalog.NewInt32(100)}},
		{Name: "p2", Bound: PartitionBound{IsMaxValue: true}},
	}
	spec := CreateRangePartitionSpec([]string{"id"}, partitions)

	if spec.Type != PartitionTypeRange {
		t.Errorf("Expected RANGE type, got %v", spec.Type)
	}
	if len(spec.Columns) != 1 || spec.Columns[0] != "id" {
		t.Errorf("Unexpected columns: %v", spec.Columns)
	}
	if len(spec.Partitions) != 2 {
		t.Errorf("Expected 2 partitions, got %d", len(spec.Partitions))
	}
}

func TestCreateListPartitionSpec(t *testing.T) {
	partitions := []PartitionDef{
		{Name: "p1", Bound: PartitionBound{Values: []catalog.Value{catalog.NewText("A")}}},
	}
	spec := CreateListPartitionSpec([]string{"status"}, partitions)

	if spec.Type != PartitionTypeList {
		t.Errorf("Expected LIST type, got %v", spec.Type)
	}
}

func TestCreateHashPartitionSpec(t *testing.T) {
	spec := CreateHashPartitionSpec([]string{"id"}, 8)

	if spec.Type != PartitionTypeHash {
		t.Errorf("Expected HASH type, got %v", spec.Type)
	}
	if spec.NumBuckets != 8 {
		t.Errorf("Expected 8 buckets, got %d", spec.NumBuckets)
	}
	if len(spec.Partitions) != 8 {
		t.Errorf("Expected 8 partitions, got %d", len(spec.Partitions))
	}
	// Check partition names
	for i, p := range spec.Partitions {
		expected := "p" + string(rune('0'+i))
		if i >= 10 {
			// For i >= 10, use Sprintf
			continue
		}
		if p.Name != expected {
			t.Errorf("Partition %d name = %q, want %q", i, p.Name, expected)
		}
	}
}

func TestGetAllPartitions(t *testing.T) {
	spec := &PartitionSpec{
		Type:    PartitionTypeRange,
		Columns: []string{"id"},
		Partitions: []PartitionDef{
			{Name: "p0"},
			{Name: "p1"},
			{Name: "p2"},
		},
	}

	router := NewRouter(spec)
	partitions := router.GetAllPartitions()

	if len(partitions) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(partitions))
	}
	expected := []string{"p0", "p1", "p2"}
	for i, p := range partitions {
		if p != expected[i] {
			t.Errorf("Partition[%d] = %q, want %q", i, p, expected[i])
		}
	}
}

func TestRouterNoPartition(t *testing.T) {
	router := NewRouter(nil)
	_, err := router.Route(catalog.NewInt32(1))
	if err == nil {
		t.Error("Expected error for nil partition spec")
	}

	router2 := NewRouter(&PartitionSpec{Type: PartitionTypeNone})
	_, err = router2.Route(catalog.NewInt32(1))
	if err == nil {
		t.Error("Expected error for NONE partition type")
	}
}

func TestPartitionSpecJSON(t *testing.T) {
	spec := &PartitionSpec{
		Type:    PartitionTypeRange,
		Columns: []string{"year"},
		Partitions: []PartitionDef{
			{Name: "p1", Bound: PartitionBound{LessThan: catalog.NewInt32(2020)}},
			{Name: "p2", Bound: PartitionBound{IsMaxValue: true}},
		},
	}

	// Marshal
	data, err := spec.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON error: %v", err)
	}

	// Unmarshal
	var spec2 PartitionSpec
	if err := spec2.UnmarshalJSON(data); err != nil {
		t.Fatalf("UnmarshalJSON error: %v", err)
	}

	// Verify
	if spec2.Type != PartitionTypeRange {
		t.Errorf("Type = %v, want RANGE", spec2.Type)
	}
	if len(spec2.Columns) != 1 || spec2.Columns[0] != "year" {
		t.Errorf("Columns = %v, want [year]", spec2.Columns)
	}
	if len(spec2.Partitions) != 2 {
		t.Errorf("Partitions count = %d, want 2", len(spec2.Partitions))
	}
}
