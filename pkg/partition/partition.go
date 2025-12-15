// Package partition provides table partitioning support for VeridicalDB.
// It supports RANGE, LIST, and HASH partitioning strategies.
package partition

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

// PartitionType defines the type of partitioning.
type PartitionType int

const (
	// PartitionTypeNone indicates no partitioning.
	PartitionTypeNone PartitionType = iota
	// PartitionTypeRange partitions by value ranges.
	PartitionTypeRange
	// PartitionTypeList partitions by discrete value lists.
	PartitionTypeList
	// PartitionTypeHash partitions by hash of the key.
	PartitionTypeHash
)

// String returns the string representation of the partition type.
func (pt PartitionType) String() string {
	switch pt {
	case PartitionTypeRange:
		return "RANGE"
	case PartitionTypeList:
		return "LIST"
	case PartitionTypeHash:
		return "HASH"
	default:
		return "NONE"
	}
}

// ParsePartitionType parses a string into a PartitionType.
func ParsePartitionType(s string) PartitionType {
	switch s {
	case "RANGE":
		return PartitionTypeRange
	case "LIST":
		return PartitionTypeList
	case "HASH":
		return PartitionTypeHash
	default:
		return PartitionTypeNone
	}
}

// PartitionBound represents the boundary for a partition.
type PartitionBound struct {
	// For RANGE partitions: the upper bound (exclusive)
	LessThan catalog.Value `json:"less_than,omitempty"`
	// For LIST partitions: the list of values in this partition
	Values []catalog.Value `json:"values,omitempty"`
	// Special marker for MAXVALUE in RANGE partitions
	IsMaxValue bool `json:"is_max_value,omitempty"`
}

// PartitionDef defines a single partition.
type PartitionDef struct {
	Name      string         `json:"name"`
	Bound     PartitionBound `json:"bound"`
	TableName string         `json:"table_name"` // Physical table storing this partition's data
}

// PartitionSpec defines the partitioning specification for a table.
type PartitionSpec struct {
	Type       PartitionType  `json:"type"`
	Columns    []string       `json:"columns"`     // Partition key columns
	Partitions []PartitionDef `json:"partitions"`  // Partition definitions
	NumBuckets int            `json:"num_buckets"` // For HASH partitioning
}

// MarshalJSON implements custom JSON marshaling for PartitionSpec.
func (ps *PartitionSpec) MarshalJSON() ([]byte, error) {
	type alias PartitionSpec
	return json.Marshal(&struct {
		TypeStr string `json:"type_str"`
		*alias
	}{
		TypeStr: ps.Type.String(),
		alias:   (*alias)(ps),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling for PartitionSpec.
func (ps *PartitionSpec) UnmarshalJSON(data []byte) error {
	type alias PartitionSpec
	aux := &struct {
		TypeStr string `json:"type_str"`
		*alias
	}{
		alias: (*alias)(ps),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	ps.Type = ParsePartitionType(aux.TypeStr)
	return nil
}

// Router routes rows to the correct partition based on key values.
type Router struct {
	spec *PartitionSpec
}

// NewRouter creates a new partition router.
func NewRouter(spec *PartitionSpec) *Router {
	return &Router{spec: spec}
}

// Route determines which partition a row belongs to based on the partition key value.
func (r *Router) Route(keyValue catalog.Value) (string, error) {
	if r.spec == nil || r.spec.Type == PartitionTypeNone {
		return "", fmt.Errorf("table is not partitioned")
	}

	switch r.spec.Type {
	case PartitionTypeRange:
		return r.routeRange(keyValue)
	case PartitionTypeList:
		return r.routeList(keyValue)
	case PartitionTypeHash:
		return r.routeHash(keyValue)
	default:
		return "", fmt.Errorf("unknown partition type: %v", r.spec.Type)
	}
}

// routeRange finds the partition for RANGE partitioning.
func (r *Router) routeRange(keyValue catalog.Value) (string, error) {
	for _, p := range r.spec.Partitions {
		if p.Bound.IsMaxValue {
			return p.Name, nil
		}

		cmp := keyValue.Compare(p.Bound.LessThan)
		if cmp < 0 {
			return p.Name, nil
		}
	}
	return "", fmt.Errorf("no partition found for value: %v", keyValue)
}

// routeList finds the partition for LIST partitioning.
func (r *Router) routeList(keyValue catalog.Value) (string, error) {
	for _, p := range r.spec.Partitions {
		for _, v := range p.Bound.Values {
			if keyValue.Compare(v) == 0 {
				return p.Name, nil
			}
		}
	}
	return "", fmt.Errorf("no partition found for value: %v", keyValue)
}

// routeHash finds the partition for HASH partitioning.
func (r *Router) routeHash(keyValue catalog.Value) (string, error) {
	if r.spec.NumBuckets <= 0 || len(r.spec.Partitions) == 0 {
		return "", fmt.Errorf("invalid hash partition configuration")
	}

	bucket := hashValue(keyValue) % uint32(r.spec.NumBuckets)
	idx := int(bucket) % len(r.spec.Partitions)
	return r.spec.Partitions[idx].Name, nil
}

// hashValue computes a hash of a catalog.Value.
func hashValue(v catalog.Value) uint32 {
	h := fnv.New32a()
	switch v.Type {
	case catalog.TypeInt32:
		h.Write([]byte(fmt.Sprintf("%d", v.Int32)))
	case catalog.TypeInt64:
		h.Write([]byte(fmt.Sprintf("%d", v.Int64)))
	case catalog.TypeFloat64:
		h.Write([]byte(fmt.Sprintf("%f", v.Float64)))
	case catalog.TypeText:
		h.Write([]byte(v.Text))
	case catalog.TypeBool:
		h.Write([]byte(fmt.Sprintf("%t", v.Bool)))
	default:
		h.Write([]byte(v.String()))
	}
	return h.Sum32()
}

// RouteMulti determines partitions for multiple key values (for queries with IN clauses).
func (r *Router) RouteMulti(keyValues []catalog.Value) ([]string, error) {
	partitionSet := make(map[string]bool)
	for _, kv := range keyValues {
		p, err := r.Route(kv)
		if err != nil {
			continue // Skip values that don't match any partition
		}
		partitionSet[p] = true
	}

	partitions := make([]string, 0, len(partitionSet))
	for p := range partitionSet {
		partitions = append(partitions, p)
	}
	sort.Strings(partitions)
	return partitions, nil
}

// GetAllPartitions returns all partition names.
func (r *Router) GetAllPartitions() []string {
	partitions := make([]string, len(r.spec.Partitions))
	for i, p := range r.spec.Partitions {
		partitions[i] = p.Name
	}
	return partitions
}

// Pruner determines which partitions can be skipped based on query predicates.
type Pruner struct {
	spec *PartitionSpec
}

// NewPruner creates a new partition pruner.
func NewPruner(spec *PartitionSpec) *Pruner {
	return &Pruner{spec: spec}
}

// PruneForEquality returns partitions that might contain rows matching the equality predicate.
func (p *Pruner) PruneForEquality(keyValue catalog.Value) []string {
	router := NewRouter(p.spec)
	partition, err := router.Route(keyValue)
	if err != nil {
		return router.GetAllPartitions() // Can't prune, return all
	}
	return []string{partition}
}

// PruneForRange returns partitions that might contain rows in the given range.
func (p *Pruner) PruneForRange(lowValue, highValue catalog.Value, lowInclusive, highInclusive bool) []string {
	if p.spec.Type != PartitionTypeRange {
		// For LIST and HASH, we can't do range pruning effectively
		return NewRouter(p.spec).GetAllPartitions()
	}

	var result []string
	for _, part := range p.spec.Partitions {
		// Check if partition might overlap with the range
		if p.partitionOverlapsRange(part, lowValue, highValue) {
			result = append(result, part.Name)
		}
	}

	if len(result) == 0 {
		return NewRouter(p.spec).GetAllPartitions()
	}
	return result
}

// partitionOverlapsRange checks if a RANGE partition might contain values in the given range.
func (p *Pruner) partitionOverlapsRange(part PartitionDef, lowValue, highValue catalog.Value) bool {
	// For RANGE partitions, partition contains values: [prev_bound, this_bound)
	// A partition overlaps if its range intersects with [lowValue, highValue]

	if part.Bound.IsMaxValue {
		// MAXVALUE partition overlaps if highValue is unbounded or very large
		return true
	}

	// If partition's upper bound <= lowValue, partition is entirely before range
	if !lowValue.IsNull && part.Bound.LessThan.Compare(lowValue) <= 0 {
		return false
	}

	return true
}

// PruneForIn returns partitions that might contain rows matching any value in the list.
func (p *Pruner) PruneForIn(keyValues []catalog.Value) []string {
	router := NewRouter(p.spec)
	partitions, _ := router.RouteMulti(keyValues)
	if len(partitions) == 0 {
		return router.GetAllPartitions()
	}
	return partitions
}

// Validator validates partition specifications.
type Validator struct{}

// NewValidator creates a new partition validator.
func NewValidator() *Validator {
	return &Validator{}
}

// ValidateSpec validates a partition specification.
func (v *Validator) ValidateSpec(spec *PartitionSpec, schema *catalog.Schema) error {
	if spec == nil || spec.Type == PartitionTypeNone {
		return nil
	}

	// Validate partition columns exist
	for _, col := range spec.Columns {
		found := false
		for _, schemaCol := range schema.Columns {
			if schemaCol.Name == col {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("partition column %q not found in table schema", col)
		}
	}

	// Validate partitions based on type
	switch spec.Type {
	case PartitionTypeRange:
		return v.validateRangePartitions(spec)
	case PartitionTypeList:
		return v.validateListPartitions(spec)
	case PartitionTypeHash:
		return v.validateHashPartitions(spec)
	}

	return nil
}

// validateRangePartitions validates RANGE partition definitions.
func (v *Validator) validateRangePartitions(spec *PartitionSpec) error {
	if len(spec.Partitions) == 0 {
		return fmt.Errorf("RANGE partitioning requires at least one partition")
	}

	// Check that bounds are in ascending order
	for i := 1; i < len(spec.Partitions); i++ {
		prev := spec.Partitions[i-1]
		curr := spec.Partitions[i]

		if prev.Bound.IsMaxValue {
			return fmt.Errorf("MAXVALUE partition must be last")
		}

		if !curr.Bound.IsMaxValue {
			if prev.Bound.LessThan.Compare(curr.Bound.LessThan) >= 0 {
				return fmt.Errorf("partition bounds must be in ascending order")
			}
		}
	}

	return nil
}

// validateListPartitions validates LIST partition definitions.
func (v *Validator) validateListPartitions(spec *PartitionSpec) error {
	if len(spec.Partitions) == 0 {
		return fmt.Errorf("LIST partitioning requires at least one partition")
	}

	// Check for duplicate values across partitions
	seen := make(map[string]string) // value -> partition name
	for _, p := range spec.Partitions {
		if len(p.Bound.Values) == 0 {
			return fmt.Errorf("LIST partition %q must have at least one value", p.Name)
		}
		for _, val := range p.Bound.Values {
			key := val.String()
			if existing, exists := seen[key]; exists {
				return fmt.Errorf("value %v appears in multiple partitions: %q and %q", val, existing, p.Name)
			}
			seen[key] = p.Name
		}
	}

	return nil
}

// validateHashPartitions validates HASH partition definitions.
func (v *Validator) validateHashPartitions(spec *PartitionSpec) error {
	if spec.NumBuckets <= 0 {
		return fmt.Errorf("HASH partitioning requires positive number of buckets")
	}

	if len(spec.Partitions) == 0 {
		return fmt.Errorf("HASH partitioning requires at least one partition")
	}

	return nil
}

// CreateRangePartitionSpec creates a RANGE partition specification.
func CreateRangePartitionSpec(columns []string, partitions []PartitionDef) *PartitionSpec {
	return &PartitionSpec{
		Type:       PartitionTypeRange,
		Columns:    columns,
		Partitions: partitions,
	}
}

// CreateListPartitionSpec creates a LIST partition specification.
func CreateListPartitionSpec(columns []string, partitions []PartitionDef) *PartitionSpec {
	return &PartitionSpec{
		Type:       PartitionTypeList,
		Columns:    columns,
		Partitions: partitions,
	}
}

// CreateHashPartitionSpec creates a HASH partition specification.
func CreateHashPartitionSpec(columns []string, numBuckets int) *PartitionSpec {
	partitions := make([]PartitionDef, numBuckets)
	for i := 0; i < numBuckets; i++ {
		partitions[i] = PartitionDef{
			Name:      fmt.Sprintf("p%d", i),
			TableName: "", // Will be set when table is created
		}
	}
	return &PartitionSpec{
		Type:       PartitionTypeHash,
		Columns:    columns,
		Partitions: partitions,
		NumBuckets: numBuckets,
	}
}
