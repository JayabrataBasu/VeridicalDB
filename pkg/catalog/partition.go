package catalog

import (
	"encoding/json"
)

// PartitionType represents the type of table partitioning.
type PartitionType int

const (
	PartitionTypeNone PartitionType = iota
	PartitionTypeRange
	PartitionTypeList
	PartitionTypeHash
)

// String returns the string representation.
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

// ParsePartitionType parses a string to PartitionType.
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
	LessThan Value `json:"less_than,omitempty"`
	// For LIST partitions: the list of values in this partition
	Values []Value `json:"values,omitempty"`
	// Special marker for MAXVALUE in RANGE partitions
	IsMaxValue bool `json:"is_max_value,omitempty"`
}

// PartitionInfo represents a single partition.
type PartitionInfo struct {
	Name      string         `json:"name"`
	Bound     PartitionBound `json:"bound"`
	TableName string         `json:"table_name"` // Physical table name for this partition
}

// PartitionSpec defines how a table is partitioned.
type PartitionSpec struct {
	Type       PartitionType   `json:"type"`
	Columns    []string        `json:"columns"`
	Partitions []PartitionInfo `json:"partitions"`
	NumBuckets int             `json:"num_buckets,omitempty"` // For HASH
}

// MarshalJSON implements custom JSON marshaling.
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

// UnmarshalJSON implements custom JSON unmarshaling.
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

// IsPartitioned returns true if the spec defines partitioning.
func (ps *PartitionSpec) IsPartitioned() bool {
	return ps != nil && ps.Type != PartitionTypeNone
}

// GetPartitionNames returns all partition names.
func (ps *PartitionSpec) GetPartitionNames() []string {
	if ps == nil {
		return nil
	}
	names := make([]string, len(ps.Partitions))
	for i, p := range ps.Partitions {
		names[i] = p.Name
	}
	return names
}
