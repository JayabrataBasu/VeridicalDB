package exec

import (
	"github.com/JayabrataBasu/VeridicalDB/pkg/catalog"
)

// aggregatorState holds aggregation state for a single aggregate function.
type aggregatorState struct {
	count    int64
	sum      int64
	sumFloat float64
	min      catalog.Value
	max      catalog.Value
	hasValue bool
}

// groupState holds the state for a single group in GROUP BY.
type groupState struct {
	groupKey    []catalog.Value   // values of GROUP BY columns
	aggregators []aggregatorState // aggregation state per output column
}
