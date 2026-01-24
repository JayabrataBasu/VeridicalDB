package components

import "fmt"

// ErrInvalidColumnIndex is returned when an invalid column index is provided.
var ErrInvalidColumnIndex = fmt.Errorf("invalid column index")

// ErrInvalidPageSize is returned when an invalid page size is provided.
var ErrInvalidPageSize = fmt.Errorf("invalid page size")
