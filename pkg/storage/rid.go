package storage

import "fmt"

// RID represents a record identifier inside a table heap file.
// For simplicity we identify a record by table name, page number and slot id.
type RID struct {
    Table string
    Page  uint32
    Slot  uint16
}

func (r RID) String() string {
    return r.Table + ":" + fmtPageSlot(r.Page, r.Slot)
}

func fmtPageSlot(p uint32, s uint16) string {
    return fmt.Sprintf("%d.%d", p, s)
}
