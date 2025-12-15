package storage

import (
	"encoding/binary"
	"errors"
)

// Simple heap page format:
// [header(16 bytes)] [slot directory (slotCount * 4 bytes)] [free space ... tuples packed from end]
// Header layout:
// 0-7: LSN (reserved for Pager)
// 8-9: magic (uint16)
// 10-11: slotCount (uint16)
// 12-15: freeStart (uint32) - offset from start where free region begins (grows upward)
// 16-19: freeEnd (uint32) - offset from start where tuples begin (grows downward)
// 20-23: reserved

const (
	pageMagic      = 0xDB01
	lsnSize        = 8
	heapHeaderSize = 16
	headerSize     = lsnSize + heapHeaderSize // 24
	slotEntrySize  = 4                        // 2 bytes offset, 2 bytes length
)

var ErrNoSpace = errors.New("no space on page")

// initPage prepares a blank page buffer with header initialized.
func initPage(buf []byte) {
	// Skip LSN (0-7)
	binary.LittleEndian.PutUint16(buf[8:10], pageMagic)
	binary.LittleEndian.PutUint16(buf[10:12], 0) // slotCount
	// freeStart begins right after header
	binary.LittleEndian.PutUint32(buf[12:16], uint32(headerSize))
	// freeEnd begins at end of page
	binary.LittleEndian.PutUint32(buf[16:20], uint32(len(buf)))
}

// pageSlotCount returns number of slots
func pageSlotCount(buf []byte) int {
	return int(binary.LittleEndian.Uint16(buf[10:12]))
}

// getSlot reads slot entry i: returns offset and length; offset==0xffff means empty
func getSlot(buf []byte, i int) (uint16, uint16) {
	off := headerSize + i*slotEntrySize
	o := binary.LittleEndian.Uint16(buf[off : off+2])
	l := binary.LittleEndian.Uint16(buf[off+2 : off+4])
	return o, l
}

// setSlot writes slot entry i
func setSlot(buf []byte, i int, o, l uint16) {
	off := headerSize + i*slotEntrySize
	binary.LittleEndian.PutUint16(buf[off:off+2], o)
	binary.LittleEndian.PutUint16(buf[off+2:off+4], l)
}

// insertTuple attempts to insert tuple bytes into the page buffer. Returns slot id.
func insertTuple(buf []byte, data []byte) (int, error) {
	slotCount := int(binary.LittleEndian.Uint16(buf[10:12]))
	freeStart := int(binary.LittleEndian.Uint32(buf[12:16]))
	freeEnd := int(binary.LittleEndian.Uint32(buf[16:20]))

	// compute required space: either reuse an empty slot or add a new slot
	// slot directory growth: each new slot consumes slotEntrySize bytes right after header.
	// free area must not overlap slot directory.

	// First check for an empty slot to reuse
	for i := 0; i < slotCount; i++ {
		off, l := getSlot(buf, i)
		if off == 0 { // empty
			// check if there's room to place data at end
			if freeEnd-len(data) < freeStart+slotEntrySize*slotCount {
				return -1, ErrNoSpace
			}
			freeEnd -= len(data)
			copy(buf[freeEnd:freeEnd+len(data)], data)
			setSlot(buf, i, uint16(freeEnd), uint16(len(data)))
			binary.LittleEndian.PutUint32(buf[16:20], uint32(freeEnd))
			return i, nil
		}
		if l == 0 { // treat as empty
			if freeEnd-len(data) < freeStart+slotEntrySize*slotCount {
				return -1, ErrNoSpace
			}
			freeEnd -= len(data)
			copy(buf[freeEnd:freeEnd+len(data)], data)
			setSlot(buf, i, uint16(freeEnd), uint16(len(data)))
			binary.LittleEndian.PutUint32(buf[16:20], uint32(freeEnd))
			return i, nil
		}
	}

	// need new slot
	newSlotDirEnd := headerSize + (slotCount+1)*slotEntrySize
	if freeEnd-len(data) < newSlotDirEnd {
		return -1, ErrNoSpace
	}
	freeEnd -= len(data)
	copy(buf[freeEnd:freeEnd+len(data)], data)
	// write new slot
	setSlot(buf, slotCount, uint16(freeEnd), uint16(len(data)))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(freeEnd))
	binary.LittleEndian.PutUint16(buf[10:12], uint16(slotCount+1))
	return slotCount, nil
}

// insertTupleAt inserts data into a specific slot.
// Used for WAL recovery.
func insertTupleAt(buf []byte, slot int, data []byte) error {
	slotCount := int(binary.LittleEndian.Uint16(buf[10:12]))
	freeEnd := int(binary.LittleEndian.Uint32(buf[16:20]))

	// If slot is beyond current count, we need to extend slot directory
	if slot >= slotCount {
		// Check if we have space for new slots
		newSlotDirEnd := headerSize + (slot+1)*slotEntrySize
		if freeEnd-len(data) < newSlotDirEnd {
			return ErrNoSpace
		}

		// Zero out intermediate slots if any
		for i := slotCount; i < slot; i++ {
			setSlot(buf, i, 0, 0)
		}

		// Update slot count
		binary.LittleEndian.PutUint16(buf[10:12], uint16(slot+1))
		slotCount = slot + 1
	}

	// Check if slot is already occupied
	off, l := getSlot(buf, slot)
	if l > 0 {
		// Already occupied.
		// If len matches, overwrite.
		if int(l) == len(data) {
			copy(buf[off:off+l], data)
			return nil
		}
		// If len differs, we treat it as a new insert into the same slot (reuse)
		// We mark old as deleted (logically) but we can't easily reclaim space without compaction.
		// For simplicity in recovery, we just allocate new space and point slot to it.
		// This leaks the old space until vacuum/compaction.
	}

	// Allocate space from freeEnd
	if freeEnd-len(data) < headerSize+slotCount*slotEntrySize {
		return ErrNoSpace
	}
	freeEnd -= len(data)
	copy(buf[freeEnd:freeEnd+len(data)], data)
	setSlot(buf, slot, uint16(freeEnd), uint16(len(data)))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(freeEnd))

	return nil
}

// fetchTuple returns tuple bytes for given slot id or nil if empty
func fetchTuple(buf []byte, slot int) ([]byte, error) {
	slotCount := pageSlotCount(buf)
	if slot < 0 || slot >= slotCount {
		return nil, errors.New("invalid slot")
	}
	off, l := getSlot(buf, slot)
	if l == 0 || off == 0 {
		return nil, errors.New("slot empty")
	}
	start := int(off)
	end := start + int(l)
	if end > len(buf) {
		return nil, errors.New("corrupt slot")
	}
	data := make([]byte, l)
	copy(data, buf[start:end])
	return data, nil
}

// deleteTuple marks a slot as deleted by setting length to 0.
// The space is not reclaimed (would need compaction), but the slot can be reused.
func deleteTuple(buf []byte, slot int) error {
	slotCount := pageSlotCount(buf)
	if slot < 0 || slot >= slotCount {
		return errors.New("invalid slot")
	}
	off, l := getSlot(buf, slot)
	if l == 0 || off == 0 {
		return errors.New("slot already empty")
	}
	// Mark slot as empty by setting offset and length to 0
	setSlot(buf, slot, 0, 0)
	return nil
}

// updateTupleInPlace updates the data at a slot in place.
// The new data must be exactly the same size as the old data.
// This is used for MVCC XMax updates which don't change tuple size.
func updateTupleInPlace(buf []byte, slot int, newData []byte) error {
	slotCount := pageSlotCount(buf)
	if slot < 0 || slot >= slotCount {
		return errors.New("invalid slot")
	}
	off, l := getSlot(buf, slot)
	if l == 0 || off == 0 {
		return errors.New("slot empty")
	}
	if len(newData) != int(l) {
		return errors.New("new data size must match existing tuple size")
	}
	start := int(off)
	copy(buf[start:start+len(newData)], newData)
	return nil
}
