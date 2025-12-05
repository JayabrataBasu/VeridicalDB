package storage

import (
    "encoding/binary"
    "errors"
)

// Simple heap page format:
// [header(16 bytes)] [slot directory (slotCount * 4 bytes)] [free space ... tuples packed from end]
// Header layout:
// 0-1: magic (uint16)
// 2-3: slotCount (uint16)
// 4-7: freeStart (uint32) - offset from start where free region begins (grows upward)
// 8-11: freeEnd (uint32) - offset from start where tuples begin (grows downward)
// 12-15: reserved

const (
    pageMagic = 0xDB01
    headerSize = 16
    slotEntrySize = 4 // 2 bytes offset, 2 bytes length
)

var ErrNoSpace = errors.New("no space on page")

// initPage prepares a blank page buffer with header initialized.
func initPage(buf []byte) {
    binary.LittleEndian.PutUint16(buf[0:2], pageMagic)
    binary.LittleEndian.PutUint16(buf[2:4], 0) // slotCount
    // freeStart begins right after header
    binary.LittleEndian.PutUint32(buf[4:8], uint32(headerSize))
    // freeEnd begins at end of page
    binary.LittleEndian.PutUint32(buf[8:12], uint32(len(buf)))
}

// pageSlotCount returns number of slots
func pageSlotCount(buf []byte) int {
    return int(binary.LittleEndian.Uint16(buf[2:4]))
}

// getSlot reads slot entry i: returns offset and length; offset==0xffff means empty
func getSlot(buf []byte, i int) (uint16, uint16) {
    off := headerSize + i*slotEntrySize
    o := binary.LittleEndian.Uint16(buf[off:off+2])
    l := binary.LittleEndian.Uint16(buf[off+2:off+4])
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
    slotCount := int(binary.LittleEndian.Uint16(buf[2:4]))
    freeStart := int(binary.LittleEndian.Uint32(buf[4:8]))
    freeEnd := int(binary.LittleEndian.Uint32(buf[8:12]))

    // compute required space: either reuse an empty slot or add a new slot
    // slot directory growth: each new slot consumes slotEntrySize bytes right after header.
    // free area must not overlap slot directory.

    // First check for an empty slot to reuse
    for i := 0; i < slotCount; i++ {
        off, l := getSlot(buf, i)
        if off == 0 { // empty
            // check if there's room to place data at end
            if freeEnd - len(data) < freeStart + slotEntrySize*slotCount {
                return -1, ErrNoSpace
            }
            freeEnd -= len(data)
            copy(buf[freeEnd:freeEnd+len(data)], data)
            setSlot(buf, i, uint16(freeEnd), uint16(len(data)))
            binary.LittleEndian.PutUint32(buf[8:12], uint32(freeEnd))
            return i, nil
        }
        if l == 0 { // treat as empty
            if freeEnd - len(data) < freeStart + slotEntrySize*slotCount {
                return -1, ErrNoSpace
            }
            freeEnd -= len(data)
            copy(buf[freeEnd:freeEnd+len(data)], data)
            setSlot(buf, i, uint16(freeEnd), uint16(len(data)))
            binary.LittleEndian.PutUint32(buf[8:12], uint32(freeEnd))
            return i, nil
        }
    }

    // need new slot
    newSlotDirEnd := headerSize + (slotCount+1)*slotEntrySize
    if freeEnd - len(data) < newSlotDirEnd {
        return -1, ErrNoSpace
    }
    freeEnd -= len(data)
    copy(buf[freeEnd:freeEnd+len(data)], data)
    // write new slot
    setSlot(buf, slotCount, uint16(freeEnd), uint16(len(data)))
    binary.LittleEndian.PutUint32(buf[8:12], uint32(freeEnd))
    binary.LittleEndian.PutUint16(buf[2:4], uint16(slotCount+1))
    return slotCount, nil
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
