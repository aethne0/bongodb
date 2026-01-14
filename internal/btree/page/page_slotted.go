package page

import (
	c "mooodb/internal"

	"bytes"

	"github.com/cespare/xxhash"
	"github.com/negrel/assert"
)

type PageSlotted struct {
	Page
}

// Only to be called from tests - just ignored a bunch of metadata fields
func PageSlottedNewTest(raw []byte, id uint64) PageSlotted {
	p := PageSlotted{Page: Page{raw: raw}}
	p.SetId(id)
	p.initializePtrs()
	return p
}

func PageSlottedNew(raw []byte, id uint64, leaf bool, gen uint64, parent uint64) PageSlotted {
	p := PageSlotted{Page: Page{raw: raw}}

	// unused part of header
	copy(p.raw[0x1c:], []byte{0xff, 0xff, 0xff, 0xff})
	copy(p.raw[0x36:], bytes.Repeat([]byte{0xff}, 10))

	if leaf {
		p.SetPagetype(PagetypeLeaf)
	} else {
		p.SetPagetype(PagetypeInner)
	}

	p.SetId(id)
	p.SetVer(Version)
	p.SetParent(parent)
	p.SetGen(gen)
	p.initializePtrs()

	return p
}


func PageSlottedFrom(raw []byte) PageSlotted {
	return PageSlotted{Page: Page{raw: raw}}
}

const (
	// Slotted Metadata (0x20 - 0x3F)
	offParent = 0x20 // 8B
	offRight  = 0x28 // 8B
	offUpper  = 0x30 // 2B
	offLower  = 0x32 // 2B Start of entries, starts at end of page
	offFree   = 0x34 // 2B Free memory including fragmented
	// reserved 0x36.., 10B
)

func (p *PageSlotted) Parent() uint64       	{ return c.Bin.Uint64(p.raw[offParent:]) }
func (p *PageSlotted) Right() uint64      	 	{ return c.Bin.Uint64(p.raw[offRight:]) }
// Gives offset that will be written to, this index does NOT yet have something in it.
func (p *PageSlotted) upper() uint16 		 	{ return c.Bin.Uint16(p.raw[offUpper:]) }
// Points to NEXT offset that will be written to, this index does NOT yet have something in it.
func (p *PageSlotted) lower() uint16     	 	{ return c.Bin.Uint16(p.raw[offLower:]) }
func (p *PageSlotted) freeBytes() uint16    	{ return c.Bin.Uint16(p.raw[offFree:]) }

func (p *PageSlotted) SetParent(pid uint64) 	{ c.Bin.PutUint64(p.raw[offParent:], pid) }
func (p *PageSlotted) SetRight(rid uint64)  	{ c.Bin.PutUint64(p.raw[offRight:], rid) }
func (p *PageSlotted) setUpper(u uint16) 	 	{ c.Bin.PutUint16(p.raw[offUpper:], u) }
func (p *PageSlotted) setLower(l uint16) 	 	{ c.Bin.PutUint16(p.raw[offLower:], l) }
func (p *PageSlotted) setFreebytes(l uint16)	{ c.Bin.PutUint16(p.raw[offFree:], l) }


// Returns (raw entry val slice, found). Binary searches through keys stored in page. Can fail to find.
func (p *PageSlotted) Get(key []byte) ([]byte, bool) {
	slotIndex, found := p.keyToSlotIndex(key)
	if !found {
		return nil, false
	}
	return p.slotIndexToVal(slotIndex), true
}

// Lazy - bool if found
func (p *PageSlotted) Delete(key []byte) bool {
	slotIndex, found := p.keyToSlotIndex(key)
	if !found {
		return false
	}

	slotOff := p.slotIndexToSlotOffset(slotIndex)
	entryOff := p.slotIndexToEntryOffset(slotIndex)
	entryLen := p.slotIndexToEntryLen(slotIndex)

	slotEndOff := p.upper()
	copy(
		p.raw[slotOff:slotEndOff],
		p.raw[slotOff+c.LEN_U16:slotEndOff+c.LEN_U16],
	)
	p.setUpper(p.upper() - c.LEN_U16)

	if entryOff == p.lower()+1 {
		// this was the last entry, so we can just completely reclaim it
		// otherwise this wasnt the last entry, so all we do is add to frag tracker
		p.setLower(p.lower() + entryLen)
	}

	p.setFreebytes(p.freeBytes() +entryLen + c.LEN_U16)

	return true
}

// Returns (existed, was_inserted) - was_inserted is false if we didnt have enough contiguous free space
func (p *PageSlotted) Put(key []byte, val []byte) (bool, bool) {
	entryLen := uint16(c.LEN_U16 + len(key) + c.LEN_U16 + len(val))
	assert.Less(c.LEN_U16+entryLen, c.PAGE_SIZE-headerSize, "Exceeds max possible key+val size")

	slotIndex, found := p.keyToSlotIndex(key)
	slotOff := p.slotIndexToSlotOffset(slotIndex)
	// subtract space from slot now so space checks are accurate after
	p.setFreebytes(p.freeBytes() - c.LEN_U16)


	insertInPlace := false

	if !found {
		// bump all slots starting at and including slotIndex
		slotEndOff := p.upper()
		copy(
			p.raw[slotOff+c.LEN_U16:slotEndOff+c.LEN_U16],
			p.raw[slotOff:slotEndOff],
		)
		p.setUpper(p.upper() + c.LEN_U16)
		p.setFreebytes(p.freeBytes() - entryLen)

	} else {
		entryLenOld := p.slotIndexToEntryLen(slotIndex)
		if entryLenOld >= entryLen {
			insertInPlace = true
			p.setFreebytes(p.freeBytes() + entryLen - entryLenOld)
		}
	}

	var entryOff uint16
	if insertInPlace {
		// If we found the entry and the old entry was >= the size of the new entry,
		// we can just overwrite the old entry in place to (somewhat) reduce fragmentation
		entryOff = p.slotIndexToEntryOffset(slotIndex)
	} else {
		if p.FreeBytesContig() < entryLen {
			// we dont have enough (contiguous) free space 
			return found, false
		}
		entryOff = p.lower() - entryLen + 1
	}

	// These are all relative to the start we chose
	entryOffK := entryOff + c.LEN_U16
	entryOffLenV := entryOffK + uint16(len(key))
	entryOffV := entryOffLenV + c.LEN_U16

	c.Bin.PutUint16(p.raw[entryOff:], uint16(len(key)))
	copy(p.raw[entryOffK:], key)
	c.Bin.PutUint16(p.raw[entryOffLenV:], uint16(len(val)))
	copy(p.raw[entryOffV:], val)

	// write slot
	c.Bin.PutUint16(p.raw[slotOff:], entryOff)

	if !insertInPlace {
		p.setLower(entryOff - 1)
	}

	return found, true
}

// Scratch must be (at least) page size, this writes to the scratch buffer THEN copies back again
// So this is basically 2*page_size worth of copy, but we can get nerdy about it later.
// At least it doesnt allocate!
func (p *PageSlotted) Defragment(scratch []byte) {
	assert.GreaterOrEqual(len(scratch), c.PAGE_SIZE, "scratch buffer smaller than page")

	// copy header
	slotPtr := headerSize
	entryPtr := uint16(c.PAGE_SIZE)

	for i := range p.EntryCount() {
		entryBytes := p.slotIndexToEntryRaw(i)
		entryPtr = entryPtr - uint16(len(entryBytes))
		c.Bin.PutUint16(scratch[slotPtr:], entryPtr)
		slotPtr += c.LEN_U16
		copy(scratch[entryPtr:], entryBytes)
	}

	// could optimize slightly
	copy(p.raw[headerSize:c.PAGE_SIZE], scratch[headerSize:c.PAGE_SIZE])
	p.setLower(entryPtr - 1)
	p.setFreebytes(p.FreeBytesContig())
}

func (p *PageSlotted) DoChecksum() {
	p.SetChecksum(xxhash.Sum64(p.raw[c.LEN_U64:c.PAGE_SIZE]))
}

func (p *PageSlotted) Iter(yield func([]byte) bool) {
	for i := range p.EntryCount() {
		val := p.slotIndexToVal(i)
		if !yield(val) {
			break
		}
	}
}

// Initializes slot pointers - should be called on new page. This does NOT initialize anything else. You
// still have to set id, parents, etc.
func (p *PageSlotted) initializePtrs() {
	p.setUpper(headerSize)
	p.setLower(c.PAGE_SIZE - 1)
	p.setFreebytes(p.FreeBytesContig())
}

// Free bytes in page.
//
// Note: slots use this space as well, so an entry exactly this large will not fit. This only counts contiguous
// free space between Upper and Lower
func (p *PageSlotted) FreeBytesContig() uint16 {
	// If 0 bytes are free these will be crossed - *lower will be the byte before *upper
	return 1 + p.lower() - p.upper()
}

func (p *PageSlotted) FreeBytesFrag() uint16 {
	return p.freeBytes()
}

// Decimal representation of how much of the pages dataspace is used. Header is ignored for this calculation,
// ie a freshly constructed page will return 0.0 - a page with 0 free bytes will return 1.0
func (p *PageSlotted) FreeDecim() float64 {
	return float64(p.FreeBytesContig()) / float64(c.PAGE_SIZE-headerSize)
}

func (p *PageSlotted) EntryCount() uint16 {
	return (p.upper() - headerSize) / c.LEN_U16
}

// Implementation

// NOTE: lets get a bit of nomenclature clear here:
// SLOT: 	The ptr in the slot array that points to the start of an entry.
// ENTRY: 	The actual raw data part of an entry - the keylen+key+vallen+value.
// KEY/VAL: The parsed []byte of key and value, can be read with no further logic.
// OFFSET:  This means literally how many bytes into the page - starts at 0, which is the beginning of the header.
// 		    Usually well oinly care about values from headerSize onwars (0x40..)
// INDEX:   Index is a logical index - so slot-2 would be 4bytes after the end of the header
//		    Similarly, entry-3 could be essentially anywhere on the page (we'd have to check with slot-3)
// Example: SlotIndexToEntryOffset
//          This takes a slot index and tells you its corresponding entries actual
//		    starting address (byte offset) on the page

// NOTE: Slots will point to the entries in key-sorted order. Entries are written in insertion order.
// 		 At the time of insertion slots will be rearranged to keep the keys given from iterating through
// 	 	 Slots ordered (iterating through slots 0..n will return entries in key-order)

// Logic for parsing entries
// Entries are of the format:
// [key_len_u16]:[key_bytes]:[val_len_u16]:[val_bytes]

// PERF: boy i hope the compiler inlines all this :)

func (p *PageSlotted) slotIndexToSlotOffset(slotIndex uint16) uint16 {
	return headerSize + slotIndex*c.LEN_U16
}

func (p *PageSlotted) slotIndexToEntryOffset(slotIndex uint16) uint16 {
	slotOff := p.slotIndexToSlotOffset(slotIndex)
	return c.Bin.Uint16(p.raw[slotOff:])
}

func (p *PageSlotted) slotIndexToEntryRaw(slotIndex uint16) []byte {
	offset := p.slotIndexToEntryOffset(slotIndex)
	lenK := c.Bin.Uint16(p.raw[offset:])
	offK := offset + c.LEN_U16
	lenV := c.Bin.Uint16(p.raw[offK+lenK:])
	offV := offK + lenK + c.LEN_U16

	return p.raw[offset : offV+lenV]
}

func (p *PageSlotted) slotIndexToKey(slotIndex uint16) []byte {
	assert.Less(slotIndex, p.EntryCount(), "Slot-index out of range")

	offset := p.slotIndexToEntryOffset(slotIndex)
	lenK := c.Bin.Uint16(p.raw[offset:])
	offK := offset + c.LEN_U16

	return p.raw[offK : offK+lenK]
}

func (p *PageSlotted) slotIndexToVal(slotIndex uint16) []byte {
	assert.Less(slotIndex, p.EntryCount(), "Slot-index out of range")

	offset := p.slotIndexToEntryOffset(slotIndex)
	lenK := c.Bin.Uint16(p.raw[offset:])
	offK := offset + c.LEN_U16
	lenV := c.Bin.Uint16(p.raw[offK+lenK:])
	offV := offK + lenK + c.LEN_U16

	return p.raw[offV : offV+lenV]
}

func (p *PageSlotted) slotIndexToEntryLen(slotIndex uint16) uint16 {
	assert.Less(slotIndex, p.EntryCount(), "Slot-index out of range")

	offset := p.slotIndexToEntryOffset(slotIndex)
	lenK := c.Bin.Uint16(p.raw[offset:])
	offK := offset + c.LEN_U16
	lenV := c.Bin.Uint16(p.raw[offK+lenK:])

	return c.LEN_U16 + lenK + c.LEN_U16 + lenV
}

// Binary searches and returns index, found.
// If found -> index is just the index of the slot.
// If !found -> index is the index
// the slot SHOULD have if this key were to be inserted (and all slots > index should be bumped). If no entries
// are present this will always return (0, false).
func (p *PageSlotted) keyToSlotIndex(key []byte) (uint16, bool) {
	// These are slot indicies
	low := uint16(0)
	high := p.EntryCount()

	for low < high {
		mid := low + (high-low)/2

		cmp := bytes.Compare(key, p.slotIndexToKey(mid))
		if cmp == 0 {
			return mid, true
		}

		if cmp < 0 {
			high = mid
		} else {
			low = mid + 1
		}
	}

	return low, false
}

