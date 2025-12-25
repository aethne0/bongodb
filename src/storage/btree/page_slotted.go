package btree

import (
	c "bongodb/src"
	"bongodb/src/util"
	"bytes"
	"fmt"

	"github.com/cespare/xxhash"
	"github.com/negrel/assert"
)

type PageSlotted struct {
	Page
	// when we re-insert a key that already exists we don't delete the old one, because doing so
	// would require us to recompact all the values on the page (if the new key didnt fit). freeBytes
	// keeps track of how many free bytes we ACTUALLY have IF we were to recompact. It will be >= (1+lower-upper)
	freeBytes uint16
}

func PageSlottedNew(raw []byte, id uint64) PageSlotted {
	p := PageSlotted{Page: Page{raw: raw}}
	p.SetId(id)
	p.initializePtrs()
	return p
}

func PageSlottedFrom(raw []byte, id uint64) PageSlotted {
	p := PageSlotted{Page: Page{raw: raw}}
	p.SetId(id)
	return p
}

func (p *Page) Parent() uint64       { return c.Bin.Uint64(p.raw[offParent:]) }
func (p *Page) Right() uint64        { return c.Bin.Uint64(p.raw[offRight:]) }
func (p *Page) SetParent(pid uint64) { c.Bin.PutUint64(p.raw[offParent:], pid) }
func (p *Page) SetRight(rid uint64)  { c.Bin.PutUint64(p.raw[offRight:], rid) }

// Returns (raw entry val slice, found). Binary searches through keys stored in page. Can fail to find.
func (p *PageSlotted) Get(key []byte) ([]byte, bool) {
	slotIndex, found := p.keyToSlotIndex(key)
	if !found {
		return nil, false
	}
	return p.slotIndexToVal(slotIndex), true
}

// Todo just make it return if it fit or not
// Returns true if entry already existed with key. It is the CALLERS responsibility to check if the entry
// will fit into the current contiguous space. If there is enough fragmented space the CALLER needs to
// call defragment first, and if there is not enough space no matter what then thats your problem.
func (p *PageSlotted) Put(key []byte, val []byte) bool {
	entryLen := uint16(c.LEN_U16 + len(key) + c.LEN_U16 + len(val))
	assert.Less(c.LEN_U16+entryLen, c.PAGE_SIZE-headerSize, "Exceeds max possible key+val size")

	slotIndex, found := p.keyToSlotIndex(key)
	slotOff := p.slotIndexToSlotOffset(slotIndex)
	// subtract space from slot now so space checks are accurate after
	p.freeBytes -= c.LEN_U16

	insertInPlace := false

	if !found {
		// bump all slots starting at and including slotIndex
		slotEndOff := p.upper()
		copy(
			p.raw[slotOff+c.LEN_U16:slotEndOff+c.LEN_U16],
			p.raw[slotOff:slotEndOff],
		)
		p.setUpper(p.upper() + c.LEN_U16)
		p.freeBytes -= entryLen

	} else {
		entryLenOld := p.slotIndexToEntryLen(slotIndex)
		if entryLenOld >= entryLen {
			insertInPlace = true
			p.freeBytes += entryLen - entryLenOld
		}
	}

	var entryOff uint16
	if insertInPlace {
		// If we found the entry and the old entry was >= the size of the new entry,
		// we can just overwrite the old entry in place to (somewhat) reduce fragmentation
		entryOff = p.slotIndexToEntryOffset(slotIndex)
	} else {
		// We didn't find or didn't have space, so we just use contiguous space
		assert.LessOrEqual(p.FreeBytesContig(), entryLen, "Page doesn't have enough free contiguous space")
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

	return found
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
	p.freeBytes = p.FreeBytesContig()
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
	p.freeBytes = p.FreeBytesContig()
}

// Its possible this gives a false positive if the key already exists and it could be overwritten
func (p *PageSlotted) CanFitContig(keyAndValLen uint16) bool {
	return keyAndValLen+3*c.LEN_U16 <= p.FreeBytesContig()
}

// Its possible this gives a false positive if the key already exists and it could be overwritten
func (p *PageSlotted) CanFitFrag(keyAndValLen uint16) bool {
	return keyAndValLen+3*c.LEN_U16 <= p.FreeBytesFrag()
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
	return p.freeBytes
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

const (
	// Slotted Metadata (0x20 - 0x3F)
	offParent = 0x20 // 8B
	offRight  = 0x28 // 8B
	offUpper  = 0x30 // 2B
	offLower  = 0x32 // 2B Start of entries, starts at end of page
	// reserved 0x34.., 12B
)

// Gives offset that will be written to, this index does NOT yet have something in it.
func (p *Page) upper() uint16 { return c.Bin.Uint16(p.raw[offUpper:]) }

// Points to NEXT offset that will be written to, this index does NOT yet have something in it.
func (p *Page) lower() uint16     { return c.Bin.Uint16(p.raw[offLower:]) }
func (p *Page) setUpper(u uint16) { c.Bin.PutUint16(p.raw[offUpper:], u) }
func (p *Page) setLower(l uint16) { c.Bin.PutUint16(p.raw[offLower:], l) }

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

// Just for debugging

func (p *PageSlotted) Dbg() {
	fmt.Printf("checksum: 0x%016x id: 0x%016x\nfree_contiguous: 0x%04x (%d) free_fragmented: 0x%04x (%d)\nupper: 0x%04x lower: 0x%04x\n",
		p.Checksum(), p.Id(),
		p.FreeBytesContig(), p.FreeBytesContig(), p.FreeBytesFrag(), p.FreeBytesFrag(),
		p.upper(), p.lower(),
	)
	util.PrettyPrintPage(p.raw, c.PAGE_SIZE)
}
