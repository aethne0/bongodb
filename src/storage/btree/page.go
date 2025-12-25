package btree

import c "bongodb/src"

const (
	// Common Header (0x00 - 0x1F)
	headerSize = uint16(0x40)

	offChecksum = 0x00 // 8B
	offPageID   = 0x08 // 8B
	offGen      = 0x10 // 8B CoW generation
	offPagetype = 0x18 // 1B
	offVer      = 0x19 // 1B
	offFlags    = 0x1a // 2B
	// reserved 0x1c.., 4B
)

// WARNING: Shouldn't be used directly - only within other pages.
// Eg: p := PageSlotted{ Page: Page{raw: raw} }
type Page struct {
	raw []byte
}

// common
func (p *Page) Checksum() uint64 { return c.Bin.Uint64(p.raw[offChecksum:]) }
func (p *Page) Id() uint64       { return c.Bin.Uint64(p.raw[offPageID:]) }
func (p *Page) Gen() uint64      { return c.Bin.Uint64(p.raw[offGen:]) }
func (p *Page) Pagetype() uint8  { return p.raw[offPagetype] }
func (p *Page) Ver() uint8       { return p.raw[offVer] }
func (p *Page) Flags() uint16    { return c.Bin.Uint16(p.raw[offFlags:]) }

func (p *Page) SetChecksum(id uint64) { c.Bin.PutUint64(p.raw[offChecksum:], id) }
func (p *Page) SetId(id uint64)       { c.Bin.PutUint64(p.raw[offPageID:], id) }
func (p *Page) SetGen(gen uint64)     { c.Bin.PutUint64(p.raw[offGen:], gen) }
func (p *Page) SetPagetype(pt uint8)  { p.raw[offPagetype] = pt }
func (p *Page) SetVer(ver uint8)      { p.raw[offVer] = ver }
func (p *Page) SetFlags(flags uint16) { c.Bin.PutUint16(p.raw[offFlags:], flags) }
