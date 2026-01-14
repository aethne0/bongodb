package page

import (
	c "mooodb/internal"
)

type PageMeta struct {
	Page
}

func PageMetaNew(raw []byte, rootId uint64) PageMeta {
	p := PageMeta{Page: Page{raw: raw}}
	
	p.SetId(0)
	p.SetVer(Version)
	p.SetPagetype(PagetypeMeta)
	p.SetGen(0)
	copy(p.raw[offMagic:], magic)
	p.SetFreeList(0)
	p.SetPageCnt(0)
	p.SetRootId(0)
	return p
}

func PageMetaFrom(raw []byte, id uint64) PageMeta {
	p := PageMeta{Page: Page{raw: raw}}
	return p
}

const (
	magic 			= "MOOOOOOO" // 4d4f 4f4f 4f4f 4f4f / 77 79 79 79 79 79 79 79
	offMagic 		= 0x20
	offRootId 		= 0x28
	offPageCnt 		= 0x30
	offFreeList		= 0x38
)

func (p *Page) RootId() uint64      	{ return c.Bin.Uint64(p.raw[offRootId:]) }
func (p *Page) PageCnt() uint64      	{ return c.Bin.Uint64(p.raw[offPageCnt:]) }
func (p *Page) FreeList() uint64      	{ return c.Bin.Uint64(p.raw[offFreeList:]) }
func (p *Page) SetRootId(rid uint64) 	{ c.Bin.PutUint64(p.raw[offRootId:], rid) }
func (p *Page) SetPageCnt(pc uint64) 	{ c.Bin.PutUint64(p.raw[offPageCnt:], pc) }
func (p *Page) SetFreeList(fl uint64) 	{ c.Bin.PutUint64(p.raw[offFreeList:], fl) }

