package page

import (
	c "mooodb/internal"
)

type PageMeta struct {
	Page
}

func PageMetaNew(raw []byte, pageId uint64, rootId uint64, gen uint64) PageMeta {
	p := PageMeta{Page: Page{raw: raw}}
	
	p.SetId(pageId)
	p.SetVer(Version)
	p.SetPagetype(PagetypeMeta)
	p.SetGen(gen)
	copy(p.raw[offMagic:], magic)
	p.SetFreeList(0)
	p.SetPageCnt(0)
	p.SetRootId(rootId)
	return p
}

func PageMetaFrom(raw []byte) PageMeta {
	return PageMeta{Page: Page{raw: raw}}
}

const (
	magic 			= "MoooDB~~"
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

