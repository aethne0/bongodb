package btree

import (
	c "mooodb/internal"
	"fmt"
	"mooodb/internal/btree/page"
	"mooodb/internal/pager"
)

var (
	BtreeErrorFrame = fmt.Errorf("Btree: Couldn't get frame")
	BtreeErrorTemp = fmt.Errorf("Btree: temp-error")
	CursorErrorTemp = fmt.Errorf("Cursor: temp-error")
)


// TODO: we need a lock to change anything on the meta page, such as root pointer
type Btree struct {
	metaFrame 	*pager.Frame
	metaPage 	*page.PageMeta

	pager 		*pager.Pager
	gen	uint64 // generation
}

// TODO we cant reopen these, we have no "manifest"
func CreateBtree(pager *pager.Pager) (*Btree, error) {
	metaFrame := pager.CreatePage()
	if metaFrame == nil {
		return nil, BtreeErrorFrame
	}

	rootFrame := pager.CreatePage()
	if rootFrame == nil {
		return nil, BtreeErrorFrame
	}

	gen := uint64(1)

	metaPage := page.PageMetaNew(metaFrame.BufferHandle(), metaFrame.PageId(), 
		rootFrame.PageId(), gen)
	rootPage := page.PageSlottedNew(rootFrame.BufferHandle(), rootFrame.PageId(),
		true, gen, metaFrame.PageId())

	metaPage.DoChecksum()
	metaPage.SetPageCnt(1)
	rootPage.DoChecksum()

	pager.WritePage(metaFrame)
	pager.WritePage(rootFrame)

	rootFrame.Release()

	btree := Btree {
		metaFrame: 	metaFrame,
		metaPage: 	&metaPage,
		pager: 		pager,
		gen: 		gen,
	}

	return &btree, nil
}

const CURSOR_STACK_DEPTH = 64
type CursorCrumb struct {
	pageId 	uint64
	slot	uint16
	_		[6]byte
}

type Cursor struct {
	btree		*Btree
	stack	[CURSOR_STACK_DEPTH]CursorCrumb
	stackPtr	int
}

func CreateCursor(btree *Btree) *Cursor {
	return &Cursor{
		btree: btree,
	}
}

// returns whether exact key was found or not
func (crs *Cursor) Seek(key []byte) (bool, error) {
	crs.stackPtr = 0

	frame := crs.btree.pager.GetPage(crs.btree.metaPage.RootId())
	if frame == nil { return false, CursorErrorTemp }
	curPage := page.PageSlottedFrom(frame.BufferHandle())

	crs.stack[crs.stackPtr].pageId = curPage.Id()

	for curPage.IsTypeInner() {
		pageIdVal, slot := curPage.GetSmallestGreater(key)
		if slot < 0 { return false, nil }

		crs.stack[crs.stackPtr].slot = uint16(slot)
		crs.stackPtr += 1

		pageId := c.Bin.Uint64(pageIdVal)

		frame = crs.btree.pager.GetPage(pageId)
		if frame == nil { return false, CursorErrorTemp }
		curPage = page.PageSlottedFrom(frame.BufferHandle())
	}

	// now we're at a leaf page

	slot := curPage.Seek(key)
	if slot < 0 { return false, nil }
	crs.stack[crs.stackPtr].slot = uint16(slot)

	return true, nil
}


