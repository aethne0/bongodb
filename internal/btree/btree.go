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
)


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
		gen: gen,
	}

	return &btree, nil
}

func (bt *Btree) Get(key []byte) ([]byte, error) {
	rootFrame := bt.pager.GetPage(bt.metaPage.RootId())
	if rootFrame == nil {
		return nil, BtreeErrorFrame
	}

	p := page.PageSlottedFrom(rootFrame.BufferHandle())

	for p.IsTypeInner() {
		pageId, found := p.Get(key)
		if !found { return nil, nil }
		frame := bt.pager.GetPage(c.Bin.Uint64(pageId))
		if frame == nil { return nil, nil }
		p = page.PageSlottedFrom(frame.BufferHandle())
	}

	value, found := p.Get(key)
	if !found { return nil, BtreeErrorTemp }
	return value, nil
}

func (bt *Btree) Insert(key []byte, value []byte) error {
	rootFrame := bt.pager.GetPage(bt.metaPage.RootId())
	if rootFrame == nil {
		return BtreeErrorFrame
	}

	rootPage := page.PageSlottedFrom(rootFrame.BufferHandle())
	_, inserted := rootPage.Put(key, value)
	if !inserted { return BtreeErrorTemp }

	bt.pager.WritePage(rootFrame)

	return nil
}


func (bt *Btree) BigTesta() {
	rootFrame := bt.pager.GetPage(bt.metaPage.RootId())
	rootPage := page.PageSlottedFrom(rootFrame.BufferHandle())

	rootPage.IterPairs(func(k []byte, v []byte) bool {
		fmt.Printf("%s : %s\n", string(k), string(v))
		return true
	})

	fmt.Println(rootPage.FreeDecim()*100, "%")
}
