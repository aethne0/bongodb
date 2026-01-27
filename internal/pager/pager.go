package pager

import (
	c "mooodb/internal"
	"mooodb/internal/iomgr"
)

const PAGER_PAGE_CNT = 16;

type Pager struct {
	rawBuf 		[]byte
	frames 		[]*frame
	nextId 		uint64
	iomgr		*iomgr.IoMgr
}

func CreatePager() (*Pager, error) {
	slab, err := iomgr.AllocSlab(c.PAGE_SIZE * PAGER_PAGE_CNT)
	if err != nil { return nil, err }

	iomgr, err := iomgr.CreateIoMgr("/tmp/wewlad")
	if err != nil { return nil, err }

	frames := make([]*frame, PAGER_PAGE_CNT)
	for i := range frames {
		frames[i].Init(slab[c.PAGE_SIZE * i: c.PAGE_SIZE * (i + 1)])
	}

	pager := Pager {
		rawBuf: slab,
		frames: frames,
		nextId: 1,
		iomgr: iomgr,
	}

	return &pager, nil
}

func (pgr *Pager) Close() error {
	pgr.iomgr.Close()
	return iomgr.DeallocSlab(pgr.rawBuf)
}

/*
we have a few things we have to be able to do here. Basically the api for workers:

1. write-out current frame contents to its page on disk
2. request a page from the pager
	- If present we just pin and give the worker a ref to the frame
	- If not present we send a load and give the frame with *some* way the worker can wait 
	- We may need to evict pages to load in new pages - solve this
	- We need to solve thundering herd problems when >1 workers request the same not-paged-in
		page - solve this
3. request a new, empty page from the pager. This page id will be autoincremented by pager.
*/



