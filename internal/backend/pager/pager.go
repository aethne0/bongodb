package pager

import (
	"errors"
	"log/slog"
	"mooodb/internal/iomgr"
	"sync"
	"sync/atomic"
	"unsafe"
)

// PERF: we can remove all runtime allocations by implementing a fixed set of "views"
// that have a fixed-len buffer of Pagerefs
// PERF: We aren't doing any sharding of frames yet - this could be paired to do
// a shard-per-ring-per-core design, pages would get hashed somehow for load balancing.
// TODO: Singleflight page fetching

const PAGE_SIZE				= 0x1000
const PAGEBUF_VIEWS_LEN		= 0x08 // these can hold up to 24(+fsync) actual io ops
var   PAGEBUF_VIEWS_SIZE	= PAGEBUF_VIEWS_LEN * unsafe.Sizeof(View{})
const PAGEBUF_BUF_PAGES 	= 0x20
const PAGEBUF_BUF_SIZE  	= PAGE_SIZE * PAGEBUF_BUF_PAGES

var (
	ErrInvalidArg 	= errors.New("Invalid arg")
	ErrIO 			= errors.New("Invalid arg")
)

type Pager struct {
	log			*slog.Logger

	pageSlab 	[]byte // to free later
	opSlab		[]byte // to free later

	iomgr		*iomgr.IoMgr

	views		[]View
	viewQ		chan int

	frames		[]frame
	framesFree	chan int
	frameRWL	sync.RWMutex
	frameMap	map[uint64]int
}


// No allocations after function.
func CreatePagebuf(path string) (*Pager, error) {
	log := *slog.With("src", "Pager")

	pageSlab, err := iomgr.AllocSlab(PAGEBUF_BUF_SIZE)
	log.Debug("CreatePagebuf", "bytes", len(pageSlab), "pages", len(pageSlab) / int(iomgr.ALIGN))
	if err != nil { return nil, err } 

	frames := make([]frame, PAGEBUF_BUF_PAGES)
	framesFree := make(chan int, PAGEBUF_BUF_PAGES)

	for i := range PAGEBUF_BUF_PAGES {
		f := &frames[i]
		framesFree <- i
		f.index = i
		f.data = pageSlab[(i)*PAGE_SIZE : (i+1)*PAGE_SIZE]
		f.pageid = 0
		f.pins.Store(0)
		f.dirty = false
	}

	viewSlab, err := iomgr.AllocSlab(int(PAGEBUF_VIEWS_SIZE))
	log.Debug("CreateViews", "bytes", len(viewSlab), "views", PAGEBUF_VIEWS_LEN)
	if err != nil { return nil, err } 
	views := unsafe.Slice((*View)(unsafe.Pointer(&viewSlab[0])), PAGEBUF_VIEWS_LEN)

	iomgr, err := iomgr.CreateIoMgr(path)
	if err != nil { return nil, err } 

	viewQ := make(chan int, PAGEBUF_VIEWS_LEN)
	for i := range PAGEBUF_VIEWS_LEN {
		viewQ <- i
		v := &views[i]
		v.index = i
		v.op.Ch = make(chan struct{})
	}


	pb := Pager {
		log: &log,
		pageSlab: pageSlab,
		opSlab: viewSlab,

		iomgr: iomgr,

		views: views,
		viewQ: viewQ,

		// NOTE: If a frame is pinned it will not be moved or evicted, so if you
		// pin a frame it is safe to release the RWLock to go load pages etc.
		// If you are doing anything to the map you need a lock though - you cannot
		// rely on any frame that YOU have not pinned, or else its a race-condition
		frames: frames,
		frameRWL: sync.RWMutex{},
		framesFree: framesFree,
		frameMap: make(map[uint64]int, PAGEBUF_BUF_PAGES),
	}

	return &pb, nil
}

func (p *Pager) DestroyPagebuf() error {
	p.iomgr.Close()
	err := iomgr.DeallocSlab(p.pageSlab)
	if err != nil { return err }
	err = iomgr.DeallocSlab(p.opSlab)
	return err
}

// frames are never modified unless they are new copies that dont yet exist in the tree,
// so we dont need a lock - if pins > 1 then they are all readers.
type frame struct {
	index 	int
	data 	[]byte
	pageid	uint64
	pins	atomic.Int32
	dirty	bool
}

type View struct {
	index	int
	op 		iomgr.Op
	Prs 	[iomgr.OP_MAX_OPS]Pageref // TODO: shouldnt be directly accessed
	cnt 	int
	freed	bool
}
type Pageref struct {
	PageId 		uint64
	frameIndex	int
	Data		[]byte
	fetched bool
}

func (p *Pager) ReleaseView(v *View) {
	if v.freed { return }
	for i := range v.cnt {
		pr := &v.Prs[i]
		p.frames[pr.frameIndex].pins.Add(-1)
	}
	p.viewQ <- v.index
	v.freed = true
}

func idToOff(pageId uint64) uint64 {
	return PAGE_SIZE * pageId
}

// The pages will be returned in the order you requested them
func (p *Pager) ReadPages(pages []uint64) (*View, error) {
	if len(pages) > PAGEBUF_BUF_PAGES { return nil, ErrInvalidArg }
	if len(pages) > iomgr.OP_MAX_OPS  { return nil, ErrInvalidArg } // this *could* be possible

	viewTicket := <- p.viewQ
	view := &p.views[viewTicket]
	view.cnt = len(pages)
	op := &view.op
	op.Opcode = iomgr.OpRead

	// first pass over frames to check which are already paged in
	p.frameRWL.RLock()
	for i, pageId := range pages {
		frameIndex, found := p.frameMap[pageId]
		var f *frame

		if found {
			f = &p.frames[frameIndex]
		} else {
			f = &p.frames[<- p.framesFree]

			op.AddSlice(f.data, idToOff(pageId))
		}

		f.pins.Add(1)
		pr := &view.Prs[i]
		pr.fetched = !found
		pr.PageId = pageId
		pr.Data = f.data
		pr.frameIndex = f.index
	}
	p.frameRWL.RUnlock()


	if op.Count > 0 {
		p.iomgr.Submit(op)
		<- op.Ch
		res := op.Res
		if res < 0 {
			p.log.Error("error reading pages", "err", op.Res)
			// unpin our pinned pages to cleanup
			for _, pr := range view.Prs {
				p.frames[pr.frameIndex].pins.Add(-1)
			}
			return nil, ErrIO
		} else {
			p.frameRWL.Lock()
			for _, pr := range view.Prs {
				if pr.fetched {
					p.frameMap[pr.PageId] = pr.frameIndex
				}
			}
			p.frameRWL.Unlock()
		}
	}

	return view, nil
}

