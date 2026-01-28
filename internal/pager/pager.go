package pager

import (
	"fmt"
	c "mooodb/internal"
	"mooodb/internal/iomgr"
	"sync"
)

type Pager struct {
	rawBuf 		[]byte

	frames 		[]Frame
	// PERF: it will be very easy to shard this in the future
	frameMap	map[uint64]int
	frameMapMu	sync.Mutex

	freeFrames  chan int

	nextId 		uint64
	iomgr		*iomgr.IoMgr
}

func CreatePager(filepath string, pageCnt int) (*Pager, error) {
	isPowerOfTwo := (pageCnt > 0) && ((pageCnt & (pageCnt - 1)) == 0);
	if !isPowerOfTwo {
		return nil, fmt.Errorf("Invalid page count, must be power of two")
	}

	slab, err := iomgr.AllocSlab(c.PAGE_SIZE * pageCnt)
	if err != nil { return nil, err }

	iomgr, err := iomgr.CreateIoMgr(filepath)
	if err != nil { return nil, err }

	frames := make([]Frame, pageCnt)
	freeFrames := make(chan int, pageCnt)
	for i := range frames {
		frames[i].Init(uint64(i), slab[c.PAGE_SIZE * i: c.PAGE_SIZE * (i + 1)])
		freeFrames <- i
	}

	pager := Pager {
		rawBuf: slab,
		frames: frames,

		frameMap: make(map[uint64]int),
		frameMapMu: sync.Mutex{},

		freeFrames: freeFrames,

		nextId: 1,
		iomgr: iomgr,
	}

	return &pager, nil
}

func (pgr *Pager) Close() error {
	pgr.iomgr.Close()
	return iomgr.DeallocSlab(pgr.rawBuf)
}

// nonblocking, returns nil if none are free
func (pgr *Pager) getFreeFrame() (int, bool) {
	// TODO: this is responsible for removing a value from the framemap as well
	// we dont have to do any locking because this is only called with a lock
	select {
	case freeIndex := <- pgr.freeFrames:
		return freeIndex, true
	default:
		return -1, false
	}
}

// Returning nil means we didn't have any free frames to load the page into (and the page 
// wasnt already paged in of course)
func (pgr *Pager) GetPage(pageId uint64) *Frame {
	pgr.frameMapMu.Lock()

	index, found := pgr.frameMap[pageId]

	if found {
		frame := &pgr.frames[index]
		frame.pins.Add(1)
		pgr.frameMapMu.Unlock()
		return frame
	} else {
		frameIndex, foundFreeFrame := pgr.getFreeFrame()

		if !foundFreeFrame {
			pgr.frameMapMu.Unlock()
			return nil
		} else {
			// we have to initialize a new frame and send a DiskOp request
			pgr.frameMap[pageId] = frameIndex
			frame := &pgr.frames[frameIndex]
			frame.pins.Add(1)
			frame.diskOp.PrepareOpSlice(iomgr.OpRead, frame.data, c.PageIdToOffset(pageId))
			frame.pageId = pageId

			// Once we have incremented pin and made the Op channel we can safely release
			pgr.frameMapMu.Unlock()

			pgr.iomgr.OpQueue <- &frame.diskOp

			return frame
		}
	}
}

// For new pages that don't exist yet
// todo: fallocate if needed - we dont strictly need to though
func (pgr *Pager) CreatePage() *Frame {
	pgr.frameMapMu.Lock()

	frameIndex, foundFreeFrame := pgr.getFreeFrame()

	if !foundFreeFrame {
		pgr.frameMapMu.Unlock()
		return nil
	}

	pageId := pgr.nextId
	pgr.nextId++

	pgr.frameMap[pageId] = frameIndex

	frame := &pgr.frames[frameIndex]
	frame.pins.Add(1)
	frame.pageId = pageId

	pgr.frameMapMu.Unlock()

	return frame
}

func (pgr *Pager) WritePage(frame *Frame) {
	frame.prepareOp(iomgr.OpWrite)
	pgr.iomgr.OpQueue <- &frame.diskOp
}

func (pgr *Pager) DelPage(pageId uint64) int32 {
	// TODO: not sure what this will return even, or if this will be the interface
	// infact i dont even know if this is possible!
	return 0
}

func (pgr *Pager) Flush() {
	// TODO: not sure what this will return even, or if this will be the interface
	// This is not really anything to do with the pager but workers only talk to the pager,
	// it just calls fsync of course
	// It might use some pager-wide channel or something? I'm not sure
	// Maybe it just blocks til its done
}
