package pager

import (
	c "mooodb/internal"
	system "mooodb/internal/system"
	"sync/atomic"

	"fmt"
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
	iomgr		*system.IoMgr

	diskOp		system.DiskOp // for fsync, truncate, etc
}

func pagerErr(errno int) error {
	// TODO: proper
	return fmt.Errorf("pager error: %d", errno)
}

func CreatePager(filepath string, pageCnt int) (*Pager, error) {
	isPowerOfTwo := (pageCnt > 0) && ((pageCnt & (pageCnt - 1)) == 0);
	if !isPowerOfTwo {
		return nil, fmt.Errorf("Invalid page count, must be power of two")
	}

	slab, err := system.AllocAlignedSlab(c.PAGE_SIZE * pageCnt)
	if err != nil { return nil, err }

	iomgr, err := system.CreateIoMgr(filepath)
	if err != nil { return nil, err }

	pager := Pager {
		rawBuf: slab,

		frameMap: make(map[uint64]int),
		frameMapMu: sync.Mutex{},

		nextId: 1,
		iomgr: iomgr,

		diskOp: system.DiskOp{},
	}

	frames := make([]Frame, pageCnt)
	freeFrames := make(chan int, pageCnt)
	for i := range frames {
		frames[i].init(i, slab[c.PAGE_SIZE * i: c.PAGE_SIZE * (i + 1)])
		frames[i].pager = &pager
		freeFrames <- i
	}

	pager.frames = frames
	pager.freeFrames = freeFrames

	return &pager, nil
}

func (pgr *Pager) Close() error {
	pgr.iomgr.Close()
	return system.DeallocAlignedSlab(pgr.rawBuf)
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
			frame.diskOp.PrepareOpSlice(system.OpRead, frame.data, c.PageIdToOffset(pageId))
			frame.pageId = pageId

			// Once we have incremented pin and made the Op channel we can safely release
			//
			// It is not safe to unlock until we've made the channel, because it will be
			// a race if some other thread goes to wait on the channel before we initialize it
			pgr.frameMapMu.Unlock()

			pgr.iomgr.OpQueue <- &frame.diskOp

			return frame
		}
	}
}

// For new pages that don't exist yet
//
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
	frame.prepareOp(system.OpWrite)
	pgr.iomgr.OpQueue <- &frame.diskOp

	// TODO temporary
	<- frame.diskOp.Ch
}

// NOTE: there is no notion of "deleting a page" at the file io level - this would just be 
// represented by the btree writing out a free-page at the page_id that was being "deleted"

func (pgr *Pager) Sync() error {
	pgr.diskOp.PrepareOpSlice(system.OpSync, nil, 0)
	pgr.iomgr.OpQueue <- &pgr.diskOp
	<- pgr.diskOp.Ch
	if pgr.diskOp.Res < 0 {
		return pagerErr(int(pgr.diskOp.Res))
	} 
	return nil
}

// A Frame has a "lifetime" which corresponds to the time that it refers to a certain page-id
// Between these "lifetime"s it will be assured that all workers vacate the Frame and nobody
// holds a reference to it (or its channel) between lifetimes.
// The "wait" channel is created once at the beginning of its lifetime and never is
// re-initialized unless the Frame begins again with a new page-id
//
// Note: If you try to access a Frame after unpinning it the universe will explode instantly
type Frame struct {
	frameIndex int // mostly for debugging

	data   	[]byte
	pageId 	uint64
	pins   	atomic.Int32

	pager 	*Pager
	_pad 	[8]byte

	diskOp system.DiskOp // a frame owns its own diskop it can reuse
}

// Just to remember what we need to set initially. Other fields should be set when
// the frame is initialized with a page_id and corresponding disk-op
func (frm *Frame) init(frameId int, data []byte) {
	frm.frameIndex = frameId
	frm.data = data
}

func (frm *Frame) BufferHandle() []byte {
	return frm.data
}

// Unpins frame (by one)
func (frm *Frame) Release() {
	old := frm.pins.Add(-1)
	// TODO: this is temporary and makes it so pages are simply not cached
	if old == 0 {
		frm.pager.freeFrames <- frm.frameIndex
	}
}

func (frm *Frame) PageId() uint64 {
	return frm.pageId
}

func (frm *Frame) prepareOp(opcode system.OpCode) {
	frm.diskOp.PrepareOpSlice(opcode, frm.data, c.PageIdToOffset(frm.pageId))
}
