package pager

import (
	_ "mooodb/internal"
	c "mooodb/internal"
	"mooodb/internal/iomgr"
	"sync/atomic"
)

// A Frame has a "lifetime" which corresponds to the time that it refers to a certain page-id
// Between these "lifetime"s it will be assured that all workers vacate the Frame and nobody
// holds a reference to it (or its channel) between lifetimes.
// The "wait" channel is created once at the beginning of its lifetime and never is
// re-initialized unless the Frame begins again with a new page-id
//
// Note: If you try to access a Frame after unpinning it the universe will explode instantly
type Frame struct {
	frameId					uint64 		// mostly for debugging

	data 					[]byte
	pageId					uint64
	pins 					atomic.Int32

	_pad					[16]byte

	diskOp					iomgr.DiskOp // a frame owns its own diskop it can reuse
}

// Just to remember what we need to set initially. Other fields should be set when 
// the frame is initialized with a page_id and corresponding disk-op
func (frm *Frame) Init(frameId uint64, data []byte) {
	frm.frameId = frameId
	frm.data = data
}

func (frm *Frame) Release() {
	frm.pins.Add(-1)
}

func (frm *Frame) prepareOp(opcode iomgr.OpCode) {
	frm.diskOp.PrepareOpSlice(opcode, frm.data, c.PageIdToOffset(frm.pageId))
}

// N worker threads will be waiting on the channel for a disk-op, the int32 iouring status
// will be sent and then the channel immediately closed, the first worker to wake will use
// this status to update state (state | res). The rest will be woken by the close(ch) broadcast,
// and can then read the state as well.

// Once this function returns the page is readable
//
// The return value is success vs failure
func (frm *Frame) WaitPage(page_id uint64) bool {
	frm.pins.Add(1)
	<- frm.diskOp.Ch
	return atomic.LoadInt32(&frm.diskOp.Res) >= 0
}

