package pager

import (
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
	frameId		uint64 		// mostly for debugging

	data 		[]byte
	pageId		uint64
	pins 		atomic.Int32

	_pad		[16]byte

	diskOp		iomgr.DiskOp // a frame owns its own diskop it can reuse
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

