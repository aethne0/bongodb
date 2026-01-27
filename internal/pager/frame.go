package pager

import (
	_ "mooodb/internal"
	"sync/atomic"
)

type frame struct {
	data 	[]byte
	pageId	uint64
	pins 	atomic.Int32
	state	uint64 // lower 32 is state, upper 32 is (signed) result from uring
	wait	chan int32
}

type State uint32
const (
	Empty 		State = iota
	InFlight
	Loaded
	Aborted
)

func (frm *frame) Init(data []byte) {
	frm.data = data
	frm.pageId = 0
	frm.state = uint64(Empty) << 32
	frm.pins.Store(0)
	frm.wait = make(chan int32, 1)
}


