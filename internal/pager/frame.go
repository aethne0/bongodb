package pager

import (
	_ "mooodb/internal"
	"sync/atomic"
)

// pager and view and frame

type frame struct {
	data 	[]byte
	pageId	uint64
	state	uint64
	pins 	atomic.Int32
}

// we could juse use a rwlock - the wlock holder is the leader and is waiting on the load 
// completion, and then when its done it will downgrade allowing others waiting to all read
// the result. This automatically sleeps the readers waiting on the writer. 
//
// a condvar can work similarly. I will think about the tradeoffs
//
// there isnt really a good way to sleep extra waiters unless we make a channel for each of them,
// maybe thats more efficient, or we have them basically spin on an atomic.


type State uint32
const (
	Empty State = iota
	InFlight
	Loaded
	Aborted
)

func (f *frame) fromState() (State, uint32) {
	raw := atomic.LoadUint64(&f.state)
	state 	:= State((raw & 0xffff_ffff_0000_0000) >> 32)
	val 	:= uint32(raw & 0x0000_0000_ffff_ffff)
	return state, val
}

func (f *frame) toState(s State, v uint32) {
	atomic.StoreUint64(&f.state, uint64(s) << 32 | uint64(v))
}


