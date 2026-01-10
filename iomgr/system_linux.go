//go:build linux

package iomgr

import (
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/aethne0/giouring"
	"golang.org/x/sys/unix"
)

/* WARN:
Right now i have a preallocated and prepopulate fixed array of "inflight structs", each of
which have a `chan IoResult` with capacity 1. This is used to block client-calls until we
have a result to send back out to them.

This fixed array is protected by a channel-based semaphore - `make(chan struct{}, ARR_SIZE)`.
The problem is: the semaphore gets release once the result is written into the channel,
but that doesnt mean the client has actually READ it from the channel, so if the client is
very slow to read, we will loop back around (because the semaphore is freed) and overwrite that inflight entry (and thus the channel with the result).

I am worried about protecting against this with a semaphore though (by making that inflight
struct unuseable til its been read), because then if we have a very slow client thread, or
a client thread that has crashed or something, we will be "stuck" on that channel for eternity.
*/

// NOTE: Even with 65536 max ring-entries we only *need* 16 out of the 64 bytes
// of the cqe/sqe userdata field - what else could we use it for that would be fun?

// TODO: We ideally should be using HugeTLB, register it fixed, and use WriteFixed
// and ReadFixed,but we need to detect it and gracefully degrade if its not available
// Right now we just do it ez mode - which should support any size passed in ( as long
// as its aligned for odirect etc) but will have slightly worse performance

const ALIGN			= uint64(0x1000)
const MMAP_MODE   	= unix.MAP_ANON  | unix.MAP_PRIVATE
const MMAP_PROT   	= unix.PROT_READ | unix.PROT_WRITE
const F_OPEN_MODE 	= unix.O_RDWR | unix.O_CREAT | unix.O_DIRECT
const F_OPEN_PERM 	= 0b_000_110_100_000
const RING_ENTRIES 	= 0x100
const SUBQ_ENTRIES	= 0x100

// For fixed/aligned buffers - not for io_uring itself, liburing handles mmap-ing for 
// io_uring setup.
func AllocSlab(size int) ([]byte, error) {
	// this will be aligned to the system page size (getconf PAGESIZE)
	// basically always 4096/0x1000
	// for odirect this must be aligned to logical block size (which is 512/4k)
	raw, err := unix.Mmap(-1, 0, int(size), MMAP_PROT, MMAP_MODE) 
	if err != nil {
		slog.Error("AllocSlab", "err", err)
	}
	return raw, err
}

func DeallocSlab(ptr []byte) error {
	err := unix.Munmap(ptr)
	if err != nil {
		slog.Error("DeallocSlab", "err", err)
	}
	return err
}

type IoMgr struct {
	log			slog.Logger
	ring 		*giouring.Ring
	opQueue		chan *Op
}

// NOTE: 	thread-safety: only one thread is polling eventfd and reading CQEs
// 			Many threads may be calling GetSQE, submit, etc

func CreateIoMgr() (*IoMgr ,error) {
	log := *slog.With("src", "IoMgr")

	ring, err := giouring.CreateRing(RING_ENTRIES)
	if err != nil { return nil, err }

	iomgr := IoMgr {
		log: 		log,
		ring: 		ring,
		opQueue: 	make(chan *Op, SUBQ_ENTRIES),
	}

	go iomgr.ringlord()
	return &iomgr, nil
}

func (m *IoMgr) Close() {
	// TODO:
	m.ring.QueueExit()
}

type OpCode uint16
const (
	OpNop 	OpCode = iota
	OpWrite 
	OpRead
	OpSync
	OpAllocate
	// OpTruncate
)

// this is fixed size and preallocable 
// we just pool these into a ring buffer and reuse them
// an op may have at most 24 operations (we can revise this later if needed)
// queue_len=256 this would be 128KiB
// This should stay 512 bytes
type Op struct {
	Fd		int
	Bufs	[24]uintptr
	Lens	[24]uint32
	Offs	[24]uint64
	Count   uint16

	seen	uint16

	Ch 		chan struct{}

	Res		int32
	Opcode	OpCode
	done 	bool
}
const OP_SIZE = uintptr(0x200)
const OP_MAX_OPS = 24

// temporary - this should handle op struct pool as well
func (m *IoMgr) Submit(op *Op) {
	m.opQueue <- op
}

func (m *IoMgr) prepSQEs(op *Op) {
	op.done = false
	op.seen = 0

	switch op.Opcode {
	case OpNop:
		for i := range op.Count {
			sqe := m.ring.GetSQE()
			sqe.PrepareNop()
			sqe.UserData = uint64(uintptr(unsafe.Pointer(op)))
			if i < op.Count - 1 { sqe.Flags |= giouring.SqeIOLink }
		}

	case OpWrite:
		for i := range op.Count {
			sqe := m.ring.GetSQE()
			sqe.PrepareWrite(op.Fd, op.Bufs[i], op.Lens[i], op.Offs[i])
			sqe.UserData = uint64(uintptr(unsafe.Pointer(op)))
			if i < op.Count - 1 { sqe.Flags |= giouring.SqeIOLink }
		}
	
	case OpRead:
		for i := range op.Count {
			sqe := m.ring.GetSQE()
			sqe.PrepareRead(op.Fd, op.Bufs[i], op.Lens[i], op.Offs[i])
			sqe.UserData = uint64(uintptr(unsafe.Pointer(op)))
			if i < op.Count - 1 { sqe.Flags |= giouring.SqeIOLink }
		}

	case OpSync:
		sqe := m.ring.GetSQE()
		sqe.PrepareFsync(op.Fd, 0)
		sqe.UserData = uint64(uintptr(unsafe.Pointer(op)))
	
	case OpAllocate:
		sqe := m.ring.GetSQE()
		sqe.PrepareFallocate(op.Fd, 0, op.Offs[0], uint64(op.Lens[0]))
		sqe.UserData = uint64(uintptr(unsafe.Pointer(op)))

	default:
		m.log.Warn("Invalid opcode", "opcode", op.Opcode)
		atomic.StoreInt32(&op.Res, -int32(unix.EINVAL))
		op.Ch <- struct{}{}
	}
}

func drain(ch chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return 
		}
	}
}

// "Those who sow the good seed
// Shall surely reap"
func (m *IoMgr) ringlord() {
	// note: it is possible to set interrupt affinity so io_uring io interupts will come 
	// 		 to this core
	// note: something something `systemctl stop irqbalance`
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var cpuSet unix.CPUSet
	cpuSet.Zero()
	if runtime.NumCPU() > 0 {
		cpuSet.Set(1)
	}
	err := unix.SchedSetaffinity(0, &cpuSet)
	if err != nil { m.log.Warn("Couldn't set core affinity for ring manager") }

	stime := syscall.Timespec { Sec: 0, Nsec: 1_000_000 }
	var sigset unix.Sigset_t

	for {
		COLLECT: for {
			select {
			case op := <- m.opQueue:
				m.prepSQEs(op)
			default:
				break COLLECT
			}
		}

		// WARN: the giouring libary changed SubmitAndWaitTimeout to return
		// a single CQE instead of a int for how many we submitted
		// I'm not sure how anyone thought that'd be remotely useful
		// I'll go in and change it back eventually
		// PERF: we are calling this even if we dont have any ops, just to get 
		//		 the timeout, we can optimize later
		_, err := m.ring.SubmitAndWaitTimeout(1, &stime, &sigset)
		if err != nil && err != unix.ETIME && err != unix.EINTR {
			// should do something here
			m.log.Error("SubmitAndWaitTimeout", "err", err)
			runtime.Gosched() // shouldnt have this
		} 

		for {		
			cqe, err := m.ring.PeekCQE()
			if err == unix.EAGAIN || err == unix.EINTR {
				break
			} else if err != nil {
				m.log.Error("Oh baby")
				panic("we dead")
			}

			if cqe == nil {
				m.log.Warn("cqe == nil, i didnt think this would happen")
			}

			op := (*Op)(unsafe.Pointer(uintptr(cqe.UserData)))
			op.seen++


			if op.done {
				goto OP_DONE
			} 

			if cqe.Res < 0 || op.seen == op.Count {
				// We should reply
				atomic.StoreInt32(&op.Res, cqe.Res)
				op.done = true
				op.Ch <- struct{}{}
				// reclaiming op struct has to be done caller channel is read
			}

			OP_DONE:
			m.ring.CQESeen(cqe)
		}
	}
}

// OPS

// func (m *IoMgr) Nop(ctx context.Context, opcnt int) IoResult {

// util/debug

func (o *Op) String() string {
	if o == nil {
		return "<nil>"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Op | Opcode: %v, Fd: 0x%x, Done: %v, Count: %d, Seen: %d, Res: 0x%x\n", 
		o.Opcode, o.Fd, o.done, o.Count, o.seen, o.Res)
	
	for i := range min(OP_MAX_OPS, o.Count) {
		var d string
		if i + 1 == o.seen {
			d = ">"
		} else {
			d = "|"
		}
		fmt.Fprintf(&b, "   %s [%02d] Buf: 0x%x | Len: 0x%08x | Off: 0x%08x\n", 
			d, i, o.Bufs[i], o.Lens[i], o.Offs[i])
	}

	return b.String()
}

