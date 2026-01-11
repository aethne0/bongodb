//go:build linux

package iomgr

import (
	"log/slog"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/aethne0/giouring"
	"golang.org/x/sys/unix"
)

// PERF:
// 1. read/write fixed
// 2. register buffer
// 3. register file
// 4. huge TLB
// During writes we are being held back by this even with just 1 ring
// on my machine we are only at only 64.90% io utilization but our kernel core is
// pegged at 6.09% (6.09*16=~97%)
// This is bottlenecked due to some combination of:
// Completion interrupts (were outta luck) (its probably this one)
// GUP for buffer slab (register the buffer)
// FD table lookups (register the file)

const ALIGN			= uint64(0x1000)
const MMAP_MODE   	= unix.MAP_ANON  | unix.MAP_PRIVATE
const MMAP_PROT   	= unix.PROT_READ | unix.PROT_WRITE
const F_OPEN_MODE 	= unix.O_RDWR | unix.O_CREAT | unix.O_DIRECT
const F_OPEN_PERM 	= 0b_000_110_100_000
const RING_ENTRIES 	= 0x80
const RING_DPTHTRG	= 0x40
const OP_Q_SIZE		= 0x100

// For fixed/aligned buffers - not for io_uring itself, liburing handles mmap-ing for 
// io_uring setup. This allocation will be aligned to the system page size (check using:
// `getconf PAGESIZE`. This will basically always be 0x1000 (4096))
func AllocSlab(size int) ([]byte, error) {
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
	opSem		chan struct{}
}

/*
func createIoMgrRegistered(slab []byte, fd int) (*IoMgr, error) {
	iomgr, err := CreateIoMgr()
	if err != nil { return nil, err }

	base := (*byte)(unsafe.Pointer(&slab[0]))
	iovecs := []syscall.Iovec {{
		Base: base, 
		Len: uint64(len(slab)),
	}}
	iomgr.ring.RegisterBuffers(iovecs) 

	files := []int{ fd }
	iomgr.ring.RegisterFiles(files)

	return iomgr, nil
}
*/

func CreateIoMgr() (*IoMgr ,error) {
	log := *slog.With("src", "IoMgr")

	ring, err := giouring.CreateRing(RING_ENTRIES)
	if err != nil { return nil, err }

	iomgr := IoMgr {
		log: 		log,
		ring: 		ring,
		opQueue: 	make(chan *Op, OP_Q_SIZE),
		opSem: 		make(chan struct{}, RING_ENTRIES),
	}

	go iomgr.ringlord()
	return &iomgr, nil
}

func (m *IoMgr) Close() {
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
const OP_SIZE = uintptr(0x200)
const OP_MAX_OPS = 24
type Op struct {
	Fd		int
	Bufs	[OP_MAX_OPS]uintptr
	Lens	[OP_MAX_OPS]uint32
	Offs	[OP_MAX_OPS]uint64
	Count   uint16

	seen	uint16

	Ch 		chan struct{}

	Res		int32
	Opcode	OpCode
	done 	bool
	Sync 	bool
}

// WARN: THIS (op) MUST HAVE A FIXED ADDRESS
//
// temporary - this should handle op struct pool as well
func (m *IoMgr) Submit(op *Op) {
	for range op.Count {
		m.opSem <- struct{}{}
	}
	if op.Opcode == OpWrite && op.Sync {
		m.opSem <- struct{}{}
	}
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
			if op.Sync || i < op.Count - 1 { sqe.Flags |= giouring.SqeIOLink }
		}
		if op.Sync {
			op.Count++
			sqe := m.ring.GetSQE()
			sqe.PrepareFsync(op.Fd, 0)
			sqe.UserData = uint64(uintptr(unsafe.Pointer(op)))
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
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var cpuSet unix.CPUSet
	cpuSet.Zero()
	cpuSet.Set(2) 
	err := unix.SchedSetaffinity(0, &cpuSet)
	if err != nil { m.log.Warn("Couldn't set core affinity for ring manager") }

	var queued   uint = 0 // SQEs that we have "got" and prepared from the opQueue
	var inflight uint = 0 // SQEs that have been SUBMITTED

	// This is our main io_uring manager loop. It is split into three phases:
	// 1. We collect submitted ops from our worker-facing opQueue, and get+prepare SQEs
	// 2. We submit new ops to the submission-queue
	// 3. We reap completed CQEs
	// This part is a bit magical
	// you have to decide how to tradeoff latency vs. throughput vs. cpu usage
	for {
		// STAGE 1
		if inflight == 0 && queued == 0 {
			// If we dont have any inflight ops, then theres no CQEs to reap and we
			// should just block on the opQueue until we have at least 1 op to submit
			// This code only takes 1, then the COLLECT loop will greedily and non-blockingly
			// take the rest (if any)
			op := <- m.opQueue
			m.prepSQEs(op)
			queued += uint(op.Count)
			if op.Opcode == OpWrite && op.Sync { queued++ }
		} 
		// Non-blocking
		COLLECT: for {
			select {
			case op := <- m.opQueue:
				m.prepSQEs(op)
				queued += uint(op.Count)
				if op.Opcode == OpWrite && op.Sync { queued++ }
			default:
				break COLLECT
			}
		}

		// STAGE 2
		if queued > 0 {
			var submitted uint
			var err error
			if inflight + queued > RING_DPTHTRG {
				submitted, err = m.ring.SubmitAndWait(8)
			} else {
				submitted, err = m.ring.Submit()
			}
			if err != nil && err != unix.ETIME && err != unix.EINTR {
				// should do something here
				m.log.Error("Submit", "err", err)
			} 
			queued   -= submitted
			inflight += submitted
		}

		for inflight > 0 {		
			cqe, err := m.ring.PeekCQE()
			if err == unix.EAGAIN || err == unix.EINTR || err == unix.ETIME {
				break
			} else if err != nil {
				m.log.Error("Peek cqe fatal error", "err", err)
				panic("Something wrong with your IO_URING!")
			}

			if cqe == nil { 
				// im pretty sure this should never happen
				m.log.Warn("cqe == nil but we didnt get an err (eagain)?") 
				break
			}

			inflight--

			op := (*Op)(unsafe.Pointer(uintptr(cqe.UserData)))
			op.seen++

			if op.done { goto OP_DONE } 

			if cqe.Res < 0 || op.seen == op.Count {
				// We should reply
				atomic.StoreInt32(&op.Res, cqe.Res)
				op.done = true
				op.Ch <- struct{}{}
				// reclaiming op struct has to be done caller channel is read
			}

			OP_DONE:
			m.ring.CQESeen(cqe)
			<- m.opSem
		}
	}
}

