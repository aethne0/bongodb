//go:build linux

package iomgr

import (
	c "mooodb/internal"
	"mooodb/internal/util"
	"sync"

	"log/slog"
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

const MMAP_MODE   	= unix.MAP_ANON  | unix.MAP_PRIVATE
const MMAP_PROT   	= unix.PROT_READ | unix.PROT_WRITE
const F_OPEN_MODE 	= unix.O_RDWR | unix.O_CREAT | unix.O_DIRECT
const F_OPEN_PERM 	= 0b_000_110_100_000

const OP_Q_SIZE			= 0x40 	// Number of OpBatches we can have queued
								// Keep in mind an OpBatch can contain like 20+ SQEs

const RING_ENTRIES 		= 0x80 	// Number of actual SQEs we can have in our ring (SQ)
const RING_TARG_DPTH	= 0x60
// assert(RING_TARG_DPTH + OP_MAX_OPS <= RING_ENTRIES)
// NOTE: it is assumed that (OP_MAX_OPS + RING_DPTHTRG) <= RING_ENTRIES
// So that as long as we have inflight+queued <= RING_TARG_DPTH we can safely 
// take an OpBatch out of the worker submission queue

// For fixed/aligned buffers - not for io_uring itself, liburing handles mmap-ing for 
// io_uring setup. This allocation will be aligned to the system page size (check using:
// `getconf PAGESIZE`. This will basically always be 0x1000 (4096))
func AllocSlab(size int) ([]byte, error) {
	sizeadj := (size + int(c.PAGE_SIZE-1)) & ^(int(c.PAGE_SIZE) - 1)
	if size != sizeadj {
		slog.Warn("AllocSlab - size rounded up to nearest multiple of page size",
			"requested", size, "adjusted-to", sizeadj, "page-size", c.PAGE_SIZE,
		)
	}
	raw, err := unix.Mmap(-1, 0, int(sizeadj), MMAP_PROT, MMAP_MODE) 
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
	OpQueue		chan *DiskOp
	fd			int
	opPtrs 		util.TicketQueue[*DiskOp]
}

func CreateIoMgr(path string) (*IoMgr ,error) {
	log := *slog.With("src", "IoMgr")

	fd, err := unix.Open(path, F_OPEN_MODE, F_OPEN_PERM)
	if err != nil { return nil, err }

	ring, err := giouring.CreateRing(RING_ENTRIES)
	if err != nil { return nil, err }

	iomgr := IoMgr {
		log: 		log,
		ring: 		ring,
		OpQueue: 	make(chan *DiskOp, OP_Q_SIZE),
		fd:			fd,
		opPtrs: 	util.CreateTicketQueue[*DiskOp](RING_ENTRIES),
	}

	go iomgr.ringlord()
	return &iomgr, nil
}

func (m *IoMgr) Close() {
	m.ring.QueueExit()
}

// we can make this smaller if we need space, but we are padding now anyway
type OpCode uint32
const (
	OpNop 	OpCode = iota
	OpWrite 
	OpRead
	OpSync
	OpAllocate
	// OpTruncate
)

// Init() MUST be called.
// DiskOp is owned by users, not IoMgr itself.
type DiskOp struct {
	Opcode	OpCode

	Bufptr	uintptr // pointer to start of buf - len is implictly PAGE_SIZE
	Offset	uint64 	// target file offset

	Res		int32

	cond	sync.Cond
	mu 		sync.Mutex
	done	bool


	_ [24]byte // pad to 128 bytes
}

func (op *DiskOp) Init() {
	op.cond.L = &op.mu
}

// func (op *DiskOp) Poll() bool
func (op *DiskOp) Wait() {
	op.mu.Lock()
	for !op.done {
		op.cond.Wait()
	}
	op.mu.Unlock()
}

// only adds a slice+offset - doesnt set OpCode or anything
func (op *DiskOp) PrepareOp(opcode OpCode) {
	op.done = false
	op.Opcode = opcode
}

func (op *DiskOp) PrepareOpSlice(opcode OpCode,slice []byte, offset uint64) {
	op.PrepareOp(opcode)
	op.Bufptr = uintptr(unsafe.Pointer(&slice[0]))
	op.Offset = offset
}

// if you call this and overflow thats on you
func (m *IoMgr) prepSQEs(op *DiskOp) {
	sqe := m.ring.GetSQE()

	// NOTE: These methods reset everything - userData must be set AFTER these
	switch op.Opcode {
	case OpNop:
		sqe.PrepareNop()

	case OpWrite:
		sqe.PrepareWrite(m.fd, op.Bufptr, c.PAGE_SIZE, op.Offset)

	case OpRead:
		sqe.PrepareRead(m.fd, op.Bufptr, c.PAGE_SIZE, op.Offset)

	case OpSync:
		sqe.PrepareFsync(m.fd, 0)

	case OpAllocate:
		sqe.PrepareFallocate(m.fd, 0, op.Offset, uint64(c.PAGE_SIZE))

	default:
		panic("Unknown opcode submitted to IoMgr")
	}

	opTicket := m.opPtrs.Acq(op)
	sqe.UserData = uint64(opTicket)
}

// "Those who sow the good seed
// Shall surely reap"
func (m *IoMgr) ringlord() {
	// note: it is possible to set interrupt affinity so io_uring io interupts will come 
	// 		 to this core
	/*
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		var cpuSet unix.CPUSet
		cpuSet.Zero()
		cpuSet.Set(2) 
		err := unix.SchedSetaffinity(0, &cpuSet)
		if err != nil { m.log.Warn("Couldn't set core affinity for ring manager") }
	*/

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
			op := <- m.OpQueue
			m.prepSQEs(op)
			queued++
		} 

		// Non-blocking - check for new submissions
		COLLECT: for inflight + queued < RING_ENTRIES {
			select {
			case op := <- m.OpQueue:
				m.prepSQEs(op)
				queued++
			default:
				break COLLECT
			}
		}

		// STAGE 2
		if queued > 0 {
			var submitted uint
			var err error
			// If we have a deep queue we will wait for some completions - can change later
			if inflight + queued > RING_TARG_DPTH { 
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

			op := m.opPtrs.Get(int(cqe.UserData))

			atomic.StoreInt32(&op.Res, cqe.Res)

			op.mu.Lock()
			op.done = true
			op.mu.Unlock()
			op.cond.Broadcast()

			m.opPtrs.Rel(int(cqe.UserData))

			m.ring.CQESeen(cqe)
		}
	}
}

