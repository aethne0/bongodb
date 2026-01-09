//go:build linux

package iomgr

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"runtime"
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

// For page buffer - not for io_uring, liburing handles mmap-ing for io_uring setup.
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
	log				slog.Logger

	ring 			*giouring.Ring
	ringMu			chan struct{}

	inFlight		[RING_ENTRIES]inFlight	// in-flight req channels
	inFlightSem		chan struct{}

	reqId			uint64

	eventf			*os.File				// eventfd go std file
}

// NOTE: 	thread-safety: only one thread is polling eventfd and reading CQEs
// 			Many threads may be calling GetSQE, submit, etc

func CreateIoMgr(slab []byte) (*IoMgr ,error) {
	log := *slog.With("src", "IoMgr")

	ring, err := giouring.CreateRing(RING_ENTRIES)
	if err != nil { return nil, err }

	eventfd, err := unix.Eventfd(0, 0)
	if err != nil { return nil, err }
	_, err = ring.RegisterEventFd(eventfd)
	if err != nil { return nil, err }

	eventf := os.NewFile(uintptr(eventfd), "io_uring_eventfd")

	/*
	iovs := []syscall.Iovec{
		{ 
			Base: &slab[0],
			Len: uint64(len(slab)),
		},
	}
	_, err = ring.RegisterBuffers(iovs)
	if err != nil { 
		log.Error("Unable to register the passed in slab with io_uring. " +
			"This is probably due mlock cap being too low. " +
			"You might be able to adjust it using `ulimit -l`. " +
			"Your limit must be the size of your slab PLUS some overhead room " +
			"for io_uring itself.")
		var rlimit unix.Rlimit
		if err := unix.Getrlimit(unix.RLIMIT_MEMLOCK, &rlimit); err != nil {
			log.Error("    ! Couldn't get system ulimit -l")
		} else {
			log.Error("mlock info:", "ulimit -l", rlimit.Cur/1024, "slab size", len(slab)/1024)
		}
		return nil, err 
	}
	*/

	iomgr := IoMgr {
		log: log,

		ring: ring,
		ringMu: make(chan struct{}, 1), // this is only for submissions

		inFlightSem: make(chan struct{}, RING_ENTRIES),

		reqId: 	 0,
		eventf: eventf,
	}

	// initialize our inflight array
	for i := range RING_ENTRIES {
		iomgr.inFlight[i].ch = make(chan IoResult, 1)
	}

	go iomgr.epoller()
	return &iomgr, nil
}

func (m *IoMgr) Close() {
	m.ring.QueueExit()
}

func (m *IoMgr) epoller() {
	// NOTE: epoller is the only thread that touches cq
	var efdbuf [8]byte

	for {
		_, err := m.eventf.Read(efdbuf[:])
		if err != nil {
			m.log.Error("Iomgr epoller", "err", err)
			panic("epoller had error reading from eventfd")
		}

		// then we process CQEs
		CQES: for {
			cqe, err := m.ring.PeekCQE()

			switch err {
			case nil:
				// continue
			case unix.EINTR:  // Go thread got SIGURGed or something greasy
				continue
			case unix.EAGAIN: // epoll is just weird
				break CQES
			default:
				m.log.Error("Iomgr epoller", "err", err)
				panic(err)
			}

			if cqe != nil {
				m.ring.CQESeen(cqe)
			}  else {
				break
			}

			infl := &m.inFlight[cqe.UserData % RING_ENTRIES]
			rem := infl.remaining.Add(-1)
			if rem < 0 {
				m.log.Error("got cqe with inflight.remaining already <= 0?")
			}

			if infl.cancelled {
				if cqe.Res != -int32(unix.ECANCELED) {
					m.log.Warn("Expected cqe.Res to be ECANCELLED", "res", cqe.Res)
				}
			} else if cqe.Res < 0 {
				// If there is an error we should just reply immediately, and 
				// mark inflight.cancelled as true so that we can ignore the rest
				// as we receive them
				infl.cancelled = true
				infl.ch <- IoResult{ Err: syscall.Errno(-cqe.Res) }
			} else if rem == 0 {
				// If there wasnt an error, but we are on the last entry, 
				// then we should reply with a successful result
				infl.ch <- IoResult{ Err: nil }
			} 
			// else we have more to recv before replying

			<- m.inFlightSem
		}
	}
}

type IoResult struct {
	Err 		error
}

type inFlight struct {
	// this is an atomic because im paranoid about memory ordering, theres a
	// 95% chance it doesnt need to be (other than memory ordering it doesnt)
	remaining 	atomic.Int32 
	id			uint64
	ch 			chan IoResult
	cancelled	bool
}

func (infl *inFlight) reinit(id uint64, rem int32) {
	infl.id = id
	infl.cancelled = false
	infl.remaining.Store(rem)
	for {
		select {
		case _, ok := <- infl.ch:
			if !ok {
				slog.With("src", "inflight").Warn(
					"inflight resp channel got closed?",
					"id", id,
				)
				return 
			}
		default:
			return
		}
	}
}

func ctxresp(ctx context.Context) IoResult {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return IoResult{Err: unix.ETIMEDOUT} 
	}
	return IoResult{ Err: unix.EINTR }
}

type WriteOp struct {
	buf 	[]byte
	offset 	uint64
}

// slices must be 3way aligned to logical block size - buffer start, offset, and len % 512==0
// (or whatever the block size is, can be 4k instead of 512, or whatever else)
// if you try to write 300 bytes starting at +600 or something youll get EINVAL/-22
func (m *IoMgr) Write(ctx context.Context, fd int, ops []WriteOp, sync bool) IoResult {
	opcnt := len(ops)
	if sync { opcnt++ }
	if opcnt > RING_ENTRIES {
		return IoResult{ Err: unix.E2BIG }
	}

	// NOTE:
	// its possible to make this locking a little more granular but not in a way that 
	// matters. the ring submission operations are not thread safe, and so either:
	// 1. 	we have "room" in the SQ and so the fastest thing we can do is just take
	// 		all the sqes we need in a row while holding the lock one time, or:
	// 2. 	we are limited by inFlightSem, in which case any other potential writers
	//		would be waiting similarly, so releasing the lock wouldn't enable them
	// there is nothing else that would "block" in this process other than waiting
	// for the sem (or lock).
	select {
	case m.ringMu <- struct{}{}:
	case <- ctx.Done():
		return ctxresp(ctx)
	}

	id := m.reqId
	m.reqId++

	infl := &m.inFlight[id % RING_ENTRIES]
	infl.reinit(id, int32(opcnt))

	for i := range ops {
		m.inFlightSem <- struct{}{}
		sqe := m.ring.GetSQE()
		if sqe == nil {
			m.log.Warn("sqe from GetSQE was nil - i thought this wouldnt happen")
			runtime.Gosched()
			continue
		} 

		op := &ops[i]

		/*
		sqe.PrepareWriteFixed(
			fd,
			uintptr(unsafe.Pointer(&op.buf[0])),
			uint32(len(op.buf)),
			op.offset,
			0, // reminder: this is the one iovec entry passed in when we registered the ring
		)
		*/
		sqe.PrepareWrite(
			fd,
			uintptr(unsafe.Pointer(&op.buf[0])),
			uint32(len(op.buf)),
			op.offset,
		)

		sqe.UserData = id

		// SQEIOLINK is to the *next*, entry, so we dont link the last entry
		if sync || i < len(ops) - 1 {
			sqe.Flags |= giouring.SqeIOLink
		}
	}

	if sync {
		m.inFlightSem <- struct{}{}
		sqe := m.ring.GetSQE()
		sqe.PrepareFsync(fd, 0)
		sqe.UserData = id
	}

	subcnt, err := m.ring.Submit()
	if subcnt != uint(opcnt) {
		m.log.Error("subcnt != opcnt", "subcnt", subcnt, "opcnt", opcnt)
		panic(	"subcnt != opcnt, which shouldn't happen due to inflightsem.\n" +
				"If this happens i misunderstood something." )
	}
	if err != nil {
		// honestly were just dead if this happens
		m.log.Error("Iomgr", "err", err)
		panic("Nooo! What have they done to my IO_URING!!!")
	}

	<- m.ringMu

	select {
	case res := <- infl.ch: 	
		return res
	case <- ctx.Done():	
		return ctxresp(ctx)
	}
}

type ReadOp struct {
	buf 	[]byte
	offset 	uint64
}

func (m *IoMgr) Read(ctx context.Context, fd int, ops []ReadOp) IoResult {
	opcnt := len(ops)
	if opcnt > RING_ENTRIES {
		return IoResult{ Err: unix.E2BIG }
	}

	select {
	case m.ringMu <- struct{}{}:
	case <- ctx.Done():
		return ctxresp(ctx)
	}

	id := m.reqId
	m.reqId++

	infl := &m.inFlight[id % RING_ENTRIES]
	infl.reinit(id, int32(opcnt))

	for i := range ops {
		m.inFlightSem <- struct{}{}
		sqe := m.ring.GetSQE()
		if sqe == nil {
			m.log.Warn("sqe from GetSQE was nil - i thought this wouldnt happen")
			runtime.Gosched()
			continue
		} 

		op := &ops[i]

		/*
		sqe.PrepareReadFixed(
			fd,
			uintptr(unsafe.Pointer(&op.buf[0])),
			uint32(len(op.buf)),
			op.offset,
			0, // reminder: this is the one iovec entry passed in when we registered the ring
		)
		*/
		sqe.PrepareRead(
			fd,
			uintptr(unsafe.Pointer(&op.buf[0])),
			uint32(len(op.buf)),
			op.offset,
		)

		sqe.UserData = id

		if i < opcnt - 1 { // don't link the last entry
			sqe.Flags |= giouring.SqeIOLink
		}
	}

	_, err := m.ring.Submit()
	if err != nil {
		// honestly were just dead if this happens
		m.log.Error("Iomgr", "err", err)
		panic("Nooo! What have they done to my IO_URING!!!")
	}

	<- m.ringMu

	select {
	case res := <- infl.ch: 	
		return res
	case <- ctx.Done():	
		return ctxresp(ctx)
	}
}

func (m *IoMgr) Nop(ctx context.Context, opcnt int) IoResult {

	if opcnt > RING_ENTRIES {
		return IoResult{ Err: unix.E2BIG }
	}

	select {
	case m.ringMu <- struct{}{}:
	case <- ctx.Done():
		return ctxresp(ctx)
	}

	id := m.reqId
	m.reqId++

	infl := &m.inFlight[id % RING_ENTRIES]
	infl.reinit(id, int32(opcnt))

	for i := range opcnt {
		m.inFlightSem <- struct{}{}
		sqe := m.ring.GetSQE()
		if sqe == nil {
			m.log.Warn("sqe from GetSQE was nil - i thought this wouldnt happen")
			runtime.Gosched()
			continue
		} 

		sqe.PrepareNop()
		sqe.UserData = id

		if i < opcnt - 1 { // don't link the last entry
			sqe.Flags |= giouring.SqeIOLink
		}
	}

	_, err := m.ring.Submit()
	if err != nil {
		// honestly were just dead if this happens
		m.log.Error("Iomgr", "err", err)
		panic("Nooo! What have they done to my IO_URING!!!")
	}

	<- m.ringMu

	select {
	case res := <- infl.ch: 	
		return res
	case <- ctx.Done():	
		return ctxresp(ctx)
	}
}

func (m *IoMgr) Fallocate(ctx context.Context, fd int, offset uint64, length uint64) IoResult {
	if offset % ALIGN != 0 {
		m.log.Error("Can't extend file at non-aligned offset", "reqd", offset, "align", ALIGN)
		return IoResult{ Err: unix.EINVAL }
	}
	if length % ALIGN != 0 {
		m.log.Error("Can't extend file by non-aligned length", "reqd", length, "align", ALIGN)
		return IoResult{ Err: unix.EINVAL }
	}

	select {
	case m.ringMu <- struct{}{}:
	case <- ctx.Done():
		return ctxresp(ctx)
	}

	id := m.reqId
	m.reqId++

	infl := &m.inFlight[id % RING_ENTRIES]
	infl.reinit(id, 1)

	var sqe *giouring.SubmissionQueueEntry
	for sqe == nil {
		m.inFlightSem <- struct{}{}
		sqe = m.ring.GetSQE()
		if sqe == nil {
			m.log.Warn("sqe from GetSQE was nil - i thought this wouldnt happen")
			runtime.Gosched()
		} else {
			break
		}
	}

	const MODE = 0
	sqe.PrepareFallocate(fd, MODE, offset, length,)

	sqe.UserData = id

	_, err := m.ring.Submit()
	if err != nil {
		// honestly were just dead if this happens
		m.log.Error("Iomgr", "err", err)
		panic("Nooo! What have they done to my IO_URING!!!")
	}

	<- m.ringMu

	select {
	case res := <- infl.ch: 	
		return res
	case <- ctx.Done():	
		return ctxresp(ctx)
	}
}

// TODO: Ftruncate isnt in giouring, it will be easy to add but i have to
func (m *IoMgr) Ftruncate(ctx context.Context, fd int, length uint64) IoResult {
	panic("Not implemented")
}

func (m *IoMgr) Fsync(ctx context.Context, fd int) IoResult {
	select {
	case m.ringMu <- struct{}{}:
	case <- ctx.Done():
		return ctxresp(ctx)
	}

	id := m.reqId
	m.reqId++

	infl := &m.inFlight[id % RING_ENTRIES]
	infl.reinit(id, 1)

	var sqe *giouring.SubmissionQueueEntry
	for sqe == nil {
		m.inFlightSem <- struct{}{}
		sqe = m.ring.GetSQE()
		if sqe == nil {
			m.log.Warn("sqe from GetSQE was nil - i thought this wouldnt happen")
			runtime.Gosched()
		} else {
			break
		}
	}

	const FLAGS = 0
	sqe.PrepareFsync(fd, FLAGS)

	sqe.UserData = id

	_, err := m.ring.Submit()
	if err != nil {
		// honestly were just dead if this happens
		m.log.Error("Iomgr", "err", err)
		panic("Nooo! What have they done to my IO_URING!!!")
	}

	<- m.ringMu

	select {
	case res := <- infl.ch: 	
		return res
	case <- ctx.Done():	
		return ctxresp(ctx)
	}
}
