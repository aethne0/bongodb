//go:build linux

package iomgr

import (
	"log/slog"
	"runtime"
	"sync"
	"unsafe"

	"github.com/aethne0/giouring"
	"golang.org/x/sys/unix"
)

const MMAP_MODE   	= unix.MAP_ANON  | unix.MAP_PRIVATE
const MMAP_PROT   	= unix.PROT_READ | unix.PROT_WRITE
const F_OPEN_MODE 	= unix.O_RDWR | unix.O_CREAT | unix.O_DIRECT | unix.O_DSYNC
const F_OPEN_PERM 	= 0b_000_110_100_000
const RING_ENTRIES 	= 0x1000

// For page buffer - not for io_uring, liburing handles mmap-ing for io_uring setup.
func AllocSlab(size int) ([]byte, error) {
	// TODO: make aligned (or check how mmap aligns)
	// I know you can specify an address (seemingly haphazardly) to mmap but idk if you 
	// can specify an alignment
	raw, err := unix.Mmap(-1,
		0, int(size), 
		unix.PROT_READ | unix.PROT_WRITE,
		unix.MAP_ANON  | unix.MAP_PRIVATE,
	) 

	return raw, err
}

func DeallocSlab(ptr []byte) error {
	err := unix.Munmap(ptr)
	return err
}

type Iomgr struct {
	ring 		*giouring.Ring
	ringmu		sync.Mutex

	inflt		map[uint64]*inflight	// in-flight req channels
	infltmu		sync.Mutex  			// in-flight limit semaphore

	reqid		uint64

	eventfd		int
}

// NOTE: 	thread-safety: only one thread is polling eventfd and reading CQEs
// 			Many threads may be calling GetSQE, submit, etc

// TODO:
// 			preallocate waits
// 			preallocate req structs
// 			semaphore waits (max in flight)
// 			maybe some batching

func CreateIomgr() (*Iomgr ,error) {
	ring, err := giouring.CreateRing(RING_ENTRIES)
	if err != nil { return nil, err }

	eventfd, err := unix.Eventfd(0, 0)
	if err != nil { return nil, err }
	_, err = ring.RegisterEventFd(eventfd)
	if err != nil { return nil, err }

	iomgr := Iomgr {
		ring: ring,
		ringmu: sync.Mutex{}, // this is only for submissions

		// TODO: uh oh ALLOCATIONS - this should just be a ring buffer
		// well improve this whole part, needs inflight limits too
		inflt: 	 make(map[uint64]*inflight), 
		infltmu: sync.Mutex{},

		reqid: 	 0,

		eventfd: eventfd,
	}

	go iomgr.epoller()
	return &iomgr, nil
}

func (m *Iomgr) Close() {
	m.ring.QueueExit()
}

func (m *Iomgr) epoller() {
	// NOTE: epoller is the only thread that touches cq
	var efdbuf [8]byte

	for {
		_, err := unix.Read(m.eventfd, efdbuf[:])
		if err != nil {
			slog.Error("Iomgr epoller", "err", err)
			panic("epoller had error reading from eventfd")
		}

		// then we process CQEs
		CQES: for {
			cqe, err := m.ring.PeekCQE()

			switch err {
			case nil:
				// continue
			case unix.EAGAIN:
				break CQES
			default:
				slog.Error("Iomgr epoller", "err", err)
				panic(err)
			}

			if cqe != nil {
				m.ring.CQESeen(cqe)
				slog.Info("cqe seen", "userdata", cqe.UserData, "res", cqe.Res)
			}  else {
				break
			}

			id := cqe.UserData

			{
				m.infltmu.Lock()

				inflight, found := m.inflt[id]
				if !found {
					slog.Error("we didnt find this id in the inflight map - huh?")
				}
				inflight.startid++
				// TODO: probably can do this better
				delete(m.inflt, id)
				if inflight.startid == inflight.endid {
					inflight.ch <- &Iorslt{ Err: nil }
				} else {
					m.inflt[inflight.startid] = inflight
				}

				m.infltmu.Unlock()
			}
		}
	}
}

// IO result
// Do not read struct until you have read from Ch
type Iorslt struct {
	Err 		*error
}

type inflight struct {
	ch 			chan *Iorslt
	startid 	uint64
	endid 		uint64 // exclusive... single entry is [1,2] (eg)
}

type Writeop struct {
	buf 	[]byte
	offset 	uint64
}

func (m *Iomgr) Write(fd int, ops []Writeop, sync bool) chan *Iorslt {
	var sqe *giouring.SubmissionQueueEntry
	// TODO: temporary - this will eventually be handled by an in-flight semaphore
	m.ringmu.Lock()
	startid := m.reqid
	for i := range ops {
		op := &ops[i]
		// we only need to hold the ring lock to claim and increment the SQueue 
		// then we can fill it at our leisure
		// its possible that itd be better to just hold the lock to get all the 
		// ones we need, but this would either require us to:
		// 1. allocate (or have preallocd a buffer of sqe pointers we could fill
		// 2. do all our work holding the lock, blocking other threads from submitting

		sqe = m.ring.GetSQE()

		// TODO: should use writefixed, also writeVec or whatever
		sqe.PrepareWrite(
			fd,
			uintptr(unsafe.Pointer(&op.buf[0])),
			uint32(len(op.buf)),
			op.offset,
		)

		if sync || i < len(ops) - 1 { // don't link the last entry
			sqe.Flags |= giouring.SqeIOLink
		}
		sqe.UserData = m.reqid
		m.reqid++

		if sqe == nil {
			runtime.Gosched()
		} else {
			break
		}
	}

	if sync {
		sqe := m.ring.GetSQE()
		sqe.PrepareFsync(fd, 0)
		sqe.UserData = m.reqid
		m.reqid++
	}

	endid := m.reqid

	_, err := m.ring.Submit()
	if err != nil {
		// honestly we probably just panic
		slog.Error("Iomgr", "err", err)
		panic("something is wrong with our IO_URING!")
	}

	m.ringmu.Unlock()

	ch := make(chan *Iorslt, 1)
	iores := inflight {
		ch: ch,
		startid: startid,
		endid: endid,
	}
	m.infltmu.Lock()
	// will get incremented every time we receive a cqe, until startid==endid
	m.inflt[startid] = &iores
	m.infltmu.Unlock()
	return ch
}

