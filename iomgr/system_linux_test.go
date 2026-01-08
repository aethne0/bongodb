package iomgr

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"golang.org/x/sys/unix"
)

func tempfile(t *testing.T) (int, string) {
	dir := t.TempDir()
	fp := filepath.Join(dir, fmt.Sprintf("testfile%016x.moo", rand.Uint64()))
	fd, err := unix.Open(fp, F_OPEN_MODE | unix.O_EXCL, F_OPEN_PERM)
	if err != nil {
		t.Fatal(err)
	}
	return fd, fp

}

/*
func Test_iouring_eventfd_batch_link_fsync(t *testing.T) {
	fd, fp := tempfile(t)

	ringsize 	:= uint32(64)
	batchsize 	:= uint32(8)

	t.Log("Will try to create ring...")
	ring, err := giouring.CreateRing(ringsize)
	if err != nil { t.Fatal(err) }
	// defer ring.QueueExit()

	//cqebuff := make([]*giouring.CompletionQueueEvent, batchsize)

	for j := range batchsize {
		i := j % 2
		sqe := ring.GetSQE()
		if sqe == nil { t.Fatal("SQE was nil - SQ was full?") }

		// try to write these 8 bytes to the start of the file
		buf := []byte("1111222233334444")
		rawptr := uintptr(unsafe.Pointer(unsafe.SliceData(buf[i*8:i*8+1])))
		sqe.PrepareWrite(fd, rawptr, 8, uint64(8*j))
		sqe.Flags |= giouring.SqeIOLink
	}

	efd, err := unix.Eventfd(0, 0)
	if err != nil { t.Fatal(err) }

	_, err = ring.RegisterEventFd(efd)
	if err != nil { t.Fatal(err) }

	submitted, err := ring.Submit()
	if err != nil { t.Fatal(err) }
	if batchsize != uint32(submitted) {
		t.Fatal("batchsize != submitted", batchsize, submitted)
	}

	// trying to fsync
	sqe := ring.GetSQE()
	if sqe == nil { t.Fatal("sq full i think - not an err") }
	sqe.PrepareFsync(fd, 0)
	t.Log("Submitted fsync | drain")
	submitted, err = ring.Submit()
	if err != nil { t.Fatal(err) }
	if submitted != 1 { t.Fatal("batchsize != submitted", batchsize, submitted) }

	readloop: for {
		t.Log("waiting on eventfd")
		var efdbuf [8]byte
		n, err := unix.Read(efd, efdbuf[:])
		t.Log("woke on eventfd")
		if err != nil { t.Fatal(err) }
		t.Log("eventfd: ", n)

		for {
			cqe, err := ring.PeekCQE()
			if err != nil && err != unix.EAGAIN { t.Fatal(err) }
			if cqe != nil {
				t.Logf("cqe: %+v\n", cqe)
				ring.CQESeen(cqe)
			} else {
				t.Log("cq empty")
				break readloop
			}
		}
	}

	// Done with io_uring
	ring.QueueExit()

	// read back what we wrote

	data, err := os.ReadFile(fp)
	if err != nil {
		panic(err)
	}

	// Print raw decimal bytes
	if string(data) != strings.Repeat("1111222233334444",4) {
		t.Fatal("read data doesnt match written")
	}
}
*/

func Test_Iomgr(t *testing.T) {
	slab, err := AllocSlab(0x1000) 
	if err != nil {
		t.Fatal(err)
	}

	iomgr, err := CreateIomgr()
	if err != nil {
		t.Fatal(err)
	}

	fd, fp := tempfile(t)
	buf := slab[0:64]
	for i := range 64 {
		buf[i] = uint8(i)
	}

	wop := []Writeop{ {
		offset: 0,
		buf: buf,
	} }

	ch := iomgr.Write(fd, wop, true)

	res := <- ch


	data, err := os.ReadFile(fp)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("err", res.Err)
	t.Log("data", data)

	if !slices.Equal(data, buf) {
		t.Fatal("read-back data didnt match", data, buf)
	}
}
