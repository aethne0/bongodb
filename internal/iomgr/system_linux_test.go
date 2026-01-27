//go:build linux

package iomgr

import (
	c "mooodb/internal"
	"path/filepath"

	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/lmittmann/tint"
	"golang.org/x/sys/unix"
)

const SLAB_MIN = 0x1000

func fillRandFast(buf []byte) {
	numCPUs := runtime.NumCPU()
	chunkSize := (len(buf) / 8 / numCPUs) * 8
	var wg sync.WaitGroup

	for i := range numCPUs {
		wg.Add(1)
		start := i * chunkSize
		end := start + chunkSize
		if i == numCPUs-1 {
			end = len(buf)
		}

		go func(subSlab []byte) {
			defer wg.Done()
			src := rand.NewPCG(uint64(time.Now().UnixNano()), uint64(i))
			r := rand.New(src)
			data := unsafe.Slice((*uint64)(unsafe.Pointer(&subSlab[0])), len(subSlab)/8)
			for j := range data {
				data[j] = r.Uint64()
			}
		}(buf[start:end])
	}
	wg.Wait()
}

func TestMain(t *testing.T) {
	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: time.TimeOnly,
		AddSource:  true,
	})))
}

func tempfile(t *testing.T) string {
	dir := t.TempDir()
	return filepath.Join(dir, fmt.Sprintf("moootest%016x.moo", rand.Uint64()))
	//return "/xblk/test/test.moo"
}

func Test_Env_O_DIRECT_And_Mmap_Align(t *testing.T) {
	pageSize := os.Getpagesize()
	t.Log("Pagesize", pageSize)
	path := "odirect_probe.tmp"

	f, err := os.OpenFile(path, unix.O_RDWR|unix.O_CREAT|unix.O_DIRECT, F_OPEN_PERM)
	if err != nil {
		t.Errorf("O_DIRECT open not supported: %v (likely tmpfs or virtualized FS)", err)
		return
	}
	defer os.Remove(path)
	defer f.Close()

	buf, err := unix.Mmap(-1, 0, pageSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		t.Fatalf("mmap failed: %v", err)
	}
	defer unix.Munmap(buf)

	n, err := unix.Pwrite(int(f.Fd()), buf, 0)
	if err != nil {
		t.Errorf("O_DIRECT write failed even with aligned memory: %v", err)
		t.Logf("This confirms the filesystem/environment rejects Direct I/O.")
	} else if n != pageSize {
		t.Errorf("Short write: expected %d, got %d", pageSize, n)
	} else {
		t.Log("O_DIRECT is fully supported and aligned.")
	}
}

func Test_Iomgr_Multi_Worker_Drifting(t *testing.T) {
	const WORKERS = 2
	const OPS_PER_WORKER = 2
	const BUFSIZE = uintptr(c.PAGE_SIZE * WORKERS * OPS_PER_WORKER)
	slab, err := AllocSlab(int(BUFSIZE * 2))
	if err != nil {
		t.Fatal(err)
	}
	defer DeallocSlab(slab)

	fp := tempfile(t)

	iomgr, err := CreateIoMgr(fp)
	if err != nil {
		t.Fatal(err)
	}
	defer iomgr.Close()

	fillRandFast(slab[:BUFSIZE])

	workerStuff := make([]DiskOp, WORKERS)

	var wg sync.WaitGroup

	const WORKER_BUF_LEN = BUFSIZE / WORKERS

	for wIndex := range WORKERS {
		wg.Add(1)

		go func(w int) {
			workerBase := WORKER_BUF_LEN * uintptr(w)
			op := &workerStuff[w]

			for opi := range OPS_PER_WORKER {
				opBase := workerBase + (c.PAGE_SIZE * uintptr(opi))
				// This merely populates the DiskOp struct
				op.PrepareOpSlice(OpWrite, slab[opBase:], uint64(opBase))
				// we need to allocate the channel, that is our job
				// This actually submits the DiskOp
				iomgr.OpQueue <- op
				<- op.Ch
			}

			wg.Done()
		}(wIndex)
	}

	wg.Wait()

	for wIndex := range WORKERS {
		wg.Add(1)

		go func(w int) {
			workerBase := WORKER_BUF_LEN * uintptr(w)
			op := &workerStuff[w]
			for opi := range OPS_PER_WORKER {
				opBase := workerBase + (c.PAGE_SIZE * uintptr(opi))
				op.PrepareOpSlice(OpRead, slab[opBase+BUFSIZE:], uint64(opBase))
				iomgr.OpQueue <- op
				<- op.Ch
			}

			wg.Done()
		}(wIndex)
	}

	wg.Wait()

	if !slices.Equal(slab[:BUFSIZE], slab[BUFSIZE:]) {
		t.Fatal(
			"read-back data didnt match",
			"\nheads:", 0, "..", 16, "\n",
			slab[:16], "\n", slab[BUFSIZE:BUFSIZE+16],
			"\ntails:", BUFSIZE-16, "..", BUFSIZE, "\n",
			slab[BUFSIZE-16:BUFSIZE], "\n", slab[BUFSIZE*2-16:BUFSIZE*2],
		)
	}
}
