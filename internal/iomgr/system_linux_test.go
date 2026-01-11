//go:build linux
package iomgr

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/lmittmann/tint"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
)

const SLAB_MIN = 0x1000
const PAGE_SIZE = 0x1000

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
		AddSource: true,
	})))
}

func tempfile(t *testing.T) string {
	dir := t.TempDir()
	return filepath.Join(dir, fmt.Sprintf("moootest%016x.moo", rand.Uint64()))
}

func Test_Env_odirectandmmapalign(t *testing.T) {
	pageSize := os.Getpagesize()
	t.Log("Pagesize", pageSize)
	path := "odirect_probe.tmp"
	
	f, err := os.OpenFile(path, unix.O_RDWR|unix.O_CREAT|unix.O_TRUNC|unix.O_DIRECT, F_OPEN_PERM)
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

func Test_Op_Size(t *testing.T) {
	assert.Equal(t, unsafe.Sizeof(Op{}), OP_SIZE)
}

func tTest_Iomgr_Just_Writes(t *testing.T) {
	const BUFSIZE = PAGE_SIZE * 24
	slab, err := AllocSlab(BUFSIZE) 
	if err != nil { t.Fatal(err) }
	defer DeallocSlab(slab)

	slab2, err := AllocSlab(SLAB_MIN) 
	if err != nil { t.Fatal(err) }
	defer DeallocSlab(slab2)

	const OPCNT = SLAB_MIN / OP_SIZE
	ops := unsafe.Slice((*Op)(unsafe.Pointer(&slab2[0])), OPCNT)

	fp := tempfile(t)
	iomgr, err := CreateIoMgr(fp)
	if err != nil { t.Fatal(err) }
	defer iomgr.Close()

	buf := slab[:]
	for i := range len(buf) {
		buf[i] = uint8(i%256)
	}

	// temp
	ops[0].Ch = make(chan struct{})

	const CNT = BUFSIZE / PAGE_SIZE

	ops[0].Opcode 	= OpWrite
	ops[0].Count 	= CNT
	for i := range CNT {
		ops[0].Bufs[i] 	= uintptr(unsafe.Pointer(&buf[0])) + uintptr(PAGE_SIZE * i)
		ops[0].Lens[i] 	= uint32(PAGE_SIZE)
		ops[0].Offs[i] 	= uint64(PAGE_SIZE * i)
		ops[0].Sync 	= true
	}

	iomgr.Submit(&ops[0])

	<- ops[0].Ch

	data, err := os.ReadFile(fp)
	if err != nil {
		t.Fatal(err)
	}

	if ops[0].Res < 0 {
		t.Fatal("Result Err", ops[0].Res)
	}

	if !slices.Equal(data, buf) {
		t.Fatal("read-back data didnt match", data[0:16], buf[0:16])
	}
}

func tTest_Iomgr_Writes_Reads(t *testing.T) {
	const BUFSIZE = uintptr(PAGE_SIZE * OP_MAX_OPS)
	slab, err := AllocSlab(int(BUFSIZE * 2)) 
	if err != nil {
		t.Fatal(err)
	}
	defer DeallocSlab(slab)

	slab2, err := AllocSlab(SLAB_MIN) 
	if err != nil {
		t.Fatal(err)
	}
	defer DeallocSlab(slab2)
	const OPCNT = SLAB_MIN / OP_SIZE
	ops := unsafe.Slice((*Op)(unsafe.Pointer(&slab2[0])), OPCNT)

	fp := tempfile(t)

	iomgr, err := CreateIoMgr(fp)
	if err != nil {
		t.Fatal(err)
	}
	defer iomgr.Close()

	fillRandFast(slab[:BUFSIZE])

	// temp
	ops[0].Ch = make(chan struct{})

	const CNT = BUFSIZE / PAGE_SIZE


	ops[0].Opcode 	= OpWrite
	ops[0].Count 	= uint16(CNT)
	for i := range CNT {
		ops[0].Bufs[i] 	= uintptr(unsafe.Pointer(&slab[0])) + uintptr(PAGE_SIZE * i)
		ops[0].Lens[i] 	= uint32(PAGE_SIZE)
		ops[0].Offs[i] 	= uint64(PAGE_SIZE * i)
	}

	iomgr.Submit(&ops[0])

	<- ops[0].Ch

	ops[0].Opcode 	= OpRead
	ops[0].Count 	= uint16(CNT)
	for i := range CNT {
		ops[0].Bufs[i] 	= uintptr(unsafe.Pointer(&slab[BUFSIZE])) + uintptr(PAGE_SIZE * i)
		ops[0].Lens[i] 	= uint32(PAGE_SIZE)
		ops[0].Offs[i] 	= uint64(PAGE_SIZE * i)
	}

	iomgr.Submit(&ops[0])

	<- ops[0].Ch

	if ops[0].Res < 0 {
		t.Fatal("Result Err", ops[0].Res)
	}

	if !slices.Equal(slab[:BUFSIZE], slab[BUFSIZE:]) {
		t.Fatal("read-back data didnt match", slab[:16], slab[BUFSIZE:BUFSIZE+16])
	}
}

func Test_Iomgr_Multi_Worker_Drifting(t *testing.T) {
	const WORKERS = 4
	const BATCHES_PER_WORKER = 64
	const BUFSIZE = uintptr(PAGE_SIZE * OP_MAX_OPS * WORKERS * BATCHES_PER_WORKER)
	slab, err := AllocSlab(int(BUFSIZE * 2)) 
	if err != nil { t.Fatal(err) }
	defer DeallocSlab(slab)


	slab2, err := AllocSlab(0x100000)
	if err != nil { t.Fatal(err) }
	defer DeallocSlab(slab2)

	// make sure opcnt = workers
	ops := unsafe.Slice((*Op)(unsafe.Pointer(&slab2[0])), WORKERS)

	// NOTE: we can have a worker just "own" an op struct, and have a fixed amount of workers
	// or just shard them or something

	fp := tempfile(t)

	iomgr, err := CreateIoMgr(fp)
	if err != nil {
		t.Fatal(err)
	}
	defer iomgr.Close()

	fillRandFast(slab[:BUFSIZE])

	for w := range WORKERS {
		ops[w].Ch = make(chan struct{})
	}

	var wg sync.WaitGroup

	const WORKER_BUF_LEN = BUFSIZE / WORKERS
	const CNT = OP_MAX_OPS

	for wIndex := range WORKERS {
		wg.Add(1)

		go func(w int) {
			defer wg.Done()
			bufOff := uintptr(unsafe.Pointer(&slab[0]))
			workerBase := WORKER_BUF_LEN * uintptr(w)
			for b := range BATCHES_PER_WORKER {
				ops[w].Opcode 	= OpWrite
				ops[w].Count 	= uint16(CNT)
				batchBase := workerBase + (PAGE_SIZE * CNT * uintptr(b))
				for i := range CNT {
					ops[w].Bufs[i] 	= batchBase + (PAGE_SIZE * uintptr(i)) + bufOff
					ops[w].Lens[i] 	= uint32(PAGE_SIZE)
					ops[w].Offs[i] 	= uint64(batchBase) + (PAGE_SIZE * uint64(i))
					ops[w].Sync 	= false
				}
				iomgr.Submit(&ops[w])
				<- ops[w].Ch
			}
		}(wIndex)
	}

	wg.Wait()

	for wIndex := range WORKERS {
		wg.Add(1)

		go func(w int) {
			defer wg.Done()
			bufOff := uintptr(unsafe.Pointer(&slab[0])) + BUFSIZE
			workerBase := WORKER_BUF_LEN * uintptr(w)
			for b := range BATCHES_PER_WORKER {
				ops[w].Opcode 	= OpRead
				ops[w].Count 	= uint16(CNT)
				batchBase := workerBase + (PAGE_SIZE * CNT * uintptr(b))
				for i := range CNT {
					ops[w].Bufs[i] 	= batchBase + (PAGE_SIZE * uintptr(i)) + bufOff
					ops[w].Lens[i] 	= uint32(PAGE_SIZE)
					ops[w].Offs[i] 	= uint64(batchBase) + (PAGE_SIZE * uint64(i))
				}
				iomgr.Submit(&ops[w])
				<- ops[w].Ch
			}
		}(wIndex)
	}

	wg.Wait()

	if ops[0].Res < 0 {
		t.Fatal("Result Err", ops[0].Res)
	}

	if !slices.Equal(slab[:BUFSIZE], slab[BUFSIZE:]) {
		t.Fatal(
			"read-back data didnt match", 
			"\nheads:", 0, "..", 16 ,"\n",
			slab[:16], "\n", slab[BUFSIZE:BUFSIZE+16], 
			"\ntails:", BUFSIZE-16, "..", BUFSIZE, "\n",
			slab[BUFSIZE-16:BUFSIZE], "\n", slab[BUFSIZE*2-16:BUFSIZE*2],
		)
	}
}
