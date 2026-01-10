package iomgr

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
	"unsafe"

	"github.com/lmittmann/tint"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
)

const SLAB_MIN = 0x1000
const PAGE_SIZE = 0x1000

func TestMain(t *testing.T) {
	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: time.TimeOnly,
	})))
}

func tempfile(t *testing.T) (int, string) {
	dir := t.TempDir()
	fp := filepath.Join(dir, fmt.Sprintf("testfile%016x.moo", rand.Uint64()))
	fd, err := unix.Open(fp, F_OPEN_MODE | unix.O_EXCL, F_OPEN_PERM)
	if err != nil {
		t.Fatal(err)
	}
	return fd, fp

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

func Test_Iomgr_Writes(t *testing.T) {
	const BUFSIZE = PAGE_SIZE * 24
	slab, err := AllocSlab(BUFSIZE) 
	if err != nil {
		t.Fatal(err)
	}

	slab2, _ := AllocSlab(SLAB_MIN) 
	const OPCNT = SLAB_MIN / OP_SIZE
	ops := unsafe.Slice((*Op)(unsafe.Pointer(&slab2[0])), OPCNT)

	iomgr, err := CreateIoMgr()
	if err != nil {
		t.Fatal(err)
	}

	fd, fp := tempfile(t)
	buf := slab[:]
	for i := range len(buf) {
		buf[i] = uint8(i%256)
	}

	// temp
	ops[0].Ch = make(chan struct{})

	const CNT = BUFSIZE / PAGE_SIZE

	ops[0].Opcode 	= OpWrite
	ops[0].Fd 		= fd
	ops[0].Count 	= CNT
	for i := range CNT {
		ops[0].Bufs[i] 	= uintptr(unsafe.Pointer(&buf[0])) + uintptr(PAGE_SIZE * i)
		ops[0].Lens[i] 	= uint32(PAGE_SIZE)
		ops[0].Offs[i] 	= uint64(PAGE_SIZE * i)
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

func Test_Iomgr_WritesReads(t *testing.T) {
	const BUFSIZE = uintptr(PAGE_SIZE * OP_MAX_OPS)
	slab, err := AllocSlab(int(BUFSIZE * 2)) 
	if err != nil {
		t.Fatal(err)
	}

	slab2, _ := AllocSlab(SLAB_MIN) 
	const OPCNT = SLAB_MIN / OP_SIZE
	ops := unsafe.Slice((*Op)(unsafe.Pointer(&slab2[0])), OPCNT)

	iomgr, err := CreateIoMgr()
	if err != nil {
		t.Fatal(err)
	}

	for i := range BUFSIZE {
		slab[i] = uint8(i%256)
	}

	fd, _ := tempfile(t)

	// temp
	ops[0].Ch = make(chan struct{})

	const CNT = BUFSIZE / PAGE_SIZE

	ops[0].Opcode 	= OpWrite
	ops[0].Fd 		= fd
	ops[0].Count 	= uint16(CNT)
	for i := range CNT {
		ops[0].Bufs[i] 	= uintptr(unsafe.Pointer(&slab[0])) + uintptr(PAGE_SIZE * i)
		ops[0].Lens[i] 	= uint32(PAGE_SIZE)
		ops[0].Offs[i] 	= uint64(PAGE_SIZE * i)
	}

	iomgr.Submit(&ops[0])

	<- ops[0].Ch

	ops[0].Opcode 	= OpRead
	ops[0].Fd 		= fd
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
