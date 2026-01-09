package iomgr

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/lmittmann/tint"
	"golang.org/x/sys/unix"
)

const SLAB_MIN = 0x1000

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

func Test_Iomgr_Writes(t *testing.T) {
	slab, err := AllocSlab(SLAB_MIN) 
	if err != nil {
		t.Fatal(err)
	}

	iomgr, err := CreateIoMgr(slab)
	if err != nil {
		t.Fatal(err)
	}

	fd, fp := tempfile(t)
	buf := slab[0:ALIGN]
	for i := range len(buf) {
		buf[i] = uint8(i%256)
	}

	wop := []WriteOp{ {
		offset: 0,
		buf: buf,
	} }

	res := iomgr.Write(context.Background(), fd, wop, true)

	data, err := os.ReadFile(fp)
	if err != nil {
		t.Fatal(err)
	}

	if res.Err != nil {
		t.Fatal("Result Err", res.Err)
	}

	if !slices.Equal(data, buf) {
		t.Fatal("read-back data didnt match", data[0:16], buf[0:16])
	}
}

func Test_Iomgr_Nops(t *testing.T) {
	slab, err := AllocSlab(SLAB_MIN)  // iomgr needs slab to setup, even if we dont use it
	if err != nil {
		t.Fatal(err)
	}

	iomgr, err := CreateIoMgr(slab)
	if err != nil {
		t.Fatal(err)
	}

	const TOTAL = 0x1000
	const BATCHSIZE = 0x40
	for range TOTAL / BATCHSIZE {
		res := iomgr.Nop(context.Background(), BATCHSIZE)
		if res.Err != nil {
			t.Fatal(res.Err)
		}
	}
}

func Test_Iomgr_WritesReads(t *testing.T) {
	const size 		= 0x400000 // 4MiB
	const half		= size / 2
	const pagesize  = ALIGN
	const batchsize = 16
	const batches 	= half / (pagesize * batchsize)
	if batches == 0 {
		t.Fatal("your batch size doesnt make sense (size is too small basically)")
	}

	slab, err := AllocSlab(size) 
	if err != nil {
		t.Fatal(err)
	}

	iomgr, err := CreateIoMgr(slab)
	if err != nil {
		t.Fatal(err)
	}

	fd, _ := tempfile(t)

	for i := range half {
		slab[i] = byte(rand.Uint32())
	}

	for b := range batches {
		wrops := make([]WriteOp, batchsize)
		for i := range batchsize {
			bufstart := uint64(b) * pagesize * batchsize + uint64(i) * pagesize
			wrops[i].buf = slab[bufstart:bufstart+pagesize]
			wrops[i].offset = uint64(bufstart)
		}
		res := iomgr.Write(context.Background(), fd, wrops, true)
		if res.Err != nil {
			t.Fatal("Result Err", res.Err)
		}
	}

	for b := range batches {
		rdops := make([]ReadOp, batchsize)
		for i := range batchsize {
			bufstart := uint64(b) * pagesize * batchsize + uint64(i) * pagesize
			rdops[i].buf = slab[half+bufstart:half+bufstart+pagesize]
			rdops[i].offset = uint64(bufstart)
		}
		res := iomgr.Read(context.Background(), fd, rdops)
		if res.Err != nil {
			t.Fatal("Result Err", res.Err)
		}
	}

	if !slices.Equal(slab[0:half], slab[half:size]) {
		t.Fatal("read-back data didnt match", slab[0:16], slab[half:half+16])
	}
}

func Test_Iomgr_Fallocate(t *testing.T) {
	slab, err := AllocSlab(SLAB_MIN) 
	if err != nil {
		t.Fatal(err)
	}

	iomgr, err := CreateIoMgr(slab)
	if err != nil {
		t.Fatal(err)
	}

	fd, fp := tempfile(t)
	data, err := os.Stat(fp)
	if err != nil {
		t.Fatal(err)
	}
	ogsize := data.Size()

	increase := ALIGN * 16
	res := iomgr.Fallocate(context.Background(), fd, uint64(ogsize), increase)

	data, err = os.Stat(fp)
	if err != nil {
		t.Fatal(err)
	}

	if res.Err != nil {
		t.Fatal("Result Err", res.Err)
	}

	if data.Size() != ogsize + int64(increase) {
		t.Fatal("File unexpected size", "size", data.Size(), "expected", ogsize + int64(increase))
	}
}

func Test_Iomgr_Fsync(t *testing.T) {
	slab, err := AllocSlab(SLAB_MIN) 
	if err != nil {
		t.Fatal(err)
	}

	iomgr, err := CreateIoMgr(slab)
	if err != nil {
		t.Fatal(err)
	}

	fd, _ := tempfile(t)
	buf := slab[0:ALIGN]
	for i := range len(buf) {
		buf[i] = uint8(i%256)
	}

	wop := []WriteOp{ {
		offset: 0,
		buf: buf,
	} }

	iomgr.Write(context.Background(), fd, wop, true)

	res := iomgr.Fsync(context.Background(), fd)

	if res.Err != nil {
		t.Fatal("Result Err", res.Err)
	}

}


