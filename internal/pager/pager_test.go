package pager

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

func tempfile(t *testing.T) string {
	dir := t.TempDir()
	return filepath.Join(dir, fmt.Sprintf("moootest%016x.moo", rand.Uint64()))
}

func Test_Pager_Main(t *testing.T) {
	pager, err := CreatePager()
	if err != nil {
		t.Fatal(err)
	}

	f1 := pager.CreatePage()
	pageId := f1.pageId

	if pageId != 1 {
		t.Error("wrong pageId, should be 1", "pageId", pageId)
	}

	for i := range f1.data {
		f1.data[i] = byte(i)
	}

	pager.WritePage(f1)
	<- f1.diskOp.Ch
	f1.Release()

	// should be same frame
	// ""two different threads""
	f2 := pager.GetPage(1)
	f3 := pager.GetPage(1)
	<- f2.diskOp.Ch
	<- f3.diskOp.Ch

	if f2.frameId != f1.frameId || f3.frameId != f1.frameId {
		t.Fatal("should have fetched paged in frame")
	}

	for i := range f2.data {
		if f2.data[i] != byte(i) {
			t.Fatal("unexpected data read back", "got", f2.data[i], "expected", byte(i))
		}
	}

	f2.Release()
	f3.Release()
	pager.Close()
}
