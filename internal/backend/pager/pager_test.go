package pager

import (
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lmittmann/tint"
)

func TestMain(t *testing.T) {
	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{
		Level:      slog.LevelDebug,
		TimeFormat: time.TimeOnly,
		AddSource: true,
	})))
}

func tempfile() string {
	return "/xblk/test/test.moo"
}

func Test_Pager_Read(t *testing.T) {
	pager, err := CreatePagebuf(tempfile())
	if err != nil { t.Fatal(err) }

	pages := []uint64{1, 2, 4, 8, 16, 32}
	view, err := pager.ReadPages(pages)
	if err != nil { panic(err) }
	pager.ReleaseView(view)

	for i := range pager.frames {
		f := &pager.frames[i]
		if f.dirty {
			t.Error("read page got marked dirty")
		}
		if f.pins.Load() != 0 {
			t.Error("pins should be 0 after free -", "findex", f._index, 
				"pageid", f.pageid, "pins", f.pins.Load())
		}
	}
}

func Test_Pager_Make(t *testing.T) {
	pager, err := CreatePagebuf(tempfile())
	if err != nil { t.Fatal(err) }

	view, err := pager.MakePages(4, true)
	if err != nil { panic(err) }

	for i := range view.Cnt {
		pr := &view.Prs[i]
		f := &pager.frames[pr.frameIndex]
		for _, v := range pr.Data {
			if v != 0 {
				t.Error("data not zeroed")
				break
			}
		}
		if !f.dirty {
			t.Error("new page should be dirty - is not")
		}
	}

	pager.ReleaseView(view)

	for i := range pager.frames {
		f := &pager.frames[i]
		if f.pins.Load() != 0 {
			t.Error("pins should be 0 after free -", "findex", f._index, 
				"pageid", f.pageid, "pins", f.pins.Load())
		}
	}
}

func tTest_Pager_Read_Singleflight(t *testing.T) {
	pager, err := CreatePagebuf(tempfile())
	if err != nil { t.Fatal(err) }

	wg1 := sync.WaitGroup{}
	wg1.Add(2)
	wg2 := sync.WaitGroup{}
	wg2.Add(1)
	wg3 := sync.WaitGroup{}
	wg3.Add(2)

	go func() {
		pages := []uint64{1, 2, 4, 8, 16, 32}
		view, err := pager.ReadPages(pages)
		if err != nil { panic(err) }

		wg1.Done()
		wg2.Wait()
		pager.ReleaseView(view)
		wg3.Done()
	}()

	go func() {
		pages := []uint64{1, 2, 3, 4, 9, 16}
		view, err := pager.ReadPages(pages)
		if err != nil { panic(err) }

		wg1.Done()
		wg2.Wait()
		pager.ReleaseView(view)
		wg3.Done()
	}()

	wg1.Wait()

	// TODO: check to see we didnt double load pages

	wg2.Done()
	wg3.Wait()
}
