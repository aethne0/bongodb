package pager

import (
	"fmt"
	"math/rand"
	c "mooodb/internal"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func tempfile(t *testing.T) string {
	dir := t.TempDir()
	return filepath.Join(dir, fmt.Sprintf("moootest%016x.moo", rand.Uint64()))
}

func Test_Pager_None_Free(t *testing.T) {
	const COUNT = 8
	pager, err := CreatePager(tempfile(t), COUNT)
	assert.NoError(t, err)
	if err != nil { t.Fatal() }

	for i := range COUNT {
		f := pager.CreatePage()
		assert.NotNil(t, f)
		assert.Equal(t, f.pageId, uint64(i + 1))
	}

	f := pager.CreatePage()
	assert.Nil(t, f)

	pager.Close()
}

func Test_Pager_Main(t *testing.T) {
	pager, err := CreatePager(tempfile(t), 16)
	assert.NoError(t, err)
	if err != nil { t.Fatal() }

	f1 := pager.CreatePage()
	pageId := f1.pageId

	assert.Equal(t, pageId, uint64(1))

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

	assert.Equal(t, f1.frameId, f2.frameId)
	assert.Equal(t, f1.frameId, f3.frameId)

	for i := range f2.data {
		assert.Equal(t, f2.data[i], byte(i))
	}

	pager.Close()
}

func Test_Pager_Multiread(t *testing.T) {
	fp := tempfile(t)

	// pre-populate
	// root page is page 0, so we need to populate 0-8 * pagesize
	// (were reading 1-8)
	data := make([]byte, c.PAGE_SIZE * 9)
	for i := range data {
		data[i] = byte(rand.Uint32())
	}
	err := os.WriteFile(fp, data, 0644)
	assert.NoError(t, err)
	if err != nil { t.Fatal() }

	pager, err := CreatePager(fp, 8)
	assert.NoError(t, err)
	if err != nil { t.Fatal() }

	frames := make([]*Frame, 8)
	for i := range frames {
		frames[i] = pager.GetPage(uint64(i+1))
	}
	for i := range frames {
		fop := &frames[i].diskOp
		<- fop.Ch
		assert.GreaterOrEqual(t, fop.Res, int32(0))
		assert.Equal(t, c.PAGE_SIZE, int(fop.Res))
	}

	// seperate worker
	frames = make([]*Frame, 8)
	for i := range frames {
		frames[i] = pager.GetPage(uint64(i+1))
	}
	for i := range frames {
		fop := &frames[i].diskOp
		<- fop.Ch
		assert.GreaterOrEqual(t, fop.Res, int32(0))
		assert.Equal(t, c.PAGE_SIZE, int(fop.Res))
	}

	pager.Close()
}

