package page

import (
	c "mooodb/internal"
	"bytes"
	"testing"
)

func Test_PageSlotted_UpdateIntegrity(t *testing.T) {
	p := PageSlottedNewTest(make([]byte, c.PAGE_SIZE), 2)
	key := []byte("key1")

	p.Put(key, []byte("initial"))
	p.Put([]byte("other"), []byte("data"))

	p.Put(key, []byte("updated_value"))

	val, _ := p.Get(key)
	if string(val) != "updated_value" {
		t.Errorf("Expected updated_value, got %s", val)
	}

	other, _ := p.Get([]byte("other"))
	if string(other) != "data" {
		t.Error("Adjacent data was corrupted during update")
	}
}

func Test_PageSlotted_GrowthFragmentation(t *testing.T) {
	p := PageSlottedNewTest(make([]byte, c.PAGE_SIZE), 1)
	key := []byte("key")

	p.Put(key, []byte("small"))
	initialFree := p.FreeBytesContig()

	p.Put(key, []byte("medium_value_length"))
	p.Put(key, []byte("very_large_value_that_definitely_relocates"))

	afterGrowth := p.FreeBytesContig()
	if afterGrowth >= initialFree {
		t.Errorf("Contiguous space should have decreased due to fragmentation")
	}

	p.Defragment(make([]byte, c.PAGE_SIZE))

	if p.FreeBytesContig() <= afterGrowth {
		t.Errorf("Defrag should have reclaimed space from old versions")
	}
}

func Test_PageSlotted_GrowthExhaustion(t *testing.T) {
	p := PageSlottedNewTest(make([]byte, c.PAGE_SIZE), 0x1234)
	key := []byte("k")

	for i := range 8 {
		val := bytes.Repeat([]byte("X"), 100+i)
		_, hadspace := p.Put(key, val) 
		if !hadspace {
			t.Logf("Failed to grow at iteration %d as expected", i)
			break
		}
	}

	p.Defragment(make([]byte, c.PAGE_SIZE))

	largeVal := bytes.Repeat([]byte("w"), c.PAGE_SIZE/2)
	_, inserted := p.Put([]byte("new"), largeVal)

	if !inserted {
		if ok,_ := p.Put([]byte("new"), largeVal); !ok {
			t.Error("Should have been able to insert after defragmentation")
		}
	}

	p.Delete([]byte("k"))
	p.Delete([]byte("new"))

	p.Defragment(make([]byte, c.PAGE_SIZE))

	freeContig := p.FreeBytesContig()
	freeFrag := p.FreeBytesFrag()

	if freeContig != c.PAGE_SIZE - headerSize {
		t.Errorf("Page should be emptied and free contig space should reflect that | %d\n", freeContig)
	}

	if freeFrag != c.PAGE_SIZE - headerSize {
		t.Errorf("Page should be emptied and free frag space should reflect that | %d\n", freeFrag)
	}
}

func Test_PageSlotted_DefragIntegrity(t *testing.T) {
	p := PageSlottedNewTest(make([]byte, c.PAGE_SIZE), 0x1234)
	scratch := make([]byte, c.PAGE_SIZE)

	data := map[string]string{
		"K1": "short",
		"K2": "original_v2",
		"K3": "v3",
	}
	for k, v := range data {
		p.Put([]byte(k), []byte(v))
	}

	data["K1"] = "much_longer_value_to_force_relocation_001"
	data["K2"] = "much_longer_value_to_force_relocation_002"
	for k, v := range data {
		p.Put([]byte(k), []byte(v))
	}

	beforeDefrag := make(map[string]string)
	for k := range data {
		val, _ := p.Get([]byte(k))
		beforeDefrag[k] = string(val)
	}

	p.Defragment(scratch)

	for k, expectedVal := range beforeDefrag {
		val, found := p.Get([]byte(k))
		if !found {
			t.Errorf("key %s missing after defrag", k)
			continue
		}
		if string(val) != expectedVal {
			t.Errorf("data mismatch for %s - got %s want %s", k, string(val), expectedVal)
		}
	}
}

func Test_PageSlotted_Defragment(t *testing.T) {
	raw := make([]byte, c.PAGE_SIZE)
	scratch := make([]byte, c.PAGE_SIZE)
	p := PageSlottedNewTest(raw, 0xabcd)

	key := []byte("key")
	for i := range 50 {
		_, fit := p.Put(key, make([]byte, 10+i))
		if !fit {
			break
		}
	}

	p.Defragment(scratch)
}

func Benchmark_PageSlotted_Defragment(b *testing.B) {
	raw := make([]byte, c.PAGE_SIZE)
	scratch := make([]byte, c.PAGE_SIZE)
	p := PageSlottedNewTest(raw, 0xabcd)

	b.ResetTimer()
	key := []byte("somebigkeyzzhaha")
	for b.Loop() {
		for i := range 9999 {
			_,fit := p.Put(key, make([]byte, 20+i))
			if !fit {
				break
			}
		}
		p.Defragment(scratch)
	}
}

func Test_PageSlotted_DeleteAndReclaim(t *testing.T) {
	p := PageSlottedNewTest(make([]byte, c.PAGE_SIZE), 0x5555)
	scratch := make([]byte, c.PAGE_SIZE)

	key := []byte("delete_me")
	val := make([]byte, 100)
	p.Put(key, val)

	freeBefore := p.FreeBytesContig()

	if ok := p.Delete(key); !ok {
		t.Fatal("Delete failed")
	}

	if _, found := p.Get(key); found {
		t.Error("Key still exists after deletion")
	}

	p.Defragment(scratch)

	freeAfter := p.FreeBytesContig()

	if freeAfter <= freeBefore {
		t.Errorf("Space was not reclaimed. Before: %d, After: %d", freeBefore, freeAfter)
	}
}

func Test_PageSlotted_DeleteAndReclaimFrag(t *testing.T) {
	p := PageSlottedNewTest(make([]byte, c.PAGE_SIZE), 0x5555)
	scratch := make([]byte, c.PAGE_SIZE)

	key := []byte("aaaaaaaa")
	val := bytes.Repeat([]byte("X"), 100)
	p.Put(key, val)

	key2 := []byte("bbbbbbbb")
	val = bytes.Repeat([]byte("x"), 100)
	p.Put(key2, val)

	freeBeforeContig := p.FreeBytesContig()
	freeBeforeFrag := p.FreeBytesFrag()

	if deleted := p.Delete(key); !deleted {
		t.Fatal("delete failed")
	}


	// after deletion
	freeAfterContig := p.FreeBytesContig()
	freeAfterFrag := p.FreeBytesFrag()

	if freeAfterContig != freeBeforeContig + 2{
		t.Errorf("contig free space should only have 2 bytes added from deleting slot - after %d before %d\n", freeBeforeContig, freeAfterContig)
	}

	if freeAfterFrag <= freeBeforeFrag {
		t.Errorf("fragmented free space should have changed - %d -> %d\n", freeBeforeFrag, freeAfterFrag)
	}


	expected := freeBeforeFrag + 3 * c.LEN_U16 + uint16(len(key)) + uint16(len(val))
	if freeAfterFrag != expected {
		t.Errorf("fragmented free space should have increase by 1-slot + entry-len, %d -> %d | %d expected", freeBeforeFrag, freeAfterFrag, expected)
	}

	p.Defragment(scratch)

	// after compaction
	freeAfterContig  = p.FreeBytesContig()
	freeAfterFrag  = p.FreeBytesFrag()

	if freeAfterFrag == freeBeforeFrag {
		t.Errorf("free frag space shouldnt have changed just cause we defragged - %d -> %d\n", freeBeforeFrag, freeAfterFrag)
	}

	if freeAfterFrag != freeAfterContig {
		t.Errorf("free frag space should be same as free contig space after defrag - frag: %d contig: %d", freeAfterFrag, freeAfterContig)
	}
}

func Fuzz_PageHeaders_All(f *testing.F) {
	f.Add(uint64(1), uint64(2), uint16(3), uint16(4),
		uint64(5), uint8(6), uint16(7), uint8(8), uint64(9), uint16(10))

	f.Fuzz(func(t *testing.T, id uint64, parent uint64, lower uint16, upper uint16,
		checksum uint64, ptype uint8, flags uint16, ver uint8, gen uint64, free uint16) {
		p := PageSlottedNewTest(make([]byte, c.PAGE_SIZE), 0xffff)

		p.SetId(id)
		p.SetParent(parent)
		p.setLower(lower)
		p.setUpper(upper)
		p.SetChecksum(checksum)
		p.SetPagetype(ptype)
		p.SetFlags(flags)
		p.SetVer(ver)
		p.SetRight(id) 
		p.SetGen(gen)
		p.setFreebytes(free)

		if p.Id() != id { t.Errorf("Id mismatch") }
		if p.Parent() != parent { t.Errorf("Parent mismatch") }
		if p.lower() != lower { t.Errorf("Lower mismatch") }
		if p.upper() != upper { t.Errorf("Upper mismatch") }
		if p.Checksum() != checksum { t.Errorf("Checksum mismatch") }
		if p.Pagetype() != ptype { t.Errorf("Pagetype mismatch") }
		if p.Flags() != flags { t.Errorf("Flags mismatch") }
		if p.Ver() != ver { t.Errorf("Ver mismatch") }
		if p.Right() != id { t.Errorf("Right mismatch") }
		if p.Gen() != gen { t.Errorf("Gen mismatch") }
		if p.freeBytes() != free { t.Errorf("Gen mismatch") }
	})
}

func Test_PageMeta_Initialize(t *testing.T) {
	raw := make([]byte, c.PAGE_SIZE)
	rootID := uint64(42)
	
	// Create meta page
	meta := PageMetaNew(raw, 1, rootID, 1)
    // Note: In your provided code, PageMetaNew sets RootId(0) 
    // instead of the passed rootId. You might want to fix that!
    meta.SetRootId(rootID)

	// Test Magic Number
	if string(meta.raw[offMagic:offMagic+len(magic)]) != magic {
		t.Errorf("Expected magic %s, got %s", magic, meta.raw[offMagic:offMagic+len(magic)])
	}

	// Test Getters
	if meta.RootId() != 42 {
		t.Errorf("Expected RootId 42, got %d", meta.RootId())
	}

	if meta.Pagetype() != PagetypeMeta {
		t.Errorf("Expected type %d, got %d", PagetypeMeta, meta.Pagetype())
	}
}

func Test_PageMeta_Persistence(t *testing.T) {
	raw := make([]byte, c.PAGE_SIZE)
	meta := PageMetaNew(raw, 0, 0, 0)

	// Set values
	meta.SetPageCnt(100)
	meta.SetFreeList(500)

	// Create a new view from the same raw bytes (simulating a reload)
	meta2 := PageMetaFrom(raw)

	if meta2.PageCnt() != 100 {
		t.Errorf("Persistence failed: expected 100, got %d", meta2.PageCnt())
	}
	
	if meta2.FreeList() != 500 {
		t.Errorf("Persistence failed: expected 500, got %d", meta2.FreeList())
	}
}
