package btree

import (
	c "bongodb/src"
	"testing"
)

/*
func Test_PageSlotted_Various(t *testing.T) {
	raw := make([]byte, c.PAGE_SIZE)

	p := PageSlottedNew(raw, 0xabcd)

	samples := []struct {
		k, v string
	}{
		{"AAAA", "UUUUUUUUUUUUUUUUUUUUUUUU"},
		{"BBBB", "555555555555555555555555"},
		{"DDDD", "666666666666666666666666"},
		{"AAAA", "wwwwwwwwwwwwwwwwwwwwwwww"},
		{"CCCC", "777777777777777777777777"},
		{"DDDD", "aaaaaaaaaaaaaaaaaaaaaaaa"},
	}

	fmt.Println()

	for _, s := range samples {
		key, val := []byte(s.k), []byte(s.v)
		p.PutValue(key, val)
	}

	p.Dbg()

	samples = []struct {
		k, v string
	}{
		{"AAAA", "AAAAAAAA"},
		{"BBBB", "BBBBBB"},
		{"CCCC", "CCCC"},
		{"DDDD", "DD"},
	}

	for _, s := range samples {
		key, val := []byte(s.k), []byte(s.v)
		p.PutValue(key, val)
	}

	p.DoChecksum()

	fmt.Println()
	p.Dbg()
	fmt.Println()

	scratch := make([]byte, c.PAGE_SIZE)
	before := p.FreeBytesContig()
	p.Defragment(scratch)
	after := p.FreeBytesContig()

	fmt.Printf("Reclaimed %d bytes\n", after-before)

	p.DoChecksum()
	fmt.Println()
	p.Dbg()


	fmt.Printf("Free: %d bytes (contig) %d bytes (frag)\n", p.FreeBytesContig(), p.FreeBytesFrag())
	fmt.Printf("\nDoes this work?\n")
	p.Iter(func(b []byte) bool {
		fmt.Printf("%s\n", b)
		return true
	})
}
*/

func Test_PageSlotted_UpdateIntegrity(t *testing.T) {
	p := PageSlottedNew(make([]byte, c.PAGE_SIZE), 2)
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
	p := PageSlottedNew(make([]byte, c.PAGE_SIZE), 1)
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
	p := PageSlottedNew(make([]byte, c.PAGE_SIZE), 2)
	key := []byte("k")

	for i := 1; i < 10; i++ {
		val := make([]byte, i*100)
		if !p.Put(key, val) {
			t.Logf("Failed to grow at iteration %d as expected", i)
			break
		}
	}

	largeVal := make([]byte, c.PAGE_SIZE/2)
	if ok := p.Put([]byte("new"), largeVal); !ok {
		p.Defragment(make([]byte, c.PAGE_SIZE))
		if ok := p.Put([]byte("new"), largeVal); !ok {
			t.Error("Should have been able to insert after defragmentation")
		}
	}
}

func Test_PageSlotted_DefragIntegrity(t *testing.T) {
	p := PageSlottedNew(make([]byte, c.PAGE_SIZE), 0x1234)
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
	p := PageSlottedNew(raw, 0xabcd)

	key := []byte("key")
	for i := range 50 {
		if !p.CanFitContig(uint16(len(key) + 10 + i)) {
			break
		}
		p.Put(key, make([]byte, 10+i))
	}

	p.Defragment(scratch)
}

func Benchmark_PageSlotted_Defragment(b *testing.B) {
	raw := make([]byte, c.PAGE_SIZE)
	scratch := make([]byte, c.PAGE_SIZE)
	p := PageSlottedNew(raw, 0xabcd)

	b.ResetTimer()
	key := []byte("somebigkeyzzhaha")
	for b.Loop() {
		for i := range 9999 {
			if p.CanFitContig(uint16(len(key) + 20 + i)) {
				p.Put(key, make([]byte, 20+i))
			} else {
				break
			}
		}
		p.Defragment(scratch)
	}
}
