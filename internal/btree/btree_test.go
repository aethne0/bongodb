package btree

import (
	"math/rand/v2"
	"mooodb/internal/pager"
	"testing"

	"github.com/brianvoe/gofakeit/v7"
)

func Test_Btree(t *testing.T) {
	seed := [32]byte{0}
	r := rand.NewChaCha8(seed)
	faker := gofakeit.NewFaker(r, true)

	pager, err := pager.CreatePager("/xblk/test/wew.moo", 16)
	if err != nil { t.Fatal(err) }

	btree, err := CreateBtree(pager)
	if err != nil { t.Fatal(err) }

	for range 100 {
		btree.Insert([]byte( faker.DomainName() ), []byte( faker.ProductUPC() ))
	}

	//res, err := btree.Get([]byte("yooo"))
	//if err != nil { t.Fatal(err) }

	btree.BigTesta()

	//assert.Equal(t, res, []byte("hahahaha!!"))
}

// TODO:
// implement cursor to see whats needed, what the io and buffer accerss patterns are
// how does a cursor hold N frames, as well.

