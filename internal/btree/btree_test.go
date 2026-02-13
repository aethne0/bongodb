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
	gofakeit.NewFaker(r, true) // faker :=

	pager, err := pager.CreatePager("/xblk/test/wew.moo", 32)
	_, err = CreateBtree(pager) // btree, err :=
	if err != nil { t.Fatal(err) }
}

/*
func Test_Btree(t *testing.T) {
	seed := [32]byte{0}
	r := rand.NewChaCha8(seed)
	faker := gofakeit.NewFaker(r, true)

	pager, err := pager.CreatePager("/xblk/test/wew.moo", 32)
	if err != nil { t.Fatal(err) }

	btree, err := CreateBtree(pager)
	if err != nil { t.Fatal(err) }

	var name []byte
	for range 128 {
		name = []byte(faker.DomainName())
		btree.Insert(name, []byte( faker.ProductUPC() ))
	}

	fmt.Println(btree.Get(name))

	//res, err := btree.Get([]byte("yooo"))
	//if err != nil { t.Fatal(err) }

	btree.BigTesta()

	//assert.Equal(t, res, []byte("hahahaha!!"))
}
*/
