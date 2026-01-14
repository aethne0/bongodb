package util_test

import (
	"mooodb/internal/util"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Queue(t *testing.T) {
	q := util.CreateQueue[int](8)
	assert.Equal(t, q.Cnt(), 0)

	for range 3 {
		for i := range 5 {
			q.Push(i)
		}
		assert.Equal(t, q.Cnt(), 5)
		for i := range 5 {
			res := q.Pop()
			assert.Equal(t, res, i)
		}
		assert.Equal(t, q.Cnt(), 0)
	}

	for range 8 {
		q.Push(0)
	}
	for range 8 {
		q.Pop()
	}
}

func Test_TicketQueue(t *testing.T) {
	tq := util.CreateTicketQueue[int](4)

	// TODO: actual test
	for range 3 {
		tq.Acq(0)
	}
	for i := range 3 {
		tq.Rel(i)
	}
	for range 3 {
		tq.Acq(0)
	}
	for i := range 3 {
		tq.Rel(i)
	}
}
