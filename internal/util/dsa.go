package util

func mod(a int, b int) int {
	return ((a % b) + b) % b
}

// fixed-size ring-buffer queue
type Queue[T any] struct {
	data	[]T
	head	int // next slot to write to
	cnt 	int
}

func CreateQueue[T any](size int) Queue[T] {
	return Queue[T] {
		head: 	0,
		cnt: 	0,
		data: 	make([]T, size),
	}
}

func (q *Queue[T]) Cnt() int {
	return q.cnt
}

// will panic if out of space.
func (q *Queue[T]) Push(val T) {
	if q.cnt == len(q.data) { panic("queue overflow") }
	q.data[q.head] = val
	q.head = mod((q.head + 1), len(q.data))
	q.cnt++
}

func (q *Queue[T]) Pop() T {
	if q.cnt == 0 { panic("queue underflow") }
	i := mod((q.head - q.cnt), len(q.data))
	q.cnt--
	return q.data[i]
}


// TicketQueue combines a fixed contiguous array and a channel of numbered "tickets" which
// correspond to slots in the contiguous array. In other words, a shared pool of items 
// guarded by a channel working as a semaphore.
type TicketQueue[T any] struct {
	queue		Queue[int]
	data		[]T
}

func CreateTicketQueue[T any](size int) TicketQueue[T] {
	queue := CreateQueue[int](size)
	for i := range size {
		queue.Push(i)
	}
	data := make([]T, size)

	return TicketQueue[T]{
		queue: queue,
		data: data,
	}
}

// This acquires a ticket and sets the slot to the passed value
func (tq *TicketQueue[T]) Acq(val T) int {
	ticket := tq.queue.Pop()
	tq.data[ticket] = val
	return ticket
}

func (tq *TicketQueue[T]) Rel(ticket int) {
	tq.queue.Push(ticket)
}

func (tq *TicketQueue[T]) Get(ticket int) T {
	return tq.data[ticket]
}
