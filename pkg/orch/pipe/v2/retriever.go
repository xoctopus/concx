package pipe

import (
	"context"
	"sync"
	"sync/atomic"
)

type Retriever[I, O any] interface {
	finish(O, error)
	failed(error)
	origin() I

	Result[O]
}

type Result[R any] interface {
	Result(ctx context.Context) (R, error)
}

type retriever[I, O any] struct {
	once sync.Once
	done chan struct{}

	in      I
	res     O
	err     error
	counter *atomic.Int64
}

func (r *retriever[I, O]) origin() I {
	return r.in
}

func (r *retriever[I, O]) finish(res O, err error) {
	r.once.Do(func() {
		r.res, r.err = res, err
		r.counter.Add(-1)
		close(r.done)
	})
}

func (r *retriever[I, O]) failed(err error) {
	r.finish(r.res, err)
}

func (r *retriever[I, O]) Result(ctx context.Context) (O, error) {
	select {
	case <-ctx.Done():
		// TODO if need to interrupt pipeline task?
		return *new(O), ctx.Err()
	case <-r.done:
	}
	return r.res, r.err
}

type flight[Head, Tail, T any] struct {
	r Retriever[Head, Tail]
	v T
}
