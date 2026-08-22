package chanx

import (
	"errors"
	"sync"
	"sync/atomic"
)

func NewNotifiableObserver[T any]() NotifiableObserver[T] {
	o := &observer[T]{
		value: make(chan T),
		in:    make(chan T),
	}
	go func() {
		defer close(o.value)
		for {
			select {
			case <-o.Done():
				return
			case x := <-o.in:
				select {
				case <-o.Done():
					return
				case o.value <- x:
				}
			}
		}
	}()

	return o
}

var (
	closedch     = make(chan struct{})
	ErrCompleted = errors.New("completed")
)

func init() {
	close(closedch)
}

type observer[T any] struct {
	value chan T
	in    chan T
	mu    sync.Mutex
	done  atomic.Value
	err   error
}

func (o *observer[T]) Value() <-chan T {
	return o.value
}

func (o *observer[T]) Err() error {
	o.mu.Lock()
	err := o.err
	o.mu.Unlock()
	return err
}

func (o *observer[T]) Send(x T) {
	o.mu.Lock()
	if o.err != nil {
		o.mu.Unlock()
		return // already canceled
	}
	o.mu.Unlock()

	select {
	case <-o.Done():
	case o.in <- x:
	}
}

// Done returns completeness signal
// copy from context/context.go:448
func (o *observer[T]) Done() <-chan struct{} {
	d := o.done.Load()
	if d != nil {
		return d.(chan struct{})
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	d = o.done.Load()
	if d == nil {
		d = make(chan struct{})
		o.done.Store(d)
	}
	return d.(chan struct{})
}

// CancelCause cancel observer with reason
func (o *observer[T]) CancelCause(err error) {
	o.mu.Lock()
	if o.err != nil {
		o.mu.Unlock()
		return // already canceled
	}

	if err == nil {
		err = ErrCompleted
	}

	o.err = err

	d, _ := o.done.Load().(chan struct{})
	if d == nil {
		o.done.Store(closedch)
	} else {
		close(d)
	}

	o.mu.Unlock()
}
