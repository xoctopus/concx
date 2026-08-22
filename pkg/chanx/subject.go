package chanx

import (
	"sync"
	"sync/atomic"
)

type Subject[T any] struct {
	mu   sync.Mutex
	done atomic.Value
	err  error
	subs map[Subscriber[T]]struct{}
}

func (s *Subject[T]) Err() error {
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()
	return err
}

func (s *Subject[T]) Done() <-chan struct{} {
	d := s.done.Load()
	if d != nil {
		return d.(chan struct{})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d = s.done.Load()
	if d == nil {
		d = make(chan struct{})
		s.done.Store(d)
	}
	return d.(chan struct{})
}

func (s *Subject[T]) CancelCause(err error) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return // already canceled
	}

	if err == nil {
		err = ErrCompleted
	}

	s.err = err

	d, _ := s.done.Load().(chan struct{})
	if d == nil {
		s.done.Store(closedch)
	} else {
		close(d)
	}

	for o := range s.subs {
		o.CancelCause(err)
	}
	s.subs = nil
	s.mu.Unlock()
}

func (s *Subject[T]) Send(x T) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return // already canceled
	}

	subs := make([]Subscriber[T], 0, len(s.subs))
	for ob := range s.subs {
		subs = append(subs, ob)
	}
	s.mu.Unlock()

	for _, ob := range subs {
		ob.Send(x)
	}
}

func (s *Subject[T]) Observe() Observer[T] {
	o := NewNotifiableObserver[T]()
	s.Subscribe(o)
	return o
}

func (s *Subject[T]) Subscribe(o Subscriber[T]) {
	s.mu.Lock()
	if s.err != nil {
		err := s.err
		s.mu.Unlock()
		o.CancelCause(err)
		return // already canceled
	}

	if s.subs == nil {
		s.subs = map[Subscriber[T]]struct{}{}
	}
	s.subs[o] = struct{}{}
	s.mu.Unlock()

	go func() {
		<-o.Done()

		s.mu.Lock()
		delete(s.subs, o)
		s.mu.Unlock()
	}()
}
