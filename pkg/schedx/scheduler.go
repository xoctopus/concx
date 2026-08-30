package schedx

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xoctopus/x/codex"
	"github.com/xoctopus/x/container/queue"
	"github.com/xoctopus/x/container/stack"
	"github.com/xoctopus/x/misc/must"

	"github.com/xoctopus/concx/pkg/nest"
)

func NewScheduler[T any](f Job[T], appliers ...SchedulerOptionApplier[T]) Scheduler[T] {
	must.BeTrueF(f != nil, "job handler is required")

	s := &scheduler[T]{
		fn:   f,
		cond: sync.NewCond(&sync.Mutex{}),
	}
	s.option.SetDefault()

	for _, applier := range appliers {
		applier(&s.option)
	}
	switch s.mode {
	case FIFO:
		s.tasks = queue.NewSafeQueue[T]()
	case LIFO:
		s.tasks = stack.NewSafeStack[T]()
	}

	if cb := s.userExitCallback; cb != nil {
		s.scheExitCallback = func(reason error) {
			jobs := make([]T, 0, s.tasks.Len())
			s.tasks.Range(func(v T) bool {
				jobs = append(jobs, v)
				return true
			})
			cb(jobs, reason)
		}
	}

	return s
}

type scheduler[T any] struct {
	option[T]

	cond *sync.Cond
	nest nest.Nest

	// fn job handler
	fn Job[T]
	// tasks task list
	tasks Tasks[T]
	// pending atomic counter for pending tasks
	pending atomic.Int64
	// running if scheduler is running
	running atomic.Bool
}

func (s *scheduler[T]) Push(_ context.Context, v T) error {
	if !s.running.Load() {
		return codex.New(ERROR__SCHEDULER_NOT_RUNNING)
	}
	if s.nest != nil && s.nest.Canceled() {
		return codex.New(ERROR__SCHEDULER_CANCELED)
	}

	if s.maxPending < 0 {
		s.pending.Add(1)
		goto Append
	}

	for {
		current := s.pending.Load()
		if current >= int64(s.maxPending) {
			return codex.New(ERROR__REACH_MAX_PENDING)
		}
		if s.pending.CompareAndSwap(current, current+1) {
			goto Append
		}
	}

Append:
	s.tasks.Push(v)
	s.cond.Broadcast()
	return nil
}

func (s *scheduler[T]) Run(ctx context.Context) (err error) {
	if !s.running.CompareAndSwap(false, true) {
		return codex.New(ERROR__SCHEDULER_RERUN)
	}

	s.nest = nest.New(
		ctx,
		nest.WithBeforeCloseFunc(func(ctx context.Context) {
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					default:
						s.cond.Broadcast()
						time.Sleep(10 * time.Millisecond)
					}
				}
			}()
		}),
		nest.WithAfterCloseFunc(func(cause error) error {
			if s.exitCbCalled.CompareAndSwap(false, true) {
				if s.scheExitCallback != nil {
					s.scheExitCallback(wrapCanceled(cause))
				}
			}
			return nil
		}),
		nest.WithShutdownTimeout(s.closeTimeout),
	)

	defer func() {
		if err != nil {
			s.nest.Cancel(err)
			<-s.nest.Done()
		}
	}()

	for range s.parallel {
		if err = s.nest.Spawn(s.run); err == nil {
			continue
		}
		return
	}
	return nil
}

func (s *scheduler[T]) run(ctx context.Context) {
	for {
		s.cond.L.Lock()
		// avoid spurious waking up
		for s.tasks.Len() == 0 && !s.nest.Canceled() {
			s.cond.Wait()
		}
		if s.nest.Canceled() {
			s.cond.L.Unlock()
			return
		}
		v, ok := s.tasks.Pop()
		s.cond.L.Unlock()

		if ok {
			s.pending.Add(-1)
			if !s.disableDetached {
				ctx = context.WithoutCancel(ctx)
			}
			s.do(ctx, v)
		}
	}
}

func (s *scheduler[T]) do(ctx context.Context, v T) {
	var err error
	defer func() {
		switch x := recover().(type) {
		case nil:
		case error:
			err = codex.Wrap(ERROR__SCHEDULER_JOB_PANICKED, x)
		default:
			err = codex.Errorf(
				ERROR__SCHEDULER_JOB_PANICKED,
				"caused by: %v", x,
			)
		}
		if s.callback != nil {
			s.callback(v, err)
		}
	}()
	err = s.fn.Do(ctx, v)
}

func (s *scheduler[T]) Pending() int {
	return int(s.pending.Load())
}

func (s *scheduler[T]) Close() error {
	if s.nest != nil {
		s.nest.Cancel(codex.New(ERROR__SCHEDULER_CANCELED))
		err := s.nest.Err()
		if codex.IsCode(err, nest.ERROR__NEST_CLOSE_TIMEOUT) {
			return err
		}
	}
	return nil
}
