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

type task[In, Out any] struct {
	in  In
	ret *result[Out]
}

type result[Out any] struct {
	once   sync.Once
	done   chan struct{}
	closed <-chan struct{}
	cause  *atomic.Value

	out Out
	err error
}

func (r *result[Out]) finish(out Out, err error) {
	r.once.Do(func() {
		// Close may have already signaled; keep Result as canceled even if Job
		// later succeeds (Job lifecycle is independent of Result completion).
		select {
		case <-r.closed:
			r.out, r.err = *new(Out), r.reason()
		default:
			r.out, r.err = out, err
		}
		close(r.done)
	})
}

func (r *result[Out]) failed(err error) {
	r.finish(*new(Out), err)
}

func (r *result[Out]) reason() error {
	if r.cause != nil {
		if v := r.cause.Load(); v != nil {
			if err, ok := v.(error); ok && err != nil {
				return err
			}
		}
	}
	return codex.New(ERROR__SCHEDULER_CANCELED)
}

func (r *result[Out]) Result(ctx context.Context) (Out, error) {
	// Prefer an already-completed Result over a concurrent Close.
	select {
	case <-r.done:
		return r.out, r.err
	default:
	}

	select {
	case <-ctx.Done():
		return *new(Out), ctx.Err()
	case <-r.done:
		return r.out, r.err
	case <-r.closed:
		select {
		case <-r.done:
			return r.out, r.err
		default:
			return *new(Out), r.reason()
		}
	}
}

// NewRetrievableScheduler builds a scheduler that returns a [Result] per Push.
//
// Lifecycle: New → Run → Push* → Close.
// Push before Run returns ERROR__SCHEDULER_NOT_RUNNING.
//
// Close fails all unfinished Results immediately so waiters do not block;
// leftover queued work is not executed. Job.Do follows nest / Detached options
// and is independent of Result completion.
func NewRetrievableScheduler[In, Out any](
	f RetrievableJob[In, Out],
	appliers ...RetrievableSchedulerApplier,
) RetrievableScheduler[In, Out] {
	must.BeTrueF(f != nil, "retrievable job handler is required")

	r := &retrievable[In, Out]{
		fn:     f,
		cond:   sync.NewCond(&sync.Mutex{}),
		closed: make(chan struct{}),
	}
	r.retrievableOption.SetDefault()
	for _, applier := range appliers {
		applier(&r.retrievableOption)
	}
	switch r.mode {
	case FIFO:
		r.tasks = queue.NewSafeQueue[task[In, Out]]()
	case LIFO:
		r.tasks = stack.NewSafeStack[task[In, Out]]()
	}

	return r
}

type retrievable[In, Out any] struct {
	retrievableOption

	cond *sync.Cond
	nest nest.Nest

	fn     RetrievableJob[In, Out]
	tasks  Tasks[task[In, Out]]
	closed chan struct{}

	pending   atomic.Int64
	running   atomic.Bool
	cause     atomic.Value
	closeOnce sync.Once
}

func (r *retrievable[In, Out]) Push(_ context.Context, in In) (Result[Out], error) {
	if !r.running.Load() {
		return nil, codex.New(ERROR__SCHEDULER_NOT_RUNNING)
	}
	if r.nest != nil && r.nest.Canceled() {
		return nil, codex.New(ERROR__SCHEDULER_CANCELED)
	}

	if r.maxPending < 0 {
		r.pending.Add(1)
		goto Append
	}

	for {
		current := r.pending.Load()
		if current >= int64(r.maxPending) {
			return nil, codex.New(ERROR__REACH_MAX_PENDING)
		}
		if r.pending.CompareAndSwap(current, current+1) {
			goto Append
		}
	}

Append:
	ret := &result[Out]{
		done:   make(chan struct{}),
		closed: r.closed,
		cause:  &r.cause,
	}
	r.tasks.Push(task[In, Out]{in: in, ret: ret})
	r.cond.Broadcast()
	return ret, nil
}

func (r *retrievable[In, Out]) Pending() int {
	return int(r.pending.Load())
}

func (r *retrievable[In, Out]) Run(ctx context.Context) (err error) {
	if !r.running.CompareAndSwap(false, true) {
		return codex.New(ERROR__SCHEDULER_RERUN)
	}

	r.nest = nest.New(
		ctx,
		nest.WithBeforeCloseFunc(func(ctx context.Context) {
			r.dismiss(r.shutdownReason())
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					default:
						r.cond.Broadcast()
						time.Sleep(10 * time.Millisecond)
					}
				}
			}()
		}),
		nest.WithShutdownTimeout(r.closeTimeout),
	)

	defer func() {
		if err != nil {
			r.cause.Store(wrapCanceled(err))
			r.nest.Cancel(wrapCanceled(err))
			<-r.nest.Done()
		}
	}()

	for range r.parallel {
		if err = r.nest.Spawn(r.loop); err == nil {
			continue
		}
		return
	}
	return nil
}

func (r *retrievable[In, Out]) Close() error {
	if r.nest != nil {
		err := codex.New(ERROR__SCHEDULER_CANCELED)
		r.cause.Store(err)
		r.nest.Cancel(err)
		joined := r.nest.Err()
		if codex.IsCode(joined, nest.ERROR__NEST_CLOSE_TIMEOUT) {
			return joined
		}
	}
	return nil
}

func (r *retrievable[In, Out]) shutdownReason() error {
	if v := r.cause.Load(); v != nil {
		if err, ok := v.(error); ok && err != nil {
			return err
		}
	}
	if r.nest != nil {
		return wrapCanceled(context.Cause(r.nest.Parent()))
	}
	return codex.New(ERROR__SCHEDULER_CANCELED)
}

func (r *retrievable[In, Out]) dismiss(reason error) {
	if reason != nil {
		r.cause.Store(reason)
	}
	r.closeOnce.Do(func() { close(r.closed) })

	for {
		t, ok := r.tasks.Pop()
		if !ok {
			break
		}
		r.pending.Add(-1)
		t.ret.failed(reason)
	}
}

func (r *retrievable[In, Out]) loop(ctx context.Context) {
	for {
		r.cond.L.Lock()
		for r.tasks.Len() == 0 && !r.nest.Canceled() {
			r.cond.Wait()
		}
		if r.nest.Canceled() {
			r.cond.L.Unlock()
			return
		}
		t, ok := r.tasks.Pop()
		r.cond.L.Unlock()

		if ok {
			r.pending.Add(-1)
			jobCtx := ctx
			if !r.disableDetached {
				jobCtx = context.WithoutCancel(ctx)
			}
			r.do(jobCtx, t)
		}
	}
}

func (r *retrievable[In, Out]) do(ctx context.Context, t task[In, Out]) {
	var (
		out Out
		err error
	)
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
		t.ret.finish(out, err)
	}()

	out, err = r.fn.Do(ctx, t.in)
}
