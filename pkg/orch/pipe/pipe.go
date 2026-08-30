package pipe

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xoctopus/x/codex"
	"github.com/xoctopus/x/misc/must"

	"github.com/xoctopus/concx/pkg/chanx"
	"github.com/xoctopus/concx/pkg/nest"
	"github.com/xoctopus/concx/pkg/schedx"
)

type option struct {
	shutdownTimeout time.Duration
	maxPending      int
	parallel        int
}

var gDefaultOption = option{
	shutdownTimeout: time.Second * 5,
	maxPending:      10,
	parallel:        10,
}

type OptionFunc func(*option)

func WithShutdownTimeout(timeout time.Duration) OptionFunc {
	return func(o *option) {
		o.shutdownTimeout = timeout
	}
}

func WithMaxPending(maxPending int) OptionFunc {
	return func(o *option) {
		o.maxPending = maxPending
	}
}

// WithParallel sets how many pipeline tickets may run concurrently
// (RetrievableScheduler worker count). Each in-flight ticket holds a worker
// until Tail or failure.
func WithParallel(n int) OptionFunc {
	return func(o *option) {
		o.parallel = n
	}
}

const (
	Lifetime_Orchestrating = iota + int32(0)
	Lifetime_Orchestrated
	Lifetime_Building
	Lifetime_Built
	Lifetime_Running
	Lifetime_Closed
)

// Scheduler is the runnable pipeline surface. Shape matches
// [schedx.RetrievableScheduler]; runtime is RetrievableScheduler plus stage
// pumps (nodes / finale) and chanx shutdown.
type Scheduler[Head, Tail any] interface {
	Push(context.Context, Head) (Result[Tail], error)
	Pending() int
	Close() error
	Run(context.Context) error
}

type pipeline[Head, Tail any] struct {
	nodes   []Node
	cancels []chanx.Cancelable

	origin chanx.ValueNotifier[*flight[Head, Tail, Head]]
	finale chanx.ValueObserver[*flight[Head, Tail, Tail]]

	lifetime atomic.Int32
	stopping atomic.Bool

	*option

	nest   nest.Nest
	sche   schedx.RetrievableScheduler[Head, Tail]
	closed chan struct{}

	once  sync.Once
	cause atomic.Value

	mtx sync.Mutex
}

func (p *pipeline[Head, Tail]) drive(ctx context.Context, in Head) (Tail, error) {
	tk := &ticket[Tail]{done: make(chan struct{})}
	p.origin.Send(&flight[Head, Tail, Head]{
		t: tk,
		v: in,
	})
	return tk.wait(ctx, p.closed)
}

func (p *pipeline[Head, Tail]) Run(ctx context.Context) error {
	abort := func(cause error) error {
		// Unlock before Cancel so nest BeforeClose → shutdown cannot deadlock on mtx.
		p.mtx.Unlock()
		if p.nest != nil {
			p.nest.Cancel(cause)
			<-p.nest.Done()
			p.nest = nil
		}
		return cause
	}

	p.mtx.Lock()

	must.BeTrue(p.lifetime.Load() == Lifetime_Built)
	must.BeTrue(p.option != nil)

	// 1. Admission: RetrievableScheduler owns Push/Pending/Result.
	//    Job.Do = drive (inject origin, block on ticket until Tail/fail/cancel).
	//    WithoutDetached so Close cancels in-flight drive waits via ctx.
	p.closed = make(chan struct{})
	p.sche = schedx.NewRetrievableScheduler(
		schedx.RetrievableJobFunc[Head, Tail](p.drive),
		schedx.WithRetrievableMaxPending(p.maxPending),
		schedx.WithRetrievableParallel(p.parallel),
		schedx.WithRetrievableFifoScheduleMode(),
		schedx.WithRetrievableCloseTimeout(p.shutdownTimeout),
		schedx.WithoutRetrievableDetached(),
	)

	// 2. Stage runtime nest: node pumps + finale; BeforeClose → shutdown
	//    (sche.Close, close(closed), cancel chanx).
	p.nest = nest.New(
		ctx,
		nest.WithShutdownTimeout(p.shutdownTimeout),
		nest.WithBeforeCloseFunc(func(context.Context) {
			_ = p.shutdown(codex.New(ERROR__PIPELINE_CANCELED))
		}),
	)

	// 3. Boot one loop per node: pull flight → TransformJob → send next / fail ticket.
	for _, ex := range p.nodes {
		node := ex
		if err := p.nest.Spawn(func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if codex.IsCode(node.execute(ctx), ERROR__PIPELINE_CANCELED) {
						return
					}
				}
			}
		}); err != nil {
			return abort(err)
		}
	}

	// 4. Boot finale: last-stage flights complete the ticket (unblocks drive).
	if err := p.nest.Spawn(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case f, ok := <-p.finale.Value():
				if !ok {
					return
				}
				f.t.finish(f.v, nil)
			}
		}
	}); err != nil {
		return abort(err)
	}

	// 5. Start admission workers last so stages are ready before Push.
	if err := p.sche.Run(ctx); err != nil {
		return abort(err)
	}

	must.BeTrue(p.lifetime.CompareAndSwap(Lifetime_Built, Lifetime_Running))
	p.mtx.Unlock()
	return nil
}

func (p *pipeline[Head, Tail]) shutdown(reason error) error {
	p.once.Do(func() {
		if reason == nil {
			reason = codex.New(ERROR__PIPELINE_CANCELED)
		}
		p.cause.Store(reason)
		p.lifetime.Store(Lifetime_Closed)

		// Wake drive/ticket.wait with PIPELINE_CANCELED and stop stage pumps
		// before sche.Close, so in-flight finish can record the pipe error
		// (Retrievable finish would overwrite once its closed is signaled).
		if p.closed != nil {
			select {
			case <-p.closed:
			default:
				close(p.closed)
			}
		}
		for _, c := range p.cancels {
			c.CancelCause(reason)
		}
		if p.sche != nil {
			_ = p.sche.Close()
		}
	})
	if v := p.cause.Load(); v != nil {
		return v.(error)
	}
	return reason
}

func (p *pipeline[Head, Tail]) Close() error {
	if !p.stopping.CompareAndSwap(false, true) {
		return nil
	}

	err := p.shutdown(codex.New(ERROR__PIPELINE_CANCELED))
	if p.nest != nil {
		p.nest.Cancel(err)
		joined := p.nest.Err()
		if codex.IsCode(joined, nest.ERROR__NEST_CLOSE_TIMEOUT) {
			return joined
		}
	}
	return nil
}

func (p *pipeline[Head, Tail]) Push(ctx context.Context, in Head) (Result[Tail], error) {
	if p.lifetime.Load() == Lifetime_Closed || p.stopping.Load() ||
		(p.nest != nil && p.nest.Canceled()) {
		return nil, codex.New(ERROR__PIPELINE_CANCELED)
	}
	if p.lifetime.Load() != Lifetime_Running || p.sche == nil {
		return nil, codex.New(ERROR__PIPELINE_NOT_RUNNING)
	}

	ret, err := p.sche.Push(ctx, in)
	if err == nil {
		return ret, nil
	}
	switch {
	case codex.IsCode(err, schedx.ERROR__SCHEDULER_NOT_RUNNING):
		return nil, codex.New(ERROR__PIPELINE_NOT_RUNNING)
	case codex.IsCode(err, schedx.ERROR__REACH_MAX_PENDING):
		return nil, codex.New(ERROR__REACH_MAX_PENDING)
	case codex.IsCode(err, schedx.ERROR__SCHEDULER_CANCELED):
		return nil, codex.New(ERROR__PIPELINE_CANCELED)
	default:
		return nil, codex.Wrap(ERROR_UNDEFINED, err)
	}
}

func (p *pipeline[Head, Tail]) Pending() int {
	if p.sche == nil {
		return 0
	}
	return p.sche.Pending()
}
