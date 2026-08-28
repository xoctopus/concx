package pipe

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xoctopus/x/codex"
	"github.com/xoctopus/x/container/queue"
	"github.com/xoctopus/x/misc/must"
	"github.com/xoctopus/x/slicex"

	"github.com/xoctopus/concx/pkg/chanx"
	"github.com/xoctopus/concx/pkg/nest"
)

type option struct {
	shutdownTimeout time.Duration
	maxPending      int
}

var gDefaultOption = option{
	shutdownTimeout: time.Second * 5,
	maxPending:      10,
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

const (
	Lifetime_Orchestrating = iota + int32(0)
	Lifetime_Orchestrated
	Lifetime_Building
	Lifetime_Built
	Lifetime_Running
	Lifetime_Closed
)

func FromJob[Head, Tail, Next any](
	name string,
	job TransformJob[Head, Next],
) *NodeOrch[Head, Tail, Next] {
	var (
		origin = chanx.NewNotifiableObserver[*flight[Head, Tail, Head]]()
		next   = chanx.NewNotifiableObserver[*flight[Head, Tail, Next]]()
	)

	orch := &NodeOrch[Head, Tail, Next]{
		p: &Pipeline[Head, Tail]{
			origin: origin,
			queue:  queue.NewSafeQueue[Retriever[Head, Tail]](),
		},
		observer: next,
	}

	orch.p.cancels = append(orch.p.cancels, origin, next)
	orch.p.nodes = append(orch.p.nodes, &SimpleNode[Head, Tail, Head, Next]{
		name:     name,
		job:      job,
		observer: origin,
		notifier: next,
	})

	return orch
}

func FromUniversalJobs[Head, Tail, Next any](
	name string,
	summary func(in Head, outs ...any) Next,
	jobs ...UniversalTransformJob[Head],
) *NodeOrch[Head, Tail, Next] {
	jobs = slicex.Filter(jobs, func(j UniversalTransformJob[Head]) bool { return j != nil })
	must.BeTrue(len(jobs) > 0)

	var (
		origin = chanx.NewNotifiableObserver[*flight[Head, Tail, Head]]()
		next   = chanx.NewNotifiableObserver[*flight[Head, Tail, Next]]()
	)

	orch := &NodeOrch[Head, Tail, Next]{
		p: &Pipeline[Head, Tail]{
			origin: origin,
			queue:  queue.NewSafeQueue[Retriever[Head, Tail]](),
		},
		observer: next,
	}

	orch.p.cancels = append(orch.p.cancels, origin, next)
	orch.p.nodes = append(orch.p.nodes, &ParallelNode[Head, Tail, Head, Next]{
		name:     name,
		jobs:     jobs,
		summary:  summary,
		observer: origin,
		notifier: next,
	})

	return orch
}

type Pipeline[Head, Tail any] struct {
	// nodes stage executor
	nodes []Node
	// cancels for closing notifiers, observers
	cancels []chanx.Cancelable

	// origin
	origin chanx.ValueNotifier[*flight[Head, Tail, Head]]
	// finale
	finale chanx.ValueObserver[*flight[Head, Tail, Tail]]
	// stage identifies orchestrating stage.
	// 0: orchestrating
	// 1: build options
	// 2: running
	lifetime atomic.Int32
	// dead/closed use closed
	closed atomic.Bool

	*option

	nest    nest.Nest
	cond    sync.Cond
	queue   queue.Queue[Retriever[Head, Tail]]
	pending atomic.Int64
	mu      sync.Mutex
}

func (p *Pipeline[Head, Tail]) Run(ctx context.Context) error {
	err := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()

		must.BeTrue(p.lifetime.Load() == Lifetime_Built)
		p.nest = nest.New(
			ctx,
			nest.WithShutdownTimeout(p.shutdownTimeout),
		)

		// booting nodes
		for _, ex := range p.nodes {
			err := p.nest.Spawn(func(ctx context.Context) {
				for {
					select {
					case <-ctx.Done():
						return // TODO should this error
					default:
						_ = ex.execute(ctx)
					}
				}
			})
			if err != nil {
				return err
			}
		}

		// triggering
		if err := p.nest.Spawn(func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					// TODO use p.cond to wake queue up
					if r, ok := p.queue.Pop(); ok {
						p.origin.Send(&flight[Head, Tail, Head]{
							r: r,
							v: r.origin(),
						})
						continue
					}
					time.Sleep(500 * time.Millisecond)
				}
			}
		}); err != nil {
			return err
		}

		// retrieving
		if err := p.nest.Spawn(func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				case f, ok := <-p.finale.Value():
					if ok {
						f.r.finish(f.v, nil)
					} else {
						// TODO notify failed
					}
				}
			}
		}); err != nil {
			return err
		}

		must.BeTrue(p.lifetime.CompareAndSwap(Lifetime_Built, Lifetime_Running))
		return nil
	}()

	if err != nil {
		return err
	}

	<-p.nest.Done()
	p.mu.Lock()
	p.closed.Store(true)
	p.mu.Unlock()

	return p.nest.Err()
}

func (p *Pipeline[Head, Tail]) Close() error {
	if p.closed.CompareAndSwap(false, true) {
		p.mu.Lock()
		defer p.mu.Unlock()

		p.lifetime.Store(Lifetime_Closed)
		err := codex.New(ERROR__PIPELINE_CLOSED)
		for _, c := range p.cancels {
			if c != nil {
				c.CancelCause(err)
			}
		}
		if p.nest != nil {
			p.nest.Cancel(err)
			<-p.nest.Done()
		}
		return err
	}
	return nil
}

func (p *Pipeline[Head, Tail]) Push(ctx context.Context, in Head) (Result[Tail], error) {
	if p.lifetime.Load() != Lifetime_Running {
		return nil, codex.New(ERROR__PIPELINE_NOT_RUNNING)
	}

	if p.closed.Load() || p.nest.Canceled() {
		return nil, codex.New(ERROR__PIPELINE_CLOSED)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if int(p.pending.Load()) >= p.maxPending {
		return nil, codex.New(ERROR__REACH_MAX_PENDING)
	}

	ret := &retriever[Head, Tail]{
		done:    make(chan struct{}),
		counter: &p.pending,
		in:      in,
	}
	p.pending.Add(1)
	p.queue.Push(ret)

	return ret, nil
}

func (p *Pipeline[Head, Tail]) Pending() int {
	return int(p.pending.Load())
}
