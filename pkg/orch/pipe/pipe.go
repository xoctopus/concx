/*
Package pipe is an orch recipe: linear Push → node₀ → … → nodeₙ → Result.

Each [Node] owns an internal [schedx.Scheduler] (ingress; default parallel=1,
FIFO, maxPending=1) and a chanx subject (egress). Jobs on a node fan out in
parallel for the same value (concurrency = len(jobs)); all must succeed before
the value is forwarded. The first job error fails fast (siblings canceled) and
does not forward.

Lifecycle:

	pipe.New(ctx, pipe.NewNode(...), …) → Push* → Result → Close → Done

[Pipeline.Result] carries values that finished the last node successfully.
[Pipeline.Done] closes when the pipeline is shut down (Close or parent ctx cancel).
Callers need not use package chanx.

Jobs are [schedx.Job] / [schedx.JobFunc]; this package does not re-export them.
*/
package pipe

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/xoctopus/x/codex"
	"github.com/xoctopus/x/misc/must"

	"github.com/xoctopus/concx/pkg/chanx"
	"github.com/xoctopus/concx/pkg/nest"
	"github.com/xoctopus/concx/pkg/schedx"
)

// NodeOption configures a [Node].
type NodeOption[T any] func(*Node[T])

// WithJobs appends jobs to the node's fan-out group.
// When multiple jobs are configured, they run concurrently for each value (fan-out)
// and fail fast on the first error.
// May be called multiple times; the final list is scheduled together.
// len(jobs)==1 is a plain stage with no fan-out overhead.
func WithJobs[T any](jobs ...schedx.Job[T]) NodeOption[T] {
	return func(n *Node[T]) {
		n.jobs = append(n.jobs, jobs...)
	}
}

// WithName sets a debug label; it does not affect scheduling.
func WithName[T any](name string) NodeOption[T] {
	return func(n *Node[T]) {
		n.name = name
	}
}

// Node describes one stage: ingress via Scheduler, egress after all jobs succeed.
// Nodes are inert until wired by [New].
type Node[T any] struct {
	name  string
	jobs  []schedx.Job[T]
	sched schedx.Scheduler[T]
	out   *chanx.Subject[T]
}

// NewNode builds a stage. At least one [WithJobs] is required.
// Jobs on the same node run in parallel for each value; order of options
// only affects the job list order (spawn order), not completion order.
func NewNode[T any](opts ...NodeOption[T]) Node[T] {
	var n Node[T]
	for _, opt := range opts {
		opt(&n)
	}
	must.BeTrueF(len(n.jobs) > 0, "pipe node requires at least one job")
	for i, job := range n.jobs {
		must.BeTrueF(job != nil, "pipe node job[%d] is required", i)
	}
	return n
}

func (n *Node[T]) run(ctx context.Context) {
	n.out = &chanx.Subject[T]{}
	wrapped := schedx.JobFunc[T](func(c context.Context, v T) error {
		if err := n.boot(c, v); err != nil {
			return err
		}
		n.out.Send(v)
		return nil
	})
	n.sched = schedx.NewScheduler(
		wrapped,
		schedx.WithParallel[T](1),
		schedx.WithFifoScheduleMode[T](),
		schedx.WithMaxPending[T](1),
	)
	must.NoError(n.sched.Run(ctx))
}

func (n *Node[T]) boot(ctx context.Context, v T) error {
	if len(n.jobs) == 1 {
		return n.jobs[0].Do(ctx, v)
	}

	g := nest.New(ctx)
	var (
		once sync.Once
		fail error
		left atomic.Int64
	)
	left.Store(int64(len(n.jobs)))

	for _, job := range n.jobs {
		if err := g.Spawn(func(c context.Context) {
			if err := job.Do(c, v); err != nil {
				once.Do(func() {
					fail = err
					// Cancel must not run under Spawn: it waits on the nest WaitGroup.
					go g.Cancel(err)
				})
				return
			}
			if left.Add(-1) == 0 {
				go g.Cancel(nil)
			}
		}); err != nil {
			return err
		}
	}

	<-g.Done()
	if fail != nil {
		return fail
	}
	return g.Err()
}

func (n *Node[T]) link(ctx context.Context, upstream chanx.Observable[T]) {
	obs := upstream.Observe()
	go func() {
		go func() {
			<-ctx.Done()
			obs.CancelCause(ctx.Err())
		}()
		for v := range obs.Value() {
			_ = n.sched.Push(ctx, v) // REACH_MAX_PENDING: drop this beat
		}
	}()
}

// Pipeline wires Push → node₀ → … → nodeₙ → Result.
type Pipeline[T any] interface {
	Push(ctx context.Context, v T) error
	// Result receives values that finished the last node successfully.
	// The channel is closed after the pipeline shuts down.
	Result() <-chan T
	// Done closes when the pipeline is shut down (Close or parent context cancel).
	Done() <-chan struct{}
	Close() error
}

// New builds a linear pipeline and starts all nodes under ctx.
// Requires at least one node. Parent ctx cancel or [Pipeline.Close] stops it.
func New[T any](ctx context.Context, nodes ...Node[T]) Pipeline[T] {
	must.BeTrueF(len(nodes) > 0, "pipe requires at least one node")

	ctx, cancel := context.WithCancel(ctx)
	refs := make([]*Node[T], len(nodes))
	for i := range nodes {
		refs[i] = &nodes[i]
		refs[i].run(ctx)
	}
	for i := 1; i < len(refs); i++ {
		refs[i].link(ctx, refs[i-1].out)
	}

	p := &pipeline[T]{
		nodes:  refs,
		cancel: cancel,
		done:   make(chan struct{}),
		result: make(chan T),
	}
	p.bridgeResult(ctx)
	go func() {
		<-ctx.Done()
		_ = p.Close()
	}()
	return p
}

type pipeline[T any] struct {
	nodes []*Node[T]

	mu     sync.Mutex
	closed atomic.Bool
	cancel context.CancelFunc

	done   chan struct{}
	result chan T
}

func (p *pipeline[T]) bridgeResult(ctx context.Context) {
	last := p.nodes[len(p.nodes)-1]
	obs := last.out.Observe()
	go func() {
		<-ctx.Done()
		obs.CancelCause(ctx.Err())
	}()
	go func() {
		defer close(p.result)
		for v := range obs.Value() {
			select {
			case p.result <- v:
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (p *pipeline[T]) Push(ctx context.Context, v T) error {
	if p.closed.Load() {
		return codex.New(schedx.ERROR__SCHEDULER_CANCELED)
	}
	return p.nodes[0].sched.Push(ctx, v)
}

func (p *pipeline[T]) Result() <-chan T {
	return p.result
}

func (p *pipeline[T]) Done() <-chan struct{} {
	return p.done
}

func (p *pipeline[T]) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	for _, n := range p.nodes {
		n.out.CancelCause(nil)
		_ = n.sched.Close()
	}
	close(p.done)
	return nil
}
