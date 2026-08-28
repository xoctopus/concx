package pipe

import (
	"context"
	"sync/atomic"

	"github.com/xoctopus/x/codex"
	"github.com/xoctopus/x/misc/must"
	"github.com/xoctopus/x/slicex"

	"github.com/xoctopus/concx/pkg/chanx"
	"github.com/xoctopus/concx/pkg/nest"
)

type TransformJob[In, Out any] interface {
	Do(ctx context.Context, in In) (Out, error)
}

type TransformFunc[In, Out any] func(context.Context, In) (Out, error)

func (f TransformFunc[In, Out]) Do(ctx context.Context, in In) (Out, error) {
	return f(ctx, in)
}

type UniversalTransformJob[In any] = TransformJob[In, any]

type Summary[I, O any] func(I, ...any) O

func execute[I, O any](ctx context.Context, in I, j TransformJob[I, O]) (out O, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case error:
				err = codex.Wrap(ERROR__PIPELINE_JOB_PANICKED, x)
			default:
				err = codex.Errorf(ERROR__PIPELINE_JOB_PANICKED, "caused by: %v", x)
			}
		}
	}()

	return j.Do(ctx, in)
}

func summary[I, O any](f Summary[I, O], in I, res ...any) (out O, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case error:
				err = codex.Wrap(ERROR__PIPELINE_SUMMARY_PANICKED, x)
			default:
				err = codex.Errorf(ERROR__PIPELINE_SUMMARY_PANICKED, "caused by: %v", x)
			}
		}
	}()

	return f(in, res...), nil
}

type Node interface {
	Name() string
	execute(context.Context) error
}

type SimpleNode[Head, Tail, I, O any] struct {
	name     string
	job      TransformJob[I, O]
	observer chanx.ValueObserver[*flight[Head, Tail, I]]
	notifier chanx.ValueNotifier[*flight[Head, Tail, O]]
}

func (n *SimpleNode[Head, Tail, I, O]) Name() string {
	return n.name
}

func (n *SimpleNode[Head, Tail, I, O]) execute(ctx context.Context) (err error) {
	var (
		in  *flight[Head, Tail, I]
		ok  bool
		out O
	)

	defer func() {
		if err != nil && in != nil {
			in.r.failed(err)
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case in, ok = <-n.observer.Value():
		if !ok {
			return codex.New(ERROR__PIPELINE_CANCELED)
		}
	}

	out, err = execute(ctx, in.v, n.job)
	if err != nil {
		return err
	}

	if n.notifier != nil {
		n.notifier.Send(&flight[Head, Tail, O]{r: in.r, v: out})
	}

	return nil
}

type ParallelNode[Head, Tail, I, O any] struct {
	name     string
	jobs     []UniversalTransformJob[I]
	summary  func(in I, outs ...any) O
	observer chanx.ValueObserver[*flight[Head, Tail, I]]
	notifier chanx.ValueNotifier[*flight[Head, Tail, O]]
}

func (n *ParallelNode[Head, Tail, I, O]) Name() string {
	return n.name
}

func (n *ParallelNode[Head, Tail, I, O]) execute(ctx context.Context) (err error) {
	var (
		in *flight[Head, Tail, I]
		ok bool
	)

	defer func() {
		if err != nil && in != nil {
			in.r.failed(err)
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case in, ok = <-n.observer.Value():
		if !ok {
			return codex.New(ERROR__PIPELINE_CANCELED)
		}
	}

	var (
		g    = nest.New(ctx)
		fail atomic.Value
		left atomic.Int64
		res  = make([]any, len(n.jobs))
	)

	left.Store(int64(len(n.jobs)))

	for idx, j := range n.jobs {
		if err := g.Spawn(func(ctx context.Context) {
			out, err := execute(ctx, in.v, j)
			if err != nil {
				fail.CompareAndSwap(nil, err)
				go g.Cancel(err)
				return
			}
			res[idx] = out
			if left.Add(-1) == 0 {
				go g.Cancel(nil)
			}
		}); err != nil {
			fail.CompareAndSwap(nil, err)
			go g.Cancel(err)
			break
		}
	}

	<-g.Done()

	if err, ok := fail.Load().(error); ok && err != nil {
		return err
	}

	if n.notifier != nil {
		out := *(new(O))
		if n.summary != nil {
			if out, err = summary(n.summary, in.v, res...); err != nil {
				return err
			}
		}
		n.notifier.Send(&flight[Head, Tail, O]{r: in.r, v: out})
	}

	return nil
}

type NodeOrch[Head, Tail, Last any] struct {
	p *Pipeline[Head, Tail]
	// observer last observer from last node
	observer chanx.ValueObserver[*flight[Head, Tail, Last]]
}

func (o *NodeOrch[Head, Tail, Last]) Then[Next any](
	name string,
	job TransformJob[Last, Next],
) *NodeOrch[Head, Tail, Next] {
	must.BeTrue(o.p.lifetime.Load() == Lifetime_Orchestrating)
	notifier := chanx.NewNotifiableObserver[*flight[Head, Tail, Next]]()

	next := &SimpleNode[Head, Tail, Last, Next]{
		name:     name,
		job:      job,
		observer: o.observer,
		notifier: notifier,
	}

	o.p.nodes = append(o.p.nodes, next)
	o.p.cancels = append(o.p.cancels, notifier)

	return &NodeOrch[Head, Tail, Next]{
		p:        o.p,
		observer: notifier,
	}
}

func (o *NodeOrch[Head, Tail, Last]) Parallel[Next any](
	name string,
	summary func(in Last, outs ...any) Next,
	jobs ...UniversalTransformJob[Last],
) *NodeOrch[Head, Tail, Next] {
	must.BeTrue(o.p.lifetime.Load() == Lifetime_Orchestrating)
	notifier := chanx.NewNotifiableObserver[*flight[Head, Tail, Next]]()

	jobs = slicex.Filter(jobs, func(j UniversalTransformJob[Last]) bool { return j != nil })
	must.BeTrue(len(jobs) > 0)

	next := &ParallelNode[Head, Tail, Last, Next]{
		name:     name,
		jobs:     jobs,
		summary:  summary,
		observer: o.observer,
		notifier: notifier,
	}

	o.p.nodes = append(o.p.nodes, next)
	o.p.cancels = append(o.p.cancels, notifier)

	return &NodeOrch[Head, Tail, Next]{
		p:        o.p,
		observer: notifier,
	}
}

func (o *NodeOrch[Head, Tail, Last]) EndJob(
	name string,
	job TransformJob[Last, Tail],
) *Builder[Head, Tail] {
	must.BeTrue(o.p.lifetime.Load() == Lifetime_Orchestrating)
	defer func() {
		must.BeTrue(o.p.lifetime.CompareAndSwap(Lifetime_Orchestrating, Lifetime_Orchestrated))
	}()

	notifier := chanx.NewNotifiableObserver[*flight[Head, Tail, Tail]]()

	last := &SimpleNode[Head, Tail, Last, Tail]{
		name:     name,
		job:      job,
		observer: o.observer,
		notifier: notifier,
	}

	o.p.finale = notifier
	o.p.nodes = append(o.p.nodes, last)
	o.p.cancels = append(o.p.cancels, notifier)

	return &Builder[Head, Tail]{p: o.p}
}

func (o *NodeOrch[Head, Tail, Last]) EndJobs(
	name string,
	summary func(in Last, outs ...any) Tail,
	jobs ...UniversalTransformJob[Last],
) *Builder[Head, Tail] {
	must.BeTrue(o.p.lifetime.Load() == Lifetime_Orchestrating)
	defer func() {
		must.BeTrue(o.p.lifetime.CompareAndSwap(Lifetime_Orchestrating, Lifetime_Orchestrated))
	}()
	notifier := chanx.NewNotifiableObserver[*flight[Head, Tail, Tail]]()

	jobs = slicex.Filter(jobs, func(j UniversalTransformJob[Last]) bool { return j != nil })
	must.BeTrue(len(jobs) > 0)

	last := &ParallelNode[Head, Tail, Last, Tail]{
		name:     name,
		jobs:     jobs,
		summary:  summary,
		observer: o.observer,
		notifier: notifier,
	}

	o.p.finale = notifier
	o.p.nodes = append(o.p.nodes, last)
	o.p.cancels = append(o.p.cancels, notifier)

	return &Builder[Head, Tail]{p: o.p}
}

type Builder[Head, Tail any] struct {
	p *Pipeline[Head, Tail]
}

func (b *Builder[Head, Tail]) Build(fs ...OptionFunc) *Pipeline[Head, Tail] {
	must.BeTrue(b.p.lifetime.CompareAndSwap(Lifetime_Orchestrated, Lifetime_Building))
	defer func() {
		must.BeTrue(b.p.lifetime.CompareAndSwap(Lifetime_Building, Lifetime_Built))
	}()

	if b.p.option == nil {
		b.p.option = new(gDefaultOption)
	}
	for _, f := range fs {
		f(b.p.option)
	}
	// TODO should log summary of orchestration and options
	return b.p
}
