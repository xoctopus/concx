package pipe

import (
	"github.com/xoctopus/x/misc/must"
	"github.com/xoctopus/x/slicex"

	"github.com/xoctopus/concx/pkg/chanx"
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
		p: &pipeline[Head, Tail]{
			origin: origin,
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
		p: &pipeline[Head, Tail]{
			origin: origin,
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

type NodeOrch[Head, Tail, Last any] struct {
	p *pipeline[Head, Tail]
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
	p *pipeline[Head, Tail]
}

func (b *Builder[Head, Tail]) Build(fs ...OptionFunc) Scheduler[Head, Tail] {
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
	return b.p
}
