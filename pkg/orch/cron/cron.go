package cron

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xoctopus/x/codex"
	"github.com/xoctopus/x/misc/must"

	"github.com/xoctopus/concx/pkg/nest"
	"github.com/xoctopus/concx/pkg/schedx"
)

// Cron is an orchestrator recipe for a single recurring scheduled job.
type Cron interface {
	// Done closes when the cron orchestrator is shut down.
	Done() <-chan struct{}
	// Close stops the cron orchestrator and waits for active jobs to exit gracefully up to shutdownTimeout.
	Close() error
}

// New creates and starts a single-task Cron orchestrator parsing spec.
// spec supports standard cron expression, Quartz 6-field with WithSeconds(), and "@every <duration>".
func New(ctx context.Context, spec string, job schedx.Job[time.Time], opts ...Option) (Cron, error) {
	cfg := defaultOption()
	for _, opt := range opts {
		opt(&cfg)
	}

	schedule, err := parseSchedule(spec, cfg.parser)
	if err != nil {
		return nil, err
	}

	return newCron(ctx, schedule, job, cfg), nil
}

// NewWithSchedule creates and starts a single-task Cron orchestrator with a custom Schedule.
func NewWithSchedule(ctx context.Context, schedule Schedule, job schedx.Job[time.Time], opts ...Option) Cron {
	cfg := defaultOption()
	for _, opt := range opts {
		opt(&cfg)
	}
	return newCron(ctx, schedule, job, cfg)
}

func defaultOption() option {
	return option{
		loc:             time.Local,
		shutdownTimeout: 5 * time.Second,
		parallel:        1,
		maxPending:      1,
		overlap:         OverlapSkip,
		parser:          gDefaultParser,
	}
}

func newCron(ctx context.Context, schedule Schedule, job schedx.Job[time.Time], cfg option) Cron {
	must.BeTrueF(job != nil, "cron job is required")
	must.BeTrueF(schedule != nil, "cron schedule is required")

	ctx, cancel := context.WithCancel(ctx)
	c := &cronImpl{
		option:   cfg,
		schedule: schedule,
		job:      job,
		cancel:   cancel,
		done:     make(chan struct{}),
	}

	wrappedJob := schedx.JobFunc[time.Time](func(ctx context.Context, t time.Time) error {
		c.running.Add(1)
		defer c.running.Add(-1)
		return c.job.Do(ctx, t)
	})

	schedOpts := []schedx.SchedulerOptionApplier[time.Time]{
		schedx.WithParallel[time.Time](cfg.parallel),
		schedx.WithMaxPending[time.Time](cfg.maxPending),
		schedx.WithFifoScheduleMode[time.Time](),
		schedx.WithCloseTimeout[time.Time](cfg.shutdownTimeout),
	}
	if cfg.unlimitedPending {
		schedOpts = append(schedOpts, schedx.WithoutPendingLimitation[time.Time]())
	}
	if cfg.callback != nil {
		schedOpts = append(schedOpts, schedx.WithCallback(cfg.callback))
	}
	if cfg.disableDetached {
		schedOpts = append(schedOpts, schedx.WithoutDetached[time.Time]())
	}

	c.nest = nest.New(ctx, nest.WithShutdownTimeout(cfg.shutdownTimeout))
	c.sched = schedx.NewScheduler(wrappedJob, schedOpts...)
	must.NoError(c.sched.Run(c.nest.Children()))

	must.NoError(c.nest.Spawn(c.loop))

	go func() {
		<-ctx.Done()
		_ = c.Close()
	}()

	return c
}

type cronImpl struct {
	option

	schedule Schedule
	job      schedx.Job[time.Time]
	sched    schedx.Scheduler[time.Time]

	running atomic.Int64
	closed  atomic.Bool

	mu     sync.Mutex
	cancel context.CancelFunc
	nest   nest.Nest
	done   chan struct{}
}

func (c *cronImpl) now() time.Time {
	return time.Now().In(c.loc)
}

func (c *cronImpl) Done() <-chan struct{} {
	return c.done
}

func (c *cronImpl) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}

	c.mu.Lock()
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	c.mu.Unlock()

	c.nest.Cancel(codex.New(schedx.ERROR__SCHEDULER_CANCELED))
	_ = c.sched.Close()

	<-c.nest.Done()
	close(c.done)
	return nil
}

func (c *cronImpl) loop(ctx context.Context) {
	for {
		now := c.now()
		next := c.schedule.Next(now)
		if next.IsZero() {
			return
		}

		d := max(next.Sub(now), 0)

		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			return

		case <-timer.C:
			c.trigger(ctx, next)
		}
	}
}

func (c *cronImpl) trigger(ctx context.Context, triggerTime time.Time) {
	if c.overlap == OverlapSkip && c.running.Load() >= int64(c.parallel) {
		return
	}
	_ = c.sched.Push(ctx, triggerTime)
}
