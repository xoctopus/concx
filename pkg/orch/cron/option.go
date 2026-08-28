package cron

import (
	"time"

	robfig "github.com/robfig/cron/v3"

	"github.com/xoctopus/concx/pkg/schedx"
)

// OverlapPolicy controls behavior when a new tick occurs while the job is still executing.
type OverlapPolicy int

const (
	// OverlapSkip drops the new tick if all parallel slots for the job are busy.
	OverlapSkip OverlapPolicy = iota

	// OverlapQueue pushes the new tick into the job's scheduler queue up to maxPending.
	OverlapQueue
)

type option struct {
	name             string
	loc              *time.Location
	parser           robfig.ScheduleParser
	shutdownTimeout  time.Duration
	parallel         int
	maxPending       int
	unlimitedPending bool
	overlap          OverlapPolicy
	callback         schedx.HandlerCallback[time.Time]
	disableDetached  bool
}

// Option configures a single-task Cron orchestrator.
type Option func(*option)

// WithName assigns a human-readable name/label to the cron orchestrator.
func WithName(name string) Option {
	return func(o *option) {
		o.name = name
	}
}

// WithLocation sets the timezone for parsing expressions and scheduling.
// Defaults to time.Local.
func WithLocation(loc *time.Location) Option {
	return func(o *option) {
		if loc != nil {
			o.loc = loc
		}
	}
}

// WithParser sets a custom cron ScheduleParser.
func WithParser(p robfig.ScheduleParser) Option {
	return func(o *option) {
		if p != nil {
			o.parser = p
		}
	}
}

// WithSeconds enables the 6-field parser where seconds is required (Quartz style).
func WithSeconds() Option {
	return func(o *option) {
		o.parser = robfig.NewParser(
			robfig.Second | robfig.Minute | robfig.Hour | robfig.Dom | robfig.Month | robfig.Dow | robfig.Descriptor,
		)
	}
}

// WithShutdownTimeout sets the maximum duration to wait for running jobs to finish during Close.
// Defaults to 5 seconds.
func WithShutdownTimeout(timeout time.Duration) Option {
	return func(o *option) {
		o.shutdownTimeout = timeout
	}
}

// WithParallel sets how many instances of the job may run concurrently.
// Defaults to 1.
func WithParallel(parallel int) Option {
	return func(o *option) {
		if parallel > 0 {
			o.parallel = parallel
		}
	}
}

// WithMaxPending sets the maximum queued tick backlog for the entry.
// Defaults to 1.
func WithMaxPending(maxPending int) Option {
	return func(o *option) {
		o.maxPending = maxPending
	}
}

// WithoutPendingLimitation disables the queue backlog limit for the entry.
func WithoutPendingLimitation() Option {
	return func(o *option) {
		o.unlimitedPending = true
	}
}

// WithOverlapSkip sets the overlap policy to skip new ticks when busy.
// This is the default policy.
func WithOverlapSkip() Option {
	return func(o *option) {
		o.overlap = OverlapSkip
	}
}

// WithOverlapQueue sets the overlap policy to queue new ticks up to maxPending.
func WithOverlapQueue(maxPending int) Option {
	return func(o *option) {
		o.overlap = OverlapQueue
		o.maxPending = maxPending
	}
}

// WithCallback sets a callback invoked after each job execution finishes (with tick time and error/panic).
func WithCallback(cb schedx.HandlerCallback[time.Time]) Option {
	return func(o *option) {
		o.callback = cb
	}
}

// WithoutDetached causes the job execution context to be canceled immediately when Cron is canceled.
func WithoutDetached() Option {
	return func(o *option) {
		o.disableDetached = true
	}
}
