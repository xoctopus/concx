package cron

import (
	"strings"
	"time"

	robfig "github.com/robfig/cron/v3"
)

// Schedule calculates the next execution time given the current time.
// It is compatible with robfig/cron/v3.Schedule.
type Schedule interface {
	Next(time.Time) time.Time
}

// gDefaultParser matches [New]'s default: optional seconds + standard fields + descriptors.
var gDefaultParser = robfig.NewParser(
	robfig.SecondOptional |
		robfig.Minute |
		robfig.Hour |
		robfig.Dom |
		robfig.Month |
		robfig.Dow |
		robfig.Descriptor,
)

// Every returns a Schedule that repeats after a constant duration.
// Unlike robfig/cron, it supports arbitrary precision including sub-second durations.
func Every(duration time.Duration) Schedule {
	return every{du: duration}
}

type every struct {
	du time.Duration
}

func (s every) Next(t time.Time) time.Time {
	return t.Add(s.du)
}

// MustSpec parses spec with the package default parser.
// Supports standard cron expressions, optional-seconds form, descriptors, and "@every <duration>".
// Panics if spec is invalid.
func MustSpec(spec string) Schedule {
	s, err := parseSchedule(spec, gDefaultParser)
	if err != nil {
		panic(err)
	}
	return s
}

func parseSchedule(spec string, parser robfig.ScheduleParser) (Schedule, error) {
	if after, ok := strings.CutPrefix(spec, "@every "); ok {
		d, err := time.ParseDuration(strings.TrimSpace(after))
		if err != nil {
			return nil, err
		}
		return Every(d), nil
	}
	return parser.Parse(spec)
}
