package pipe

import (
	"context"
	"errors"

	"github.com/xoctopus/x/codex"

	"github.com/xoctopus/concx/pkg/schedx"
)

// Error defines error codes in pipe
// +genx:code
type Error uint8

const (
	ERROR_UNDEFINED Error = iota
	ERROR__PIPELINE_NOT_RUNNING
	ERROR__REACH_MAX_PENDING
	ERROR__PIPELINE_CANCELED

	ERROR__JOB_FAILED
	ERROR__JOB_CANCELED
	ERROR__PIPELINE_JOB_PANICKED
	ERROR__PIPELINE_SUMMARY_PANICKED
)

func IsPipelineError(err error) bool {
	return codex.IsCode(err, ERROR__PIPELINE_NOT_RUNNING) ||
		codex.IsCode(err, ERROR__REACH_MAX_PENDING)
}

func IsTaskError(err error) bool {
	if err == nil {
		return false
	}
	if IsPipelineError(err) || IsShutdown(err) {
		return false
	}
	return codex.IsCode(err, ERROR__PIPELINE_JOB_PANICKED) ||
		codex.IsCode(err, ERROR__PIPELINE_SUMMARY_PANICKED) ||
		codex.IsCode(err, ERROR__JOB_FAILED) ||
		codex.IsCode(err, ERROR__JOB_CANCELED)
}

func IsShutdown(err error) bool {
	return codex.IsCode(err, ERROR__PIPELINE_CANCELED) ||
		codex.IsCode(err, schedx.ERROR__SCHEDULER_CANCELED) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}
