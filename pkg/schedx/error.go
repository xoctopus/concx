package schedx

import "github.com/xoctopus/x/codex"

// Error defines error codes of scheduler
// +genx:code
// @code domain=SCHED
type Error uint8

const (
	ERROR_UNDEFINED               Error = iota
	ERROR__REACH_MAX_PENDING            // reached max pending limitation
	ERROR__SCHEDULER_NOT_RUNNING        // Push before Run
	ERROR__SCHEDULER_RERUN              // scheduler is already running
	ERROR__SCHEDULER_CANCELED           // scheduler is canceled (Close or parent ctx)
	ERROR__SCHEDULER_JOB_PANICKED       // scheduler job panicked
)

// wrapCanceled maps any shutdown cause to ERROR__SCHEDULER_CANCELED.
// Parent ctx cancel and manual Close are not distinguished at the code level.
func wrapCanceled(cause error) error {
	if cause == nil {
		return codex.New(ERROR__SCHEDULER_CANCELED)
	}
	if codex.IsCode(cause, ERROR__SCHEDULER_CANCELED) {
		return cause
	}
	return codex.Wrap(ERROR__SCHEDULER_CANCELED, cause)
}
