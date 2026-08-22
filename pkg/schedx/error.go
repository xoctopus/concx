package schedx

// Error defines error codes of scheduler
// +genx:code
type Error uint8

const (
	ERROR_UNDEFINED               Error = iota
	ERROR__REACH_MAX_PENDING            // reached max pending limitation
	ERROR__SCHEDULER_RERUN              // scheduler is already running
	ERROR__SCHEDULER_CANCELED           // scheduler is manual canceled
	ERROR__SCHEDULER_JOB_PANICKED       // scheduler job panicked
)
