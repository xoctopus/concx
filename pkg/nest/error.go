package nest

// Error defines error codes in nest
// +genx:code
type Error uint8

const (
	ERROR_UNDEFINED Error = iota
	ERROR__NEST_CLOSED
	ERROR__NEST_CLOSE_TIMEOUT
)
