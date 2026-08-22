/*
Package chanx provides paradigmatic goroutine communication: observable
values, subjects, and cancelable subscriptions over channels.

This is the communication axis of the concx toolkit (alongside nest for
lifecycle and schedx for orchestration). Prefer this module path over the
archived github.com/xoctopus/x/chanx package.

Key types:
  - [NotifiableObserver]: send and receive values until CancelCause
  - [Subject]: fan-out Send to subscribers; Observe returns an [Observer]
  - [Observer] / [Subscriber]: value stream plus Done/Err lifecycle

A nil cancel cause is normalized to [ErrCompleted].
*/
package chanx
