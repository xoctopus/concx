/*
Package orch is the recipe layer of concx: opinionated scaffolding built on
nest, chanx, and schedx.

It is not a fourth capability axis. Advanced callers compose the three
primitives directly; orch packages name fixed, low-friction patterns.

Current recipes:

  - [github.com/xoctopus/concx/pkg/orch/pipe] - linear multi-stage pipeline
  - [github.com/xoctopus/concx/pkg/orch/piper] - inline typed operator chain
  - [github.com/xoctopus/concx/pkg/orch/cron] - cron and periodic job orchestrator
*/
package orch
