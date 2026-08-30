/*
Package pipe is an orch recipe: typed linear pipeline with per-Push [Result].

Build with [FromJob] / [FromUniversalJobs], then [NodeOrch.Then], [NodeOrch.Parallel],
[NodeOrch.EndJob] / [NodeOrch.EndJobs], and [Builder.Build]. Lifecycle:

	Build → Run → Push* → Close

[Scheduler] matches [schedx.RetrievableScheduler] in shape. Admission uses a
RetrievableScheduler whose Job injects into stage pumps (chanx) and waits on a
ticket until Tail or failure. [Scheduler.Pending] is admission queue depth, not
in-stage count. [Scheduler.Close] fails unfinished Results immediately and stops
stage pumps.

Node jobs are [TransformJob] / [TransformFunc] (In → Out). Parallel stages fan out
UniversalTransformJob and merge with a summary. This package does not re-export
chanx.
*/
package pipe
