package orchestrator

import (
	"context"
	"time"
)

// runAttempt executes exactly one attempt of a task on a worker goroutine.
//
// Duplicate-effect safety:
//   - the scheduler only ever has one in-flight attempt per task (it leaves
//     "running" exactly once until the reply arrives),
//   - if the attempt timeout fires, ctx is cancelled and the attempt is
//     closed as AttemptTimeout; a Result that comes back afterwards is
//     delivered but the scheduler ignores it because the task's recorded
//     attempt seq no longer matches.
func (e *Engine) runAttempt(taskID string, seq int) {
	defer func() { <-e.sem }()

	def := e.defs[taskID]
	// Derive from the engine's execution context so cancelling Run cancels
	// every in-flight executor; the timeout gets its own independent cancel.
	ctx, cancel := context.WithCancel(e.execCtx)
	defer cancel()

	var timeoutCh <-chan time.Time
	if def.Timeout > 0 {
		timeoutCh = e.clock.After(def.Timeout)
	}

	// Buffered channel: if the timeout wins and the scheduler stops reading,
	// the executor goroutine can still return without leaking.
	resCh := make(chan Result, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				resCh <- Result{OK: false, Retryable: true,
					Reason: "executor panicked: " + panicText(r)}
			}
		}()
		resCh <- e.executor.Execute(ctx, taskID)
	}()

	select {
	case <-timeoutCh:
		cancel()
		e.decide("task=%q attempt #%d TIMED OUT after %s; cancelling executor context",
			taskID, seq, def.Timeout)
		e.deliver(event{kind: evAttemptDone, taskID: taskID, seq: seq, result: nil})
		// Wait for the (cancelled) executor goroutine to actually return so no
		// user goroutine is leaked. Its late result is ignored by the scheduler
		// via the attempt-seq check.
		<-resCh
	case r := <-resCh:
		e.deliver(event{kind: evAttemptDone, taskID: taskID, seq: seq, result: &r})
	}
}

func (e *Engine) deliver(ev event) {
	if e.execCtx == nil {
		// Run never started (defensive; events should not exist then).
		return
	}
	select {
	case e.events <- ev:
	case <-e.execCtx.Done():
	case <-e.stop:
	}
}

func panicText(r any) string {
	switch v := r.(type) {
	case error:
		return v.Error()
	case string:
		return v
	default:
		return "unknown panic"
	}
}

// dependencyStatus summarises what happened to a task's dependencies.
type dependencyStatus int

const (
	depsReady   dependencyStatus = iota // every dependency succeeded
	depsPending                         // still something running / waiting
	depsBlocked                         // a dependency failed terminally
)

// checkDeps is a pure read of current states.
func (e *Engine) checkDeps(id string) dependencyStatus {
	for _, dep := range e.defs[id].Deps {
		switch e.snap.Tasks[dep].Status {
		case StatusSucceeded:
			continue
		case StatusFailed:
			return depsBlocked
		default:
			return depsPending
		}
	}
	return depsReady
}

// allTerminal reports whether every task reached a terminal state.
func (e *Engine) allTerminal() bool {
	for _, st := range e.snap.Tasks {
		if !st.Status.terminal() {
			return false
		}
	}
	return true
}

// terminalCounts is used in scheduling summaries and tests.
func (e *Engine) terminalCounts() (succeeded, failed int) {
	for _, st := range e.snap.Tasks {
		switch st.Status {
		case StatusSucceeded:
			succeeded++
		case StatusFailed:
			failed++
		}
	}
	return
}
