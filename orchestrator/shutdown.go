package orchestrator

import (
	"context"
)

// ErrShutdownInProgress is returned by Trigger after a graceful shutdown has
// been requested: no new work is admitted once draining starts.
var ErrShutdownInProgress = shutdownError{}

type shutdownError struct{}

func (shutdownError) Error() string {
	return "orchestrator: shutdown in progress, no new tasks are dispatched"
}

// Shutdown requests a graceful stop. It must be called after Run has started.
//
// Semantics once the request is accepted:
//   - no new task is dispatched; Trigger calls are rejected with
//     ErrShutdownInProgress and previously parked triggers are released with
//     that error;
//   - every attempt already in flight is allowed to finish and its result is
//     committed exactly as under normal operation (success, retry_wait with
//     its absolute NextRunAt, or terminal failure), so no task is left in an
//     ambiguous "running" state;
//   - pending / retry_wait tasks keep their exact persisted state and resume on
//     the next start from the durable progress.
//
// Shutdown blocks until draining completes or ctx is done. If ctx expires
// first, the execution context of the remaining attempts is cancelled; their
// open attempts are reconciled as "interrupted" on the next start, and
// Shutdown returns ctx.Err().
func (e *Engine) Shutdown(ctx context.Context) error {
	if !e.runStarted.Load() {
		return ErrEngineNotRunning
	}
	// At most one shutdown request; concurrent/later callers wait for the same
	// completion.
	if !e.shutdownRequested.CompareAndSwap(false, true) {
		select {
		case <-e.shutdownCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	reply := make(chan error, 1)
	ev := event{kind: evShutdown, reply: reply}

	// Arm the deadline watcher first: when the caller's ctx ends it cancels
	// the execution context. That makes the scheduler exit crash-style (open
	// attempts reconciled as interrupted on the next start), so the stop cannot
	// hang waiting on an uncooperative executor. Closing "abort" stops the
	// watcher once the request completes normally.
	abort := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			if e.execCancel != nil {
				e.execCancel()
			}
		case <-abort:
		}
	}()

	select {
	case e.events <- ev:
	case <-e.stop:
		// Run already ended (e.g. all tasks had settled).
		close(abort)
		<-watcherDone
		return nil
	case <-ctx.Done():
		// The scheduler may or may not have received the request. Either way it
		// will observe the cancellation/abort and stop; wait for that so the
		// caller's next start cannot race this process.
		<-e.stop
		return ctx.Err()
	}

	select {
	case err := <-reply:
		close(abort)
		<-watcherDone
		return err
	case <-e.shutdownCh:
		close(abort)
		<-watcherDone
		return nil
	case <-ctx.Done():
		// Scheduler still draining: the abort event/cancel already fired; wait
		// for the scheduler to actually return.
		<-e.stop
		return ctx.Err()
	}
}

// handleShutdown enters draining mode on the scheduler goroutine. It is called
// at most once.
func (e *Engine) handleShutdown(ev event) {
	e.shutdownReply = ev.reply
	e.draining = true
	e.decide("graceful shutdown requested: no new dispatches; draining %d in-flight attempt(s)", e.inFlight)
	// Parked explicit triggers must not wait forever for work that will never
	// be dispatched during this process.
	for id := range e.waiters {
		e.resolveWaiters(id, ErrShutdownInProgress)
	}
}

// drainCheck runs in place of scheduleAll while draining. It reports whether
// draining is complete (every in-flight worker has returned), in which case the
// scheduler loop returns.
func (e *Engine) drainCheck() bool {
	if e.inFlight > 0 {
		return false
	}
	e.decide("graceful shutdown: all in-flight attempts settled; exiting with durable progress intact")
	e.finishShutdown(nil)
	return true
}

// finishShutdown is invoked by the scheduler when it is about to return. It
// answers the Shutdown caller (if any) exactly once and unblocks waiters.
func (e *Engine) finishShutdown(err error) {
	if e.shutdownReply != nil {
		e.shutdownReply <- err
		e.shutdownReply = nil
	}
	select {
	case <-e.shutdownCh:
	default:
		close(e.shutdownCh)
	}
}
