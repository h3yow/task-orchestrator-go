package orchestrator

import (
	"context"
	"time"
)

// Shutdown requests a graceful drain of the engine and blocks until the drain
// is complete or ctx is done.
//
// Drain semantics:
//
//  1. the scheduler is marked "draining" immediately: it dispatches no new
//     task (neither first runs nor retries) and accepts no new Trigger parking;
//  2. attempts already in flight keep running on their own context and their
//     normal results are committed if they finish in time;
//  3. if ctx has a deadline, attempts still running when it expires have their
//     context cancelled; their attempt is closed as "interrupted" with an
//     auditable reason, and the task lands in retry_wait (budget permitting) or
//     terminal failed — the exact same stable destinations crash recovery uses;
//  4. parked Trigger callers are unblocked with ErrEngineNotRunning.
//
// Every state produced during the drain is committed before Shutdown returns,
// so a subsequent New over the same Store resumes without any open ambiguity.
//
// Run returns ErrGracefulShutdown after the drain. A ctx deadline does not make
// Shutdown return an error by itself: it only triggers forced cancellation of
// remaining attempts; Shutdown then waits for those reconciliations to be
// persisted (well-behaved executors return promptly on context cancellation).
// Calling Shutdown before Run, or after the scheduler already stopped, is a
// no-op returning nil.
func (e *Engine) Shutdown(ctx context.Context) error {
	if !e.shutdownStart.CompareAndSwap(false, true) {
		// Another caller already initiated the drain; just wait for the stop.
		return waitStopped(ctx, e.stop)
	}
	if !e.runStarted.Load() {
		// Run may not have flipped runStarted yet when Shutdown is called right
		// after "go e.Run(...)". Wait briefly for the scheduler to come up (or to
		// stop) instead of wrongly treating this as "Run never started".
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if e.runStarted.Load() {
				break
			}
			select {
			case <-e.stop:
				return nil // Run already terminated (e.g. everything settled)
			case <-ctx.Done():
				return ctx.Err()
			default:
				time.Sleep(time.Millisecond)
			}
		}
		if !e.runStarted.Load() {
			return nil
		}
	}

	close(e.shutdown)
	e.decide("graceful shutdown requested: no new tasks will be dispatched")

	// Phase 1: let in-flight attempts finish naturally until the caller's
	// grace period expires (or forever, with context.Background).
	if deadline, ok := ctx.Deadline(); ok {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		select {
		case <-e.stop:
			return nil
		case <-ctx.Done():
			// ctx was cancelled outright before the deadline: still force the
			// drain below so persisted state cannot be left with open attempts.
		case <-timer.C:
		}
	} else {
		select {
		case <-e.stop:
			return nil
		case <-ctx.Done():
		}
	}

	// Phase 2: grace period over — cancel every still-running attempt.
	e.decide("shutdown grace period over; forcing cancellation of in-flight attempts")
	close(e.forceDrain)

	// Phase 3: wait for the scheduler to reconcile and persist every forced
	// attempt. We deliberately do NOT honor ctx here: returning while attempts
	// were still open would defeat the purpose of graceful shutdown.
	<-e.stop
	return nil
}

func waitStopped(ctx context.Context, stop <-chan struct{}) error {
	select {
	case <-stop:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// beginDrain enters draining mode. Runs on the scheduler goroutine. From this
// point on startAttempt refuses every dispatch (initial scheduling and retry
// scheduling still run, so deps-failed syntheses are committed, but no task is
// sent to an executor). Parked Trigger callers are released.
func (e *Engine) beginDrain() {
	if e.draining {
		return
	}
	e.draining = true
	e.decide("scheduler entering drain mode: dispatch is disabled, resolving parked triggers")
	for id := range e.waiters {
		e.resolveWaiters(id, ErrEngineNotRunning)
	}
}

// drainComplete reports whether the drain has no more work: every attempt
// dispatched by this scheduler has had its result event reconciled and its new
// state persisted. It is deliberately NOT derived from the concurrency
// semaphore: a worker releases its semaphore slot before delivering the
// attempt-done event, so with several attempts finishing at nearly the same
// instant len(sem) can be zero while their result events are still queued in
// the buffered events channel. inFlight, tracked on the scheduler goroutine,
// only reaches zero after those events have each committed a task state — that
// is what "drained" means.
func (e *Engine) drainComplete() bool { return e.inFlight == 0 }

// finishDrain emits the final drain summary and verifies the stopped state is
// self-consistent. The deferred close(stop) in runLoop unblocks Shutdown
// afterwards.
func (e *Engine) finishDrain() error {
	pending, running, waiting, succeeded, failed := 0, 0, 0, 0, 0
	for _, st := range e.snap.Tasks {
		switch st.Status {
		case StatusPending:
			pending++
		case StatusRunning:
			running++
		case StatusRetryWait:
			waiting++
		case StatusSucceeded:
			succeeded++
		case StatusFailed:
			failed++
		}
	}
	if running != 0 || e.inFlight != 0 {
		// Must be unreachable: drainComplete() is only true when every
		// dispatched attempt has committed its terminal/waiting state. A
		// mismatch here is an engine state-machine bug; failing loudly is far
		// better than handing an inconsistent snapshot to a restart.
		panic("orchestrator: internal drain invariant violated: " +
			"in-flight attempts without a committed home")
	}
	e.decide("graceful drain complete: every in-flight attempt reconciled and persisted; "+
		"stable state at version %d (succeeded=%d failed=%d pending=%d retry_wait=%d running=%d)",
		e.snap.Version, succeeded, failed, pending, waiting, running)
	return ErrGracefulShutdown
}
