package orchestrator

import (
	"context"
	"strconv"
	"time"
)

// DrainStats is a self-consistent summary of task states at the end of a
// drain. InFlight is the number of attempts whose result has not yet been
// reconciled into persisted state; after a completed graceful drain it is
// always zero and Running is always zero.
type DrainStats struct {
	InFlight  int
	Pending   int
	Running   int
	RetryWait int
	Succeeded int
	Failed    int
	// SnapshotVersion is the version of the last state committed during drain.
	SnapshotVersion int
}

// drainStats computes the current drain statistics on (or independently of)
// the scheduler goroutine. The caller must hold e.mu.
func (e *Engine) drainStatsLocked() DrainStats {
	s := DrainStats{InFlight: e.inFlight, SnapshotVersion: e.snap.Version}
	for _, st := range e.snap.Tasks {
		switch st.Status {
		case StatusPending:
			s.Pending++
		case StatusRunning:
			s.Running++
		case StatusRetryWait:
			s.RetryWait++
		case StatusSucceeded:
			s.Succeeded++
		case StatusFailed:
			s.Failed++
		}
	}
	return s
}

// DrainStats returns a snapshot of the current drain statistics. It is safe to
// call concurrently. After a completed graceful drain InFlight and Running are
// guaranteed zero and the per-state counts exactly match Snapshot().
func (e *Engine) DrainStats() DrainStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.drainStatsLocked()
}

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

// drainComplete reports whether the drain has no more work: every dispatched
// attempt has had its result reconciled AND persisted on the scheduler
// goroutine (inFlight == 0), and no task is left in "running". The snapshot
// check is defense in depth: a drain must never be reported complete while the
// committed state still contains an ownerless running task.
func (e *Engine) drainComplete() bool {
	if e.inFlight != 0 {
		return false
	}
	for _, st := range e.snap.Tasks {
		if st.Status == StatusRunning {
			return false
		}
	}
	return true
}

// finishDrain verifies the post-drain invariant and emits the final summary.
// The deferred close(stop) in runLoop unblocks Shutdown afterwards.
func (e *Engine) finishDrain() error {
	s := e.drainStatsLocked()
	if s.InFlight != 0 || s.Running != 0 {
		// Never finish a drain with unreconciled attempts or stranded running
		// tasks: that is exactly the ambiguity graceful shutdown exists to
		// remove.
		return &ValidationError{
			Reason: ReasonInvalidState,
			Detail: "internal: refusing to finish drain with in-flight=" +
				strconv.Itoa(s.InFlight) + " running=" + strconv.Itoa(s.Running) +
				" tasks",
		}
	}
	e.decide("graceful drain complete: no in-flight attempts remain; "+
		"stable state at version %d (succeeded=%d failed=%d pending=%d retry_wait=%d running=%d)",
		s.SnapshotVersion, s.Succeeded, s.Failed, s.Pending, s.RetryWait, s.Running)
	return ErrGracefulShutdown
}
