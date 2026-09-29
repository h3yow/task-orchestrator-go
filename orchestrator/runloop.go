package orchestrator

import (
	"context"
	"errors"
	"strconv"
	"time"
)

// runLoop is the single scheduler goroutine. It owns the snapshot; worker
// goroutines and Trigger callers communicate only through events.
func (e *Engine) runLoop(ctx context.Context) error {
	if !e.runStarted.CompareAndSwap(false, true) {
		return errors.New("orchestrator: Run called more than once")
	}
	execCtx, cancel := context.WithCancel(ctx)
	e.execCtx = execCtx
	e.execCancel = cancel
	defer cancel()
	defer close(e.stop)

	e.decide("scheduler starting: %d tasks, max concurrent = %d", len(e.order), e.maxConc)

	// Initial scheduling pass.
	e.mu.Lock()
	e.scheduleAll("initial scheduling")
	e.armPersistedRetryWaits()
	e.mu.Unlock()

	for {
		e.mu.RLock()
		done := e.allTerminal()
		e.mu.RUnlock()
		if done {
			e.finishSettled()
			s, f := e.terminalCounts()
			e.decide("all tasks terminal: succeeded=%d failed=%d; scheduler stopping", s, f)
			return nil
		}

		ctxCh := ctx.Done()
		// A closed channel stays readable forever: once a drain signal has been
		// consumed, nil its select case so the loop goes back to waiting for
		// attempt events instead of spinning on the closed channel.
		shutdownCh := e.shutdown
		if e.draining {
			shutdownCh = nil
		}
		forceCh := e.forceDrain
		if e.forceDrainClosed {
			forceCh = nil
		}
		select {
		case <-ctxCh:
			e.decide("scheduler cancelled by context: %v", ctx.Err())
			return ctx.Err()
		case <-shutdownCh:
			e.beginDrain()
			// A drain request may arrive when nothing is in flight (every task
			// was already terminal or pending-with-blocked-deps is impossible
			// here since terminal is handled above; but parked waiters and a
			// zero-semaphore state must still be reconciled).
			e.mu.Lock()
			e.scheduleAll("shutdown scheduling (no new dispatches)")
			if e.drainComplete() {
				err := e.finishDrain()
				e.mu.Unlock()
				return err
			}
			e.mu.Unlock()
		case <-forceCh:
			e.forceDrainClosed = true
			e.decide("forced drain: cancelling executor contexts of in-flight attempts")
			e.execCancel()
		case ev := <-e.events:
			// Shutdown is treated exactly like a crash: when ctx and an event
			// race, prefer the shutdown interpretation once ctx is done. No
			// attempt result is committed; in-flight tasks stay "running" with
			// an open attempt and get their deterministic home from recovery
			// on the next start.
			select {
			case <-ctxCh:
				e.decide("ignoring late event %d for task=%q during shutdown", ev.kind, ev.taskID)
				return ctx.Err()
			default:
			}
			e.mu.Lock()
			switch ev.kind {
			case evAttemptDone:
				e.handleAttemptDone(ev)
			case evRetryDue:
				e.handleRetryDue(ev)
			case evTrigger:
				e.handleTrigger(ev)
			}
			// Any event may have unblocked multiple tasks.
			e.scheduleAll("post-event scheduling")
			if e.draining && e.drainComplete() {
				err := e.finishDrain()
				e.mu.Unlock()
				return err
			}
			e.mu.Unlock()
		}
	}
}

func (e *Engine) finishSettled() {
	select {
	case <-e.settled:
	default:
		close(e.settled)
	}
}

// scheduleAll makes every currently-possible scheduling decision once, in a
// fixed order, and explains each one. It runs entirely on the scheduler
// goroutine.
func (e *Engine) scheduleAll(why string) {
	e.decide("scheduling pass (%s)", why)
	for _, id := range e.order {
		st := e.snap.Tasks[id]
		switch st.Status {
		case StatusPending:
			e.schedulePending(id, st)
		case StatusRetryWait:
			e.scheduleRetryWait(id, st)
		}
	}
}

func (e *Engine) schedulePending(id string, st *TaskState) {
	switch e.checkDeps(id) {
	case depsPending:
		// no decision yet; this log is suppressed to avoid noise per pass
		return
	case depsBlocked:
		e.failTaskFromDeps(id, st)
	case depsReady:
		e.startAttempt(id, "dependencies satisfied, dispatching automatically")
	}
}

func (e *Engine) scheduleRetryWait(id string, st *TaskState) {
	if e.clock.Now().Before(st.NextRunAt) {
		return
	}
	e.decide("task=%q retry deadline %s already elapsed at %s; eligible immediately",
		id, st.NextRunAt.Format(time.RFC3339Nano), e.clock.Now().Format(time.RFC3339Nano))
	// Backoff elapsed. A dependency may have failed while we waited.
	switch e.checkDeps(id) {
	case depsBlocked:
		e.failTaskFromDeps(id, st)
	case depsReady, depsPending:
		e.startAttempt(id, "retry backoff elapsed, dispatching next attempt")
	}
}

// failTaskFromDeps records a synthesized non-retryable attempt
// (AttemptSkippedDeps) without consuming the executor budget, and puts the
// task in terminal failed. A pending task with 0 attempts needs the record
// created first.
func (e *Engine) failTaskFromDeps(id string, st *TaskState) {
	now := e.clock.Now()
	var failedDep string
	for _, dep := range e.defs[id].Deps {
		if e.snap.Tasks[dep].Status == StatusFailed {
			failedDep = dep
			break
		}
	}
	rec := Attempt{
		Seq:       len(st.Attempts) + 1,
		StartedAt: now,
		EndedAt:   now,
		Class:     AttemptSkippedDeps,
		Reason:    "skipped because dependency " + quote(failedDep) + " reached terminal failure",
	}
	st.Attempts = append(st.Attempts, rec)
	if err := e.transition(id, st, StatusFailed, "upstream failure: "+failedDep, now); err != nil {
		panic(err) // indicates an internal state-machine bug, not a user failure
	}
	st.NextRunAt = time.Time{}
	if err := e.persist("task " + id + " failed due to upstream task " + failedDep); err != nil {
		panic(err)
	}
	e.resolveWaiters(id, ErrDependencyFailed)
}

// startAttempt is the ONLY place a task is dispatched to a worker. It checks
// the concurrency semaphore and rejects any dispatch that would violate the
// state machine, so a task can never execute twice concurrently.
func (e *Engine) startAttempt(id, reason string) {
	st := e.snap.Tasks[id]
	from := st.Status
	if e.draining {
		// Graceful shutdown in progress: nothing new is dispatched, not even a
		// task whose backoff just elapsed or whose deps just succeeded. Its
		// persisted status (pending / retry_wait with the ORIGINAL NextRunAt) is
		// its stable home until the next start resumes it.
		e.decide("task=%q NOT dispatched during drain (status=%s): %s", id, from, reason)
		return
	}
	if !canTransition(from, StatusRunning) {
		e.decide("task=%q NOT dispatched: illegal transition %s -> running (ignored)", id, from)
		return
	}
	select {
	case e.sem <- struct{}{}:
	default:
		e.decide("task=%q ready but concurrency limit (%d) reached; deferred", id, e.maxConc)
		return
	}

	now := e.clock.Now()
	seq := len(st.Attempts) + 1
	st.Attempts = append(st.Attempts, Attempt{Seq: seq, StartedAt: now})
	st.Status = StatusRunning
	st.NextRunAt = time.Time{}
	st.UpdatedAt = now
	if err := validateSnapshot(e.snap, e.defs); err != nil {
		e.decide("INTERNAL validation after dispatch of %q: %v", id, err)
		panic(err)
	}
	if err := e.persist("task " + id + " transition " + string(from) + " -> running"); err != nil {
		panic(err)
	}
	e.decide("DISPATCH task=%q attempt #%d: %s", id, seq, reason)
	e.resolveWaiters(id, nil)
	e.inFlight++

	go e.runAttempt(id, seq)
}

func (e *Engine) handleAttemptDone(ev event) {
	st := e.snap.Tasks[ev.taskID]
	if st.Status != StatusRunning {
		e.decide("task=%q stale attempt reply ignored: task is %s, not running", ev.taskID, st.Status)
		return
	}
	n := len(st.Attempts)
	if n == 0 || st.Attempts[n-1].Seq != ev.seq {
		e.decide("task=%q stale attempt reply ignored: current seq=%d reply seq=%d",
			ev.taskID, func() int {
				if n == 0 {
					return -1
				}
				return st.Attempts[n-1].Seq
			}(), ev.seq)
		return
	}
	// This is the reconciliation of exactly one dispatched attempt. The
	// in-flight count may only drop once this function has classified the
	// result, transitioned the task and persisted it below — never on entry
	// (the worker releasing its semaphore slot is not the same event as the
	// result being committed to state).
	defer func() { e.inFlight-- }()

	now := e.clock.Now()
	a := &st.Attempts[n-1]
	a.EndedAt = now
	policy := e.defs[ev.taskID].Retry
	used := n

	switch {
	case ev.forced:
		// Forced graceful drain: the executor context was cancelled because the
		// shutdown grace period expired. The executor's own verdict is not
		// trusted (its side effects may or may not have happened); the attempt
		// is closed as interrupted exactly like a crash, and gets the same
		// retry/budget decision on the next start.
		a.Class = AttemptInterrupted
		a.Reason = "cancelled during forced graceful shutdown drain"
	case ev.result == nil:
		// Timeout fired on the engine side — a deterministic terminal-ish event.
		a.Class = AttemptTimeout
		a.Reason = "attempt exceeded task timeout"
	case ev.result.OK:
		a.Class = AttemptSucceeded
		a.Reason = ev.result.Reason
	default:
		a.Class = AttemptFailed
		a.Retryable = ev.result.Retryable
		a.Reason = ev.result.Reason
	}

	// Decide the next state. The decision table:
	//   success                    -> succeeded (exactly once; unique attempt)
	//   attempts exhausted         -> failed   (deterministic terminal cause)
	//   timeout / retryable fail   -> retry_wait (only while budget remains)
	//   non-retryable failure      -> failed
	var next Status
	var why string
	switch {
	case a.Class == AttemptSucceeded:
		next, why = StatusSucceeded, "attempt succeeded: exactly one effective success committed"
	case used >= policy.MaxAttempts:
		next, why = StatusFailed, "retry budget exhausted ("+strconv.Itoa(used)+"/"+strconv.Itoa(policy.MaxAttempts)+")"
	case a.Class == AttemptTimeout:
		next, why = StatusRetryWait, "attempt timed out; scheduling retry"
	case a.Class == AttemptInterrupted:
		next, why = StatusRetryWait, "attempt interrupted by forced drain; scheduling retry on next start"
	case a.Class == AttemptFailed && !a.Retryable:
		next, why = StatusFailed, "failure marked non-retryable by executor"
	default:
		next, why = StatusRetryWait, "retryable failure; scheduling retry"
	}

	st.UpdatedAt = now
	if next == StatusRetryWait {
		wait := policy.Backoff(used + 1)
		st.NextRunAt = now.Add(wait)
	} else {
		st.NextRunAt = time.Time{}
	}
	if err := e.transition(ev.taskID, st, next, why, now); err != nil {
		panic(err)
	}
	if err := e.persist("task " + ev.taskID + " attempt #" + strconv.Itoa(ev.seq) + " finished [" + string(a.Class) + "]: " + why); err != nil {
		panic(err)
	}
	e.decide("RESULT task=%q attempt #%d class=%s reason=%q -> %s",
		ev.taskID, ev.seq, a.Class, a.Reason, next)

	if next == StatusRetryWait {
		if e.draining {
			// The persisted NextRunAt is the stable home; the next start re-arms
			// the original deadline. Do not register a timer the scheduler is
			// about to stop observing.
			e.decide("task %q parked in retry_wait until restart; deadline %s preserved (not reset)",
				ev.taskID, st.NextRunAt.Format(time.RFC3339Nano))
		} else {
			// Register the backoff timer synchronously, inside this scheduling
			// decision, so its registration is part of the atomic "move to
			// retry_wait" decision and cannot race with clock advancement. Only
			// the WAIT on it happens on another goroutine.
			e.armRetry(ev.taskID, st.NextRunAt)
		}
	}
	if next == StatusFailed || next == StatusSucceeded {
		e.resolveWaiters(ev.taskID, nil)
	}
}

func (e *Engine) handleRetryDue(ev event) {
	st := e.snap.Tasks[ev.taskID]
	if st.Status != StatusRetryWait {
		e.decide("retry timer for task=%q ignored: task is %s", ev.taskID, st.Status)
		return
	}
	e.decide("retry timer fired for task=%q (scheduled at %s)", ev.taskID, st.NextRunAt.Format(time.RFC3339Nano))
	// The next scheduling pass will dispatch it; dependencies are rechecked.
}

// armRetry registers the backoff timer synchronously (so registration is part
// of the retry_wait decision and cannot race clock advancement) and waits for
// it on a dedicated goroutine.
func (e *Engine) armRetry(taskID string, t time.Time) {
	now := e.clock.Now()
	d := t.Sub(now)
	if d < 0 {
		d = 0
	}
	ch := e.clock.After(d)
	go func() {
		select {
		case at := <-ch:
			e.deliver(event{kind: evRetryDue, taskID: taskID, at: at})
		case <-e.stop:
		}
	}()
}

func (e *Engine) handleTrigger(ev event) {
	st := e.snap.Tasks[ev.taskID]
	if e.draining {
		ev.reply <- ErrEngineNotRunning
		return
	}
	if st.Status != StatusPending {
		ev.reply <- &ErrTriggerIgnored{TaskID: ev.taskID}
		return
	}
	switch e.checkDeps(ev.taskID) {
	case depsBlocked:
		ev.reply <- ErrDependencyFailed
	case depsPending:
		// Park the caller; it is resolved when the task gets dispatched or
		// fails. The scheduler re-runs scheduling on every event, so there is
		// no missed wakeup.
		e.waiters[ev.taskID] = append(e.waiters[ev.taskID], ev.reply)
		e.decide("trigger for task=%q parked: dependencies not yet satisfied", ev.taskID)
	case depsReady:
		ev.reply <- nil
		// startAttempt is invoked by scheduleAll which runs right after this
		// event is handled.
	}
}

// transition mutates st.Status after enforcing the state machine. Any
// transition not present in allowedTransitions is an explicit hard error:
// state rollback can never slip through.
func (e *Engine) transition(id string, st *TaskState, dst Status, why string, now time.Time) error {
	if !canTransition(st.Status, dst) {
		return &ValidationError{
			Reason: ReasonInvalidState,
			TaskID: id,
			Detail: "illegal state transition " + string(st.Status) + " -> " + string(dst) + ": " + why,
		}
	}
	e.decide("STATE task=%q %s -> %s (%s)", id, st.Status, dst, why)
	st.Status = dst
	st.UpdatedAt = now
	return nil
}

func (e *Engine) resolveWaiters(id string, err error) {
	ws := e.waiters[id]
	for _, ch := range ws {
		ch <- err
	}
	delete(e.waiters, id)
}
