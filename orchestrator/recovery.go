package orchestrator

import "time"

// recover reconciles a freshly loaded snapshot before the scheduler starts.
//
// The ONLY ambiguous persisted situation is a task in "running" state with an
// open attempt: the process died while the executor held the attempt. Its fate
// must be explicit and stable:
//
//   - the open attempt is closed as AttemptInterrupted (auditable reason),
//   - if attempt budget remains, the task goes to retry_wait with NextRunAt=now
//     (i.e. it is immediately eligible to run again — at-least-once delivery),
//   - otherwise the task reaches the terminal "failed" state with the
//     interruption recorded as the terminal cause.
//
// Every reconciliation decision is committed separately and logged. No task
// ever resumes silently from "running".
func (e *Engine) recover() error {
	now := e.clock.Now()
	recoveredAny := false
	for _, id := range e.order {
		st := e.snap.Tasks[id]
		if st.Status != StatusRunning || len(st.Attempts) == 0 {
			continue
		}
		recoveredAny = true
		open := &st.Attempts[len(st.Attempts)-1]
		if !open.EndedAt.IsZero() {
			// validateSnapshot should already have prevented this; keep the
			// guard so recovery never trusts inconsistent input.
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
				Detail: "running task has no open attempt during recovery"}
		}
		open.Class = AttemptInterrupted
		open.Reason = "engine process interrupted while attempt was in flight"
		open.EndedAt = now

		policy := e.defs[id].Retry
		used := len(st.Attempts)
		if used < policy.MaxAttempts {
			st.Status = StatusRetryWait
			st.NextRunAt = now
			st.UpdatedAt = now
			e.decide("recovery: task=%q attempt #%d closed as interrupted; %d/%d attempts used, "+
				"task moves to retry_wait and is immediately eligible", id, open.Seq, used, policy.MaxAttempts)
		} else {
			st.Status = StatusFailed
			st.NextRunAt = time.Time{}
			st.UpdatedAt = now
			e.decide("recovery: task=%q attempt #%d closed as interrupted; retry budget exhausted "+
				"(%d/%d), task moves to terminal failed", id, open.Seq, used, policy.MaxAttempts)
		}
		if err := e.persist("recovery reconciliation for task " + id); err != nil {
			return err
		}
	}
	if !recoveredAny {
		e.decide("recovery: no in-flight attempts found, state is consistent")
	}
	return nil
}

// armPersistedRetryWaits (re)arms the backoff timer for every task that is
// already in retry_wait when the scheduler starts:
//
//   - tasks that arrived here via recover() have NextRunAt=start time and are
//     already eligible (the first scheduling pass dispatches them; no timer is
//     needed for a due task),
//   - tasks that were parked in retry_wait when the process stopped keep the
//     ORIGINAL NextRunAt that was persisted. A restart must neither restart the
//     wait nor strand the task: if the deadline already passed it is dispatched
//     immediately by the scheduling pass; if it is still in the future the
//     original deadline is re-armed here, so the total wait is never extended by
//     repeated restarts.
//
// Called once at scheduler start, on the scheduler goroutine.
func (e *Engine) armPersistedRetryWaits() {
	rearmed := 0
	for _, id := range e.order {
		st := e.snap.Tasks[id]
		if st.Status != StatusRetryWait {
			continue
		}
		if !e.clock.Now().Before(st.NextRunAt) {
			// Already due: the initial scheduling pass logs the decision and
			// dispatches (or, during a drain, leaves the task eligible).
			continue
		}
		remaining := st.NextRunAt.Sub(e.clock.Now())
		e.decide("restart: task=%q still within persisted retry wait; re-arming original deadline %s (%s remaining, wait is not reset)",
			id, st.NextRunAt.Format(time.RFC3339Nano), remaining)
		e.armRetry(id, st.NextRunAt)
		rearmed++
	}
	if rearmed == 0 {
		e.decide("restart: no pending retry waits need re-arming")
	}
}

// persist validates the current snapshot, advances its version atomically and
// commits it. It must be called on the scheduler goroutine (or before Run).
func (e *Engine) persist(why string) error {
	e.snap.Version++
	if err := validateSnapshot(e.snap, e.defs); err != nil {
		return err
	}
	if err := e.store.Commit(e.snap); err != nil {
		return err
	}
	e.decide("persisted decision (%s) at version %d", why, e.snap.Version)
	return nil
}
