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
