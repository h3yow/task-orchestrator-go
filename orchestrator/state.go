package orchestrator

import (
	"fmt"
	"time"
)

// allowedTransitions is the legal state machine. Any transition not listed
// is rejected explicitly (no silent state rollback).
//
//	pending    -> running | retry_wait (recovery only: running crashed) | failed (deps failed)
//	running    -> succeeded | failed | retry_wait
//	retry_wait -> running | failed (deps failed while waiting)
//	succeeded  -> (terminal, no outgoing edge)
//	failed     -> (terminal, no outgoing edge)
var allowedTransitions = map[Status]map[Status]bool{
	StatusPending: {
		StatusRunning:   true,
		StatusFailed:    true, // synthesized dependency-failure
		StatusRetryWait: true, // recovery: crashed while running (interruption counted)
		StatusSucceeded: true, // recovery: commit of a success was durable, attempt lost
	},
	StatusRunning: {
		StatusSucceeded: true,
		StatusFailed:    true,
		StatusRetryWait: true,
	},
	StatusRetryWait: {
		StatusRunning: true,
		StatusFailed:  true, // dependency failed while this task was waiting
	},
	StatusSucceeded: {},
	StatusFailed:    {},
}

// canTransition reports whether dst is a legal successor of src.
func canTransition(src, dst Status) bool {
	return allowedTransitions[src][dst]
}

// newInitialState builds the state of a task that has never run.
func newInitialState(now time.Time) *TaskState {
	return &TaskState{
		Status:    StatusPending,
		Attempts:  nil,
		UpdatedAt: now,
	}
}

// validateSnapshot rejects persisted content that is self-contradictory or
// could cause state rollback. defs are the validated task definitions; the
// snapshot must describe exactly the same task set.
func validateSnapshot(snap *Snapshot, defs map[string]TaskDef) error {
	if snap == nil {
		return &ValidationError{Reason: ReasonInvalidState, Detail: "snapshot is nil"}
	}
	if snap.Version < 0 {
		return &ValidationError{Reason: ReasonInvalidState, Detail: fmt.Sprintf("snapshot version must be >= 0, got %d", snap.Version)}
	}
	if snap.Tasks == nil {
		return &ValidationError{Reason: ReasonInvalidState, Detail: "snapshot has nil tasks map"}
	}
	if len(snap.Tasks) != len(defs) {
		return &ValidationError{Reason: ReasonInvalidState, Detail: fmt.Sprintf(
			"snapshot task count %d does not match definition count %d", len(snap.Tasks), len(defs))}
	}
	for id := range defs {
		if _, ok := snap.Tasks[id]; !ok {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id, Detail: "task missing from persisted state"}
		}
	}
	for id, st := range snap.Tasks {
		if _, ok := defs[id]; !ok {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id, Detail: "persisted state contains task that is not defined"}
		}
		if err := validateTaskState(id, st); err != nil {
			return err
		}
	}
	return nil
}

func validateTaskState(id string, st *TaskState) error {
	if st == nil {
		return &ValidationError{Reason: ReasonInvalidState, TaskID: id, Detail: "state is nil"}
	}
	switch st.Status {
	case StatusPending, StatusRunning, StatusRetryWait, StatusSucceeded, StatusFailed:
	default:
		return &ValidationError{Reason: ReasonInvalidState, TaskID: id, Detail: "unknown status " + quote(string(st.Status))}
	}

	// Attempts are an append-only, gap-free, 1-based sequence. The LAST
	// attempt of a running task is the open in-flight attempt (zero EndedAt).
	n := len(st.Attempts)
	for i, a := range st.Attempts {
		isOpen := st.Status == StatusRunning && i == n-1
		if a.Seq != i+1 {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
				Detail: fmt.Sprintf("attempt at position %d has seq %d (must be %d)", i, a.Seq, i+1)}
		}
		if a.StartedAt.IsZero() || (!isOpen && a.EndedAt.IsZero()) {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
				Detail: fmt.Sprintf("attempt %d has zero timestamps", a.Seq)}
		}
		if !isOpen && a.EndedAt.Before(a.StartedAt) {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
				Detail: fmt.Sprintf("attempt %d ended before it started", a.Seq)}
		}
		switch a.Class {
		case AttemptSucceeded, AttemptFailed, AttemptTimeout, AttemptInterrupted, AttemptSkippedDeps:
		case "":
			if !isOpen {
				return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
					Detail: fmt.Sprintf("attempt %d has empty class", a.Seq)}
			}
		default:
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
				Detail: fmt.Sprintf("attempt %d has unknown class %q", a.Seq, a.Class)}
		}
		// Exactly one success attempt is ever recorded.
		if a.Class == AttemptSucceeded {
			for j, b := range st.Attempts {
				if j != i && b.Class == AttemptSucceeded {
					return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
						Detail: fmt.Sprintf("multiple success attempts recorded (seq %d and %d)", b.Seq, a.Seq)}
				}
			}
		}
	}

	switch st.Status {
	case StatusPending:
		if n != 0 {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
				Detail: fmt.Sprintf("pending task already has %d attempts", n)}
		}
	case StatusRunning:
		if n == 0 {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id, Detail: "running task has no attempt"}
		}
		// The last attempt of a normally-committed running state must be open
		// (zero EndedAt). All earlier attempts must be closed.
		last := st.Attempts[n-1]
		if !last.EndedAt.IsZero() {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
				Detail: "running task's last attempt is already closed"}
		}
	case StatusSucceeded:
		if n == 0 || st.Attempts[n-1].Class != AttemptSucceeded {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
				Detail: "succeeded task must end with a success attempt"}
		}
	case StatusRetryWait:
		if n == 0 {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id, Detail: "retry_wait task has no attempt"}
		}
		if st.NextRunAt.IsZero() {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
				Detail: "retry_wait task has no NextRunAt"}
		}
	case StatusFailed:
		// failed tasks must carry at least one closed terminal attempt
		// (failure/timeout/interruption/deps_failed/success is impossible).
		if n == 0 {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id, Detail: "failed task has no attempt"}
		}
		last := st.Attempts[n-1]
		if last.Class == AttemptSucceeded {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
				Detail: "failed task ends with a success attempt (contradictory state)"}
		}
	}

	// Every attempt except the open "running" one must be closed, and closed
	// attempts must be ordered chronologically (they are append-only evidence).
	for i, a := range st.Attempts {
		if st.Status == StatusRunning && i == n-1 {
			continue // open in-flight attempt
		}
		if a.EndedAt.IsZero() {
			return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
				Detail: fmt.Sprintf("attempt %d is not running but is unclosed", a.Seq)}
		}
		if i > 0 {
			prev := st.Attempts[i-1]
			if !prev.EndedAt.IsZero() && a.StartedAt.Before(prev.EndedAt) {
				return &ValidationError{Reason: ReasonInvalidState, TaskID: id,
					Detail: fmt.Sprintf("attempt %d started (%s) before previous attempt ended (%s)",
						a.Seq, a.StartedAt.Format(time.RFC3339Nano), prev.EndedAt.Format(time.RFC3339Nano))}
			}
		}
	}

	return nil
}
