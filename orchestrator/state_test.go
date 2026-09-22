package orchestrator

import (
	"errors"
	"testing"
	"time"
)

func t0() time.Time { return time.Unix(1000, 0) }

func TestValidateSnapshotRejectsContradictions(t *testing.T) {
	defs := map[string]TaskDef{"a": {ID: "a", Retry: RetryPolicy{MaxAttempts: 3}}}
	closed := func(class AttemptClass) Attempt {
		return Attempt{Seq: 1, StartedAt: t0(), EndedAt: t0().Add(time.Second), Class: class}
	}
	cases := map[string]*Snapshot{
		"unknown status": {
			Version: 1,
			Tasks:   map[string]*TaskState{"a": {Status: Status("bogus"), Attempts: []Attempt{closed(AttemptSucceeded)}, UpdatedAt: t0()}},
		},
		"succeeded without success attempt": {
			Version: 1,
			Tasks:   map[string]*TaskState{"a": {Status: StatusSucceeded, Attempts: []Attempt{closed(AttemptFailed)}, UpdatedAt: t0()}},
		},
		"two success attempts": {
			Version: 1,
			Tasks: map[string]*TaskState{"a": {Status: StatusSucceeded, Attempts: []Attempt{
				closed(AttemptSucceeded),
				{Seq: 2, StartedAt: t0().Add(2 * time.Second), EndedAt: t0().Add(3 * time.Second), Class: AttemptSucceeded},
			}, UpdatedAt: t0()}},
		},
		"gap in attempt sequence": {
			Version: 1,
			Tasks: map[string]*TaskState{"a": {Status: StatusFailed, Attempts: []Attempt{
				{Seq: 7, StartedAt: t0(), EndedAt: t0().Add(time.Second), Class: AttemptFailed},
			}, UpdatedAt: t0()}},
		},
		"pending with attempts": {
			Version: 1,
			Tasks:   map[string]*TaskState{"a": {Status: StatusPending, Attempts: []Attempt{closed(AttemptFailed)}, UpdatedAt: t0()}},
		},
		"retry_wait without NextRunAt": {
			Version: 1,
			Tasks:   map[string]*TaskState{"a": {Status: StatusRetryWait, Attempts: []Attempt{closed(AttemptFailed)}, UpdatedAt: t0()}},
		},
		"failed ends with success": {
			Version: 1,
			Tasks:   map[string]*TaskState{"a": {Status: StatusFailed, Attempts: []Attempt{closed(AttemptSucceeded)}, UpdatedAt: t0()}},
		},
		"unknown attempt class": {
			Version: 1,
			Tasks: map[string]*TaskState{"a": {Status: StatusSucceeded, Attempts: []Attempt{
				{Seq: 1, StartedAt: t0(), EndedAt: t0().Add(time.Second), Class: AttemptClass("weird")},
			}, UpdatedAt: t0()}},
		},
		"attempt ended before start": {
			Version: 1,
			Tasks: map[string]*TaskState{"a": {Status: StatusFailed, Attempts: []Attempt{
				{Seq: 1, StartedAt: t0().Add(time.Hour), EndedAt: t0(), Class: AttemptFailed},
			}, UpdatedAt: t0()}},
		},
		"task count mismatch": {
			Version: 1,
			Tasks:   map[string]*TaskState{},
		},
	}
	for name, snap := range cases {
		t.Run(name, func(t *testing.T) {
			err := validateSnapshot(snap, defs)
			var ve *ValidationError
			if !errors.As(err, &ve) || ve.Reason != ReasonInvalidState {
				t.Fatalf("err=%v want ValidationError(%s)", err, ReasonInvalidState)
			}
		})
	}
}

func TestValidateSnapshotAcceptsRunningOpenAttempt(t *testing.T) {
	defs := map[string]TaskDef{"a": {ID: "a"}}
	snap := &Snapshot{Version: 1, Tasks: map[string]*TaskState{"a": {
		Status:    StatusRunning,
		Attempts:  []Attempt{{Seq: 1, StartedAt: t0()}}, // open attempt, no class/end
		UpdatedAt: t0(),
	}}}
	if err := validateSnapshot(snap, defs); err != nil {
		t.Fatalf("legit running state rejected: %v", err)
	}
}
