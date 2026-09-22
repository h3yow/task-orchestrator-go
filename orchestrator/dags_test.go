package orchestrator

import (
	"errors"
	"testing"
	"time"
)

func TestValidationErrors(t *testing.T) {
	cases := []struct {
		name   string
		defs   []TaskDef
		reason ValidationReason
		taskID string
	}{
		{
			name: "duplicate id",
			defs: []TaskDef{
				{ID: "a"}, {ID: "a"},
			},
			reason: ReasonDuplicateID,
			taskID: "a",
		},
		{
			name: "empty id",
			defs: []TaskDef{
				{ID: ""},
			},
			reason: ReasonInvalidTask,
		},
		{
			name: "unknown dependency",
			defs: []TaskDef{
				{ID: "a", Deps: []string{"ghost"}},
			},
			reason: ReasonUnknownDep,
			taskID: "a",
		},
		{
			name: "self cycle",
			defs: []TaskDef{
				{ID: "a", Deps: []string{"a"}},
			},
			reason: ReasonCycle,
			taskID: "a",
		},
		{
			name: "two node cycle",
			defs: []TaskDef{
				{ID: "a", Deps: []string{"b"}},
				{ID: "b", Deps: []string{"a"}},
			},
			reason: ReasonCycle,
		},
		{
			name: "three node cycle",
			defs: []TaskDef{
				{ID: "a", Deps: []string{"b"}},
				{ID: "b", Deps: []string{"c"}},
				{ID: "c", Deps: []string{"a"}},
				{ID: "d", Deps: []string{"a"}},
			},
			reason: ReasonCycle,
		},
		{
			name: "duplicate dep listing",
			defs: []TaskDef{
				{ID: "a"},
				{ID: "b", Deps: []string{"a", "a"}},
			},
			reason: ReasonInvalidTask,
			taskID: "b",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.defs, Options{
				Store:    NewMemoryStore(),
				Executor: &countingExecutor{results: map[string]Result{}},
				Clock:    NewFakeClock(time.Unix(0, 0)),
				Logger:   testLogger(t),
			})
			if err == nil {
				t.Fatalf("expected ValidationError(%s), got nil", tc.reason)
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("expected *ValidationError, got %T: %v", err, err)
			}
			if ve.Reason != tc.reason {
				t.Fatalf("reason=%s, want %s (detail=%s)", ve.Reason, tc.reason, ve.Detail)
			}
			if tc.taskID != "" && ve.TaskID != tc.taskID {
				t.Fatalf("taskID=%q, want %q", ve.TaskID, tc.taskID)
			}
		})
	}
}

// TestValidationBeforeExecution proves nothing runs when the graph is invalid.
func TestValidationBeforeExecution(t *testing.T) {
	exec := newScriptedExecutor()
	e, err := New([]TaskDef{
		{ID: "a"},
		{ID: "a"},
	}, Options{Store: NewMemoryStore(), Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err == nil {
		t.Fatalf("expected error, got engine %v", e)
	}
	if exec.callCount("a") != 0 {
		t.Fatalf("executor ran %d times despite invalid definition", exec.callCount("a"))
	}
	// New returned no engine, so Run can never start.
	if e != nil {
		t.Fatalf("expected nil engine on validation failure")
	}
}
