package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRecoverRunningTaskRetries: a task recorded as "running" with an open
// attempt at restart must be reconciled to retry_wait with an explicit
// "interrupted" attempt record, then run again and succeed.
func TestRecoverRunningTaskRetries(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	store := NewFileStore(path)
	defs := []TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 3}}}

	// Engine 1: dispatch and crash (simulate by stopping without closing).
	e1, err := New(defs, Options{Store: store, Executor: &alwaysBlockExecutor{}, Clock: NewFakeClock(time.Unix(100, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e1.Run(context.Background()) }()
	pollStatus(t, e1, "job", StatusRunning, 2*time.Second)
	// Simulate a hard process crash: leave the persisted "running" state and
	// start a brand new engine over the same file.

	exec2 := newScriptedExecutor().set("job", Result{OK: true, Reason: "after restart"})
	log := newCaptureLogger(t)
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: NewFakeClock(time.Unix(200, 0)), Logger: log})
	if err != nil {
		t.Fatalf("New after crash: %v", err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	st, _ := e2.State("job")
	if st.Status != StatusSucceeded {
		t.Fatalf("status=%s attempts=%+v", st.Status, st.Attempts)
	}
	if len(st.Attempts) != 2 {
		t.Fatalf("attempts=%d want 2 (interrupted + replay)", len(st.Attempts))
	}
	if st.Attempts[0].Class != AttemptInterrupted {
		t.Fatalf("first attempt class=%s want interrupted", st.Attempts[0].Class)
	}
	if st.Attempts[1].Class != AttemptSucceeded {
		t.Fatalf("second attempt class=%s want succeeded", st.Attempts[1].Class)
	}
	if exec2.callCount("job") != 1 {
		t.Fatalf("executor ran %d times after restart, want 1", exec2.callCount("job"))
	}
	if !log.contains("recovery") || !log.contains("interrupted") {
		t.Fatal("recovery decision not logged")
	}
}

// TestRecoverRunningTaskBudgetExhausted: crash on the last allowed attempt
// must produce a deterministic terminal failure, not an infinite retry.
func TestRecoverRunningTaskBudgetExhausted(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir + "/state.json")
	defs := []TaskDef{{ID: "job", Retry: RetryPolicy{MaxAttempts: 1}}}

	e1, err := New(defs, Options{Store: store, Executor: &alwaysBlockExecutor{}, Clock: NewFakeClock(time.Unix(100, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e1.Run(context.Background()) }()
	pollStatus(t, e1, "job", StatusRunning, 2*time.Second)

	exec2 := newScriptedExecutor().set("job", Result{OK: true}) // must never run
	log := newCaptureLogger(t)
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: NewFakeClock(time.Unix(200, 0)), Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	st, _ := e2.State("job")
	if st.Status != StatusFailed {
		t.Fatalf("status=%s want failed", st.Status)
	}
	if st.Attempts[len(st.Attempts)-1].Class != AttemptInterrupted {
		t.Fatalf("terminal attempt class=%s want interrupted", st.Attempts[len(st.Attempts)-1].Class)
	}
	if exec2.callCount("job") != 0 {
		t.Fatalf("executor ran despite exhausted budget: %d", exec2.callCount("job"))
	}
}

// TestRecoverCompletedTaskNotRerun: tasks already terminal before the crash
// stay exactly as they were and never re-execute.
func TestRecoverCompletedTaskNotRerun(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir + "/state.json")
	defs := []TaskDef{
		{ID: "a"},
		{ID: "b", Deps: []string{"a"}},
	}

	exec1 := &countingExecutor{results: map[string]Result{"a": {OK: true}, "b": {OK: true}}}
	e1, err := New(defs, Options{Store: store, Executor: exec1, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	verAfterRun := e1.Snapshot().Version

	// Restart over the completed state with a fresh counting executor.
	exec2 := &countingExecutor{results: map[string]Result{"a": {OK: true}, "b": {OK: true}}}
	log := newCaptureLogger(t)
	e2, err := New(defs, Options{Store: store, Executor: exec2, Clock: NewFakeClock(time.Unix(1000, 0)), Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if exec2.callCount("a") != 0 || exec2.callCount("b") != 0 {
		t.Fatalf("completed tasks re-executed: a=%d b=%d", exec2.callCount("a"), exec2.callCount("b"))
	}
	if e2.Snapshot().Version != verAfterRun {
		t.Fatalf("version advanced after no-op restart: %d -> %d", verAfterRun, e2.Snapshot().Version)
	}
	for _, id := range []string{"a", "b"} {
		st, _ := e2.State(id)
		if st.Status != StatusSucceeded {
			t.Fatalf("%s=%s", id, st.Status)
		}
	}
}

// TestCorruptOrRolledBackSnapshotRejected: hand-edit the persisted file into
// self-contradictory content; New must refuse it with a distinguishable error
// rather than resuming from garbage.
func TestCorruptOrRolledBackSnapshotRejected(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	store := NewFileStore(path)
	defs := []TaskDef{{ID: "a"}}
	exec := &countingExecutor{results: map[string]Result{"a": {OK: true}}}
	e, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	good, rerr := readFile(path)
	if rerr != nil {
		t.Fatal(rerr)
	}

	cases := map[string]string{
		"rolled back to pending but keeps success attempt": jsonReplaceStatus(good, "succeeded", "pending"),
		"unknown status":                    jsonReplaceStatus(good, "succeeded", "zombie"),
		"state rollback succeeded->running": jsonReplaceStatus(good, "succeeded", "running"),
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			if err := writeFile(path, content); err != nil {
				t.Fatal(err)
			}
			_, err := New(defs, Options{Store: store, Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
			var ve *ValidationError
			if !errors.As(err, &ve) || ve.Reason != ReasonInvalidState {
				t.Fatalf("err=%v want ValidationError(%s)", err, ReasonInvalidState)
			}
		})
	}
}

// TestStoreRejectsVersionRollback directly exercises the Store contract.
func TestStoreRejectsVersionRollback(t *testing.T) {
	s := NewMemoryStore()
	snap1 := &Snapshot{Version: 1, Tasks: map[string]*TaskState{}}
	snap0 := &Snapshot{Version: 0, Tasks: map[string]*TaskState{}}
	if err := s.Commit(snap1); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(snap0); err == nil {
		t.Fatal("rollback commit was accepted")
	}
	if err := s.Commit(snap1); err == nil {
		t.Fatal("same-version commit was accepted")
	}
}
