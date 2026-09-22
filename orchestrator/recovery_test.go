package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestRecoverInterruptedRunningTask: an attempt was in flight when the
// process died. Restart must record the interruption, never silently
// re-run before recording it, and retry the task when budget allows.
func TestRecoverInterruptedRunningTask(t *testing.T) {
	env := newTestEnv(t)

	// Simulated task: the first invocation hangs forever (the "crashed"
	// process never reports an outcome); the invocation after recovery
	// succeeds immediately. No real wall-clock scheduling is involved.
	crashGate := make(chan struct{})
	var invocations int32
	run := func(ctx context.Context) error {
		n := atomic.AddInt32(&invocations, 1)
		if n == 1 {
			<-crashGate // never released during the test
			return nil
		}
		return nil
	}
	defs := []TaskDef{{
		ID:    "a",
		Run:   run,
		Retry: RetryPolicy{MaxAttempts: 3},
	}}
	plan, _ := BuildPlan("run-recover-running", defs)

	// First engine: task enters running and that boundary is durable,
	// then the process "dies" while the attempt is in flight.
	eng1, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = eng1.Run(context.Background()) }()
	// Wait until the first invocation is inside the (hung) handler, which
	// only happens after the "running attempt 1" boundary was persisted.
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&invocations) < 1 {
		if time.Now().After(deadline) {
			t.Fatal("attempt 1 never started")
		}
		time.Sleep(time.Millisecond)
	}
	_ = eng1.Close() // engine gone; the hung goroutine reports nothing

	// Second engine over the same store recovers purely from persisted state.
	eng2, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	st, _ := eng2.State("a")
	if st.Status != StatusReady {
		t.Fatalf("recovered status=%s, want ready", st.Status)
	}
	if st.Attempt != 1 || len(st.Records) != 1 || st.Records[0].Reason != ReasonInterrupted {
		t.Fatalf("interrupted attempt not recorded deterministically: attempt=%d records=%d",
			st.Attempt, len(st.Records))
	}
	t.Logf("recovery decision: interrupted attempt recorded (reason=%s), re-homed to ready",
		st.Records[0].Reason)

	// Resuming executes the next attempt, which succeeds exactly once more.
	if err := eng2.Run(context.Background()); err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	st, _ = eng2.State("a")
	if st.Status != StatusSucceeded {
		t.Fatalf("after resume status=%s", st.Status)
	}
	if st.Attempt != 2 || len(st.Records) != 2 {
		t.Fatalf("want attempts=2 records=2, got %d/%d", st.Attempt, len(st.Records))
	}
	t.Logf("resumed to success; attempts=%d records=[interrupted, success]", st.Attempt)
}

// TestRecoverSucceededTaskNotRerun: finished work is never repeated.
func TestRecoverSucceededTaskNotRerun(t *testing.T) {
	env := newTestEnv(t)
	var invocations int32
	defs := []TaskDef{{ID: "a", Run: func(ctx context.Context) error {
		atomic.AddInt32(&invocations, 1)
		return nil
	}}}
	plan, _ := BuildPlan("run-recover-done", defs)
	eng1, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	eng2, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng2.Run(context.Background()); err != nil {
		t.Fatalf("restart over completed run: %v", err)
	}
	if got := atomic.LoadInt32(&invocations); got != 1 {
		t.Fatalf("succeeded task executed %d times across restart, want 1", got)
	}
	st, _ := eng2.State("a")
	t.Logf("completed task stays terminal across restart: status=%s invocations=1", st.Status)
}

// TestRecoverInterruptedBudgetExhausted: crash on the last allowed attempt
// must end in a definite terminal failed state.
func TestRecoverInterruptedBudgetExhausted(t *testing.T) {
	env := newTestEnv(t)
	defs := []TaskDef{{
		ID: "a", Run: noopRun,
		Retry: RetryPolicy{MaxAttempts: 1},
	}}
	plan, _ := BuildPlan("run-recover-exhaust", defs)
	// Forge a realistic snapshot: attempt 1 was running when interrupted.
	snap := &Snapshot{
		RunID: plan.RunID(), PlanHash: plan.Hash(),
		CreatedAt: env.clock.Now(), UpdatedAt: env.clock.Now(),
		Tasks: map[string]*TaskState{},
	}
	snap.Tasks["a"] = &TaskState{
		ID: "a", Status: StatusRunning, Attempt: 1, UpdatedAt: env.clock.Now(),
	}
	if err := env.store.Save(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	eng, err := NewEngine(plan, env.opts())
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	st, _ := eng.State("a")
	if st.Status != StatusFailed || st.LastReason != ReasonRetriesExhausted {
		t.Fatalf("status=%s reason=%s, want failed/retries_exhausted", st.Status, st.LastReason)
	}
	t.Logf("interruption on final attempt has stable terminal destination: failed(%s)", st.LastReason)
}

// TestRecoverPlanMismatchRejected: changing the plan between runs is a
// hard, distinguishable rejection.
func TestRecoverPlanMismatchRejected(t *testing.T) {
	env := newTestEnv(t)
	plan1, _ := BuildPlan("run-same", []TaskDef{{ID: "a", Run: noopRun}})
	eng1, err := NewEngine(plan1, env.opts())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	plan2, _ := BuildPlan("run-same", []TaskDef{
		{ID: "a", Run: noopRun},
		{ID: "b", Deps: []string{"a"}, Run: noopRun},
	})
	_, err = NewEngine(plan2, env.opts())
	if !errors.Is(err, ErrPlanMismatch) {
		t.Fatalf("want ErrPlanMismatch, got %v", err)
	}
	t.Logf("resuming with a changed graph rejected explicitly: %v", err)
}

// TestCorruptSnapshotRejected: torn/garbage bytes never resume silently.
func TestCorruptSnapshotRejected(t *testing.T) {
	dir := t.TempDir()
	store, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "run-bad.json"),
		[]byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	plan, _ := BuildPlan("run-bad", []TaskDef{{ID: "a", Run: noopRun}})
	_, err = NewEngine(plan, Options{Store: store, Logger: newTestLogger(t)})
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("want ErrInvalidState for corrupt file, got %v", err)
	}
	t.Logf("corrupt snapshot rejected before any scheduling: %v", err)
}

// TestSelfContradictoryStateRejected: state that regresses (e.g. succeeded
// task with a failing final record) is refused rather than advanced.
func TestSelfContradictoryStateRejected(t *testing.T) {
	env := newTestEnv(t)
	plan, _ := BuildPlan("run-weird", []TaskDef{{ID: "a", Run: noopRun}})
	bad := &Snapshot{
		RunID: plan.RunID(), PlanHash: plan.Hash(),
		CreatedAt: env.clock.Now(), UpdatedAt: env.clock.Now(),
		Tasks: map[string]*TaskState{
			"a": {
				ID: "a", Status: StatusSucceeded, Attempt: 1,
				Records: []AttemptRecord{{
					Attempt: 1, Reason: ReasonNonRetryable,
					Err: "boom",
				}},
			},
		},
	}
	if err := env.store.Save(context.Background(), bad); err != nil {
		t.Fatal(err)
	}
	_, err := NewEngine(plan, env.opts())
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("want ErrInvalidState, got %v", err)
	}
	t.Logf("self-contradictory persistence rejected: %v", err)
}

// TestFileStoreRestartRoundTrip: the real atomic JSON store survives a
// simulated process restart and keeps the result consistent.
func TestFileStoreRestartRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store1, _ := NewFileStore(dir)
	plan, _ := BuildPlan("run-fs", []TaskDef{{ID: "a", Run: noopRun}})
	eng1, err := NewEngine(plan, Options{Store: store1, Logger: newTestLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Brand new store/engine instances = process restart, same directory.
	store2, _ := NewFileStore(dir)
	eng2, err := NewEngine(plan, Options{Store: store2, Logger: newTestLogger(t)})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if err := eng2.Run(context.Background()); err != nil {
		t.Fatalf("restart run: %v", err)
	}
	if st, _ := eng2.State("a"); st.Status != StatusSucceeded {
		t.Fatalf("after file-store restart status=%s", st.Status)
	}
	t.Logf("file store round-trip across restart consistent: a=succeeded")
}
