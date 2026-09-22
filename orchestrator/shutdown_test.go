package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRunCancellationStopsCleanly: cancelling Run while a task is in flight
// stops the scheduler; the persisted state keeps that task recoverable
// (running with an open attempt) instead of fabricating a result.
func TestRunCancellationStopsCleanly(t *testing.T) {
	store := NewMemoryStore()
	defs := []TaskDef{
		{ID: "a", Retry: RetryPolicy{MaxAttempts: 3}},
		{ID: "b", Deps: []string{"a"}},
	}
	e, err := New(defs, Options{
		Store: store,
		// "a" blocks until cancelled; "b" must never start.
		Executor: &mixedExec{block: "a", ok: map[string]bool{"b": true}},
		Clock:    NewFakeClock(time.Unix(0, 0)),
		Logger:   testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- e.Run(ctx) }()

	pollStatus(t, e, "a", StatusRunning, 2*time.Second)
	cancel()
	select {
	case err := <-runErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run err=%v want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	stA, _ := e.State("a")
	if stA.Status != StatusRunning || len(stA.Attempts) != 1 {
		t.Fatalf("a=%+v, want running with one open attempt", stA)
	}
	stB, _ := e.State("b")
	if stB.Status != StatusPending || len(stB.Attempts) != 0 {
		t.Fatalf("b=%+v, must still be untouched", stB)
	}

	// A new engine over the same store reconciles the interrupted attempt.
	exec2 := newScriptedExecutor().set("a", Result{OK: true}).set("b", Result{OK: true})
	e2, err := New(defs, Options{
		Store: store, Executor: exec2,
		Clock: NewFakeClock(time.Unix(100, 0)), Logger: testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		st, _ := e2.State(id)
		if st.Status != StatusSucceeded {
			t.Fatalf("%s=%s", id, st.Status)
		}
	}
	if st, _ := e2.State("a"); st.Attempts[0].Class != AttemptInterrupted {
		t.Fatalf("a first attempt=%s want interrupted", st.Attempts[0].Class)
	}
}

// mixedExec blocks on one task (until ctx cancellation), succeeds on others.
type mixedExec struct {
	block string
	ok    map[string]bool
}

func (m *mixedExec) Execute(ctx context.Context, id string) Result {
	if id == m.block {
		<-ctx.Done()
		return Result{OK: false, Retryable: true, Reason: "cancelled"}
	}
	return Result{OK: m.ok[id]}
}
