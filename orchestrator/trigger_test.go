package orchestrator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestTriggerBeforeRunIsRejected: API misuse gets a distinct error.
func TestTriggerBeforeRunIsRejected(t *testing.T) {
	e, err := New([]TaskDef{{ID: "a"}}, Options{
		Store:    NewMemoryStore(),
		Executor: &countingExecutor{results: map[string]Result{"a": {OK: true}}},
		Clock:    NewFakeClock(time.Unix(0, 0)),
		Logger:   testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Trigger(context.Background(), "a"); !errors.Is(err, ErrEngineNotRunning) {
		t.Fatalf("err=%v want ErrEngineNotRunning", err)
	}
}

// TestTriggerUnknownTask:
func TestTriggerUnknownTask(t *testing.T) {
	e, _ := New([]TaskDef{{ID: "a"}}, Options{
		Store:    NewMemoryStore(),
		Executor: &countingExecutor{results: map[string]Result{"a": {OK: true}}},
		Clock:    NewFakeClock(time.Unix(0, 0)),
	})
	var ve *ValidationError
	err := e.Trigger(context.Background(), "ghost")
	if !errors.As(err, &ve) || ve.Reason != ReasonInvalidTask {
		t.Fatalf("err=%v", err)
	}
}

// TestDuplicateTriggerNoDoubleExecution hammers Trigger on a task whose
// executor blocks. Exactly one attempt must ever be in flight / committed.
func TestDuplicateTriggerNoDoubleExecution(t *testing.T) {
	exec := newScriptedExecutor()
	gate := exec.blockOn("a")
	exec.set("a", Result{OK: true})

	defs := []TaskDef{{ID: "a"}}
	log := newCaptureLogger(t)
	e, err := New(defs, Options{Store: NewMemoryStore(), Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()

	// The scheduler auto-dispatches a dependency-free pending task on the
	// initial pass. Wait until it is running, then fire many triggers.
	pollStatus(t, e, "a", StatusRunning, 2*time.Second)

	const n = 20
	var wg sync.WaitGroup
	ignored := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := e.Trigger(context.Background(), "a")
			if err != nil {
				ignored <- err
			}
		}()
	}
	wg.Wait()
	close(ignored)

	count := 0
	for err := range ignored {
		var tie *ErrTriggerIgnored
		if !errors.As(err, &tie) {
			t.Fatalf("unexpected trigger error: %v", err)
		}
		count++
	}
	if count != n {
		t.Fatalf("ignored=%d want %d", count, n)
	}

	close(gate)
	st := pollStatus(t, e, "a", StatusSucceeded, 2*time.Second)
	if len(st.Attempts) != 1 {
		t.Fatalf("attempts=%d want exactly 1", len(st.Attempts))
	}
	if exec.callCount("a") != 1 {
		t.Fatalf("executor ran %d times, want 1", exec.callCount("a"))
	}
}

// TestTriggerParksUntilDepsSatisfied: an early trigger waits rather than
// failing or running early; it resolves exactly when deps succeed.
func TestTriggerParksUntilDepsSatisfied(t *testing.T) {
	exec := newScriptedExecutor()
	gate := exec.blockOn("a")
	exec.set("a", Result{OK: true})
	exec.set("b", Result{OK: true})
	defs := []TaskDef{{ID: "a"}, {ID: "b", Deps: []string{"a"}}}
	e, err := New(defs, Options{Store: NewMemoryStore(), Executor: exec, Clock: NewFakeClock(time.Unix(0, 0)), Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = e.Run(context.Background()) }()
	pollStatus(t, e, "a", StatusRunning, 2*time.Second)

	triggered := make(chan error, 1)
	go func() { triggered <- e.Trigger(context.Background(), "b") }()

	// b is pending and its dep is still running; trigger must park.
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-triggered:
		t.Fatalf("trigger returned too early: %v", err)
	default:
	}
	stB, _ := e.State("b")
	if stB.Status != StatusPending || len(stB.Attempts) != 0 {
		t.Fatalf("b started before deps satisfied: %+v", stB)
	}

	close(gate)
	select {
	case err := <-triggered:
		if err != nil {
			t.Fatalf("trigger err=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked trigger never resolved")
	}
	pollStatus(t, e, "b", StatusSucceeded, 2*time.Second)
	if exec.callCount("b") != 1 {
		t.Fatalf("b ran %d times", exec.callCount("b"))
	}
}
