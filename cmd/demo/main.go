// Command demo exercises the orchestration engine end to end against an
// on-disk JSON store:
//
//  1. a first run is asked to shut down gracefully while work is in flight —
//     in-flight results are committed and no new task is dispatched;
//  2. a second engine resumes from the same file: a task parked in retry_wait
//     keeps its ORIGINAL retry deadline, completed tasks never re-run;
//  3. a third start with modified task definitions is refused with a
//     PlanMismatchError explaining the difference.
//
// Run with:
//
//	go run ./cmd/demo
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/h3yow/task-orchestrator-go/orchestrator"
)

// flakyExecutor holds each attempt for 120ms so a graceful shutdown requested
// immediately after start reliably overlaps an in-flight attempt, and fails the
// first two attempts of "build" so a retry_wait window is observable.
type flakyExecutor struct{ n atomic.Int32 }

func (f *flakyExecutor) Execute(ctx context.Context, id string) orchestrator.Result {
	select {
	case <-time.After(120 * time.Millisecond):
	case <-ctx.Done():
		return orchestrator.Result{OK: false, Retryable: true, Reason: "cancelled while " + id + " was running"}
	}
	switch id {
	case "build":
		if f.n.Add(1) <= 2 {
			return orchestrator.Result{OK: false, Retryable: true, Reason: fmt.Sprintf("transient build error #%d", f.n.Load())}
		}
		return orchestrator.Result{OK: true, Reason: "build green"}
	case "test", "lint":
		return orchestrator.Result{OK: true, Reason: id + " passed"}
	case "package":
		return orchestrator.Result{OK: true, Reason: "artifact packaged"}
	default:
		return orchestrator.Result{OK: false, Retryable: false, Reason: "unknown task"}
	}
}

func defs() []orchestrator.TaskDef {
	return []orchestrator.TaskDef{
		{ID: "build", Timeout: 30 * time.Second, Retry: orchestrator.RetryPolicy{MaxAttempts: 4, InitialBackoff: 200 * time.Millisecond, MaxBackoff: 2 * time.Second}},
		{ID: "lint", Deps: []string{"build"}},
		{ID: "test", Deps: []string{"build"}},
		{ID: "package", Deps: []string{"lint", "test"}},
	}
}

func main() {
	logger := log.New(os.Stdout, "", log.Ltime)
	statePath := filepath.Join(os.TempDir(), "orchestrator-demo-state.json")
	_ = os.Remove(statePath) // clean demo start

	// --- Run 1: start, then request graceful shutdown almost immediately. ---
	e1, err := orchestrator.New(defs(), orchestrator.Options{
		Store:    orchestrator.NewFileStore(statePath),
		Executor: &flakyExecutor{},
		Logger:   logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	runErr := make(chan error, 1)
	go func() { runErr <- e1.Run(context.Background()) }()

	// A 1s grace period lets the first 120ms attempt finish naturally (it fails
	// and parks the build in a 200ms retry wait); the drain then arrives while
	// that retry would otherwise be dispatched, so the parked retry_wait state
	// is what gets persisted for the restart.
	sctx, scancel := context.WithTimeout(context.Background(), time.Second)
	if err := e1.Shutdown(sctx); err != nil {
		scancel()
		log.Fatalf("graceful shutdown: %v", err)
	}
	scancel()
	if err := <-runErr; !errors.Is(err, orchestrator.ErrGracefulShutdown) {
		log.Fatalf("run 1 err=%v want ErrGracefulShutdown", err)
	}
	printStates("after-shutdown", e1)

	// --- Run 2: restart over the same file and let the DAG settle. ----------
	fmt.Println("--- restarting process, reloading state from disk ---")
	e2, err := orchestrator.New(defs(), orchestrator.Options{
		Store:    orchestrator.NewFileStore(statePath),
		Executor: &flakyExecutor{},
		Logger:   logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e2.Run(ctx); err != nil {
		log.Fatalf("run 2: %v", err)
	}
	printStates("restarted     ", e2)

	// --- Run 3: start with modified definitions; resume must be refused. ----
	fmt.Println("--- attempting restart with a modified plan (extra task \"deploy\") ---")
	changed := append(defs(), orchestrator.TaskDef{ID: "deploy", Deps: []string{"package"}})
	_, err = orchestrator.New(changed, orchestrator.Options{
		Store:    orchestrator.NewFileStore(statePath),
		Executor: &flakyExecutor{},
		Logger:   logger,
	})
	var pm *orchestrator.PlanMismatchError
	if errors.As(err, &pm) {
		fmt.Printf("resume refused as expected:\n%v\n", err)
	} else {
		log.Fatalf("expected PlanMismatchError, got %v", err)
	}
}

func printStates(label string, e *orchestrator.Engine) {
	for _, id := range []string{"build", "lint", "test", "package"} {
		st, _ := e.State(id)
		fmt.Printf("%s: %-8s status=%-10s attempts=%d\n", label, id, st.Status, len(st.Attempts))
	}
}
