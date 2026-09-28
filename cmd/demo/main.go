// Command demo exercises the orchestration engine end to end against an
// on-disk JSON store. It runs a small DAG, then asks the engine to shut down
// gracefully while a task is parked in retry_wait, and finally restarts a new
// engine over the same state file: the retry deadline and every other piece of
// progress are read back from disk, so no completed task re-runs and the
// waiting task resumes from its original deadline.
//
// Run with:
//
//	go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/h3yow/task-orchestrator-go/orchestrator"
)

// flakyBuildFails is the external (process-independent) counter of transient
// build failures. The engine's own attempt budget, by contrast, lives in the
// durable snapshot — that is what survives a restart regardless of process
// state.
var flakyBuildFails atomic.Int32

// flakyExecutor fails "build" for its first two (cross-process) attempts.
type flakyExecutor struct{}

func (f *flakyExecutor) Execute(ctx context.Context, id string) orchestrator.Result {
	switch id {
	case "build":
		if flakyBuildFails.Add(1) <= 2 {
			return orchestrator.Result{OK: false, Retryable: true, Reason: fmt.Sprintf("transient build error #%d", flakyBuildFails.Load())}
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
		{ID: "build", Timeout: 30 * time.Second, Retry: orchestrator.RetryPolicy{MaxAttempts: 4, InitialBackoff: 800 * time.Millisecond, MaxBackoff: 5 * time.Second}},
		{ID: "lint", Deps: []string{"build"}},
		{ID: "test", Deps: []string{"build"}},
		{ID: "package", Deps: []string{"lint", "test"}},
	}
}

func printStates(label string, e *orchestrator.Engine) {
	for _, id := range []string{"build", "lint", "test", "package"} {
		st, _ := e.State(id)
		next := ""
		if st.Status == orchestrator.StatusRetryWait {
			next = " next_run_at=" + st.NextRunAt.Format(time.RFC3339Nano)
		}
		fmt.Printf("%s: %-8s status=%-10s attempts=%d%s\n", label, id, st.Status, len(st.Attempts), next)
	}
}

func main() {
	logger := log.New(os.Stdout, "", log.Ltime|log.Lmicroseconds)
	statePath := filepath.Join(os.TempDir(), "orchestrator-demo-state.json")
	_ = os.Remove(statePath) // clean demo start

	// Phase 1: run until "build" fails its first attempt and parks in
	// retry_wait, then request a graceful shutdown.
	e1, err := orchestrator.New(defs(), orchestrator.Options{
		Store:    orchestrator.NewFileStore(statePath),
		Executor: &flakyExecutor{},
		Logger:   logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- e1.Run(runCtx) }()

	// Wait briefly (real wall clock in the demo only) for build to enter
	// retry_wait, then drain & stop gracefully.
	time.Sleep(100 * time.Millisecond)
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := e1.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("graceful shutdown: %v", err)
	}
	cancelRun()
	<-runErr
	fmt.Println("--- engine 1 stopped gracefully ---")
	printStates("first-run ", e1)

	// Phase 2: restart over the same file. The retry deadline is read from
	// disk; the same definition plan must be presented or New refuses to
	// resume. Everything completes without re-running finished work.
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
		log.Fatalf("restarted run: %v", err)
	}
	printStates("restarted ", e2)
	fmt.Println("done: progress resumed from disk; completed tasks never re-ran.")
}
