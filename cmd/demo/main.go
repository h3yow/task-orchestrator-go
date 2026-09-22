// Command demo exercises the orchestration engine end to end against an
// on-disk JSON store: it runs a small DAG where one task fails twice and
// retries, then simulates a restart by constructing a second engine over the
// same state file.
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

type flakyExecutor struct{ n atomic.Int32 }

func (f *flakyExecutor) Execute(ctx context.Context, id string) orchestrator.Result {
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

	run := func(label string) error {
		e, err := orchestrator.New(defs(), orchestrator.Options{
			Store:    orchestrator.NewFileStore(statePath),
			Executor: &flakyExecutor{},
			Logger:   logger,
		})
		if err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := e.Run(ctx); err != nil {
			return fmt.Errorf("%s: run: %w", label, err)
		}
		for _, id := range []string{"build", "lint", "test", "package"} {
			st, _ := e.State(id)
			fmt.Printf("%s: %-8s status=%s attempts=%d\n", label, id, st.Status, len(st.Attempts))
		}
		return nil
	}

	if err := run("first-run "); err != nil {
		log.Fatal(err)
	}
	fmt.Println("--- restarting process, reloading state from disk ---")
	if err := run("restarted "); err != nil {
		log.Fatal(err)
	}
	fmt.Println("second run executed nothing (completed tasks never re-run).")
}
