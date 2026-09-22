// Command demo runs a small task DAG with retries, timeout, failure
// propagation and (on re-run) file-backed restart recovery.
//
// Local verification:
//
//		go run ./cmd/demo            # first run, writes state to ./.demo-state
//	go run ./cmd/demo            # second run: restores completed tasks
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/h3yow/task-orchestrator-go/orchestrator"
)

type stdLogger struct{}

func (stdLogger) Printf(format string, args ...any) {
	log.Printf(format, args...)
}

func main() {
	dir := ".demo-state"
	store, err := orchestrator.NewFileStore(dir)
	if err != nil {
		log.Fatal(err)
	}

	attempt := 0
	defs := []orchestrator.TaskDef{
		{ID: "fetch-config", Run: func(ctx context.Context) error {
			fmt.Println("-> fetch-config ok")
			return nil
		}},
		{ID: "fetch-data", Deps: []string{"fetch-config"},
			Retry: orchestrator.RetryPolicy{MaxAttempts: 3, Backoff: 10 * time.Millisecond},
			Run: func(ctx context.Context) error {
				attempt++
				if attempt < 2 {
					fmt.Printf("-> fetch-data attempt %d transient failure\n", attempt)
					return fmt.Errorf("network blip")
				}
				fmt.Println("-> fetch-data ok")
				return nil
			}},
		{ID: "render", Deps: []string{"fetch-data"},
			Run: func(ctx context.Context) error { fmt.Println("-> render ok"); return nil }},
		{ID: "notify", Deps: []string{"render"},
			Run: func(ctx context.Context) error {
				fmt.Println("-> notify skipped-able branch ok")
				return nil
			}},
	}

	plan, err := orchestrator.BuildPlan("demo-run", defs)
	if err != nil {
		log.Fatal(err)
	}

	eng, err := orchestrator.NewEngine(plan, orchestrator.Options{
		Store:  store,
		Clock:  orchestrator.NewRealClock(),
		Logger: stdLogger{},
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := eng.Run(context.Background()); err != nil {
		var runErr *orchestrator.RunError
		if errors.As(err, &runErr) {
			fmt.Printf("run finished with unsuccessful tasks: %v\n", err)
			os.Exit(1)
		}
		log.Fatal(err)
	}
	fmt.Println("all tasks succeeded; state persisted in", dir)
}
