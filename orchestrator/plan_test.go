package orchestrator

import (
	"errors"
	"testing"
)

func TestBuildPlanRejectsDuplicateIDs(t *testing.T) {
	defs := []TaskDef{
		{ID: "a", Run: noopRun},
		{ID: "a", Run: noopRun},
	}
	_, err := BuildPlan("r1", defs)
	if !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("want ErrDuplicateTask, got %v", err)
	}
	t.Logf("duplicate rejected with distinguishable cause: %v", err)
}

func TestBuildPlanRejectsMissingDependency(t *testing.T) {
	defs := []TaskDef{
		{ID: "a", Deps: []string{"ghost"}, Run: noopRun},
	}
	_, err := BuildPlan("r1", defs)
	if !errors.Is(err, ErrMissingDependency) {
		t.Fatalf("want ErrMissingDependency, got %v", err)
	}
	t.Logf("missing dependency rejected before execution: %v", err)
}

func TestBuildPlanRejectsCircularDependency(t *testing.T) {
	defs := []TaskDef{
		{ID: "a", Deps: []string{"c"}, Run: noopRun},
		{ID: "b", Deps: []string{"a"}, Run: noopRun},
		{ID: "c", Deps: []string{"b"}, Run: noopRun},
	}
	_, err := BuildPlan("r1", defs)
	if !errors.Is(err, ErrCircularDependency) {
		t.Fatalf("want ErrCircularDependency, got %v", err)
	}
	t.Logf("cycle rejected with path shown: %v", err)
}

func TestBuildPlanRejectsSelfDependency(t *testing.T) {
	defs := []TaskDef{{ID: "a", Deps: []string{"a"}, Run: noopRun}}
	_, err := BuildPlan("r1", defs)
	if !errors.Is(err, ErrCircularDependency) {
		t.Fatalf("want ErrCircularDependency, got %v", err)
	}
}

func TestBuildPlanHashChangesWithGraph(t *testing.T) {
	p1, err := BuildPlan("r", []TaskDef{{ID: "a", Run: noopRun}, {ID: "b", Deps: []string{"a"}, Run: noopRun}})
	if err != nil {
		t.Fatal(err)
	}
	p2, err := BuildPlan("r", []TaskDef{{ID: "a", Run: noopRun}, {ID: "b", Run: noopRun}})
	if err != nil {
		t.Fatal(err)
	}
	if p1.Hash() == p2.Hash() {
		t.Fatal("hash must differ when the dependency graph differs")
	}
}

func TestBuildPlanTopologicalOrder(t *testing.T) {
	defs := []TaskDef{
		{ID: "d", Deps: []string{"b", "c"}, Run: noopRun},
		{ID: "b", Deps: []string{"a"}, Run: noopRun},
		{ID: "c", Deps: []string{"a"}, Run: noopRun},
		{ID: "a", Run: noopRun},
	}
	p, err := BuildPlan("r", defs)
	if err != nil {
		t.Fatal(err)
	}
	pos := map[string]int{}
	for i, id := range p.IDs() {
		pos[id] = i
	}
	if !(pos["a"] < pos["b"] && pos["a"] < pos["c"] && pos["b"] < pos["d"] && pos["c"] < pos["d"]) {
		t.Fatalf("topological order violated: %v", p.IDs())
	}
	t.Logf("stable topological order: %v", p.IDs())
}
