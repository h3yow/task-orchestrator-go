package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// Plan is an immutable, validated task graph.
type Plan struct {
	runID string
	tasks map[string]TaskDef
	order []string // topological order; independent tasks keep registration order
	hash  string
}

// BuildPlan validates defs and constructs a Plan. No task has executed by
// the time validation fails; the returned error always wraps exactly one of
// ErrDuplicateTask / ErrMissingDependency / ErrCircularDependency.
func BuildPlan(runID string, defs []TaskDef) (*Plan, error) {
	seen := make(map[string]bool, len(defs))
	for _, d := range defs {
		if d.ID == "" {
			return nil, fmt.Errorf("%w: empty task id", ErrDuplicateTask)
		}
		if seen[d.ID] {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateTask, d.ID)
		}
		seen[d.ID] = true
	}

	indeg := make(map[string]int, len(defs))
	children := make(map[string][]string, len(defs))
	for _, d := range defs {
		indeg[d.ID] = len(d.Deps)
		for _, dep := range d.Deps {
			if !seen[dep] {
				return nil, fmt.Errorf("%w: task %q depends on %q",
					ErrMissingDependency, d.ID, dep)
			}
			if dep == d.ID {
				return nil, fmt.Errorf("%w: task %q depends on itself",
					ErrCircularDependency, d.ID)
			}
			children[dep] = append(children[dep], d.ID)
		}
	}

	// Kahn's algorithm; queue preserves registration order so scheduling
	// decisions are stable and explainable.
	queue := make([]string, 0)
	for _, d := range defs {
		if indeg[d.ID] == 0 {
			queue = append(queue, d.ID)
		}
	}
	order := make([]string, 0, len(defs))
	head := 0
	for head < len(queue) {
		id := queue[head]
		head++
		order = append(order, id)
		// Children are examined in registration order.
		for _, ch := range children[id] {
			indeg[ch]--
			if indeg[ch] == 0 {
				queue = append(queue, ch)
			}
		}
	}
	if len(order) != len(defs) {
		cycle := findCycle(defs)
		return nil, fmt.Errorf("%w: %s", ErrCircularDependency,
			strings.Join(cycle, " -> "))
	}

	p := &Plan{
		runID: runID,
		tasks: make(map[string]TaskDef, len(defs)),
		order: order,
	}
	for _, d := range defs {
		p.tasks[d.ID] = d
	}
	p.hash = computeHash(defs)
	return p, nil
}

// findCycle returns one cycle path (ids), used only for error messages.
func findCycle(defs []TaskDef) []string {
	color := map[string]int{} // 0 white, 1 gray, 2 black
	var stack []string
	var cycle []string
	adj := map[string][]string{}
	for _, d := range defs {
		adj[d.ID] = d.Deps
	}
	var dfs func(string) bool
	dfs = func(u string) bool {
		color[u] = 1
		stack = append(stack, u)
		for _, v := range adj[u] {
			if color[v] == 0 {
				if dfs(v) {
					return true
				}
			} else if color[v] == 1 {
				start := 0
				for i, x := range stack {
					if x == v {
						start = i
						break
					}
				}
				cycle = append([]string(nil), stack[start:]...)
				cycle = append(cycle, v)
				return true
			}
		}
		stack = stack[:len(stack)-1]
		color[u] = 2
		return false
	}
	for _, d := range defs {
		if color[d.ID] == 0 && dfs(d.ID) {
			return cycle
		}
	}
	return nil
}

func (p *Plan) RunID() string { return p.runID }

func (p *Plan) Tasks() map[string]TaskDef { return p.tasks }

func (p *Plan) Task(id string) (TaskDef, bool) {
	d, ok := p.tasks[id]
	return d, ok
}

func (p *Plan) Hash() string { return p.hash }

// IDs returns task ids in topological (registration-stable) order.
func (p *Plan) IDs() []string { return p.order }

// computeHash is a deterministic fingerprint of the *structure* of a plan
// (ids + dependency edges + retry/timeout configuration), not the code of
// the task functions. A resumed process must submit the same plan; changing
// the graph changes the hash and recovery is explicitly rejected.
func computeHash(defs []TaskDef) string {
	ids := make([]string, 0, len(defs))
	for _, d := range defs {
		ids = append(ids, d.ID)
	}
	sort.Strings(ids)
	byID := make(map[string]TaskDef, len(defs))
	for _, d := range defs {
		byID[d.ID] = d
	}
	h := sha256.New()
	fmt.Fprintf(h, "v1\n")
	for _, id := range ids {
		d := byID[id]
		deps := append([]string(nil), d.Deps...)
		sort.Strings(deps)
		fmt.Fprintf(h, "task=%s deps=%s maxAttempts=%d backoff=%s timeout=%s timeoutRetryable=%v\n",
			id, strings.Join(deps, ","), d.Retry.MaxAttempts, d.Retry.Backoff,
			d.Timeout, d.Retry.TimeoutIsRetryable)
	}
	return hex.EncodeToString(h.Sum(nil))
}
