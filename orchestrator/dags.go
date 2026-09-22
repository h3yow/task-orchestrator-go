package orchestrator

import (
	"sort"
	"strconv"
)

// validateDAG checks the static task graph BEFORE anything executes.
// It returns a *ValidationError whose Reason distinguishes the failure:
//   - ReasonDuplicateID: two tasks share an ID
//   - ReasonInvalidTask: empty ID
//   - ReasonUnknownDep:   a dependency points at a task that does not exist
//   - ReasonCycle:        the dependency graph contains a cycle
//
// Checks run in a fixed order so the reported cause is deterministic.
func validateDAG(defs []TaskDef) error {
	// 1. Identity checks, in declaration order.
	seen := make(map[string]int, len(defs))
	for i, d := range defs {
		if d.ID == "" {
			return &ValidationError{
				Reason: ReasonInvalidTask,
				Detail: "task at index " + strconv.Itoa(i) + " has an empty ID",
			}
		}
		if first, ok := seen[d.ID]; ok {
			return &ValidationError{
				Reason: ReasonDuplicateID,
				TaskID: d.ID,
				Detail: "task ID defined more than once (first defined at index " + strconv.Itoa(first) +
					", redefined at index " + strconv.Itoa(i) + ")",
			}
		}
		seen[d.ID] = i
	}

	// 2. Dependency target checks, in declaration order then dep order.
	index := make(map[string]TaskDef, len(defs))
	for _, d := range defs {
		index[d.ID] = d
	}
	for _, d := range defs {
		depSeen := make(map[string]bool, len(d.Deps))
		for _, dep := range d.Deps {
			if depSeen[dep] {
				return &ValidationError{
					Reason: ReasonInvalidTask,
					TaskID: d.ID,
					Detail: "dependency " + quote(dep) + " is listed more than once",
				}
			}
			depSeen[dep] = true
			if _, ok := index[dep]; !ok {
				return &ValidationError{
					Reason: ReasonUnknownDep,
					TaskID: d.ID,
					Detail: "depends on task " + quote(dep) + " which is not defined",
				}
			}
			if dep == d.ID {
				return &ValidationError{
					Reason: ReasonCycle,
					TaskID: d.ID,
					Detail: "task depends on itself via " + quote(dep),
				}
			}
		}
	}

	// 3. Cycle detection with three-color DFS. Traversal root order is sorted
	//    for deterministic reporting.
	ids := make([]string, 0, len(defs))
	for id := range index {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	const (
		white = 0
		gray  = 1 // on the current DFS stack
		black = 2 // fully explored
	)
	color := make(map[string]int, len(ids))
	var stack []string

	var dfs func(id string) *ValidationError
	dfs = func(id string) *ValidationError {
		color[id] = gray
		stack = append(stack, id)
		deps := append([]string(nil), index[id].Deps...)
		sort.Strings(deps)
		for _, dep := range deps {
			switch color[dep] {
			case gray:
				// Extract the cycle portion of the stack for an explainable error.
				from := 0
				for i, s := range stack {
					if s == dep {
						from = i
						break
					}
				}
				cycle := append(append([]string(nil), stack[from:]...), dep)
				return &ValidationError{
					Reason: ReasonCycle,
					TaskID: dep,
					Detail: "dependency cycle: " + joinArrows(cycle),
				}
			case white:
				if err := dfs(dep); err != nil {
					return err
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[id] = black
		return nil
	}

	for _, id := range ids {
		if color[id] == white {
			if err := dfs(id); err != nil {
				return err
			}
		}
	}
	return nil
}

func quote(s string) string { return "\"" + s + "\"" }

func joinArrows(ids []string) string {
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += " -> "
		}
		out += quote(id)
	}
	return out
}
