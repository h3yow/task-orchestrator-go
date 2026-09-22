package orchestrator

import (
	"os"
	"strings"
)

func readFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	return string(b), err
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

// jsonReplaceStatus rewrites the status of task "a" in a marshaled snapshot.
// The JSON layout marshals tasks in map order, so we do a targeted replacement
// of the first `"status": "X"` occurrence.
func jsonReplaceStatus(content, from, to string) string {
	old := `"status": "` + from + `"`
	nw := `"status": "` + to + `"`
	return strings.Replace(content, old, nw, 1)
}
