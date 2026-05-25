package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCollectTuplesInto_ErrorNeverDropped is a static guard for
// BUG_ITERATOR_ERRORS_DROPPED_IN_MATERIALIZATION_PATHS. Every collectTuplesInto
// call in production code must capture its returned error; a bare call statement
// silently launders a failed partial stream into a clean materialized relation.
//
// A captured call reads "err := collectTuplesInto(...)" or
// "if err := collectTuplesInto(...)"; a dropped call is the bare statement
// "collectTuplesInto(...)". This guard flags the latter.
func TestCollectTuplesInto_ErrorNeverDropped(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)

	var violations []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		require.NoError(t, err)
		for i, line := range strings.Split(string(src), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "collectTuplesInto(") {
				violations = append(violations, fmt.Sprintf("%s:%d", f, i+1))
			}
		}
	}

	if len(violations) > 0 {
		t.Fatalf("collectTuplesInto called without capturing its error (launders iterator "+
			"failures into clean relations):\n  %s", strings.Join(violations, "\n  "))
	}
}
