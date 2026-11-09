package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// reviewDecorrelationFacts is the fixture the generated correlated subqueries
// read: six entities, each holding a :p/k and a :p/v, some equal to each
// other so a binding repeating a name unifies on some tuples and not others.
func reviewDecorrelationFacts() ([]fact, map[datalog.Identity]string) {
	e1, e2, e3, e4, e5, e6 := named("e1"), named("e2"), named("e3"), named("e4"), named("e5"), named("e6")
	facts := []fact{
		{e1, ":p/k", int64(1)}, {e1, ":p/v", int64(1)},
		{e2, ":p/k", int64(1)}, {e2, ":p/v", int64(2)},
		{e3, ":p/k", int64(2)}, {e3, ":p/v", int64(2)},
		{e4, ":p/k", int64(2)}, {e4, ":p/v", int64(1)},
		{e5, ":p/k", int64(3)}, {e5, ":p/v", int64(3)},
		{e6, ":p/k", int64(1)}, {e6, ":p/v", int64(3)},
	}
	names := map[datalog.Identity]string{}
	for _, f := range facts {
		names[f.of.id] = f.of.name
	}
	return facts, names
}

// reviewDecorrelationQueries enumerates correlated subqueries over the
// fixture: each inner :find, each body, and every relation binding over the
// names ?b (the correlation variable), ?q (an outer variable no call
// passes), ?m and ?n, repeats included, plus the scalar and collection
// bindings of a one-element :find. Each query is written bare and, for an
// aggregate, as the or-default-join with a ground default that compiles to a
// lateral join with defaults.
func reviewDecorrelationQueries() []string {
	type inner struct {
		find      string
		body      string
		arity     int
		aggregate bool
	}
	inners := []inner{
		{"(count ?z)", "[?z :p/k ?b]", 1, true},
		{"?b (count ?z)", "[?z :p/k ?b]", 2, true},
		{"?v (count ?z)", "[?z :p/k ?b] [?z :p/v ?v]", 2, true},
		{"?b ?v (count ?z)", "[?z :p/k ?b] [?z :p/v ?v]", 3, true},
		{"?v ?b (max ?z)", "[?z :p/k ?b] [?z :p/v ?v]", 3, true},
		{"?y (count ?z)", "[?z :p/k ?y] [(= ?y ?b)]", 2, true},
		{"?v (count ?z)", "[?z :p/k ?y] [?z :p/v ?v] [(= ?y ?b)]", 2, true},
		{"?v", "[?z :p/k ?b] [?z :p/v ?v] [(> ?v 1)]", 1, false},
		{"?b ?v", "[?z :p/k ?b] [?z :p/v ?v] [(> ?v 1)]", 2, false},
		{"?v ?b", "[?z :p/k ?b] [?z :p/v ?v] [(> ?v 1)]", 2, false},
		{"?z ?v", "[?z :p/k ?b] [?z :p/v ?v] [(> ?v 1)]", 2, false},
		{"?b ?v ?z", "[?z :p/k ?b] [?z :p/v ?v] [(> ?v 1)]", 3, false},
	}
	names := []string{"?b", "?q", "?m", "?n"}
	var bindings func(arity int) [][]string
	bindings = func(arity int) [][]string {
		if arity == 0 {
			return [][]string{nil}
		}
		var out [][]string
		for _, rest := range bindings(arity - 1) {
			for _, name := range names {
				out = append(out, append(append([]string(nil), rest...), name))
			}
		}
		return out
	}
	var queries []string
	for _, in := range inners {
		subquery := fmt.Sprintf("(q [:find %s :in $ ?b :where %s] $ ?b)", in.find, in.body)
		var forms []string
		for _, binding := range bindings(in.arity) {
			forms = append(forms, "[["+strings.Join(binding, " ")+"]]")
		}
		if in.arity == 1 {
			for _, name := range names {
				forms = append(forms, name, "["+name+" ...]")
			}
		}
		for _, form := range forms {
			bound := []string{"?q", "?b"}
			for _, sym := range strings.Fields(strings.NewReplacer("[", " ", "]", " ", "...", " ").Replace(form)) {
				if !reviewNamesString(bound, sym) {
					bound = append(bound, sym)
				}
			}
			queries = append(queries, fmt.Sprintf("[:find %s :where [?q :p/k ?b] [%s %s]]",
				strings.Join(bound, " "), subquery, form))
			if !in.aggregate || in.arity != 2 || !strings.HasPrefix(form, "[[?b ") {
				continue
			}
			output := strings.TrimSuffix(strings.TrimPrefix(form, "[[?b "), "]]")
			if output == "?b" || output == "?q" {
				continue
			}
			queries = append(queries, fmt.Sprintf("[:find %s :where [?q :p/k ?b] (or-default-join [[?b] %s] [%s %s] [(ground -1) %s])]",
				strings.Join(bound, " "), output, subquery, form, output))
		}
	}
	return queries
}

func reviewNamesString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// reviewOutcome is what a query answers through its iterator: its tuples,
// rendered and sorted, or its error's first line.
func reviewOutcome(d *Database, text string, names map[datalog.Identity]string) string {
	return reviewOutcomeWith(d, text, names)
}

// reviewOutcomeWith is reviewOutcome under query options.
func reviewOutcomeWith(d *Database, text string, names map[datalog.Identity]string, options ...interface{}) string {
	tuples, err := executor.CollectTuples(d.Query(text, options...))
	if err != nil {
		line := strings.SplitN(err.Error(), "\n", 2)[0]
		return "error: " + line
	}
	rendered := make([]string, len(tuples))
	for i, tuple := range tuples {
		parts := make([]string, len(tuple))
		for j, v := range tuple {
			if id, ok := v.(datalog.Identity); ok {
				parts[j] = names[id]
				continue
			}
			parts[j] = fmt.Sprint(v)
		}
		rendered[i] = "[" + strings.Join(parts, " ") + "]"
	}
	sort.Strings(rendered)
	return strings.Join(rendered, " ")
}

// A correlated subquery answers alike on every leg of the optimizer matrix,
// whatever its inner :find and whatever names its binding writes (CLAUDE.md,
// Semantic Preservation; optimizer_modes_test.go's observational
// equivalence). The reference is the leg with the algebra optimizer and scan
// sharing off, on the same tree. With REVIEW_SOURCES_GOLDEN naming a file,
// each leg's answer is also compared with the reference's answer recorded
// there (REVIEW_SOURCES_WRITE=1 records it), the merge-base's answer for the
// cross-tree differential.
func TestReviewSources_DecorrelationAnswersAlikeOnBothPaths(t *testing.T) {
	facts, names := reviewDecorrelationFacts()
	queries := reviewDecorrelationQueries()
	outcomes := map[string]map[string]string{}
	var off map[string]string
	for _, mode := range legsFor(optimizerModes, algebraOption, sharingOption, fusionOption, aggregationOption) {
		d := createOptimizerModeDB(t, mode, DatabaseOptions{})
		commitFacts(t, d, facts)
		answered := map[string]string{}
		for _, text := range queries {
			answered[text] = reviewOutcome(d, text, names)
		}
		outcomes[mode.name] = answered
		if !mode.algebra && !mode.sharing {
			off = answered
		}
	}
	require.NotNil(t, off, "the matrix holds the leg with the algebra optimizer and scan sharing off")
	for _, text := range queries {
		t.Run(text, func(t *testing.T) {
			require.False(t, strings.HasPrefix(off[text], "error:"), "the reference leg answers: %s", off[text])
			for leg, answered := range outcomes {
				require.Equal(t, off[text], answered[text], "%s answers what the reference leg answers", leg)
			}
		})
	}
	golden := os.Getenv("REVIEW_SOURCES_GOLDEN")
	if golden == "" {
		return
	}
	if os.Getenv("REVIEW_SOURCES_WRITE") == "1" {
		encoded, err := json.MarshalIndent(off, "", " ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(golden, encoded, 0o644))
		return
	}
	encoded, err := os.ReadFile(golden)
	require.NoError(t, err)
	var reference map[string]string
	require.NoError(t, json.Unmarshal(encoded, &reference))
	for _, text := range queries {
		t.Run("reference "+text, func(t *testing.T) {
			for leg, answered := range outcomes {
				require.Equal(t, reference[text], answered[text], "%s answers what the merge-base answers", leg)
			}
		})
	}
}

// Every generated subquery is decorrelated on the optimizer path, so the
// differential above runs Rule 1 and Rule 4 rather than the per-combination
// run on both paths (CLAUDE.md, Testing Query Optimizations: structural
// invariants through annotations).
func TestReviewSources_EveryGeneratedSubqueryIsDecorrelated(t *testing.T) {
	facts, names := reviewDecorrelationFacts()
	applied := 0
	handler := func(event annotations.Event) {
		if event.Name == annotations.AlgebraDecorrelateApply {
			applied++
		}
	}
	d := createOptimizerModeDB(t, legsWhere(legsFor(optimizerModes, algebraOption, aggregationOption), func(m optimizerMode) bool { return m.algebra })[0], DatabaseOptions{AnnotationHandler: handler})
	commitFacts(t, d, facts)
	for _, text := range reviewDecorrelationQueries() {
		t.Run(text, func(t *testing.T) {
			d.ClearPlanCache()
			applied = 0
			outcome := reviewOutcome(d, text, names)
			require.False(t, strings.HasPrefix(outcome, "error:"), outcome)
			require.Positive(t, applied, "the optimizer decorrelates the subquery")
		})
	}
}
