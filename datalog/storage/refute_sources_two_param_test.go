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

// refuteTwoParamFacts is six entities, each with a :p/name, a :p/k and a
// :p/v, some k equal to some v, so two correlation parameters bound by
// equal or different outer symbols select different groups.
func refuteTwoParamFacts() []fact {
	e1, e2, e3, e4, e5, e6 := named("e1"), named("e2"), named("e3"), named("e4"), named("e5"), named("e6")
	return []fact{
		{e1, ":p/name", "e1"}, {e1, ":p/k", int64(1)}, {e1, ":p/v", int64(1)},
		{e2, ":p/name", "e2"}, {e2, ":p/k", int64(1)}, {e2, ":p/v", int64(2)},
		{e3, ":p/name", "e3"}, {e3, ":p/k", int64(2)}, {e3, ":p/v", int64(2)},
		{e4, ":p/name", "e4"}, {e4, ":p/k", int64(2)}, {e4, ":p/v", int64(1)},
		{e5, ":p/name", "e5"}, {e5, ":p/k", int64(3)}, {e5, ":p/v", int64(3)},
		{e6, ":p/name", "e6"}, {e6, ":p/k", int64(1)}, {e6, ":p/v", int64(3)},
	}
}

// refuteTwoParamQuery is one generated query and whether its inner :find
// aggregates. Every inner query's body is the same two data patterns, so an
// inner query with an aggregate is one Rule 1 decorrelates, and one without
// is the bare data patterns Rule 4 declines.
type refuteTwoParamQuery struct {
	text       string
	aggregates bool
}

// refuteTwoParamQueries enumerates correlated subqueries with two inner
// correlation parameters ?a and ?c, the call passing them (?b ?w), (?b ?b)
// or (?w ?b) from the outer [?q :p/k ?b] [?q :p/v ?w], and every relation
// binding over ?b ?w ?q ?m ?n of each inner :find, repeats included,
// plus the scalar and collection bindings of a one-element :find. Each
// query carries whether its inner :find aggregates.
func refuteTwoParamQueries() []refuteTwoParamQuery {
	type inner struct {
		find       string
		arity      int
		aggregates bool
	}
	inners := []inner{
		{"(count ?z)", 1, true},
		{"?a (count ?z)", 2, true},
		{"?c (count ?z)", 2, true},
		{"(count ?z) ?a", 2, true},
		{"?a ?c (count ?z)", 3, true},
		{"?z", 1, false},
		{"?a ?z", 2, false},
	}
	body := "[?z :p/k ?a] [?z :p/v ?c]"
	calls := []string{"?b ?w", "?b ?b", "?w ?b"}
	names := []string{"?b", "?w", "?q", "?m", "?n"}
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
	var queries []refuteTwoParamQuery
	for _, call := range calls {
		for _, in := range inners {
			subquery := fmt.Sprintf("(q [:find %s :in $ ?a ?c :where %s] $ %s)", in.find, body, call)
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
				bound := []string{"?q", "?b", "?w"}
				for _, sym := range strings.Fields(strings.NewReplacer("[", " ", "]", " ", "...", " ").Replace(form)) {
					if !reviewNamesString(bound, sym) {
						bound = append(bound, sym)
					}
				}
				queries = append(queries, refuteTwoParamQuery{
					text: fmt.Sprintf("[:find %s :where [?q :p/k ?b] [?q :p/v ?w] [%s %s]]",
						strings.Join(bound, " "), subquery, form),
					aggregates: in.aggregates,
				})
			}
		}
	}
	return queries
}

// refuteOutcome is what a query answers through its iterator, each identity
// rendered by its :p/name read back from the database, the tuples sorted, or
// its error's first line.
func refuteOutcome(t *testing.T, d *Database, text string) string {
	nameTuples, err := executor.CollectTuples(d.Query(`[:find ?e ?n :where [?e :p/name ?n]]`))
	require.NoError(t, err)
	names := map[datalog.Identity]string{}
	for _, tuple := range nameTuples {
		names[tuple[0].(datalog.Identity)] = tuple[1].(string)
	}
	tuples, err := executor.CollectTuples(d.Query(text))
	if err != nil {
		return "error: " + strings.SplitN(err.Error(), "\n", 2)[0]
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

// A correlated subquery with two correlation parameters answers alike on
// both optimizer paths whatever names its binding writes, relation or tuple
// (CLAUDE.md, Semantic Preservation). REFUTE_SOURCES_GOLDEN names a file of
// the merge-base's unoptimized answers (REFUTE_SOURCES_WRITE=1 records it).
func TestRefuteSources_TwoParameterDecorrelationAnswersAlikeOnBothPaths(t *testing.T) {
	var queries []string
	for _, generated := range refuteTwoParamQueries() {
		queries = append(queries, generated.text)
	}
	outcomes := map[bool]map[string]string{}
	applied := map[string]bool{}
	for _, mode := range optimizerModes {
		var decorrelated bool
		handler := func(event annotations.Event) {
			if event.Name == annotations.AlgebraDecorrelateApply {
				decorrelated = true
			}
		}
		d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: handler})
		commitFacts(t, d, refuteTwoParamFacts())
		answered := map[string]string{}
		for _, text := range queries {
			decorrelated = false
			answered[text] = refuteOutcome(t, d, text)
			if mode.algebra && decorrelated {
				applied[text] = true
			}
		}
		outcomes[mode.algebra] = answered
	}
	on, off := outcomes[true], outcomes[false]
	if dump := os.Getenv("REFUTE_SOURCES_DUMP"); dump != "" {
		encoded, err := json.MarshalIndent(map[string]map[string]string{"on": on, "off": off}, "", " ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(dump, encoded, 0o644))
	}
	for _, text := range queries {
		t.Run(text, func(t *testing.T) {
			require.False(t, strings.HasPrefix(off[text], "error:"), "the unoptimized path answers: %s", off[text])
			require.Equal(t, off[text], on[text], "the optimizer answers what the unoptimized path answers (decorrelated: %v)", applied[text])
		})
	}
	golden := os.Getenv("REFUTE_SOURCES_GOLDEN")
	if golden == "" {
		return
	}
	if os.Getenv("REFUTE_SOURCES_WRITE") == "1" {
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
			require.Equal(t, reference[text], off[text], "the unoptimized path answers what the merge-base answers")
			require.Equal(t, reference[text], on[text], "the optimizer answers what the merge-base answers")
		})
	}
}

// Every generated two-parameter query takes the decorrelation decision of its
// shape, so the differential runs Rule 1 and Rule 4's decline (CLAUDE.md,
// structural invariants through annotations; ALGEBRA.md, Rule 4): a subquery
// whose inner :find aggregates is decorrelated, and one whose inner query is
// bare data patterns stays correlated, declined with Rule 4's reason. The
// generator builds both shapes.
func TestRefuteSources_EachTwoParameterSubqueryTakesTheDecorrelationDecisionOfItsShape(t *testing.T) {
	applied := 0
	var declined []interface{}
	handler := func(event annotations.Event) {
		switch event.Name {
		case annotations.AlgebraDecorrelateApply:
			applied++
		case annotations.AlgebraDecorrelateSkip:
			declined = append(declined, event.Data["reason"])
		}
	}
	d := createOptimizerModeDB(t, pinned(optimizerModes, true)[0], DatabaseOptions{AnnotationHandler: handler})
	commitFacts(t, d, refuteTwoParamFacts())
	shapes := map[bool]int{}
	for _, query := range refuteTwoParamQueries() {
		shapes[query.aggregates]++
		t.Run(query.text, func(t *testing.T) {
			d.ClearPlanCache()
			applied, declined = 0, nil
			outcome := refuteOutcome(t, d, query.text)
			require.False(t, strings.HasPrefix(outcome, "error:"), outcome)
			if query.aggregates {
				require.Positive(t, applied, "the optimizer decorrelates a subquery whose inner :find aggregates (Rule 1)")
				require.Empty(t, declined, "no decorrelation of an aggregating subquery is declined")
				return
			}
			require.Zero(t, applied, "the optimizer leaves a subquery of bare data patterns correlated (Rule 4)")
			require.NotEmpty(t, declined, "Rule 4 records its decline")
			for _, reason := range declined {
				require.Equal(t, "pure DataPattern query — indexed lookup is faster", reason, "Rule 4 declines with its reason")
			}
		})
	}
	require.Positive(t, shapes[true], "the generator builds subqueries whose inner :find aggregates")
	require.Positive(t, shapes[false], "the generator builds subqueries of bare data patterns")
}
