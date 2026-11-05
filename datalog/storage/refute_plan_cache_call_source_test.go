package storage

import (
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// refuteCallSourceWrite is one generated write of :t/a.
type refuteCallSourceWrite struct {
	tx     int
	entity int
	remove bool
	value  string
}

// refuteCallSourceHistory is a generated history: the cardinality :t/a is
// declared under (nil for none) and its writes.
type refuteCallSourceHistory struct {
	card   datalog.Keyword
	txs    int
	writes []refuteCallSourceWrite
}

func generateRefuteCallSourceHistories(seed int64, n int) []refuteCallSourceHistory {
	r := rand.New(rand.NewSource(seed))
	cards := []datalog.Keyword{nil, schema.CardinalityOne, schema.CardinalityOne, schema.CardinalityMany}
	values := []string{"p", "q", "r", "s"}
	var out []refuteCallSourceHistory
	for i := 0; i < n; i++ {
		h := refuteCallSourceHistory{card: cards[r.Intn(len(cards))], txs: 1 + r.Intn(4)}
		for tx := 0; tx < h.txs; tx++ {
			for w := 0; w < 1+r.Intn(3); w++ {
				h.writes = append(h.writes, refuteCallSourceWrite{tx: tx, entity: r.Intn(4), remove: tx > 0 && r.Intn(3) == 0, value: values[r.Intn(len(values))]})
			}
		}
		out = append(out, h)
	}
	return out
}

// refuteCallSourceDatabase is a database holding h under the algebra
// optimizer or not, with the transactions its writes landed in.
func refuteCallSourceDatabase(t *testing.T, h refuteCallSourceHistory, algebra bool) (*Database, []datalog.ElementID) {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: datalog.NewKeyword(":t/name"), ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	if h.card != nil {
		s.Add(&schema.AttributeDefinition{Ident: datalog.NewKeyword(":t/a"), ValueType: schema.TypeString, Cardinality: h.card})
	}
	opts := DefaultPlannerOptions()
	opts.EnableAlgebraOptimizer = algebra
	d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory-trees", Schema: s, PlannerOptions: &opts})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	name := datalog.NewKeyword(":t/name")
	a := datalog.NewKeyword(":t/a")
	var entities []datalog.Identity
	for i := 0; i < 4; i++ {
		entities = append(entities, datalog.NewIdentity(fmt.Sprintf("refute-call-source:e%d", i)))
	}
	var txIDs []datalog.ElementID
	for tx := 0; tx < h.txs; tx++ {
		w := d.NewTransaction()
		if tx == 0 {
			for i, e := range entities {
				require.NoError(t, w.Add(e, name, fmt.Sprintf("e%d", i)))
			}
		}
		for _, op := range h.writes {
			if op.tx != tx {
				continue
			}
			if op.remove {
				require.NoError(t, w.Remove(entities[op.entity], a, op.value))
			} else {
				require.NoError(t, w.Add(entities[op.entity], a, op.value))
			}
		}
		id, err := w.Commit()
		require.NoError(t, err)
		txIDs = append(txIDs, id)
	}
	return d, txIDs
}

// refuteCallSourceAnswer is a relation's tuples as a sorted set, each set
// value's members sorted, or the error.
func refuteCallSourceAnswer(rel executor.Relation, err error) ([][]interface{}, error) {
	tuples, err := executor.CollectTuples(rel, err)
	if err != nil {
		return nil, err
	}
	for _, tuple := range tuples {
		for j, value := range tuple {
			if members, isSet := value.([]interface{}); isSet {
				sorted := slices.Clone(members)
				slices.SortFunc(sorted, datalog.CompareValues)
				tuple[j] = sorted
			}
		}
	}
	slices.SortFunc(tuples, func(x, y []interface{}) int {
		for i := range x {
			if c := datalog.CompareValues(x[i], y[i]); c != 0 {
				return c
			}
		}
		return 0
	})
	return slices.CompactFunc(tuples, func(x, y []interface{}) bool { return reflect.DeepEqual(x, y) }), nil
}

const refuteCallSourceText = `[:find ?e ?v :in $ $src % :where [?e :t/name _] (r ?e ?v)]`

// refuteCallSourceShapes are rule bodies whose subquery reads its inner
// sources under the names its call passes.
var refuteCallSourceShapes = []struct {
	name  string
	rules string
}{
	{"constant input, inner invocation", `[[(r ?e ?v) [?e :t/name _] [(q [:find ?w :in $ % ?n :where [?x :t/name ?n] (s ?x) [(get-else $ ?x :t/a "none") ?w]] $src % "e1") [[?v]]]] [(s ?y) [?y :t/name _]]]`},
	{"constant input, inner invocation after the get-else", `[[(r ?e ?v) [?e :t/name _] [(q [:find ?w :in $ % ?n :where [?x :t/name ?n] [(get-else $ ?x :t/a "none") ?w] (s ?x)] $src % "e1") [[?v]]]] [(s ?y) [?y :t/name _]]]`},
	{"constant input, aggregating, inner invocation", `[[(r ?e ?v) [?e :t/name _] [(q [:find (count ?w) :in $ % ?n :where [?x :t/name ?n] (s ?x) [(get-else $ ?x :t/a "none") ?w]] $src % "e1") [[?v]]]] [(s ?y) [?y :t/name _]]]`},
	{"constant input, inner invocation, nested call passing $", `[[(r ?e ?v) [?e :t/name _] [(q [:find ?w :in $ % ?n :where [?x :t/name ?n] (s ?x) [(q [:find (max ?u) :in $ ?z :where [?z :t/name _] [(get-else $ ?z :t/a "none") ?u]] $ ?x) [[?w]]]] $src % "e1") [[?v]]]] [(s ?y) [?y :t/name _]]]`},
	{"constant input, inner invocation, under an or-join", `[[(r ?e ?v) [?e :t/name _] (or-join [[?e] ?v] (and [(q [:find ?w :in $ % ?n :where [?x :t/name ?n] (s ?x) [(get-else $ ?x :t/a "none") ?w]] $src % "e1") [[?v]]]) (and [?e :t/none ?v]))] [(s ?y) [?y :t/name _]]]`},
	{"control: inner source named $a", `[[(r ?e ?v) [?e :t/name _] [(q [:find ?w :in $a % ?n :where [$a ?x :t/name ?n] (s ?x) [(get-else $a ?x :t/a "none") ?w]] $src % "e1") [[?v]]]] [(s ?y) [?y :t/name _]]]`},
	{"control: the call passes $", `[[(r ?e ?v) [?e :t/name _] [(q [:find ?w :in $ % ?n :where [?x :t/name ?n] (s ?x) [(get-else $ ?x :t/a "none") ?w]] $ % "e1") [[?v]]]] [(s ?y) [?y :t/name _]]]`},
	{"control: inner invocation's body holds the get-else", `[[(r ?e ?v) [?e :t/name _] [(q [:find ?w :in $ % ?n :where [?x :t/name ?n] (g ?x ?w)] $src % "e1") [[?v]]]] [(g ?y ?w) [?y :t/name _] [(get-else $ ?y :t/a "none") ?w]]]`},
}

// Over generated histories, a bound invocation whose body holds a subquery
// passing $src at its inner source answers with the algebra optimizer on
// what it answers with it off, for $src each handle of the history and a
// second database holding the same facts under another declaration of :t/a.
// Each answer is read through Query and Analyze on a fresh database and
// through Query on a database that first ran the text with $src the latest
// view, so one plan cache serves the second run.
func TestRefutePlanCache_ARuleBodySubqueryReadsTheSourceItsCallPassesOnBothOptimizerPaths(t *testing.T) {
	type view struct {
		name string
		of   func(t *testing.T, h refuteCallSourceHistory, d *Database, txIDs []datalog.ElementID) *Database
	}
	twin := func(card datalog.Keyword) func(t *testing.T, h refuteCallSourceHistory, d *Database, txIDs []datalog.ElementID) *Database {
		return func(t *testing.T, h refuteCallSourceHistory, d *Database, txIDs []datalog.ElementID) *Database {
			other := h
			other.card = card
			db, dbTx := refuteCallSourceDatabase(t, other, false)
			require.Len(t, dbTx, len(txIDs))
			return db
		}
	}
	views := []view{
		{"latest", func(t *testing.T, h refuteCallSourceHistory, d *Database, txIDs []datalog.ElementID) *Database { return d }},
		{"history", func(t *testing.T, h refuteCallSourceHistory, d *Database, txIDs []datalog.ElementID) *Database { return d.History() }},
		{"as-of first", func(t *testing.T, h refuteCallSourceHistory, d *Database, txIDs []datalog.ElementID) *Database { return d.AsOf(txIDs[0]) }},
		{"cardinality-many database", twin(schema.CardinalityMany)},
		{"cardinality-one database", twin(schema.CardinalityOne)},
	}
	sources := func(src *Database) QueryOption {
		return WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): src})
	}
	run := func(d, src *Database, rules string) ([][]interface{}, error) {
		rs, err := parser.ParseRules(rules)
		if err != nil {
			return nil, err
		}
		return refuteCallSourceAnswer(d.Query(refuteCallSourceText, rs, sources(src)))
	}
	analyze := func(d, src *Database, rules string) ([][]interface{}, error) {
		rs, err := parser.ParseRules(rules)
		if err != nil {
			return nil, err
		}
		result, err := d.Analyze(refuteCallSourceText, rs, sources(src))
		if err != nil {
			return nil, err
		}
		return refuteCallSourceAnswer(result.Result, nil)
	}
	diverged := map[string]int{}
	first := map[string]string{}
	total := 0
	for i, h := range generateRefuteCallSourceHistories(4112026, 30) {
		for _, shape := range refuteCallSourceShapes {
			for _, v := range views {
				total++
				off, offTx := refuteCallSourceDatabase(t, h, false)
				want, wantErr := run(off, v.of(t, h, off, offTx), shape.rules)
				on, onTx := refuteCallSourceDatabase(t, h, true)
				got, gotErr := run(on, v.of(t, h, on, onTx), shape.rules)
				analyzed, analyzedTx := refuteCallSourceDatabase(t, h, true)
				gotAnalyzed, gotAnalyzedErr := analyze(analyzed, v.of(t, h, analyzed, analyzedTx), shape.rules)
				cached, cachedTx := refuteCallSourceDatabase(t, h, true)
				gotCached, gotCachedErr := want, wantErr
				if latest, latestErr := run(cached, cached, shape.rules); latestErr != nil || latest == nil {
					key := shape.name + " / latest run refused"
					diverged[key]++
					first[key] = fmt.Sprint(latestErr)
				} else {
					gotCached, gotCachedErr = run(cached, v.of(t, h, cached, cachedTx), shape.rules)
				}
				for _, path := range []struct {
					name string
					got  [][]interface{}
					err  error
				}{{"Query", got, gotErr}, {"Analyze", gotAnalyzed, gotAnalyzedErr}, {"Query after a latest run", gotCached, gotCachedErr}} {
					if (wantErr == nil) != (path.err == nil) || !reflect.DeepEqual(want, path.got) {
						key := shape.name + " / " + v.name + " / " + path.name
						diverged[key]++
						if _, seen := first[key]; !seen {
							first[key] = fmt.Sprintf("history %d card=%v writes=%v: off %v %v, on %v %v", i, h.card, h.writes, want, wantErr, path.got, path.err)
						}
					}
				}
			}
		}
	}
	t.Logf("compared %d (history, shape, view) triples", total)
	keys := make([]string, 0, len(diverged))
	for key := range diverged {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		t.Logf("%s: %d, first: %s", key, diverged[key], first[key])
	}
	require.Empty(t, diverged, "shape / view / path where the optimizer-on answer differs from the optimizer-off answer")
}

// refuteCallSourceAnswerInto is the struct QueryInto and QueryOneInto fill.
type refuteCallSourceAnswerInto struct {
	E datalog.Identity `datalog:"?e"`
	V string           `datalog:"?v"`
}

// :t/a, declared cardinality-one, holds "old" and then "new" at the entity
// named "one". A bound invocation's body holds a subquery over a constant
// that invokes a rule and reads a get-else of :t/a on its inner $, which the
// call binds to $src, the history handle. Every entry answers with the
// algebra optimizer on what it answers with it off.
func TestRefutePlanCache_EveryEntryAnswersARuleBodySubqueryOverAHistoryHandleAlikeOnBothOptimizerPaths(t *testing.T) {
	name := datalog.NewKeyword(":t/name")
	a := datalog.NewKeyword(":t/a")
	one := datalog.NewIdentity("refute-call-source:one")
	text := `[:find ?e ?v :in $ $src % :where [?e :t/name _] (r ?e ?v)]`
	rules := `[[(r ?e ?v) [?e :t/name _] [(q [:find ?w :in $ % ?n :where [?x :t/name ?n] (s ?x) [(get-else $ ?x :t/a "none") ?w]] $src % "one") [[?v]]]] [(s ?y) [?y :t/name _]]]`
	open := func(algebra bool) *Database {
		s := schema.NewSchema()
		s.Add(&schema.AttributeDefinition{Ident: name, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
		s.Add(&schema.AttributeDefinition{Ident: a, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
		opts := DefaultPlannerOptions()
		opts.EnableAlgebraOptimizer = algebra
		d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory-trees", Schema: s, PlannerOptions: &opts})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, d.Close()) })
		tx := d.NewTransaction()
		require.NoError(t, tx.Add(one, name, "one"))
		require.NoError(t, tx.Add(one, a, "old"))
		_, err = tx.Commit()
		require.NoError(t, err)
		tx = d.NewTransaction()
		require.NoError(t, tx.Add(one, a, "new"))
		_, err = tx.Commit()
		require.NoError(t, err)
		return d
	}
	answers := map[bool]map[string]string{}
	for _, algebra := range []bool{false, true} {
		byEntry := map[string]string{}
		rs, err := parser.ParseRules(rules)
		require.NoError(t, err)
		d := open(algebra)
		src := WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): d.History()})
		queried, err := refuteCallSourceAnswer(d.Query(text, rs, src))
		byEntry["Query"] = fmt.Sprint(queried, err)
		analyzed, err := d.Analyze(text, rs, src)
		require.NoError(t, err)
		analyzedTuples, err := refuteCallSourceAnswer(analyzed.Result, nil)
		byEntry["Analyze"] = fmt.Sprint(analyzedTuples, err)
		var into []refuteCallSourceAnswerInto
		err = d.QueryInto(&into, text, rs, src)
		var intoValues []string
		for _, x := range into {
			intoValues = append(intoValues, x.V)
		}
		slices.Sort(intoValues)
		byEntry["QueryInto"] = fmt.Sprint(intoValues, err)
		var single refuteCallSourceAnswerInto
		found, err := d.QueryOneInto(&single, text, rs, src)
		byEntry["QueryOneInto"] = fmt.Sprint(found, single.V, err)
		cached := open(algebra)
		latest, err := refuteCallSourceAnswer(cached.Query(text, rs, WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): cached})))
		require.NoError(t, err)
		require.NotEmpty(t, latest)
		served, err := refuteCallSourceAnswer(cached.Query(text, rs, WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): cached.History()})))
		byEntry["Query after a latest run"] = fmt.Sprint(served, err)
		answers[algebra] = byEntry
	}
	differ := map[string]string{}
	for entry, off := range answers[false] {
		if on := answers[true][entry]; on != off {
			differ[entry] = fmt.Sprintf("off %s, on %s", off, on)
		}
	}
	require.Empty(t, differ, "entries where the optimizer-on answer differs from the optimizer-off answer")
}
