package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// The consumers' shapes of docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md
// §8, each run over a bindery and compared with a reference computed in Go
// from the model the fixture is written from. Sheets feed quires and quires
// feed blocks, eight to a block. A quire sits in one of the press hall's
// states, joined by moves two of which spoil it. A quire follows its
// predecessors, and its lifecycle events carry a kind and a sequence number.

// binderyStates are the states a quire passes through, and binderyNext the
// moves between them: the straight run from collated to cased, a torn quire
// recut and collated again, and a warped quire lost.
var binderyStates = []string{"collated", "stitched", "pressed", "trimmed", "cased", "torn", "recut", "warped"}

var binderyNext = [][2]string{
	{"collated", "stitched"}, {"stitched", "pressed"}, {"stitched", "torn"},
	{"pressed", "trimmed"}, {"pressed", "warped"}, {"trimmed", "cased"},
	{"torn", "recut"}, {"recut", "collated"},
}

// binderyEvent is one lifecycle event of a quire: its kind and its place in
// the quire's sequence.
type binderyEvent struct {
	kind string
	seq  int64
}

// bindery is the model a fixture is written from and a reference is computed
// over: quires numbered from zero, each in the state its number selects, after
// its predecessors, with its events; sheetsPerQuire sheets feeding each
// quire; and each quire feeding its block.
type bindery struct {
	quires, sheetsPerQuire int
	after                  map[int][]int
	events                 map[int][]binderyEvent
	state                  map[int]string
	reach                  map[[2]string]bool
	names                  map[datalog.Identity]string
}

func binderySheetID(i int) datalog.Identity { return datalog.NewIdentity(fmt.Sprintf("bindery/sheet/%d", i)) }
func binderyQuireID(j int) datalog.Identity { return datalog.NewIdentity(fmt.Sprintf("bindery/quire/%d", j)) }
func binderyBlockID(k int) datalog.Identity { return datalog.NewIdentity(fmt.Sprintf("bindery/block/%d", k)) }
func binderyStateID(name string) datalog.Identity {
	return datalog.NewIdentity("bindery/state/" + name)
}

// newBindery builds the model: quire j is in state j mod eight, follows quire
// j-1 unless it opens a block and quire j-4 as well when it is the fifth of
// one, and is collated at sequence one, stitched at two unless j is a
// multiple of three, and collated again at three when j is a multiple of
// five.
func newBindery(quires, sheetsPerQuire int) *bindery {
	b := &bindery{
		quires: quires, sheetsPerQuire: sheetsPerQuire,
		after: map[int][]int{}, events: map[int][]binderyEvent{}, state: map[int]string{},
		reach: closureOf(graphSpec{nodes: binderyStates, edges: binderyNext}),
		names: map[datalog.Identity]string{},
	}
	for _, name := range binderyStates {
		b.names[binderyStateID(name)] = name
	}
	for j := 0; j < quires; j++ {
		b.names[binderyQuireID(j)] = fmt.Sprintf("q%d", j)
		b.names[binderyBlockID(j/8)] = fmt.Sprintf("b%d", j/8)
		b.state[j] = binderyStates[j%len(binderyStates)]
		if j%8 != 0 {
			b.after[j] = append(b.after[j], j-1)
		}
		if j%8 == 5 {
			b.after[j] = append(b.after[j], j-4)
		}
		b.events[j] = []binderyEvent{{"collated", 1}}
		if j%3 != 0 {
			b.events[j] = append(b.events[j], binderyEvent{"stitched", 2})
		}
		if j%5 == 0 {
			b.events[j] = append(b.events[j], binderyEvent{"collated", 3})
		}
		for i := 0; i < sheetsPerQuire; i++ {
			s := j*sheetsPerQuire + i
			b.names[binderySheetID(s)] = fmt.Sprintf("s%d", s)
		}
	}
	return b
}

// binderySchema declares the model's attributes.
func binderySchema() *schema.Schema {
	return schema.NewBuilder().
		Attribute(":bindery/feeds").Type(schema.TypeRef).Many().Add().
		Attribute(":quire/id").Type(schema.TypeLong).Add().
		Attribute(":quire/state").Type(schema.TypeRef).Add().
		Attribute(":quire/after").Type(schema.TypeRef).Many().Add().
		Attribute(":state/name").Type(schema.TypeKeyword).Add().
		Attribute(":state/next").Type(schema.TypeRef).Many().Add().
		Attribute(":event/quire").Type(schema.TypeRef).Add().
		Attribute(":event/kind").Type(schema.TypeKeyword).Add().
		Attribute(":event/seq").Type(schema.TypeLong).Add().
		MustBuild()
}

// write commits the model into d, graphBatch datoms per transaction, and
// returns the graph that renders its identities by name.
func (b *bindery) write(t testing.TB, d *Database) graph {
	t.Helper()
	tx := d.NewTransaction()
	written := 0
	add := func(e datalog.Identity, a datalog.Keyword, v interface{}) {
		require.NoError(t, tx.Add(e, a, v))
		written++
		if written == graphBatch {
			_, err := tx.Commit()
			require.NoError(t, err)
			tx = d.NewTransaction()
			written = 0
		}
	}
	for _, name := range binderyStates {
		add(binderyStateID(name), kw(":state/name"), kw(":"+name))
	}
	for _, move := range binderyNext {
		add(binderyStateID(move[0]), kw(":state/next"), binderyStateID(move[1]))
	}
	for j := 0; j < b.quires; j++ {
		q := binderyQuireID(j)
		add(q, kw(":quire/id"), int64(j))
		add(q, kw(":quire/state"), binderyStateID(b.state[j]))
		add(q, kw(":bindery/feeds"), binderyBlockID(j/8))
		for _, p := range b.after[j] {
			add(q, kw(":quire/after"), binderyQuireID(p))
		}
		for _, ev := range b.events[j] {
			e := datalog.NewIdentity(fmt.Sprintf("bindery/event/%d/%d", j, ev.seq))
			add(e, kw(":event/quire"), q)
			add(e, kw(":event/kind"), kw(":"+ev.kind))
			add(e, kw(":event/seq"), ev.seq)
		}
		for i := 0; i < b.sheetsPerQuire; i++ {
			add(binderySheetID(j*b.sheetsPerQuire+i), kw(":bindery/feeds"), q)
		}
	}
	_, err := tx.Commit()
	require.NoError(t, err)
	return graph{names: b.names}
}

// fedBy is what sheet s feeds at any distance, its quire and its block,
// rendered as the graph renders a pair.
func (b *bindery) fedBy(s int) []string {
	q := s / b.sheetsPerQuire
	return []string{fmt.Sprintf("s%d>q%d", s, q), fmt.Sprintf("s%d>b%d", s, q/8)}
}

// latestKind is the kind of quire j's last event.
func (b *bindery) latestKind(j int) string {
	events := b.events[j]
	return events[len(events)-1].kind
}

// blocked reports whether a predecessor of quire j, at any distance, has a
// latest event of another kind than kind.
func (b *bindery) blocked(j int, kind string) bool {
	for _, p := range b.after[j] {
		if b.latestKind(p) != kind || b.blocked(p, kind) {
			return true
		}
	}
	return false
}

// quiresWhere is each quire for which pred holds, rendered by name.
func (b *bindery) quiresWhere(pred func(j int) bool) []string {
	var out []string
	for j := 0; j < b.quires; j++ {
		if pred(j) {
			out = append(out, fmt.Sprintf("q%d", j))
		}
	}
	return out
}

// ready is each quire none of whose predecessors, at any distance, has a
// latest event of another kind than kind.
func (b *bindery) ready(kind string) []string {
	return b.quiresWhere(func(j int) bool { return !b.blocked(j, kind) })
}

// finishable is each quire whose state reaches cased, and stranded each
// quire whose state does not.
func (b *bindery) finishable() []string {
	return b.quiresWhere(func(j int) bool { return b.reach[[2]string{b.state[j], "cased"}] })
}

func (b *bindery) stranded() []string {
	return b.quiresWhere(func(j int) bool { return !b.reach[[2]string{b.state[j], "cased"}] })
}

// statesReached counts the states reachable from state.
func (b *bindery) statesReached(state string) int {
	n := 0
	for _, t := range binderyStates {
		if b.reach[[2]string{state, t}] {
			n++
		}
	}
	return n
}

// stateCounts is each quire beside the count of states reachable from its
// state, rendered as the graph renders a quire and a long.
func (b *bindery) stateCounts() []string {
	var out []string
	for j := 0; j < b.quires; j++ {
		out = append(out, fmt.Sprintf("q%d>%d", j, b.statesReached(b.state[j])))
	}
	return out
}

// binderyReaches derives reaches, the closure of :bindery/feeds, through an
// or-join holding the recursion in one branch. Its required run names ?x, the
// position a caller binds, so each branch seeks on it and the recursion is
// seeded through it.
const binderyReaches = `[[(reaches [?x] ?y) (or-join [[?x] ?y] [?x :bindery/feeds ?y] (and [?x :bindery/feeds ?z] (reaches ?z ?y)))]]`

// binderyReadiness derives ready: a quire's latest event is the one no
// later event of the quire follows; a quire is blocked for a kind when a
// predecessor's latest event is of another kind, or when a predecessor is
// blocked; a quire is ready for the kind when it is not blocked.
const binderyReadiness = `[[(latest ?q ?k) [?e :event/quire ?q] [?e :event/kind ?k] [?e :event/seq ?n]
                            (not-join [?q ?n] [?f :event/quire ?q] [?f :event/seq ?m] [(> ?m ?n)])]
                           [(blocked [?k] ?q) [?q :quire/after ?p] (not (latest ?p ?k))]
                           [(blocked [?k] ?q) [?q :quire/after ?p] (blocked ?k ?p)]
                           [(ready [?k] ?q) [?q :quire/id _] (not (blocked ?k ?q))]]`

// binderyStateReach derives reaches, the closure of :state/next.
const binderyStateReach = `[[(reaches ?x ?y) [?x :state/next ?y]]
                            [(reaches ?x ?z) [?x :state/next ?y] (reaches ?y ?z)]]`

const (
	binderyReachesQuery    = `[:find ?s ?y :in $ % [?s ...] :where (reaches ?s ?y)]`
	binderyReadyQuery      = `[:find ?q :in $ % ?k :where (ready ?k ?q)]`
	binderyFinishableQuery = `[:find ?q :in $ % ?fin :where [?q :quire/state ?s] (reaches ?s ?fin)]`
	binderyReconcileQuery  = `[:find ?q :in $ % ?fin :where [?q :quire/state ?s] (not-join [?s ?fin] (reaches ?s ?fin))]`
	binderyCountQuery      = `[:find ?q ?n :in $ % :where [?q :quire/state ?s] (or-default [(q [:find (count ?t) :in $ % ?s :where (reaches ?s ?t)] $ % ?s) [[?n]]] [(ground 0) ?n])]`
)

// A reaches rule whose body holds an or-join, seeded by a collection input of
// changed sheets, answers each sheet's quire and block and costs one rewrite,
// seeded by every changed sheet at once.
func TestBindery_ReachesFromChangedSheetsAnswersTheReference(t *testing.T) {
	changed := []int{0, 9, 50, 127}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			counts := &ruleEventCounts{}
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: binderySchema(), AnnotationHandler: counts.handle})
			model := newBindery(16, 8)
			g := model.write(t, d)
			var sheets []interface{}
			var want []string
			for _, s := range changed {
				sheets = append(sheets, binderySheetID(s))
				want = append(want, model.fedBy(s)...)
			}
			rel, err := d.Query(binderyReachesQuery, mustRuleSet(t, binderyReaches), sheets)
			require.NoError(t, err)
			require.ElementsMatch(t, want, g.tuples(t, rel))
			require.Equal(t, 1, counts.snapshot()[0], "one rewrite, seeded by the changed sheets; the events:\n%s", counts.events())
		})
	}
}

// A readiness rule over predecessor quires whose latest lifecycle event is a
// given kind, with stratified negation over the latest event and a recursive
// propagation of blocking, answers the quires the reference finds ready.
func TestBindery_ReadinessOverPredecessorQuiresAnswersTheReference(t *testing.T) {
	model := newBindery(64, 0)
	want := model.ready("stitched")
	require.NotEmpty(t, want, "the model holds ready quires for the contract to be testable")
	require.Less(t, len(want), model.quires, "the model holds blocked quires for the contract to be testable")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: binderySchema()})
			g := model.write(t, d)
			rel, err := d.Query(binderyReadyQuery, mustRuleSet(t, binderyReadiness), kw(":stitched"))
			require.NoError(t, err)
			require.ElementsMatch(t, want, g.tuples(t, rel))
		})
	}
}

// A reachability query over the state graph answers the quires whose state
// still reaches cased.
func TestBindery_FinishableQuiresAnswerTheReference(t *testing.T) {
	model := newBindery(64, 0)
	want := model.finishable()
	require.NotEmpty(t, want, "the model holds finishable quires for the contract to be testable")
	require.Less(t, len(want), model.quires, "the model holds stranded quires for the contract to be testable")
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: binderySchema()})
			g := model.write(t, d)
			rel, err := d.Query(binderyFinishableQuery, mustRuleSet(t, binderyStateReach), binderyStateID("cased"))
			require.NoError(t, err)
			require.ElementsMatch(t, want, g.tuples(t, rel))
		})
	}
}

// binderyEventSizes are the quire counts the event-counting tests run at: one
// window of the subject, and several windows up to the default cap.
var binderyEventSizes = []int{64, 1200}

// strataBegun counts the stratum-begin events a query emits, per stratum by
// its first predicate, from the parallel workers the engine does not
// serialize.
type strataBegun struct {
	mu     sync.Mutex
	begins map[query.Symbol]int
}

func (s *strataBegun) handle(e annotations.Event) {
	if e.Name != annotations.RulesStratumBegin {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.begins == nil {
		s.begins = map[query.Symbol]int{}
	}
	s.begins[e.Data["predicates"].([]query.Symbol)[0]]++
}

// perStratum is the begin count per stratum.
func (s *strataBegun) perStratum() map[query.Symbol]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[query.Symbol]int, len(s.begins))
	for stratum, n := range s.begins {
		out[stratum] = n
	}
	return out
}

// A reconcile query over N quires invoking the reachability rule inside a
// not-join answers the quires whose state does not reach cased and begins
// each stratum of the rewritten set once whatever N is, the fixpoint
// continued across the windows: the strata begun over one window of quires
// are those begun over several, each once.
func TestBindery_ReconcileUnderNotJoinBeginsEachStratumOnce(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var strata []map[query.Symbol]int
			for _, n := range binderyEventSizes {
				model := newBindery(n, 0)
				want := model.stranded()
				require.NotEmpty(t, want, "the model holds stranded quires for the contract to be testable")
				begun := &strataBegun{}
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: binderySchema(), AnnotationHandler: begun.handle})
				g := model.write(t, d)
				rel, err := d.Query(binderyReconcileQuery, mustRuleSet(t, binderyStateReach), binderyStateID("cased"))
				require.NoError(t, err)
				require.ElementsMatch(t, want, g.tuples(t, rel))
				begins := begun.perStratum()
				require.NotEmpty(t, begins, "the rule set's strata began over %d quires", n)
				for stratum, count := range begins {
					require.Equal(t, 1, count, "stratum %s begun once over %d quires", stratum, n)
				}
				strata = append(strata, begins)
			}
			require.Equal(t, strata[0], strata[1], "the strata begun over %d quires and over %d are the same", binderyEventSizes[0], binderyEventSizes[1])
		})
	}
}

// A fallback over N quires whose first branch is a zero-default count
// subquery invoking the reachability rule answers each quire's count of
// reachable states, zero where its state reaches none, and begins each
// stratum of the rewritten set once whatever N is.
func TestBindery_FallbackCountSubqueryBeginsEachStratumOnce(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var strata []map[query.Symbol]int
			for _, n := range binderyEventSizes {
				model := newBindery(n, 0)
				begun := &strataBegun{}
				d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: binderySchema(), AnnotationHandler: begun.handle})
				g := model.write(t, d)
				rel, err := d.Query(binderyCountQuery, mustRuleSet(t, binderyStateReach))
				require.NoError(t, err)
				require.ElementsMatch(t, model.stateCounts(), g.tuples(t, rel))
				begins := begun.perStratum()
				require.NotEmpty(t, begins, "the rule set's strata began over %d quires", n)
				for stratum, count := range begins {
					require.Equal(t, 1, count, "stratum %s begun once over %d quires", stratum, n)
				}
				strata = append(strata, begins)
			}
			require.Equal(t, strata[0], strata[1], "the strata begun over %d quires and over %d are the same", binderyEventSizes[0], binderyEventSizes[1])
		})
	}
}

// BenchmarkBinderyReachesFromChangedSheets measures the reaches query seeded
// by ten changed sheets at a thousand and ten thousand sheets, eight to a
// quire.
func BenchmarkBinderyReachesFromChangedSheets(b *testing.B) {
	for _, sheets := range []int{1000, 10000} {
		for _, mode := range optimizerModes {
			b.Run(fmt.Sprintf("%d-sheets/%s", sheets, mode.name), func(b *testing.B) {
				opts := mode.plannerOptions()
				d := openBackendDB(b, mode.backend, DatabaseOptions{Schema: binderySchema(), PlannerOptions: &opts})
				model := newBindery(sheets/8, 8)
				model.write(b, d)
				var changed []interface{}
				for s := 0; s < sheets; s += sheets / 10 {
					changed = append(changed, binderySheetID(s))
				}
				rs := mustRuleSet(b, binderyReaches)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tuples, err := executor.CollectTuples(d.Query(binderyReachesQuery, rs, changed))
					if err != nil {
						b.Fatal(err)
					}
					if len(tuples) != 2*len(changed) {
						b.Fatalf("got %d tuples, want %d", len(tuples), 2*len(changed))
					}
				}
			})
		}
	}
}

// BenchmarkBinderyReconcile measures the reconcile query at a thousand and
// ten thousand quires.
func BenchmarkBinderyReconcile(b *testing.B) {
	for _, quires := range []int{1000, 10000} {
		for _, mode := range optimizerModes {
			b.Run(fmt.Sprintf("%d-quires/%s", quires, mode.name), func(b *testing.B) {
				opts := mode.plannerOptions()
				d := openBackendDB(b, mode.backend, DatabaseOptions{Schema: binderySchema(), PlannerOptions: &opts})
				model := newBindery(quires, 0)
				model.write(b, d)
				want := len(model.stranded())
				rs := mustRuleSet(b, binderyStateReach)
				fin := binderyStateID("cased")
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tuples, err := executor.CollectTuples(d.Query(binderyReconcileQuery, rs, fin))
					if err != nil {
						b.Fatal(err)
					}
					if len(tuples) != want {
						b.Fatalf("got %d tuples, want %d", len(tuples), want)
					}
				}
			})
		}
	}
}
