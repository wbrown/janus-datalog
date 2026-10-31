package storage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/edn"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// The kernel facts are a downstream's export: the facts of one program's
// kernel as their writer exported them, under testdata/janus-repro beside
// the schema the writer declared and the rule set and the queries the
// downstream runs over them. The rule set derives the cells reached from the
// entry (reach) and the cells reached while a vector is unfilled (unfilled),
// a mutually recursive stratum over several lower ones, and the bound query
// asks unfilled at the jumps that rely on a vector.

// kernelFactsDir holds the dump, its schema and its queries.
const kernelFactsDir = "testdata/janus-repro"

// kernelFacts is the fixture read: the schema, the rule set, and the four
// queries in the queries file's order.
type kernelFacts struct {
	schema *schema.Schema
	rules  *query.RuleSet
	// The queries file's forms after the rule set: the whole reach relation,
	// the whole unfilled relation, the reach count, and the bound query.
	reachWhole, unfilledWhole, reachCount, bound string
}

func readKernelFacts(t testing.TB) kernelFacts {
	t.Helper()
	s, err := schema.ParseSchemaFile(filepath.Join(kernelFactsDir, "kernel-facts.jdzl.schema.edn"))
	require.NoError(t, err)
	text, err := os.ReadFile(filepath.Join(kernelFactsDir, "kernel-facts.jdzl.queries.edn"))
	require.NoError(t, err)
	lexer := edn.NewLexer(string(text))
	require.NoError(t, lexer.Lex())
	forms, err := edn.NewParser(lexer).ParseAll()
	require.NoError(t, err)
	require.Len(t, forms, 5, "the queries file holds the rule set and four queries")
	return kernelFacts{
		schema:        s,
		rules:         mustRuleSet(t, forms[0].String()),
		reachWhole:    forms[1].String(),
		unfilledWhole: forms[2].String(),
		reachCount:    forms[3].String(),
		bound:         forms[4].String(),
	}
}

// openKernelFacts opens a database on mode's leg with the dump loaded under
// the writer's schema, every attribute a query names declared, and logs the
// load's time.
func openKernelFacts(t testing.TB, mode optimizerMode, handler annotations.Handler) (*Database, kernelFacts) {
	t.Helper()
	facts := readKernelFacts(t)
	dump, err := os.Open(filepath.Join(kernelFactsDir, "kernel-facts.jdzl"))
	require.NoError(t, err)
	started := time.Now()
	d := createOptimizerModeDB(t, mode, DatabaseOptions{
		Schema:                  facts.schema,
		RefuseUnknownAttributes: true,
		AnnotationHandler:       handler,
		Dump:                    dump,
	})
	t.Logf("the kernel facts loaded in %s", time.Since(started))
	require.NoError(t, dump.Close())
	return d, facts
}

// kernelEvents records a query's events through a handler the engine does
// not serialize: the predicates of every stratum begun, and the counts of
// kernelRecord.
type kernelEvents struct {
	mu     sync.Mutex
	record kernelRecord
}

// kernelRecord is one query's events: the predicates per stratum begun; the
// tuples the passes derived, per predicate; the hash joins and the phases
// completed; the tuples the joins probed, which a join into a kept index
// reads in place of draining it; the tuples the joins drained into an index
// of their own; and the subject tuples read into windows.
type kernelRecord struct {
	strata        [][]query.Symbol
	derived       map[query.Symbol]int
	hashJoins     int
	phases        int
	probed        int
	drained       int
	subjectTuples int
}

// derivedTotal is the tuples the passes derived over every predicate.
func (r kernelRecord) derivedTotal() int {
	total := 0
	for _, n := range r.derived {
		total += n
	}
	return total
}

func (k *kernelEvents) handle(e annotations.Event) {
	k.mu.Lock()
	defer k.mu.Unlock()
	switch e.Name {
	case annotations.RulesStratumBegin:
		k.record.strata = append(k.record.strata, append([]query.Symbol(nil), e.Data["predicates"].([]query.Symbol)...))
	case annotations.RulesIteration:
		if k.record.derived == nil {
			k.record.derived = map[query.Symbol]int{}
		}
		for predicate, n := range e.Data["delta"].(map[query.Symbol]int) {
			k.record.derived[predicate] += n
		}
	case annotations.JoinHash:
		k.record.hashJoins++
	case annotations.PhaseComplete:
		k.record.phases++
	case annotations.JoinProbe:
		k.record.probed += e.Data["tuple_count"].(int)
	case annotations.JoinBuild:
		k.record.drained += e.Data["copied"].(int) + e.Data["passthru"].(int)
	case annotations.ScopeSubjectTuple:
		k.record.subjectTuples++
	}
}

// take answers the record and clears it, so each query's events are read
// alone.
func (k *kernelEvents) take() kernelRecord {
	k.mu.Lock()
	defer k.mu.Unlock()
	record := k.record
	k.record = kernelRecord{}
	return record
}

// The whole relations over the kernel facts are the fixture's: reach holds
// its 9,865 reached cells, the reach count agrees, and unfilled holds its
// 70,458 pairs, on both planner paths.
func TestRules_TheWholeRelationsOverTheKernelFactsAreTheRecorded(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, facts := openKernelFacts(t, mode, nil)
			reach, err := executor.CollectTuples(d.Query(facts.reachWhole, facts.rules))
			require.NoError(t, err)
			require.Len(t, reach, 9865, "the cells reached from the entry")
			count, err := executor.CollectTuples(d.Query(facts.reachCount, facts.rules))
			require.NoError(t, err)
			require.Len(t, count, 1)
			require.EqualValues(t, 9865, count[0][0], "the reach count agrees with the relation")
			unfilled, err := executor.CollectTuples(d.Query(facts.unfilledWhole, facts.rules))
			require.NoError(t, err)
			require.Len(t, unfilled, 70458, "the cells reached with a vector unfilled")
		})
	}
}

// The bound query over the kernel facts answers the whole relation at its
// bindings: of the jumps that rely on a vector, exactly those whose vector
// and cell the whole unfilled relation pairs, which on these facts is every
// one of the sixteen.
func TestRules_ABoundInvocationOverTheKernelFactsAnswersTheWholeRelationAtItsBindings(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, facts := openKernelFacts(t, mode, nil)
			unfilled, err := executor.CollectTuples(d.Query(facts.unfilledWhole, facts.rules))
			require.NoError(t, err)
			whole := make(map[[2]datalog.Identity]bool, len(unfilled))
			for _, pair := range unfilled {
				whole[[2]datalog.Identity{pair[0].(datalog.Identity), pair[1].(datalog.Identity)}] = true
			}
			jumps, err := executor.CollectTuples(d.Query(`[:find ?r ?m ?c :where [?r :relies/cell ?c] [?r :relies/mem ?m]]`))
			require.NoError(t, err)
			require.Len(t, jumps, 16, "the jumps relying on a vector")
			var want []datalog.Identity
			for _, jump := range jumps {
				if whole[[2]datalog.Identity{jump[1].(datalog.Identity), jump[2].(datalog.Identity)}] {
					want = append(want, jump[0].(datalog.Identity))
				}
			}
			bound, err := executor.CollectTuples(d.Query(facts.bound, facts.rules))
			require.NoError(t, err)
			var answered []datalog.Identity
			for _, tuple := range bound {
				answered = append(answered, tuple[0].(datalog.Identity))
			}
			require.ElementsMatch(t, want, answered, "the bound query answers the jumps whose vector and cell the whole relation pairs")
			require.Len(t, answered, 16, "every jump answers as unfilled")
		})
	}
}

// The bound query over the kernel facts reports less work than the whole
// relation: no stratum it begins derives a copy, adorned or magic, of a
// predicate the run derives whole
// (docs/proposals/A_PREDICATE_DERIVED_WHOLE_IS_READ_WHOLE_PROOF.md T3), and
// its joins read fewer tuples, probed and drained together, than the whole
// relation's. The hash joins and the phases each run completes are logged
// beside the tuples.
func TestRules_ABoundInvocationOverTheKernelFactsReportsLessWorkThanTheWholeRelation(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			events := &kernelEvents{}
			d, facts := openKernelFacts(t, mode, events.handle)
			events.take()
			unfilled, err := executor.CollectTuples(d.Query(facts.unfilledWhole, facts.rules))
			require.NoError(t, err)
			require.Len(t, unfilled, 70458)
			whole := events.take()
			bound, err := executor.CollectTuples(d.Query(facts.bound, facts.rules))
			require.NoError(t, err)
			require.Len(t, bound, 16)
			record := events.take()
			require.NotEmpty(t, record.strata, "the bound query begins its strata")
			derivedWhole := map[string]bool{}
			for _, stratum := range record.strata {
				for _, predicate := range stratum {
					if !query.SeedRestricted(predicate) {
						derivedWhole[predicate.Name()] = true
					}
				}
			}
			for _, stratum := range record.strata {
				for _, predicate := range stratum {
					if query.SeedRestricted(predicate) && derivedWhole[predicate.Name()] {
						t.Errorf("stratum %v derives %v, a copy of a predicate the run derives whole", stratum, predicate)
					}
				}
			}
			t.Logf("the whole relation: %d hash joins, %d phases, %d tuples probed, %d tuples drained, %d subject tuples, %d tuples derived: %v", whole.hashJoins, whole.phases, whole.probed, whole.drained, whole.subjectTuples, whole.derivedTotal(), whole.derived)
			t.Logf("the bound query: %d hash joins, %d phases, %d tuples probed, %d tuples drained, %d subject tuples, %d tuples derived: %v", record.hashJoins, record.phases, record.probed, record.drained, record.subjectTuples, record.derivedTotal(), record.derived)
			require.Less(t, record.probed+record.drained, whole.probed+whole.drained, "the bound query's joins read fewer tuples than the whole relation's")
		})
	}
}

// BenchmarkKernelFacts measures, on every leg of the matrix, the queries over
// the kernel facts: the whole reach relation, the whole unfilled relation,
// and the bound query.
func BenchmarkKernelFacts(b *testing.B) {
	for _, mode := range optimizerModes {
		d, facts := openKernelFacts(b, mode, nil)
		for _, bench := range []struct {
			name string
			text string
			want int
		}{
			{"reach", facts.reachWhole, 9865},
			{"unfilled", facts.unfilledWhole, 70458},
			{"bound", facts.bound, 16},
		} {
			b.Run(bench.name+"/"+mode.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					tuples, err := executor.CollectTuples(d.Query(bench.text, facts.rules))
					if err != nil {
						b.Fatal(err)
					}
					if len(tuples) != bench.want {
						b.Fatalf("got %d tuples, want %d", len(tuples), bench.want)
					}
				}
			})
		}
	}
}
