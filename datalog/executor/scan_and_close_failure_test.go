package executor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

var errInjectedClose = errors.New("injected close failure")

// scanAndCloseFailing is a relation over symbols holding tuples, whose scan
// fails after failAfter tuples and whose close fails with errInjectedClose.
func scanAndCloseFailing(symbols []query.Symbol, failAfter int, tuples ...Tuple) failingRelation {
	return failingRelation{
		Relation:  NewMaterializedRelation(symbols, tuples),
		failAfter: failAfter,
		closeErr:  errInjectedClose,
	}
}

// scanAndCloseFailingStream is that relation as a stream, read once.
func scanAndCloseFailingStream(symbols []query.Symbol, failAfter int, tuples ...Tuple) *StreamingRelation {
	return NewStreamingRelation(symbols, scanAndCloseFailing(symbols, failAfter, tuples...).Iterator())
}

// relationMatcher answers every match with the relation it holds.
type relationMatcher struct{ rel Relation }

func (m relationMatcher) Match(*query.Query, Relations) (Relation, error) { return m.rel, nil }

// A consumer reading a source whose scan fails and whose close fails answers
// both failures: the close's error is joined with the scan's, never dropped
// beside it.
func TestAConsumerAnswersItsSourcesScanFailureAndCloseFailure(t *testing.T) {
	x := datalog.NewSymbol("?x")
	xs := []query.Symbol{x}
	one, two := Tuple{int64(1)}, Tuple{int64(2)}
	onX := NewMaterializedRelation(xs, []Tuple{one})
	onY := NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?y")}, []Tuple{{int64(9)}})
	count := query.FindAggregate{Function: datalog.SymCount, Arg: x}
	grouped := []query.FindElement{query.FindVariable{Symbol: x}, count}
	entity := datalog.NewIdentity("scan-and-close-failure")
	attr := datalog.NewKeyword(":scan/value")
	v := datalog.NewSymbol("?v")

	cases := []struct {
		name   string
		answer func(t *testing.T) error
	}{
		{"the emptiness of a relation", func(t *testing.T) error {
			return EmptyRelationError(scanAndCloseFailing(xs, 0, one))
		}},
		{"a selection", func(t *testing.T) error {
			return driveErr(Select(scanAndCloseFailing(xs, 1, one, two), func(Tuple) bool { return true }))
		}},
		{"a single aggregate", func(t *testing.T) error {
			return driveErr(executeSingleAggregation(scanAndCloseFailing(xs, 1, one, two), newAggregationLayout([]query.FindElement{count})))
		}},
		{"a grouped aggregate", func(t *testing.T) error {
			return driveErr(executeGroupedAggregation(scanAndCloseFailing(xs, 1, one, two), newAggregationLayout(grouped)))
		}},
		{"a streaming aggregate", func(t *testing.T) error {
			return driveErr(NewStreamingAggregateRelation(scanAndCloseFailing(xs, 1, one, two), grouped))
		}},
		{"a single aggregate over a scan failing before its first tuple", func(t *testing.T) error {
			return driveErr(executeSingleAggregation(scanAndCloseFailing(xs, 0, one, two), newAggregationLayout([]query.FindElement{count})))
		}},
		{"a grouped aggregate over a scan failing before its first tuple", func(t *testing.T) error {
			return driveErr(executeGroupedAggregation(scanAndCloseFailing(xs, 0, one, two), newAggregationLayout(grouped)))
		}},
		{"a streaming single aggregate over a scan failing before its first tuple", func(t *testing.T) error {
			return driveErr(NewStreamingAggregateRelation(scanAndCloseFailing(xs, 0, one, two), []query.FindElement{count}))
		}},
		{"a buffered iterator's clone", func(t *testing.T) error {
			clone := NewBufferedIterator(scanAndCloseFailing(xs, 1, one, two).Iterator()).Clone()
			for clone.Next() {
			}
			return clone.Error()
		}},
		{"the query's inputs", func(t *testing.T) error {
			q, err := parser.ParseQuery(`[:find ?x :in $ [?x ...] :where [?x :scan/value ?v]]`)
			require.NoError(t, err)
			_, err = BindQueryInputs(q, []Relation{scanAndCloseFailing(xs, 1, one, two)})
			return err
		}},
		{"a semi-join's left side", func(t *testing.T) error {
			return driveErr(SemiJoin(scanAndCloseFailing(xs, 1, one, two), onX, xs))
		}},
		{"a semi-join's right side", func(t *testing.T) error {
			return driveErr(SemiJoin(onX, scanAndCloseFailing(xs, 1, one, two), xs))
		}},
		{"an anti-join's left side", func(t *testing.T) error {
			return driveErr(AntiJoin(scanAndCloseFailing(xs, 1, one, two), onX, xs))
		}},
		{"an anti-join's right side", func(t *testing.T) error {
			return driveErr(AntiJoin(onX, scanAndCloseFailing(xs, 1, one, two), xs))
		}},
		{"a cross product's left side", func(t *testing.T) error {
			return driveErr(crossProduct(scanAndCloseFailing(xs, 1, one, two), onY))
		}},
		{"a cross product's right side", func(t *testing.T) error {
			return driveErr(crossProduct(onY, scanAndCloseFailing(xs, 1, one, two)))
		}},
		{"a hash join's build side", func(t *testing.T) error {
			_, err := CollectTuples(HashJoin(scanAndCloseFailingStream(xs, 1, one, two), streamOf("?x", one), xs), nil)
			return err
		}},
		{"a branch collected under a form", func(t *testing.T) error {
			_, err := collectBranch(scanAndCloseFailing(xs, 1, one, two))
			return err
		}},
		{"a tuple seq", func(t *testing.T) error {
			_, err := LazySeqToSlice(NewTupleSeq(scanAndCloseFailing(xs, 1, one, two).Iterator(), false))
			return err
		}},
		{"a limit", func(t *testing.T) error {
			return driveErr(NewLimitRelation(scanAndCloseFailing(xs, 1, one, two), 5))
		}},
		{"a top-n", func(t *testing.T) error {
			return driveErr(TopNRelation(scanAndCloseFailing(xs, 1, one, two), []query.OrderByClause{{Variable: x}}, 1))
		}},
		{"the existence verdict over a group that evaluates", func(t *testing.T) error {
			group := NewStreamingRelationWithProperties(xs, scanAndCloseFailing(xs, 1, one, two).Iterator(), ExecutorOptions{}, RelationProperties{Evaluates: true})
			_, err := existenceVerdict([]Relation{group}, nil, NewUnitRelation(ExecutorOptions{}))
			return err
		}},
		{"the existence verdict over a group that evaluates nothing", func(t *testing.T) error {
			_, err := existenceVerdict([]Relation{scanAndCloseFailing(xs, 0, one)}, nil, NewUnitRelation(ExecutorOptions{}))
			return err
		}},
		{"the pulled entities", func(t *testing.T) error {
			entitySymbol := datalog.NewSymbol("?entity")
			find := []query.FindElement{query.FindPull{
				Variable: entitySymbol,
				Pattern:  &query.PullPattern{Specs: []query.PullAttrSpec{&query.PullWildcard{}}},
			}}
			_, err := applyFindPulls(nil, &recordingBatchEntityResolver{}, scanAndCloseFailing([]query.Symbol{entitySymbol}, 0, Tuple{entity}), find)
			return err
		}},
		{"a pulled attribute", func(t *testing.T) error {
			pe := NewPullExecutorWithHandler(relationMatcher{scanAndCloseFailing([]query.Symbol{v}, 0, Tuple{"value"})}, nil, nil)
			_, _, err := pe.lookupAttributeViaPattern(entity, attr)
			return err
		}},
		{"a pulled entity's attributes", func(t *testing.T) error {
			pe := NewPullExecutorWithHandler(relationMatcher{scanAndCloseFailing([]query.Symbol{datalog.NewSymbol("?a"), v}, 0, Tuple{attr, "value"})}, nil, nil)
			_, err := pe.getAllAttributesInternal(entity)
			return err
		}},
		{"a pulled attribute's values", func(t *testing.T) error {
			pe := NewPullExecutorWithHandler(relationMatcher{scanAndCloseFailing([]query.Symbol{v}, 0, Tuple{"value"})}, nil, nil)
			_, err := pe.lookupAllValuesInternal(entity, attr)
			return err
		}},
		{"the unique combinations of a relation", func(t *testing.T) error {
			_, err := getUniqueCombinations(scanAndCloseFailing(xs, 1, one, two), xs)
			return err
		}},
		{"a union", func(t *testing.T) error {
			return driveErr(unionRelations([]Relation{scanAndCloseFailing(xs, 1, one, two)}, xs, ExecutorOptions{}))
		}},
		{"an eager filter", func(t *testing.T) error {
			_, err := filterWithPredicateAndLookup(scanAndCloseFailingStream(xs, 1, one, two), alwaysTrue(), nil, nil)
			return err
		}},
		{"a theta join's outer side", func(t *testing.T) error {
			_, err := thetaJoinPair(scanAndCloseFailingStream(xs, 1, one, two), onY, nil, nil, nil, ExecutorOptions{})
			return err
		}},
		{"a theta join's inner side", func(t *testing.T) error {
			_, err := thetaJoinPair(onY, scanAndCloseFailingStream(xs, 1, one, two), nil, nil, nil, ExecutorOptions{})
			return err
		}},
		{"sorted tuples", func(t *testing.T) error {
			_, err := collectSortedTuples(scanAndCloseFailing(xs, 1, one, two))
			return err
		}},
		{"sorted tuples reordered", func(t *testing.T) error {
			_, err := collectSortedTuplesReordered(scanAndCloseFailing(xs, 1, one, two), map[int]int{0: 0})
			return err
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.answer(t)
			require.ErrorIs(t, err, errInjectedIterator, "the scan's failure reaches the caller")
			require.ErrorIs(t, err, errInjectedClose, "the close's failure reaches the caller beside the scan's")
		})
	}

	t.Run("a relation's summary", func(t *testing.T) {
		summary := FormatRelationSummary(scanAndCloseFailing(xs, 1, one, two))
		require.Contains(t, summary, errInjectedIterator.Error(), "the scan's failure is in the summary")
		require.Contains(t, summary, errInjectedClose.Error(), "the close's failure is in the summary beside the scan's")
	})
}

// A product answers a member's failure, and the close failure of every member
// iterator it opened. Its members are streams, so NewProductRelation reads
// the members after the leftmost through Materialize.
func TestAProductAnswersAMembersFailureAndEveryMembersCloseFailure(t *testing.T) {
	a := []query.Symbol{datalog.NewSymbol("?a")}
	b := []query.Symbol{datalog.NewSymbol("?b")}
	stream := func(symbols []query.Symbol, failAfter int, closeErr error, tuples ...Tuple) *StreamingRelation {
		source := failingRelation{Relation: NewMaterializedRelation(symbols, tuples), failAfter: failAfter, closeErr: closeErr}
		return NewStreamingRelation(symbols, source.Iterator())
	}

	t.Run("a member failing past its first tuple", func(t *testing.T) {
		outer := NewMaterializedRelation(a, []Tuple{{int64(1)}, {int64(2)}})
		inner := stream(b, 1, nil, Tuple{int64(10)}, Tuple{int64(20)})
		_, err := CollectTuples(NewProductRelation([]Relation{outer, inner}), nil)
		require.ErrorIs(t, err, errInjectedIterator, "the member's failure is the product's")
	})

	t.Run("the members' close failures", func(t *testing.T) {
		outerClose := errors.New("outer member close failure")
		innerClose := errors.New("inner member close failure")
		outer := stream(a, 100, outerClose, Tuple{int64(1)}, Tuple{int64(2)})
		inner := stream(b, 100, innerClose, Tuple{int64(10)}, Tuple{int64(20)})
		_, err := CollectTuples(NewProductRelation([]Relation{outer, inner}), nil)
		require.ErrorIs(t, err, outerClose, "the outer member's close failure reaches the caller")
		require.ErrorIs(t, err, innerClose, "the inner member's close failure reaches the caller")
	})
}

// The existence verdict answers the failure it raises together with the close
// failure of every group it read whose reading evaluates.
func TestTheExistenceVerdictAnswersItsFailureAndEveryEvaluatingGroupsCloseFailure(t *testing.T) {
	xs := []query.Symbol{datalog.NewSymbol("?x")}
	unit := NewUnitRelation(ExecutorOptions{})
	evaluating := func(failAfter int, closeErr error) Relation {
		source := failingRelation{
			Relation:  NewMaterializedRelation(xs, []Tuple{{int64(1)}, {int64(2)}}),
			failAfter: failAfter,
			closeErr:  closeErr,
		}
		return NewStreamingRelationWithProperties(xs, source.Iterator(), ExecutorOptions{}, RelationProperties{Evaluates: true})
	}
	firstClose := errors.New("first group close failure")
	secondClose := errors.New("second group close failure")

	t.Run("a group failing at its first tuple", func(t *testing.T) {
		_, err := existenceVerdict([]Relation{evaluating(0, errInjectedClose)}, nil, unit)
		require.ErrorIs(t, err, errInjectedIterator, "the group's failure is the verdict's")
		require.ErrorIs(t, err, errInjectedClose, "the group's close failure reaches the caller beside it")
	})

	t.Run("groups with tuples before a group failing at its first tuple", func(t *testing.T) {
		groups := []Relation{evaluating(100, firstClose), evaluating(100, secondClose), evaluating(0, nil)}
		_, err := existenceVerdict(groups, nil, unit)
		require.ErrorIs(t, err, errInjectedIterator, "the failing group's failure is the verdict's")
		require.ErrorIs(t, err, firstClose, "the first group's close failure reaches the caller")
		require.ErrorIs(t, err, secondClose, "the second group's close failure reaches the caller")
	})

	t.Run("a group failing past its first tuple before a group with tuples", func(t *testing.T) {
		groups := []Relation{evaluating(1, firstClose), evaluating(100, secondClose)}
		_, err := existenceVerdict(groups, nil, unit)
		require.ErrorIs(t, err, errInjectedIterator, "the failing group's failure is the verdict's")
		require.ErrorIs(t, err, firstClose, "the failing group's close failure reaches the caller")
		require.ErrorIs(t, err, secondClose, "the other group's close failure reaches the caller")
	})
}

// The existence verdict answers the close failure of every group it opens,
// held groups included, whatever its verdict. Its own failure is the first
// read failure, raised only when every group has a tuple.
func TestTheExistenceVerdictAnswersTheCloseFailureOfEveryGroupItOpens(t *testing.T) {
	xs := []query.Symbol{datalog.NewSymbol("?x")}
	unit := NewUnitRelation(ExecutorOptions{})
	group := func(failAfter int, closeErr error) Relation {
		return failingRelation{
			Relation:  NewMaterializedRelation(xs, []Tuple{{int64(1)}, {int64(2)}}),
			failAfter: failAfter,
			closeErr:  closeErr,
		}
	}
	evaluating := func(failAfter int, closeErr error) Relation {
		return NewStreamingRelationWithProperties(xs, group(failAfter, closeErr).Iterator(), ExecutorOptions{}, RelationProperties{Evaluates: true})
	}
	empty := NewMaterializedRelation(xs, nil)
	failingClose := errors.New("failing group close failure")
	laterClose := errors.New("later group close failure")

	t.Run("a group with tuples whose close fails, read after a failing group", func(t *testing.T) {
		_, err := existenceVerdict([]Relation{group(0, failingClose), group(100, laterClose)}, nil, unit)
		require.ErrorIs(t, err, errInjectedIterator, "the failing group's failure is the verdict's")
		require.ErrorIs(t, err, failingClose, "the failing group's close failure reaches the caller")
		require.ErrorIs(t, err, laterClose, "the later group's close failure reaches the caller")
	})

	t.Run("a failing group, then a group with no tuple", func(t *testing.T) {
		_, err := existenceVerdict([]Relation{group(0, failingClose), empty}, nil, unit)
		require.ErrorIs(t, err, failingClose, "the failing group's close failure reaches the caller")
		require.NotErrorIs(t, err, errInjectedIterator, "a group has no tuple, so the read failure is not raised")
	})

	t.Run("a failing group, then a held group with no tuple", func(t *testing.T) {
		_, err := existenceVerdict([]Relation{group(0, failingClose)}, []Relation{empty}, unit)
		require.ErrorIs(t, err, failingClose, "the failing group's close failure reaches the caller")
		require.NotErrorIs(t, err, errInjectedIterator, "a held group has no tuple, so the read failure is not raised")
	})

	t.Run("a held group whose close fails, read after a failure", func(t *testing.T) {
		_, err := existenceVerdict([]Relation{group(0, failingClose)}, []Relation{group(100, laterClose)}, unit)
		require.ErrorIs(t, err, errInjectedIterator, "the failing group's failure is the verdict's")
		require.ErrorIs(t, err, failingClose, "the failing group's close failure reaches the caller")
		require.ErrorIs(t, err, laterClose, "the held group's close failure reaches the caller")
	})

	t.Run("a group with tuples whose close fails, beside a held group with no tuple", func(t *testing.T) {
		_, err := existenceVerdict([]Relation{group(100, failingClose)}, []Relation{empty}, unit)
		require.ErrorIs(t, err, failingClose, "the group's close failure reaches the caller")
	})

	t.Run("an evaluating group whose close fails, before an evaluating group failing past its first tuple", func(t *testing.T) {
		_, err := existenceVerdict([]Relation{evaluating(100, failingClose), evaluating(1, laterClose)}, nil, unit)
		require.ErrorIs(t, err, errInjectedIterator, "the later group's failure is the verdict's")
		require.ErrorIs(t, err, failingClose, "the first group's close failure reaches the caller")
		require.ErrorIs(t, err, laterClose, "the later group's close failure reaches the caller")
	})
}

// A hash join answers the close failure of each side whatever the other side
// holds. Its own failure is a side's read failure, raised only where the other
// side holds a tuple or a read failure.
func TestAHashJoinAnswersEachSidesCloseFailureWhateverTheOtherSideHolds(t *testing.T) {
	xs := []query.Symbol{datalog.NewSymbol("?x")}
	stream := func(failAfter int, closeErr error, tuples ...Tuple) *StreamingRelation {
		return NewStreamingRelation(xs, failingRelation{
			Relation:  NewMaterializedRelation(xs, tuples),
			failAfter: failAfter,
			closeErr:  closeErr,
		}.Iterator())
	}
	buildClose := errors.New("build side close failure")
	probeClose := errors.New("probe side close failure")

	t.Run("a build side with tuples whose close fails, over a probe side with no tuple", func(t *testing.T) {
		_, err := CollectTuples(HashJoin(stream(100, buildClose, Tuple{int64(1)}), stream(100, nil), xs), nil)
		require.ErrorIs(t, err, buildClose, "the build side's close failure reaches the caller")
	})

	t.Run("an empty build side whose close fails, beside a probe side with no tuple", func(t *testing.T) {
		_, err := CollectTuples(HashJoin(stream(100, buildClose), stream(100, nil), xs), nil)
		require.ErrorIs(t, err, buildClose, "the build side's close failure reaches the caller")
	})

	t.Run("an empty build side that failed, beside a probe side whose close fails and which holds no tuple", func(t *testing.T) {
		_, err := CollectTuples(HashJoin(stream(0, nil), stream(100, probeClose), xs), nil)
		require.ErrorIs(t, err, probeClose, "the probe side's close failure reaches the caller")
		require.NotErrorIs(t, err, errInjectedIterator, "the probe side holds no tuple and no read failure, so the build's failure is not raised")
	})
}

// A symmetric hash join answers the close failure of each side.
func TestASymmetricHashJoinAnswersEachSidesCloseFailure(t *testing.T) {
	xs := []query.Symbol{datalog.NewSymbol("?x")}
	leftClose := errors.New("left side close failure")
	rightClose := errors.New("right side close failure")
	side := func(closeErr error) *StreamingRelation {
		return NewStreamingRelation(xs, failingRelation{
			Relation:  NewMaterializedRelation(xs, []Tuple{{int64(1)}}),
			failAfter: 100,
			closeErr:  closeErr,
		}.Iterator())
	}
	_, err := CollectTuples(HashJoinWithOptions(side(leftClose), side(rightClose), xs, ExecutorOptions{EnableSymmetricHashJoin: true}), nil)
	require.ErrorIs(t, err, leftClose, "the left side's close failure reaches the caller")
	require.ErrorIs(t, err, rightClose, "the right side's close failure reaches the caller")
}
