package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// aggregationReportingFixture builds a relation of the given size with a
// two-symbol schema, grouped over ?g so a fold has real work to do.
func aggregationReportingFixture(size int, opts ExecutorOptions) Relation {
	g := datalog.NewSymbol("?g")
	v := datalog.NewSymbol("?v")
	tuples := make([]Tuple, size)
	for i := range tuples {
		tuples[i] = Tuple{fmt.Sprintf("group-%d", i%8), int64(i)}
	}
	return NewMaterializedRelationWithOptions([]query.Symbol{g, v}, tuples, opts)
}

func aggregationFindElements() []query.FindElement {
	return []query.FindElement{
		query.FindVariable{Symbol: datalog.NewSymbol("?g")},
		query.FindAggregate{
			Function: datalog.NewSymbol("sum"),
			Arg:      datalog.NewSymbol("?v"),
		},
	}
}

// TestAggregationExecutedTimesTheAggregation holds aggregation/executed to the
// two things its name claims: that it is reported after the work, and that it
// reaches whoever is collecting.
//
// The latency assertion is what pins the ordering. An event emitted before the
// fold cannot report a duration containing it, so a start taken at the emit
// reads as zero however much work follows — and zero is indistinguishable from
// an aggregation that was genuinely instantaneous.
//
// The batch path is where the assertion has teeth: it folds inline, so the
// duration is the fold's. The streaming path builds a relation and defers the
// fold to consumption, and reports that separately.
func TestAggregationExecutedTimesTheAggregation(t *testing.T) {
	const tupleCount = 20_000

	// The handler keeps the one event under test, supplied the only way
	// aggregation has one: through the source relation's options. A nil handler
	// means disabled and drops it.
	var executed *annotations.Event
	rel := aggregationReportingFixture(tupleCount, ExecutorOptions{
		Handler: func(e annotations.Event) {
			if e.Name == annotations.AggregationExecuted {
				executed = &e
			}
		},
	})

	// The call is timed from outside so the assertion calibrates against this
	// same run. A reference fold measured separately does not work: whichever
	// runs first pays the warm-up, and under wasm that alone is a six-fold
	// difference — larger than the effect being measured.
	callStart := time.Now()
	result := ExecuteAggregations(rel, aggregationFindElements())
	callWall := time.Since(callStart)
	require.NotNil(t, result)
	tuples, err := CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, tuples, 8, "eight groups")

	require.NotNil(t, executed,
		"the relation's handler receives the strategy decision and must also "+
			"receive what the decision led to")
	require.Equal(t, "batch", executed.Data["aggregation_mode"])
	// The reported duration is a sub-interval of the call, so it cannot exceed
	// the wall time; containing the fold makes it nearly all of it.
	require.Greater(t, executed.Latency, callWall/2,
		"the call took %s and the fold is nearly all of it, but the event "+
			"reported %s", callWall, executed.Latency)
	require.Equal(t, 1, executed.Data["groupby_count"],
		"the find clause's shape is what the decorrelation regressions read "+
			"off this event, and moving the emit must not drop it")
}

// TestStreamingAggregationReportsConstructionAndFoldSeparately pins the split
// the streaming path relies on. This call builds a relation and returns; the
// fold happens when it is consumed. So aggregation/executed covers the build
// and aggregation/materialized covers the fold, and neither stands in for the
// other.
func TestStreamingAggregationReportsConstructionAndFoldSeparately(t *testing.T) {
	// One capture per event under test. The nil-ness of folded is itself an
	// assertion below — it must still be nil after the build and non-nil after
	// the fold, which is the split this test exists to pin.
	var built, folded *annotations.Event
	rel := aggregationReportingFixture(1_000, ExecutorOptions{
		Handler: func(e annotations.Event) {
			switch e.Name {
			case annotations.AggregationExecuted:
				built = &e
			case annotations.AggregationMaterialized:
				folded = &e
			}
		},
		EnableStreamingAggregation: true,
	})

	result := ExecuteAggregations(rel, aggregationFindElements())
	require.IsType(t, &StreamingAggregateRelation{}, result,
		"this fixture must reach the streaming path or the split is untested")

	require.NotNil(t, built, "the build reports even though the fold has not run")
	require.Equal(t, "streaming", built.Data["aggregation_mode"])
	require.Nil(t, folded,
		"nothing has consumed the relation, so no fold has happened to report")

	tuples, err := CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, tuples, 8)

	require.NotNil(t, folded,
		"consumption is where the streaming fold happens, and it reports there")
	require.Equal(t, 1_000, folded.Data["input_count"])
	require.Equal(t, 8, folded.Data["result_count"])
}
