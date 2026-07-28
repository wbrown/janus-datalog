package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestJoinBuildCopyReportsTheIntervalItsCountsWereMadeIn pins the duration on
// join/build.copy to the build drain, which is where the copies it counts
// happen.
//
// The event carried two consecutive clock reads and no Latency field at all, so
// it reported a zero duration for work that had already finished. That is what
// the self-consistency assertion below catches: a Latency that disagrees with
// its own Start and End is not a slow build or a fast one, it is an event whose
// timing was never taken.
func TestJoinBuildCopyReportsTheIntervalItsCountsWereMadeIn(t *testing.T) {
	const buildSize = 20_000

	collector := annotations.NewCollector(func(annotations.Event) {})
	opts := ExecutorOptions{Collector: collector}

	k := datalog.NewSymbol("?k")
	left := datalog.NewSymbol("?l")
	right := datalog.NewSymbol("?r")

	buildTuples := make([]Tuple, buildSize)
	for i := range buildTuples {
		buildTuples[i] = Tuple{fmt.Sprintf("key-%d", i), int64(i)}
	}
	buildRel := NewMaterializedRelationWithOptions(
		[]query.Symbol{k, left}, buildTuples, opts)

	// Larger than the build side, because the join takes the smaller relation
	// as its build side. Two keys match; the rest exist to make this the probe.
	probeTuples := make([]Tuple, 2*buildSize)
	for i := range probeTuples {
		probeTuples[i] = Tuple{fmt.Sprintf("probe-%d", i), "probe"}
	}
	probeTuples[0] = Tuple{"key-7", "probe"}
	probeTuples[1] = Tuple{"key-19999", "probe"}
	probeRel := NewMaterializedRelationWithOptions(
		[]query.Symbol{k, right}, probeTuples, opts)

	result := HashJoinWithOptions(buildRel, probeRel, []query.Symbol{k}, opts)
	tuples, err := CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, tuples, 2)

	var copyEvent *annotations.Event
	events := collector.Events()
	for i := range events {
		if events[i].Name == annotations.JoinBuildCopy {
			copyEvent = &events[i]
		}
	}
	require.NotNil(t, copyEvent,
		"draining the build relation is what the counts describe, and it happened")

	require.Equal(t, copyEvent.End.Sub(copyEvent.Start), copyEvent.Latency,
		"an event whose Latency disagrees with its own Start and End never had "+
			"its timing taken")
	require.Positive(t, int64(copyEvent.Latency),
		"draining %d tuples is not instantaneous; a zero here means the two "+
			"endpoints were read at the same moment, after the work", buildSize)

	// The counts the duration belongs to. A materialized build side hands back
	// stable tuples, so every one passes through uncopied.
	require.Equal(t, buildSize, copyEvent.Data["passthru"],
		"the interval must cover the whole drain, so the counts made in it "+
			"must be the whole build side")
	require.Equal(t, 0, copyEvent.Data["copied"])
	require.Equal(t, false, copyEvent.Data["requires_copy"])

	// Read rather than assumed: the join takes the smaller relation as its
	// build side, so a fixture that got the sizes the wrong way round would
	// time a two-tuple drain and still pass everything above.
	var build *annotations.Event
	for i := range events {
		if events[i].Name == annotations.JoinBuild {
			build = &events[i]
		}
	}
	require.NotNil(t, build)
	require.Equal(t, "left", build.Data["build_side"])
	require.Equal(t, buildSize, build.Data["tuple_count"])
}
