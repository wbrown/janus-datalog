package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// WithContext carries a cancellation into a query among its inputs. A query
// under a cancelled context returns the context's error through every
// reading surface, the plan-only surfaces accept the option with nothing to
// cancel, and a closure under evaluation stops at its next iteration once the
// context is cancelled.

const nodesQuery = `[:find ?n :where [?n :type :node]]`

func TestQueryCancelled_BeforeExecution(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			writeGraph(t, d, chain(3))
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			rel, err := d.Query(nodesQuery, WithContext(ctx))
			if err == nil {
				err = executor.ForEach(rel, func(executor.Tuple) error { return nil })
			}
			require.ErrorIs(t, err, context.Canceled)

			var nodes []datalog.Identity
			require.ErrorIs(t, d.QueryInto(&nodes, nodesQuery, WithContext(ctx)), context.Canceled)

			_, err = d.Analyze(nodesQuery, WithContext(ctx))
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestExplainAcceptsContextOption(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			_, err := d.Explain(nodesQuery, WithContext(ctx))
			require.NoError(t, err)
			_, err = d.ExplainAlgebra(nodesQuery, WithContext(ctx))
			require.NoError(t, err)
		})
	}
}

func TestRuleEvaluationCancelled_WithinOneIteration(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			iterations := 0
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: func(e annotations.Event) {
				if e.Name == annotations.RulesIteration {
					iterations++
					cancel()
				}
			}})
			writeGraph(t, d, chain(10))

			_, err := d.Query(closureQuery, mustRuleSet(t, closureRules), WithContext(ctx))
			require.ErrorIs(t, err, context.Canceled)
			require.LessOrEqual(t, iterations, 2, "the closure ran on after the cancellation")
		})
	}
}
