package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

func TestAlgebraBridgeOptimizedOffExactTupleDifferential(t *testing.T) {
	testCases := []struct {
		name    string
		query   string
		inputs  []interface{}
		ordered bool
	}{
		{
			name: "join fanout",
			query: `[:find ?entity ?title ?tag
				:where [?entity :scenario/title ?title]
				       [?entity :entity/tag ?tag]]`,
		},
		{
			name: "anti join",
			query: `[:find ?entity ?tag
				:where [?entity :entity/tag ?tag]
				       (not [?entity :entity/deleted true])]`,
		},
		{
			name: "union deduplication",
			query: `[:find ?entity
				:where (or [?entity :entity/tag "alpha"]
				           [?entity :entity/tag "beta"]
				           [?entity :entity/tag "alpha"])]`,
		},
		{
			name: "grouped aggregate",
			query: `[:find ?scenario (count ?task)
				:where [?task :task/root ?scenario]]`,
		},
		{
			name: "correlated tuple subquery",
			query: `[:find ?scenario ?count
				:where [?scenario :entity/type :entity.type/scenario]
				       [(q [:find (count ?task)
				            :in $ ?parent
				            :where [?task :task/root ?parent]]
				           $ ?scenario) [[?count]]]]`,
		},
		{
			name: "missing fallback",
			query: `[:find ?scenario ?count
				:where [?scenario :entity/type :entity.type/scenario]
				       (or-default
				         [(q [:find (count ?task)
				              :in $ ?parent
				              :where [?task :task/root ?parent]]
				             $ ?scenario) [[?count]]]
				         [(ground 0) ?count])]`,
		},
		{
			name:   "scalar input",
			query:  `[:find ?entity ?title :in $ ?tag :where [?entity :entity/tag ?tag] [?entity :scenario/title ?title]]`,
			inputs: []interface{}{"alpha"},
		},
		{
			name:   "collection input",
			query:  `[:find ?entity ?title :in $ [?tag ...] :where [?entity :entity/tag ?tag] [?entity :scenario/title ?title]]`,
			inputs: []interface{}{[]interface{}{"alpha", "beta"}},
		},
		{
			name:   "relation input",
			query:  `[:find ?entity ?title :in $ [[?tag] ...] :where [?entity :entity/tag ?tag] [?entity :scenario/title ?title]]`,
			inputs: []interface{}{[][]interface{}{{"alpha"}, {"gamma"}}},
		},
		{
			name:    "order and limit",
			query:   `[:find ?title :where [?entity :scenario/title ?title] :order-by [[?title :desc]] :limit 2]`,
			ordered: true,
		},
		{
			name:   "empty result",
			query:  `[:find ?entity :in $ ?tag :where [?entity :entity/tag ?tag]]`,
			inputs: []interface{}{"absent"},
		},
	}

	// Every leg answers each shape as the reference leg does: the same tuples,
	// in the same order where the query orders.
	referenceDB := setupAlgebraTestDB(t, referenceMode)
	for _, mode := range legsFor(optimizerModes, algebraOption, sharingOption, fusionOption, aggregationOption) {
		t.Run(mode.name, func(t *testing.T) {
			db := setupAlgebraTestDB(t, mode)
			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					want := executePlannerOptions(t, referenceDB, testCase.query, testCase.inputs, referenceMode.plannerOptions())
					got := executePlannerOptions(t, db, testCase.query, testCase.inputs, mode.plannerOptions())
					if testCase.ordered {
						require.Equal(t, want, got)
					} else {
						require.ElementsMatch(t, want, got)
					}
				})
			}
		})
	}
}

func executePlannerOptions(
	t testing.TB,
	db *Database,
	source string,
	inputs []interface{},
	options planner.PlannerOptions,
) [][]interface{} {
	t.Helper()
	options.Handler = recordingReach(t, options.Handler)
	tuples, err := runPlannerOptions(db, source, inputs, options)
	require.NoError(t, err)
	return tuples
}

// runPlannerOptions executes source against db under the given planner
// options, returning the error instead of failing the test — for pins that
// assert a query is rejected.
func runPlannerOptions(
	db *Database,
	source string,
	inputs []interface{},
	options planner.PlannerOptions,
) ([][]interface{}, error) {
	q, err := db.resolveQuery(source)
	if err != nil {
		return nil, err
	}
	relations, _, err := db.convertInputsToRelations(q, inputs)
	if err != nil {
		return nil, err
	}

	options.Cache = nil
	router := executor.NewSourceRouter(buildSourceMap(nil, db.Matcher()))
	exec := executor.NewExecutorWithOptions(router, db, planner.PlannerOptions(options))
	result, err := exec.ExecuteWithRelations(executor.NewContext(), q, relations)
	if err != nil {
		return nil, err
	}
	return executor.CollectTuples(result, nil)
}
