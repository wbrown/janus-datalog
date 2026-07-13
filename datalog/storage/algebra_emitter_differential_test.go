package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

func TestAlgebraEmitterOptimizedOffExactTupleDifferential(t *testing.T) {
	db, cleanup := setupAlgebraTestDB(t)
	defer cleanup()

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

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			baseline := executeAlgebraMode(t, db, testCase.query, testCase.inputs, false)
			optimized := executeAlgebraMode(t, db, testCase.query, testCase.inputs, true)
			if testCase.ordered {
				require.Equal(t, baseline, optimized)
			} else {
				require.ElementsMatch(t, baseline, optimized)
			}
		})
	}
}

func TestJoinProjectInsertionExactTupleDifferential(t *testing.T) {
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: t.TempDir()})
	require.NoError(t, err)
	defer db.Close()

	score := datalog.NewKeyword(":item/score")
	payload := datalog.NewKeyword(":item/payload")
	tx := db.NewTransaction()
	for _, item := range []struct {
		id      string
		score   int64
		payload string
	}{
		{id: "item-a", score: 95, payload: "alpha"},
		{id: "item-b", score: 80, payload: "beta"},
		{id: "item-c", score: 100, payload: "gamma"},
	} {
		entity := datalog.NewIdentity(item.id)
		tx.Add(entity, score, item.score)
		tx.Add(entity, payload, item.payload)
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	source := `[:find ?entity ?payload
		:where [?entity :item/score ?score]
		       [(> ?score 90)]
		       [?entity :item/payload ?payload]]`
	baselineOptions := DefaultPlannerOptions()
	baselineOptions.EnableJoinProjectInsertion = false
	projectOptions := baselineOptions
	projectOptions.EnableJoinProjectInsertion = true
	baseline := executePlannerOptions(t, db, source, nil, baselineOptions)
	optimized := executePlannerOptions(t, db, source, nil, projectOptions)
	require.ElementsMatch(t, baseline, optimized)
	require.Len(t, optimized, 2)
	require.ElementsMatch(t, []interface{}{"alpha", "gamma"}, []interface{}{
		optimized[0][1],
		optimized[1][1],
	})
}

func executeAlgebraMode(
	t *testing.T,
	db *Database,
	source string,
	inputs []interface{},
	enabled bool,
) [][]interface{} {
	t.Helper()
	options := DefaultPlannerOptions()
	options.EnableAlgebraOptimizer = enabled
	return executePlannerOptions(t, db, source, inputs, options)
}

func executePlannerOptions(
	t testing.TB,
	db *Database,
	source string,
	inputs []interface{},
	options planner.PlannerOptions,
) [][]interface{} {
	t.Helper()
	q, err := db.resolveQuery(source)
	require.NoError(t, err)
	relations, err := db.convertInputsToRelations(q, inputs)
	require.NoError(t, err)

	options.Cache = nil
	router := executor.NewSourceRouter(buildSourceMap(nil, db.Matcher()))
	exec := executor.NewExecutorWithOptions(router, db, planner.PlannerOptions(options))
	result, err := exec.ExecuteWithRelations(executor.NewContext(nil), q, relations)
	require.NoError(t, err)
	tuples, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)
	return tuples
}
