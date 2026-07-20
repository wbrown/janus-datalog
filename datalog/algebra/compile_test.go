package algebra

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestCompileDecompileRoundTrip verifies that compiling clauses to algebra
// and decompiling back produces functionally equivalent clauses.
func TestCompileDecompileRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "simple pattern",
			query: `[:find ?e ?name :where [?e :person/name ?name]]`,
		},
		{
			name:  "two patterns",
			query: `[:find ?e ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]`,
		},
		{
			name:  "pattern with predicate",
			query: `[:find ?e ?age :where [?e :person/age ?age] [(> ?age 21)]]`,
		},
		{
			name:  "pattern with expression",
			query: `[:find ?e ?total :where [?e :item/price ?p] [(+ ?p 10) ?total]]`,
		},
		{
			name:  "not clause",
			query: `[:find ?e :where [?e :person/name ?name] (not [?e :person/fired true])]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.query)
			require.NoError(t, err)

			// Compile
			root, err := Compile(q)
			require.NoError(t, err)
			require.NotNil(t, root)

			t.Logf("Algebra tree:\n%s", root.String())

			// Decompile
			clauses, err := Decompile(root)
			require.NoError(t, err)
			require.NotEmpty(t, clauses)

			t.Logf("Decompiled %d clauses", len(clauses))
			for i, c := range clauses {
				t.Logf("  [%d] %T: %s", i, c, c.String())
			}

			// Verify same number of clauses
			assert.Equal(t, len(q.Where), len(clauses),
				"round-trip should preserve clause count")
		})
	}
}

// TestRoundTrip_CorrelatedOrJoinPreservesClauseType pins the correlated
// or-join round-trip: branches with correlated predicates compile against
// the outer schema and decompile back to the or-join itself. Union
// semantics have no fallback encoding — decompiling to an or-default form
// re-executes the clause as first-match-wins and drops rows (see
// docs/bugs/resolved/BUG_CORRELATED_ORJOIN_GLOBAL_FALLBACK_DROPS_ROWS.md).
func TestRoundTrip_CorrelatedOrJoinPreservesClauseType(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?v
	  :where
	  [?e :x/tag ?tag]
	  (or-join [?e ?v]
	    (and [?e :x/a ?v]
	         (not [?e :x/flag true]))
	    [?e :x/b ?v])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	clauses, err := Decompile(root)
	require.NoError(t, err)

	var orJoin *query.OrJoinClause
	for _, c := range clauses {
		if oj, ok := c.(*query.OrJoinClause); ok {
			orJoin = oj
		}
	}
	require.NotNil(t, orJoin, "correlated or-join must decompile as or-join, got %v", clauses)
	require.Equal(t,
		[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")},
		orJoin.JoinVars,
	)
	require.Len(t, orJoin.Branches, 2)
	// The bridge normalizes plain not to not-join with statically resolved
	// join vars (compileNot sets ExplicitJoin); the branch must keep its
	// anti-join in that form.
	var branchNot *query.NotJoinClause
	for _, c := range orJoin.Branches[0] {
		if nc, ok := c.(*query.NotJoinClause); ok {
			branchNot = nc
		}
	}
	require.NotNil(t, branchNot, "correlated branch must keep its NOT clause")
	require.Equal(t, []query.Symbol{datalog.NewSymbol("?e")}, branchNot.JoinVars)
}

// TestRoundTrip_CorrelatedOrPreservesClauseType pins the same identity
// round-trip for plain (or ...) with a correlated predicate branch.
func TestRoundTrip_CorrelatedOrPreservesClauseType(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?v
	  :where
	  [?e :x/tag ?tag]
	  (or (and [?e :x/a ?v]
	           (not [?e :x/flag true]))
	      [?e :x/b ?v])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	clauses, err := Decompile(root)
	require.NoError(t, err)

	var or *query.OrClause
	for _, c := range clauses {
		if oc, ok := c.(*query.OrClause); ok {
			or = oc
		}
	}
	require.NotNil(t, or, "correlated or must decompile as or, got %v", clauses)
	require.Len(t, or.Branches, 2)
}

// TestCompileOrFallback verifies that OR-fallback with subquery compiles
// to a LateralJoin with defaults.
func TestCompileOrFallback(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?count
	  :where
	  [?e :entity/type :entity.type/scenario]
	  (or-default [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/status :status/complete]]
	          $ ?e) [[?count]]]
	      [(ground 0) ?count])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	require.NotNil(t, root)

	t.Logf("Algebra tree:\n%s", root.String())

	// The tree should contain a LateralJoin node
	lj := findLateralJoin(root)
	if lj != nil {
		ljData := lj.Data.(*LateralJoin)
		t.Logf("Found LateralJoin: correlation=%v, defaults=%v",
			ljData.CorrelationVars, ljData.DefaultValues)
		assert.NotEmpty(t, ljData.CorrelationVars, "should have correlation variables")
	} else {
		t.Log("No LateralJoin found — OR-fallback compiled to different structure")
		// This is acceptable if the pattern wasn't detected
	}
}

// TestCompileDecompileNotAtEnd reproduces a production crash where the NOT
// clause is at the END of the clause list (after two data patterns).
func TestCompileDecompileNotAtEnd(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?scenario ?tag
		:where [?scenario :entity/type :entity.type/scenario]
		       [?scenario :entity/tag ?tag]
		       (not [?scenario :entity/deleted true])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Compiled:\n%s", root.String())

	optimizer := NewOptimizer(DefaultPasses()...)
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("Optimized:\n%s", optimized.String())

	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}
	assert.Equal(t, len(q.Where), len(clauses))
}

// TestCompileDecompileWithNot verifies that NOT clauses survive the
// compile → optimize → decompile pipeline. This covers the production
// pattern: [?e :entity/type :foo] (not [?e :entity/deleted true])
func TestCompileDecompileWithNot(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?tag
		:where [?e :entity/type :entity.type/scenario]
		       (not [?e :entity/deleted true])
		       [?e :entity/tag ?tag]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Compiled:\n%s", root.String())

	// Optimize (should be a no-op — no LateralJoins)
	optimizer := NewOptimizer(DefaultPasses()...)
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	t.Logf("Optimized:\n%s", optimized.String())

	// Decompile
	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	require.NotEmpty(t, clauses)
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}

	// Should have 3 clauses: pattern, NOT, pattern
	assert.Equal(t, len(q.Where), len(clauses))
}

func TestCompileCorrelatedNotJoinSeparatesKeysAndRequirements(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?goal
		:where
		[?goal :entity/type :type/goal]
		[?setEvent :event/goal ?goal]
		[?setEvent :event/type ?goalSet]
		(not-join [?goal ?goalSet]
			[?termEvent :event/goal ?goal]
			[?termEvent :event/type ?termType]
			[(!= ?termType ?goalSet)])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	antiNode := findAlgebraNode(root, RuleAntiJoin)
	require.NotNil(t, antiNode)
	anti := antiNode.Data.(*AntiJoin)
	require.Equal(t,
		[]query.Symbol{datalog.NewSymbol("?goal"), datalog.NewSymbol("?goalSet")},
		anti.JoinSymbols,
	)
	require.Equal(t,
		[]query.Symbol{datalog.NewSymbol("?goalSet")},
		anti.Required,
	)

	optimizer := NewOptimizer(DefaultPasses()...)
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	notJoin, ok := clauses[len(clauses)-1].(*query.NotJoinClause)
	require.True(t, ok)
	require.Equal(t, anti.JoinSymbols, notJoin.JoinVars)
}

func TestCompileRejectsInvalidNotJoinHeaders(t *testing.T) {
	testCases := []struct {
		name  string
		query string
		want  string
	}{
		{
			name: "header symbol not bound outside",
			query: `[:find ?goal
				:where
				[?goal :entity/type :type/goal]
				(not-join [?goal ?missing]
					[?event :event/goal ?goal])]`,
			want: "header symbol ?missing is not bound by the outer relation",
		},
		{
			name: "header symbol unused by body",
			query: `[:find ?goal
				:where
				[?goal :entity/type :type/goal]
				[?goal :entity/name ?name]
				(not-join [?goal ?name]
					[?event :event/goal ?goal])]`,
			want: "header symbol ?name is neither produced nor consumed by the body",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			q, err := parser.ParseQuery(testCase.query)
			require.NoError(t, err)
			_, err = Compile(q)
			require.Error(t, err)
			require.Contains(t, err.Error(), testCase.want)
		})
	}
}

func TestCompileRejectsPlainNotWithUnboundOuterRequirement(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?goal
		:where
		[?goal :entity/type :type/goal]
		(not
			[?event :event/goal ?goal]
			[?event :event/type ?termType]
			[(!= ?termType ?missing)])]`)
	require.NoError(t, err)

	_, err = Compile(q)
	require.Error(t, err)
	require.Contains(t, err.Error(), "NOT body requires unbound outer symbol ?missing")
}

func TestOrJoinSchemaDiagnosticExplainsHeaderContract(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?entity
		:in $ ?room ?crawl ?world
		:where
		[?entity :entity/location ?room]
		(or-join [?entity ?crawl ?world]
			[?entity :entity/crawl ?crawl]
			[?entity :entity/world ?world])]`)
	require.NoError(t, err)
	root, err := Compile(q)
	require.NoError(t, err)

	optimizer := NewOptimizer(DefaultPasses()...)
	_, err = optimizer.Optimize(root)
	require.Error(t, err)
	require.Contains(t, err.Error(), "or-join header declares ?world")
	require.Contains(t, err.Error(), "every branch must bind every header variable")
	require.Contains(t, err.Error(), "remove it from the header")
}

// TestCompileDecompileWithGetElse verifies get-else survives the pipeline.
func TestCompileDecompileWithGetElse(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?title
		:where [?e :entity/type :entity.type/scenario]
		       (not [?e :entity/deleted true])
		       [(get-else $ ?e :scenario/title "") ?title]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	optimizer := NewOptimizer(DefaultPasses()...)
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)

	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	require.NotEmpty(t, clauses)
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}
	assert.Equal(t, len(q.Where), len(clauses))
}

func TestCompileNormalizesUnionOutputsAndOuterJoinKeys(t *testing.T) {
	parsed, err := parser.ParseQuery(
		`[:find ?entity ?value
		  :where [?entity :item/type :item.type/scored]
		         (or [?entity :item/value ?value]
		             [(ground 0) ?value])]`,
	)
	require.NoError(t, err)
	root, err := Compile(parsed)
	require.NoError(t, err)

	unionNode := findAlgebraNode(root, RuleUnion)
	require.NotNil(t, unionNode)
	union := unionNode.Data.(*Union)
	require.Equal(t,
		[]query.Symbol{datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")},
		union.JoinVars,
	)
	require.Equal(t,
		[]query.Symbol{datalog.NewSymbol("?entity")},
		union.Required,
	)
	require.Equal(t,
		[]query.Symbol{datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")},
		union.Output,
	)
}

func TestCompileNormalizesLateralUnionBranchLocalSymbols(t *testing.T) {
	parsed, err := parser.ParseQuery(
		`[:find ?entity ?value
		  :where [?entity :item/type :item.type/scored]
		         (or-default
		           (and [?entity :item/child ?child]
		                [?child :item/value ?value])
		           [(ground 0) ?value])]`,
	)
	require.NoError(t, err)
	root, err := Compile(parsed)
	require.NoError(t, err)

	unionNode := findAlgebraNode(root, RuleLateralUnion)
	require.NotNil(t, unionNode)
	union := unionNode.Data.(*LateralUnion)
	require.Equal(t,
		[]query.Symbol{datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")},
		union.Output,
	)
	require.NotContains(t, union.Output, datalog.NewSymbol("?child"))
}

func TestNormalizedBranchSymbolsKeepJoinKeysAndCommonPayload(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	value := datalog.NewSymbol("?value")
	local := datalog.NewSymbol("?local")
	branches := []*Node{
		algebraTestScan(entity, value, local),
		algebraTestScan(value),
	}
	require.Equal(t,
		[]query.Symbol{entity, value},
		normalizedBranchSymbols(branches, []query.Symbol{entity}),
	)
	require.Equal(t,
		[]query.Symbol{value},
		normalizedBranchSymbols(branches, nil),
	)
}

func findAlgebraNode(node *Node, operator string) *Node {
	if node == nil {
		return nil
	}
	if node.Op == operator {
		return node
	}
	for _, child := range node.Children {
		if found := findAlgebraNode(child, operator); found != nil {
			return found
		}
	}
	return nil
}

// === Phase 0: Operator Round-Trip Tests ===
//
// Each test verifies: compile(clauses) produces the correct algebra node type,
// and decompile(compile(clauses)) produces semantically equivalent clauses.
// Per ALGEBRA.md round-trip specifications.

func TestRoundTrip_Scan(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?v :where [?e :attr ?v]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	// Single pattern compiles to a Scan
	assert.Equal(t, RuleScan, root.Op, "single pattern → Scan")
	scan := root.Data.(*Scan)
	assert.NotNil(t, scan.Pattern, "Scan carries the DataPattern")
	assert.Len(t, scan.Output, 2, "Scan output: [?e, ?v]")

	// Decompile
	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 1)
	_, ok := clauses[0].(*query.DataPattern)
	assert.True(t, ok, "decompiled clause is DataPattern")
}

func TestRoundTrip_Constant(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?x :where [(ground 42) ?x]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	// Ground compiles to a Map wrapping a Constant (or directly to Constant)
	// The compiler may wrap in Map depending on binding type
	t.Logf("Tree: %s", root.String())

	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 1)

	expr, ok := clauses[0].(*query.Expression)
	assert.True(t, ok, "decompiled clause is Expression")
	if ok {
		// Parser produces *GroundFunction (pointer). Compile preserves the original
		// Expression in the Map node. Decompile returns it unchanged. So the function
		// type must match the parser's output: pointer, not value.
		_, isGround := expr.Function.(*query.GroundFunction)
		assert.True(t, isGround, "function is *GroundFunction (pointer, matching parser output)")
	}
}

func TestRoundTrip_ConstantTuple(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?a ?b :where [(ground [1 2]) [[?a ?b]]]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Tree: %s", root.String())

	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 1)

	expr, ok := clauses[0].(*query.Expression)
	assert.True(t, ok, "decompiled clause is Expression")
	if ok {
		_, isGround := expr.Function.(*query.GroundFunction)
		assert.True(t, isGround, "function is *GroundFunction (pointer, matching parser output)")
		_, isTuple := expr.Binding.(query.TupleBinding)
		assert.True(t, isTuple, "binding is TupleBinding")
	}
}

func TestRoundTrip_Select(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?age :where [?e :person/age ?age] [(> ?age 21)]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	// Pattern + predicate → Select wrapping Scan
	assert.Equal(t, RuleSelect, root.Op, "predicate → Select")
	assert.Len(t, root.Children, 1, "Select has 1 child")
	assert.Equal(t, RuleScan, root.Children[0].Op, "Select child is Scan")

	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 2, "pattern + predicate = 2 clauses")

	_, isPattern := clauses[0].(*query.DataPattern)
	assert.True(t, isPattern, "first clause is DataPattern")

	// Second clause should be a predicate (Comparison or similar)
	t.Logf("clause[1] type: %T", clauses[1])
}

func TestRoundTrip_Map(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?total :where [?e :item/price ?p] [(+ ?p 10) ?total]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	// Pattern + expression → Map wrapping Scan
	assert.Equal(t, RuleMap, root.Op, "expression → Map")
	assert.Len(t, root.Children, 1, "Map has 1 child")

	mapData := root.Data.(*Map)
	assert.NotNil(t, mapData.Expression, "Map carries the Expression")

	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 2, "pattern + expression = 2 clauses")

	_, isPattern := clauses[0].(*query.DataPattern)
	assert.True(t, isPattern, "first clause is DataPattern")
	_, isExpr := clauses[1].(*query.Expression)
	assert.True(t, isExpr, "second clause is Expression")
}

func TestRoundTrip_Map_GetElse(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?title :where [?e :entity/type :scenario] [(get-else $ ?e :title "") ?title]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Tree: %s", root.String())

	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 2)

	_, isPattern := clauses[0].(*query.DataPattern)
	assert.True(t, isPattern, "first clause is DataPattern")
	expr, isExpr := clauses[1].(*query.Expression)
	assert.True(t, isExpr, "second clause is Expression (get-else)")
	if isExpr {
		_, isDB := expr.Function.(query.DatabaseFunction)
		assert.True(t, isDB, "get-else is a DatabaseFunction")
	}
}

func TestRoundTrip_JoinInner(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	// Two patterns sharing ?e → Join(Inner)
	assert.Equal(t, RuleJoin, root.Op, "two patterns → Join")
	join := root.Data.(*Join)
	assert.Equal(t, InnerJoin, join.Kind, "natural join is Inner")
	assert.Len(t, root.Children, 2, "Join has 2 children")

	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 2, "two patterns round-trip to 2 clauses")

	for i, c := range clauses {
		_, isPattern := c.(*query.DataPattern)
		assert.True(t, isPattern, "clause[%d] is DataPattern", i)
	}
}

func TestRoundTrip_AntiJoin(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e :where [?e :person/name ?name] (not [?e :person/fired true])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	// Pattern + NOT → AntiJoin with explicit join vars (not → not-join conversion)
	assert.Equal(t, RuleAntiJoin, root.Op, "NOT → AntiJoin")
	aj := root.Data.(*AntiJoin)
	assert.NotEmpty(t, aj.JoinSymbols, "AntiJoin has join symbols")
	assert.True(t, aj.ExplicitJoin, "NOT is converted to explicit join (not → not-join)")
	assert.Len(t, root.Children, 2, "AntiJoin has 2 children (outer, inner)")

	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 2, "pattern + NOT = 2 clauses")

	_, isPattern := clauses[0].(*query.DataPattern)
	assert.True(t, isPattern, "first clause is DataPattern")
	notJoin, isNotJoin := clauses[1].(*query.NotJoinClause)
	assert.True(t, isNotJoin, "NOT decompiles to NotJoinClause (explicit join vars)")
	assert.Contains(t, notJoin.JoinVars, datalog.NewSymbol("?e"), "join var is ?e")
}

func TestRoundTrip_AntiJoinExplicit(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e :where [?e :person/name ?name] (not-join [?e] [?e :person/fired true])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	assert.Equal(t, RuleAntiJoin, root.Op, "NOT-JOIN → AntiJoin")
	aj := root.Data.(*AntiJoin)
	assert.True(t, aj.ExplicitJoin, "not-join → ExplicitJoin=true")

	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 2)

	_, isNJC := clauses[1].(*query.NotJoinClause)
	assert.True(t, isNJC, "second clause is NotJoinClause (not NotClause)")
}

func TestRoundTrip_OrUnion(t *testing.T) {
	// OR with two pattern branches compiles to Union with the inferred outer key
	// made explicit, then decompiles to the equivalent OrJoinClause.
	q, err := parser.ParseQuery(`[:find ?e ?val
	  :where
	  [?e :item/type :type/active]
	  (or [?e :item/cost ?val]
	      [?e :item/weight ?val])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Tree:\n%s", root.String())

	// Top level should be a Join (outer pattern joined with Union)
	assert.Equal(t, RuleJoin, root.Op, "pattern + OR → Join")

	// Find the Union child
	var unionNode *Node
	for _, child := range root.Children {
		if child.Op == RuleUnion {
			unionNode = child
		}
	}
	require.NotNil(t, unionNode, "should have Union child")

	// Decompile
	clauses, err := Decompile(root)
	require.NoError(t, err)
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}

	// Round-trips to DataPattern + OrJoinClause (union and outer key preserved).
	_, isPattern := clauses[0].(*query.DataPattern)
	assert.True(t, isPattern, "first clause is DataPattern")

	orJoin, isOrJoin := clauses[1].(*query.OrJoinClause)
	require.True(t, isOrJoin, "OR round-trips to OrJoinClause")
	require.Equal(t,
		[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?val")},
		orJoin.JoinVars,
	)
}

func TestRoundTrip_LateralJoin(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?count
	  :where
	  [?e :entity/type :entity.type/scenario]
	  (or-default [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/status :status/complete]]
	          $ ?e) [[?count]]]
	      [(ground 0) ?count])]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Tree:\n%s", root.String())

	// Should contain a LateralJoin with defaults
	lj := findLateralJoin(root)
	require.NotNil(t, lj, "OR-fallback with subquery → LateralJoin")

	ljData := lj.Data.(*LateralJoin)
	assert.NotEmpty(t, ljData.CorrelationVars, "has correlation variables")
	assert.NotEmpty(t, ljData.DefaultValues, "has defaults from ground branch")
	assert.NotNil(t, ljData.InnerQuery, "has inner query")

	// Decompile
	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.NotEmpty(t, clauses)
	t.Logf("Decompiled %d clauses:", len(clauses))
	for i, c := range clauses {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}

	// Should have a DataPattern and an OrDefaultJoinClause (the OR-fallback with join vars)
	hasPattern := false
	hasOrDefaultJoin := false
	for _, c := range clauses {
		switch c.(type) {
		case *query.DataPattern:
			hasPattern = true
		case *query.OrDefaultJoinClause:
			hasOrDefaultJoin = true
		}
	}
	assert.True(t, hasPattern, "decompiled has DataPattern")
	assert.True(t, hasOrDefaultJoin, "decompiled has OrDefaultJoinClause (OR-fallback with defaults and join vars)")
}

func TestRoundTrip_LateralJoinNoDefaults(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?count
	  :where
	  [?e :entity/type :entity.type/scenario]
	  [(q [:find (count ?t)
	       :in $ ?s
	       :where [?t :task/root ?s]]
	      $ ?e) [[?count]]]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)
	t.Logf("Tree:\n%s", root.String())

	// Should contain a LateralJoin WITHOUT defaults
	lj := findLateralJoin(root)
	require.NotNil(t, lj, "correlated subquery → LateralJoin")

	ljData := lj.Data.(*LateralJoin)
	assert.Empty(t, ljData.DefaultValues, "no defaults (no OR-fallback)")

	clauses, err := Decompile(root)
	require.NoError(t, err)

	// Without defaults: bare SubqueryPattern, no OrClause
	hasSubquery := false
	hasOr := false
	for _, c := range clauses {
		switch c.(type) {
		case *query.SubqueryPattern:
			hasSubquery = true
		case *query.OrClause:
			hasOr = true
		}
	}
	assert.True(t, hasSubquery, "decompiled has SubqueryPattern")
	assert.False(t, hasOr, "no OrClause without defaults")
}

// TestCompileAdapterRoundTrip verifies the parse.Node adapter preserves
// the algebra tree structure through ToParseTree/FromParseTree.
func TestCompileAdapterRoundTrip(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?name :where [?e :person/name ?name] [?e :person/age ?age] [(> ?age 21)]]`)
	require.NoError(t, err)

	root, err := Compile(q)
	require.NoError(t, err)

	// Adapt to parse tree and back
	tree := ToParseTree(root)
	require.NotNil(t, tree)
	require.NotNil(t, tree.Root)

	recovered := FromParseTree(tree)
	require.NotNil(t, recovered)

	assert.Equal(t, root.Op, recovered.Op)
	assert.Equal(t, len(root.Children), len(recovered.Children))
}
