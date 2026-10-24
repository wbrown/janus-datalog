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
			parsed, err := parser.ParseQuery(tt.query)
			require.NoError(t, err)
			q := mustPrepare(t, parsed, nil)

			// Compile
			root, err := Compile(q, nil)
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
// re-executes the clause as first-match-wins and drops tuples (see
// BUG_CORRELATED_ORJOIN_GLOBAL_FALLBACK_DROPS_TUPLES.md).
func TestRoundTrip_CorrelatedOrJoinPreservesClauseType(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?v
	  :where
	  [?e :x/tag ?tag]
	  (or-join [?e ?v]
	    (and [?e :x/a ?v]
	         (not [?e :x/flag true]))
	    [?e :x/b ?v])]`)
	require.NoError(t, err)

	root, err := Compile(mustPrepare(t, q, nil), nil)
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
		orJoin.OutputVars,
	)
	require.Len(t, orJoin.Branches, 2)
	// Prepare writes the plain not as the not-join over its inferred header,
	// and the branch must keep its anti-join in that form.
	var branchNot *query.NotJoinClause
	for _, c := range orJoin.Branches[0] {
		if nc, ok := c.(*query.NotJoinClause); ok {
			branchNot = nc
		}
	}
	require.NotNil(t, branchNot, "correlated branch must keep its NOT clause")
	require.Equal(t, []query.Symbol{datalog.NewSymbol("?e")}, branchNot.RequiredVars)
}

// TestRoundTrip_CorrelatedOrStatesItsInferredHeader: a plain (or ...) with a
// correlated predicate branch shares ?e and ?v with the pattern and the
// find, and every branch binds both, so it decompiles as the or-join whose
// flat header [?e ?v] its text states. The branch's not shares ?e with the
// branch and decompiles as a not-join on ?e.
func TestRoundTrip_CorrelatedOrStatesItsInferredHeader(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?v
	  :where
	  [?e :x/tag ?tag]
	  (or (and [?e :x/a ?v]
	           (not [?e :x/flag true]))
	      [?e :x/b ?v])]`)
	require.NoError(t, err)

	root, err := Compile(mustPrepare(t, q, nil), nil)
	require.NoError(t, err)
	clauses, err := Decompile(root)
	require.NoError(t, err)

	var orJoin *query.OrJoinClause
	for _, c := range clauses {
		if oj, ok := c.(*query.OrJoinClause); ok {
			orJoin = oj
		}
	}
	require.NotNil(t, orJoin, "the correlated or decompiles as an or-join, got %v", clauses)
	require.Empty(t, orJoin.RequiredVars)
	require.Equal(t,
		[]query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")},
		orJoin.OutputVars,
	)
	require.Len(t, orJoin.Branches, 2)
	var branchNot *query.NotJoinClause
	for _, c := range orJoin.Branches[0] {
		if nc, ok := c.(*query.NotJoinClause); ok {
			branchNot = nc
		}
	}
	require.NotNil(t, branchNot, "the correlated branch keeps its not as a not-join, got %v", orJoin.Branches[0])
	require.Equal(t, []query.Symbol{datalog.NewSymbol("?e")}, branchNot.RequiredVars)
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

	root, err := Compile(mustPrepare(t, q, nil), nil)
	require.NoError(t, err)
	require.NotNil(t, root)

	t.Logf("Algebra tree:\n%s", root.String())

	// The tree should contain a LateralJoin node
	lj := findLateralJoin(root)
	if lj != nil {
		ljData := lj.Data.(*LateralJoin)
		t.Logf("Found LateralJoin: correlation=%v, defaults=%v",
			ljData.CorrelationVars, ljData.Defaults)
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
	q = mustPrepare(t, q, nil)

	root, err := Compile(q, nil)
	require.NoError(t, err)
	t.Logf("Compiled:\n%s", root.String())

	optimizer := NewOptimizer(DefaultPasses(nil)...)
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
	q = mustPrepare(t, q, nil)

	root, err := Compile(q, nil)
	require.NoError(t, err)
	t.Logf("Compiled:\n%s", root.String())

	// Optimize (should be a no-op — no LateralJoins)
	optimizer := NewOptimizer(DefaultPasses(nil)...)
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

	root, err := Compile(q, nil)
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

	optimizer := NewOptimizer(DefaultPasses(nil)...)
	optimized, err := optimizer.Optimize(root)
	require.NoError(t, err)
	clauses, err := Decompile(optimized)
	require.NoError(t, err)
	notJoin, ok := clauses[len(clauses)-1].(*query.NotJoinClause)
	require.True(t, ok)
	require.Equal(t, anti.JoinSymbols, notJoin.RequiredVars)
}

// A union of one branch compiles to a Union of one child on the correlated
// route, the branch holding a not-join
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §5).
func TestCompileAUnionOfOneBranch(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e :where [?e :t/n ?n] (or-join [?e] (and [?e :t/p ?p] (not-join [?e] [?e :t/q _])))]`)
	require.NoError(t, err)
	root, err := Compile(mustPrepare(t, q, nil), nil)
	require.NoError(t, err)
	var union *Node
	var find func(n *Node)
	find = func(n *Node) {
		if union != nil || n == nil {
			return
		}
		if n.Op == RuleUnion {
			union = n
			return
		}
		for _, child := range n.Children {
			find(child)
		}
	}
	find(root)
	require.NotNil(t, union, "the or-join compiles to a Union")
	require.Len(t, union.Children, 1, "the Union has the one branch as its child")
}

// A fallback branch the fold cannot order is refused naming the branch by its
// position counted from one (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.4).
func TestCompileNamesAFailingFallbackBranchCountedFromOne(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?v :where [?e :t/n _] (or-default-join [[?e] ?v] (and (or-join [[?w] ?v] [?w :t/b ?v]) (or-join [[?v] ?w] [?v :t/c ?w])) [(ground 0) ?v])]`)
	require.NoError(t, err)
	_, err = Compile(mustPrepare(t, q, nil), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "OR fallback branch 1:")
}

// The analyzer's union check names a branch by its position counted from one
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §2.4).
func TestAnalyzeUnionBranchesNamesTheBranchCountedFromOne(t *testing.T) {
	e, v := datalog.NewSymbol("?e"), datalog.NewSymbol("?v")
	children := []Analysis{{Output: []query.Symbol{e}}, {Output: []query.Symbol{e, v}}}
	_, err := analyzeUnionBranches([]query.Symbol{e, v}, []query.Symbol{v}, nil, children)
	require.Error(t, err)
	require.Contains(t, err.Error(), "branch 1 schema")
	_, err = analyzeUnionBranches([]query.Symbol{e, v}, nil, nil, children)
	require.Error(t, err)
	require.Contains(t, err.Error(), "union branch 1 schema")
}

// The analyzer's union check is the contract every pass keeps on a union
// node: a header variable a branch's schema does not bind is refused naming
// the variable, with the explanation that every branch must bind every
// header variable and that a branch-specific input filter belongs out of the
// header (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §5). The first
// branch binds ?entity and ?crawl and the second ?entity and ?world, under a
// header of all three.
func TestOrJoinSchemaDiagnosticExplainsHeaderContract(t *testing.T) {
	entity, crawl, world := datalog.NewSymbol("?entity"), datalog.NewSymbol("?crawl"), datalog.NewSymbol("?world")
	header := []query.Symbol{entity, crawl, world}
	children := []Analysis{{Output: []query.Symbol{entity, crawl}}, {Output: []query.Symbol{entity, world}}}
	_, err := analyzeUnionBranches(header, header, nil, children)
	require.Error(t, err)
	require.Contains(t, err.Error(), "or-join header declares ?world")
	require.Contains(t, err.Error(), "every branch must bind every header variable")
	require.Contains(t, err.Error(), "remove it from the header")
}

// TestCompileRejectsInvalidNotJoinHeaders: the header demands ?missing,
// which the body reads and no input or clause binds; the compile-order pass
// rejects it before the fold reaches compileNotJoin's own header checks.
func TestCompileRejectsInvalidNotJoinHeaders(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?goal
		:where
		[?goal :entity/type :type/goal]
		(not-join [?goal ?missing]
			[?event :event/goal ?goal]
			[?event :event/type ?missing])]`)
	require.NoError(t, err)
	_, err = Compile(q, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "waits on ?missing (no input or clause binds it)")
}

// The fold's stall is the planner's: it refuses the clauses no order places
// under one message, naming for each the variables it waits on, with the
// suffix on a variable no input and no clause binds and none on a variable a
// clause binds that no order places before the waiting clause
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.3).
func TestOrderClausesForCompileRefusalSaysWhetherAnythingBindsAWaitedVariable(t *testing.T) {
	cases := []struct {
		name, text    string
		waits, absent []string
	}{
		{
			"nothing binds the variable",
			`[:find ?e :where (or-join [[?x] ?e] [?e :t/a ?x])]`,
			[]string{"waits on ?x (no input or clause binds it)"},
			nil,
		},
		{
			"a clause binds it that no order places first",
			`[:find ?v ?w :where (or-join [[?w] ?v] [?w :t/b ?v]) (or-join [[?v] ?w] [?v :t/c ?w])]`,
			[]string{"waits on ?w", "waits on ?v"},
			[]string{"(no input or clause binds it)"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, err := parser.ParseQuery(c.text)
			require.NoError(t, err)
			_, err = orderClausesForCompile(q.Where, query.Bindings{})
			require.Error(t, err, "no order places every clause")
			require.Contains(t, err.Error(), "cannot order clauses: ")
			for _, waits := range c.waits {
				require.Contains(t, err.Error(), waits)
			}
			for _, absent := range c.absent {
				require.NotContains(t, err.Error(), absent)
			}
		})
	}
}

// TestPrepareRejectsAnUnreadNotJoinHeader: the header names ?name, which the
// body neither binds nor reads; preparation refuses the header naming the
// form, the variable and the repair, before any fold sees the query
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §2.3).
func TestPrepareRejectsAnUnreadNotJoinHeader(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?goal
		:where
		[?goal :entity/type :type/goal]
		[?goal :entity/name ?name]
		(not-join [?goal ?name]
			[?event :event/goal ?goal])]`)
	require.NoError(t, err)
	_, err = query.Prepare(q, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "(not-join [?goal ?name]", "the refusal names the form")
	require.Contains(t, err.Error(), "the header names ?name, which the body neither binds nor reads", "the refusal names the variable")
	require.Contains(t, err.Error(), "drop ?name from the header or read it in the body", "the refusal names the repair")
}

func TestPrepareRefusesPlainNotWithUnboundOuterRequirement(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?goal
		:where
		[?goal :entity/type :type/goal]
		(not
			[?event :event/goal ?goal]
			[?event :event/type ?termType]
			[(!= ?termType ?missing)])]`)
	require.NoError(t, err)

	// The body predicate reads ?missing, which neither the body nor the
	// outer query binds, so preparation refuses the query naming the form
	// and the symbol.
	_, err = query.Prepare(q, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "(not ")
	require.Contains(t, err.Error(), "?missing")
}

// TestCompileDecompileWithGetElse verifies get-else survives the pipeline.
func TestCompileDecompileWithGetElse(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?title
		:where [?e :entity/type :entity.type/scenario]
		       (not [?e :entity/deleted true])
		       [(get-else $ ?e :scenario/title "") ?title]]`)
	require.NoError(t, err)
	q = mustPrepare(t, q, nil)

	root, err := Compile(q, nil)
	require.NoError(t, err)

	optimizer := NewOptimizer(DefaultPasses(nil)...)
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
	root, err := Compile(mustPrepare(t, parsed, nil), nil)
	require.NoError(t, err)

	unionNode := findAlgebraNode(root, RuleUnion)
	require.NotNil(t, unionNode)
	union := unionNode.Data.(*Union)
	require.Equal(t,
		[]query.Symbol{datalog.NewSymbol("?value")},
		union.OutputVars,
	)
	require.Equal(t,
		[]query.Symbol{datalog.NewSymbol("?entity")},
		union.Required,
	)
	require.Equal(t,
		[]query.Symbol{datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")},
		union.Output,
	)
	require.Equal(t,
		[]query.Symbol{datalog.NewSymbol("?entity")},
		union.RequiredVars,
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
	root, err := Compile(mustPrepare(t, parsed, nil), nil)
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

	root, err := Compile(q, nil)
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

	root, err := Compile(q, nil)
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
	q, err := parser.ParseQuery(`[:find ?a ?b :where [(ground [1 2]) [?a ?b]]]`)
	require.NoError(t, err)

	root, err := Compile(q, nil)
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

// A ground under the relation form compiles to a Map node whose output
// carries the binding's symbols, passes analysis, and decompiles to the same
// relation binding.
func TestRoundTrip_RelationGround(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?a ?b :where [?e :t/n _] [(ground [[1 2] [3 4]]) [[?a ?b]]]]`)
	require.NoError(t, err)

	root, err := Compile(q, nil)
	require.NoError(t, err)
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	require.Contains(t, root.Symbols(), a)
	require.Contains(t, root.Symbols(), b)

	_, err = Analyze(root)
	require.NoError(t, err, "the Map node's output is the relation binding's symbols beside its child's")

	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 2)
	expr, ok := clauses[1].(*query.Expression)
	require.True(t, ok, "decompiled clause is Expression, got %T", clauses[1])
	require.Equal(t, query.RelationBinding{Variables: []query.Symbol{a, b}}, expr.Binding)
}

func TestRoundTrip_Select(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?age :where [?e :person/age ?age] [(> ?age 21)]]`)
	require.NoError(t, err)

	root, err := Compile(q, nil)
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

	root, err := Compile(q, nil)
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

	root, err := Compile(q, nil)
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

	root, err := Compile(q, nil)
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

	root, err := Compile(mustPrepare(t, q, nil), nil)
	require.NoError(t, err)

	// Pattern + NOT → AntiJoin keyed on the header Prepare inferred for the not
	assert.Equal(t, RuleAntiJoin, root.Op, "NOT → AntiJoin")
	aj := root.Data.(*AntiJoin)
	assert.NotEmpty(t, aj.JoinSymbols, "AntiJoin has join symbols")
	assert.Len(t, root.Children, 2, "AntiJoin has 2 children (outer, inner)")

	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 2, "pattern + NOT = 2 clauses")

	_, isPattern := clauses[0].(*query.DataPattern)
	assert.True(t, isPattern, "first clause is DataPattern")
	notJoin, isNotJoin := clauses[1].(*query.NotJoinClause)
	assert.True(t, isNotJoin, "NOT decompiles to NotJoinClause over the inferred header")
	assert.Contains(t, notJoin.RequiredVars, datalog.NewSymbol("?e"), "join var is ?e")
}

func TestRoundTrip_AntiJoinExplicit(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e :where [?e :person/name ?name] (not-join [?e] [?e :person/fired true])]`)
	require.NoError(t, err)

	root, err := Compile(q, nil)
	require.NoError(t, err)

	assert.Equal(t, RuleAntiJoin, root.Op, "NOT-JOIN → AntiJoin")
	aj := root.Data.(*AntiJoin)
	assert.Equal(t, []query.Symbol{datalog.NewSymbol("?e")}, aj.JoinSymbols, "the anti-join is keyed on the written header")

	clauses, err := Decompile(root)
	require.NoError(t, err)
	require.Len(t, clauses, 2)

	_, isNJC := clauses[1].(*query.NotJoinClause)
	assert.True(t, isNJC, "second clause is NotJoinClause (not NotClause)")
}

// TestDecompileAntiJoinEmitsTheNotJoinOverItsJoinSymbols: the decompiler
// renders an anti-join as the not-join over its join symbols, whatever
// built the node, since every anti-join comes from a not-join: Compile
// takes a prepared query, and Prepare writes a plain not as the not-join
// over its inferred header (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §6).
func TestDecompileAntiJoinEmitsTheNotJoinOverItsJoinSymbols(t *testing.T) {
	entity := datalog.NewSymbol("?e")
	outer, err := parser.ParseQuery(`[:find ?e :where [?e :person/name ?name]]`)
	require.NoError(t, err)
	body, err := parser.ParseQuery(`[:find ?e :where [?e :person/fired true]]`)
	require.NoError(t, err)
	left, err := Compile(outer, nil)
	require.NoError(t, err)
	right, err := Compile(body, nil)
	require.NoError(t, err)
	anti := &Node{
		Op:       RuleAntiJoin,
		Children: []*Node{left, right},
		Data: &AntiJoin{
			JoinSymbols: []query.Symbol{entity},
			Output:      left.Symbols(),
		},
	}

	clauses, err := Decompile(anti)
	require.NoError(t, err)
	require.Len(t, clauses, 2)
	notJoin, isNotJoin := clauses[1].(*query.NotJoinClause)
	require.True(t, isNotJoin, "an anti-join decompiles to the not-join over its join symbols, got %T", clauses[1])
	require.Equal(t, []query.Symbol{entity}, notJoin.RequiredVars)
	require.Equal(t, body.Where, notJoin.Clauses)
}

// TestDecompileUnionEmitsTheOrJoinOverItsHeader: a union carries its
// header's two halves, the required run and the outputs, states them in
// its rendering, and decompiles to the or-join over them, whatever built
// the node (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §5.3, §6).
func TestDecompileUnionEmitsTheOrJoinOverItsHeader(t *testing.T) {
	entity, value := datalog.NewSymbol("?e"), datalog.NewSymbol("?v")
	first, err := parser.ParseQuery(`[:find ?e ?v :where [?e :t/a ?v]]`)
	require.NoError(t, err)
	second, err := parser.ParseQuery(`[:find ?e ?v :where [?e :t/b ?v]]`)
	require.NoError(t, err)
	left, err := Compile(first, nil)
	require.NoError(t, err)
	right, err := Compile(second, nil)
	require.NoError(t, err)
	union := &Union{
		Output:       []query.Symbol{entity, value},
		RequiredVars: []query.Symbol{entity},
		OutputVars:   []query.Symbol{value},
		Required:     []query.Symbol{entity},
	}
	node := &Node{Op: RuleUnion, Children: []*Node{left, right}, Data: union}

	require.Equal(t, "∪_join([?e] ?v)", union.String())
	clauses, err := Decompile(node)
	require.NoError(t, err)
	require.Len(t, clauses, 1)
	orJoin, isOrJoin := clauses[0].(*query.OrJoinClause)
	require.True(t, isOrJoin, "a union decompiles to the or-join over its header, got %T", clauses[0])
	require.Equal(t, []query.Symbol{entity}, orJoin.RequiredVars)
	require.Equal(t, []query.Symbol{value}, orJoin.OutputVars)
	require.Equal(t, [][]query.Clause{first.Where, second.Where}, orJoin.Branches)
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

	root, err := Compile(mustPrepare(t, q, nil), nil)
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
		orJoin.OutputVars,
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

	root, err := Compile(mustPrepare(t, q, nil), nil)
	require.NoError(t, err)
	t.Logf("Tree:\n%s", root.String())

	// Should contain a LateralJoin with defaults
	lj := findLateralJoin(root)
	require.NotNil(t, lj, "OR-fallback with subquery → LateralJoin")

	ljData := lj.Data.(*LateralJoin)
	assert.NotEmpty(t, ljData.CorrelationVars, "has correlation variables")
	assert.NotEmpty(t, ljData.Defaults, "has defaults from ground branch")
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

	root, err := Compile(q, nil)
	require.NoError(t, err)
	t.Logf("Tree:\n%s", root.String())

	// Should contain a LateralJoin WITHOUT defaults
	lj := findLateralJoin(root)
	require.NotNil(t, lj, "correlated subquery → LateralJoin")

	ljData := lj.Data.(*LateralJoin)
	assert.Empty(t, ljData.Defaults, "no defaults (no OR-fallback)")

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

// The default branch of an or-default lowered onto a lateral join keeps each
// default value with the symbol its ground binds through compile and
// decompile: [(ground [7 8]) [?u ?v]] decompiles to a ground binding 7 to
// ?u and 8 to ?v, whatever order the subquery's binding names them in, and
// the decompiled clauses parse.
func TestRoundTrip_DefaultsKeepTheirSymbols(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?v ?u
	  :where [?e :t/b _]
	         (or-default-join [[?e] ?v ?u]
	           [(q [:find (max ?w) (min ?w) :in $ ?e :where [?e :t/c ?w]] $ ?e) [[?v ?u]]]
	           [(ground [7 8]) [?u ?v]])]`)
	require.NoError(t, err)

	root, err := Compile(mustPrepare(t, q, nil), nil)
	require.NoError(t, err)
	clauses, err := Decompile(root)
	require.NoError(t, err)

	var fallback *query.OrDefaultJoinClause
	for _, c := range clauses {
		if oj, ok := c.(*query.OrDefaultJoinClause); ok {
			fallback = oj
		}
	}
	require.NotNil(t, fallback, "the lowering decompiles to an or-default-join")
	require.Len(t, fallback.Branches, 2)
	require.Len(t, fallback.Branches[1], 1, "the default branch is one ground")
	ground, ok := fallback.Branches[1][0].(*query.Expression)
	require.True(t, ok, "the default branch is an expression, got %T", fallback.Branches[1][0])
	fn, ok := ground.Function.(*query.GroundFunction)
	require.True(t, ok, "the default branch is a ground, got %T", ground.Function)
	values, ok := query.VectorElements(fn.Value)
	require.True(t, ok, "a ground of two defaults holds a vector, got %T", fn.Value)
	symbols := ground.BoundVariables()
	require.Len(t, values, len(symbols), "one value per bound symbol: %s", ground)
	bound := map[query.Symbol]interface{}{}
	for i, sym := range symbols {
		bound[sym] = values[i]
	}
	assert.Equal(t, int64(7), bound[datalog.NewSymbol("?u")], "?u takes 7: %s", ground)
	assert.Equal(t, int64(8), bound[datalog.NewSymbol("?v")], "?v takes 8: %s", ground)

	decompiled := &query.Query{Find: q.Find, Where: clauses}
	_, err = parser.ParseQuery(decompiled.String())
	require.NoError(t, err, "the decompiled clauses parse: %s", decompiled)
}

// TestCompileAdapterRoundTrip verifies the parse.Node adapter preserves
// the algebra tree structure through ToParseTree/FromParseTree.
func TestCompileAdapterRoundTrip(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?name :where [?e :person/name ?name] [?e :person/age ?age] [(> ?age 21)]]`)
	require.NoError(t, err)

	root, err := Compile(q, nil)
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
