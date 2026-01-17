package qb

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestNewVar tests that variables get unique IDs
func TestNewVar(t *testing.T) {
	v1 := NewVar()
	v2 := NewVar()
	v3 := NewVar()

	if v1.id == v2.id {
		t.Error("v1 and v2 should have different IDs")
	}
	if v2.id == v3.id {
		t.Error("v2 and v3 should have different IDs")
	}

	// Symbol should start with ?v
	sym1 := v1.Symbol()
	if sym1[0] != '?' {
		t.Errorf("Symbol should start with ?, got %s", sym1)
	}
}

// TestKw tests keyword creation
func TestKw(t *testing.T) {
	attr := Kw(":person/name")

	// Should create a proper keyword
	if attr.kw == nil {
		t.Error("Keyword should not be nil")
	}

	// Test Keyword method
	kw := attr.Keyword()
	if kw.String() != ":person/name" {
		t.Errorf("Expected :person/name, got %s", kw.String())
	}
}

// TestVal tests value wrapping
func TestVal(t *testing.T) {
	// String value
	strVal := V("hello")
	if strVal.value != "hello" {
		t.Errorf("Expected hello, got %v", strVal.value)
	}

	// Int value
	intVal := V(42)
	if intVal.value != 42 {
		t.Errorf("Expected 42, got %v", intVal.value)
	}

	// Float value
	floatVal := V(3.14)
	if floatVal.value != 3.14 {
		t.Errorf("Expected 3.14, got %v", floatVal.value)
	}
}

// TestBlank tests blank/wildcard
func TestBlank(t *testing.T) {
	b := Blank()

	// Convert to pattern element and check type
	elem := b.toPatternElement()
	if _, ok := elem.(query.Blank); !ok {
		t.Error("Blank should convert to query.Blank")
	}
}

// TestPat tests 3-element pattern creation
func TestPat(t *testing.T) {
	e := NewVar()
	name := NewVar()
	PersonName := Kw(":person/name")

	pat := Pat(e, PersonName, name)

	// Should have 3 elements
	clause := pat.toClause()
	dp, ok := clause.(*query.DataPattern)
	if !ok {
		t.Fatalf("Expected *query.DataPattern, got %T", clause)
	}
	if len(dp.Elements) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(dp.Elements))
	}
}

// TestPat4 tests 4-element pattern creation (with tx)
func TestPat4(t *testing.T) {
	e := NewVar()
	name := NewVar()
	tx := NewVar()
	PersonName := Kw(":person/name")

	pat := Pat(e, PersonName, name, tx)

	clause := pat.toClause()
	dp, ok := clause.(*query.DataPattern)
	if !ok {
		t.Fatalf("Expected *query.DataPattern, got %T", clause)
	}
	if len(dp.Elements) != 4 {
		t.Errorf("Expected 4 elements, got %d", len(dp.Elements))
	}
}

// TestPat5 tests 5-element pattern creation (history with op)
func TestPat5(t *testing.T) {
	e := NewVar()
	name := NewVar()
	tx := NewVar()
	op := NewVar()
	PersonName := Kw(":person/name")

	pat := Pat(e, PersonName, name, tx, op)

	clause := pat.toClause()
	dp, ok := clause.(*query.DataPattern)
	if !ok {
		t.Fatalf("Expected *query.DataPattern, got %T", clause)
	}
	if len(dp.Elements) != 5 {
		t.Errorf("Expected 5 elements, got %d", len(dp.Elements))
	}
}

// TestPatternWithValues tests pattern with constant values
func TestPatternWithValues(t *testing.T) {
	e := NewVar()
	PersonName := Kw(":person/name")

	pat := Pat(e, PersonName, V("Alice"))

	clause := pat.toClause()
	dp := clause.(*query.DataPattern)

	// Third element should be a constant
	if _, ok := dp.Elements[2].(query.Constant); !ok {
		t.Errorf("Expected constant, got %T", dp.Elements[2])
	}
}

// TestPatInvalidArity tests that Pat panics with zero arguments
func TestPatInvalidArity(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Pat() should panic with zero arguments")
		}
	}()
	Pat() // Should panic
}

// TestPat2 tests 2-element pattern (for input relations)
func TestPat2(t *testing.T) {
	name := NewVar()
	age := NewVar()

	pat := Pat(name, age)

	clause := pat.toClause()
	dp, ok := clause.(*query.DataPattern)
	if !ok {
		t.Fatalf("Expected *query.DataPattern, got %T", clause)
	}
	if len(dp.Elements) != 2 {
		t.Errorf("Expected 2 elements, got %d", len(dp.Elements))
	}
}

// TestPredicates tests comparison predicates
func TestPredicates(t *testing.T) {
	age := NewVar()

	tests := []struct {
		name string
		pred *Comparison
		op   query.CompareOp
	}{
		{"Lt", Lt(age, V(30)), query.OpLT},
		{"Lte", Lte(age, V(30)), query.OpLTE},
		{"Gt", Gt(age, V(30)), query.OpGT},
		{"Gte", Gte(age, V(30)), query.OpGTE},
		{"Eq", Eq(age, V(30)), query.OpEQ},
		{"Ne", Ne(age, V(30)), query.OpNE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause := tt.pred.toClause()
			comp, ok := clause.(*query.Comparison)
			if !ok {
				t.Fatalf("Expected *query.Comparison, got %T", clause)
			}
			if comp.Op != tt.op {
				t.Errorf("Expected %v, got %v", tt.op, comp.Op)
			}
		})
	}
}

// TestChainedComparison tests range predicates
func TestChainedComparison(t *testing.T) {
	x := NewVar()

	// Range: 0 < x < 100
	chain := Chained(query.OpLT, V(0), x, V(100))

	clause := chain.toClause()
	cpc, ok := clause.(*query.ChainedComparison)
	if !ok {
		t.Fatalf("Expected *query.ChainedComparison, got %T", clause)
	}
	if len(cpc.Terms) != 3 {
		t.Errorf("Expected 3 terms, got %d", len(cpc.Terms))
	}
	if cpc.Op != query.OpLT {
		t.Errorf("Expected OpLT, got %v", cpc.Op)
	}
}

// TestRange tests convenience range function
func TestRange(t *testing.T) {
	x := NewVar()

	// Range: 10 < x < 20
	chain := Range(V(10), x, V(20))

	clause := chain.toClause()
	cpc := clause.(*query.ChainedComparison)
	if len(cpc.Terms) != 3 {
		t.Errorf("Expected 3 terms, got %d", len(cpc.Terms))
	}
}

// TestAggregations tests aggregation functions
func TestAggregations(t *testing.T) {
	salary := NewVar()

	tests := []struct {
		name string
		agg  Agg
		fn   string
	}{
		{"Sum", Sum(salary), "sum"},
		{"Count", Count(salary), "count"},
		{"Avg", Avg(salary), "avg"},
		{"Min", Min(salary), "min"},
		{"Max", Max(salary), "max"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			elem := tt.agg.toFindElement()
			aggElem, ok := elem.(query.FindAggregate)
			if !ok {
				t.Fatalf("Expected query.FindAggregate, got %T", elem)
			}
			if aggElem.Function != tt.fn {
				t.Errorf("Expected %s, got %s", tt.fn, aggElem.Function)
			}
		})
	}
}

// TestArithmeticExpressions tests arithmetic expression builders
func TestArithmeticExpressions(t *testing.T) {
	a := NewVar()
	b := NewVar()
	result := NewVar()

	tests := []struct {
		name string
		expr *Expression
		op   query.ArithmeticOp
	}{
		{"Add", Add(a, b).As(result), query.OpAdd},
		{"Sub", Sub(a, b).As(result), query.OpSubtract},
		{"Mul", Mul(a, b).As(result), query.OpMultiply},
		{"Div", Div(a, b).As(result), query.OpDivide},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause := tt.expr.toClause()
			expr, ok := clause.(*query.Expression)
			if !ok {
				t.Fatalf("Expected *query.Expression, got %T", clause)
			}
			arith, ok := expr.Function.(query.ArithmeticFunction)
			if !ok {
				t.Fatalf("Expected ArithmeticFunction, got %T", expr.Function)
			}
			if arith.Op != tt.op {
				t.Errorf("Expected %v, got %v", tt.op, arith.Op)
			}
		})
	}
}

// TestStrConcat tests string concatenation expression
func TestStrConcat(t *testing.T) {
	firstName := NewVar()
	lastName := NewVar()
	fullName := NewVar()

	expr := Str(firstName, V(" "), lastName).As(fullName)

	clause := expr.toClause()
	e := clause.(*query.Expression)
	_, ok := e.Function.(query.StringConcatFunction)
	if !ok {
		t.Errorf("Expected StringConcatFunction, got %T", e.Function)
	}
}

// TestGround tests ground expression
func TestGround(t *testing.T) {
	taxRate := NewVar()

	expr := Ground(0.08).As(taxRate)

	clause := expr.toClause()
	e := clause.(*query.Expression)
	gf, ok := e.Function.(query.GroundFunction)
	if !ok {
		t.Fatalf("Expected GroundFunction, got %T", e.Function)
	}
	if gf.Value != 0.08 {
		t.Errorf("Expected 0.08, got %v", gf.Value)
	}
}

// TestTupleGround tests tuple ground expression
func TestTupleGround(t *testing.T) {
	a, b, c := NewVar(), NewVar(), NewVar()

	expr := TupleGround(0, 0, 0).As(a, b, c)

	clause := expr.toClause()
	e := clause.(*query.Expression)

	// Verify GroundFunction with []interface{}
	gf, ok := e.Function.(query.GroundFunction)
	if !ok {
		t.Fatalf("Expected GroundFunction, got %T", e.Function)
	}

	values, ok := gf.Value.([]interface{})
	if !ok {
		t.Fatalf("Expected []interface{}, got %T", gf.Value)
	}
	if len(values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(values))
	}

	// Verify TupleBinding
	tb, ok := e.Binding.(query.TupleBinding)
	if !ok {
		t.Fatalf("Expected TupleBinding, got %T", e.Binding)
	}
	if len(tb.Variables) != 3 {
		t.Errorf("Expected 3 variables, got %d", len(tb.Variables))
	}
}

// TestTupleGroundMixedTypes tests tuple ground with different value types
func TestTupleGroundMixedTypes(t *testing.T) {
	s, n := NewVar(), NewVar()

	expr := TupleGround("hello", 42).As(s, n)

	clause := expr.toClause()
	e := clause.(*query.Expression)
	gf := e.Function.(query.GroundFunction)

	values := gf.Value.([]interface{})
	if values[0] != "hello" {
		t.Errorf("Expected 'hello', got %v", values[0])
	}
	if values[1] != 42 {
		t.Errorf("Expected 42, got %v", values[1])
	}
}

// TestTupleGroundInOr tests tuple ground in Or clause (primary use case)
func TestTupleGroundInOr(t *testing.T) {
	count, total := NewVar(), NewVar()

	// Build an OR clause with a tuple ground fallback
	orClause := Or(
		[]interface{}{
			// First branch: some pattern
			Pat(NewVar(), Kw(":dummy/attr"), NewVar()),
		},
		[]interface{}{
			// Second branch: tuple ground fallback
			TupleGround(0, 0).As(count, total),
		},
	)

	clause := orClause.toClause()
	oc, ok := clause.(*query.OrClause)
	if !ok {
		t.Fatalf("Expected OrClause, got %T", clause)
	}

	if len(oc.Branches) != 2 {
		t.Errorf("Expected 2 branches, got %d", len(oc.Branches))
	}

	// Second branch should have the tuple ground expression
	if len(oc.Branches[1]) != 1 {
		t.Fatalf("Expected 1 clause in second branch, got %d", len(oc.Branches[1]))
	}

	expr, ok := oc.Branches[1][0].(*query.Expression)
	if !ok {
		t.Fatalf("Expected Expression in second branch, got %T", oc.Branches[1][0])
	}

	// Verify it's a ground function with tuple binding
	_, ok = expr.Function.(query.GroundFunction)
	if !ok {
		t.Errorf("Expected GroundFunction, got %T", expr.Function)
	}

	_, ok = expr.Binding.(query.TupleBinding)
	if !ok {
		t.Errorf("Expected TupleBinding, got %T", expr.Binding)
	}
}

// TestIdentity tests identity expression
func TestIdentity(t *testing.T) {
	original := NewVar()
	alias := NewVar()

	expr := Identity(original).As(alias)

	clause := expr.toClause()
	e := clause.(*query.Expression)
	_, ok := e.Function.(query.IdentityFunction)
	if !ok {
		t.Errorf("Expected IdentityFunction, got %T", e.Function)
	}
}

// TestTimeExtraction tests time extraction functions
func TestTimeExtraction(t *testing.T) {
	createdAt := NewVar()
	y := NewVar()

	tests := []struct {
		name  string
		expr  *Expression
		field string
	}{
		{"Year", Year(createdAt).As(y), "year"},
		{"Month", Month(createdAt).As(y), "month"},
		{"Day", Day(createdAt).As(y), "day"},
		{"Hour", Hour(createdAt).As(y), "hour"},
		{"Minute", Minute(createdAt).As(y), "minute"},
		{"Second", Second(createdAt).As(y), "second"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause := tt.expr.toClause()
			e := clause.(*query.Expression)
			te, ok := e.Function.(query.TimeExtractionFunction)
			if !ok {
				t.Fatalf("Expected TimeExtractionFunction, got %T", e.Function)
			}
			if te.Field != tt.field {
				t.Errorf("Expected field %s, got %s", tt.field, te.Field)
			}
		})
	}
}

// TestInputSpecs tests input parameter specifications
func TestInputSpecs(t *testing.T) {
	// Test DB input
	dbSpec := DB.toInputSpec()
	if _, ok := dbSpec.(query.DatabaseInput); !ok {
		t.Errorf("DB should produce DatabaseInput, got %T", dbSpec)
	}

	// Test Scalar input
	minAge := NewVar()
	scalarSpec := Scalar(minAge).toInputSpec()
	if _, ok := scalarSpec.(query.ScalarInput); !ok {
		t.Errorf("Scalar should produce ScalarInput, got %T", scalarSpec)
	}

	// Test Collection input
	id := NewVar()
	collSpec := Collection(id).toInputSpec()
	if _, ok := collSpec.(query.CollectionInput); !ok {
		t.Errorf("Collection should produce CollectionInput, got %T", collSpec)
	}

	// Test Tuple input
	x, y := NewVar(), NewVar()
	tupleSpec := Tuple(x, y).toInputSpec()
	if _, ok := tupleSpec.(query.TupleInput); !ok {
		t.Errorf("Tuple should produce TupleInput, got %T", tupleSpec)
	}

	// Test Relation input
	relSpec := Relation(x, y).toInputSpec()
	if _, ok := relSpec.(query.RelationInput); !ok {
		t.Errorf("Relation should produce RelationInput, got %T", relSpec)
	}
}

// TestNotClause tests NOT clause
func TestNotClause(t *testing.T) {
	e := NewVar()
	PersonCity := Kw(":person/city")

	notClause := Not(Pat(e, PersonCity, V("NYC")))

	clause := notClause.toClause()
	nc, ok := clause.(*query.NotClause)
	if !ok {
		t.Fatalf("Expected *query.NotClause, got %T", clause)
	}
	if len(nc.Clauses) != 1 {
		t.Errorf("Expected 1 clause, got %d", len(nc.Clauses))
	}
}

// TestNotJoinClause tests NOT-JOIN clause
func TestNotJoinClause(t *testing.T) {
	e := NewVar()
	PersonStatus := Kw(":person/status")

	notJoin := NotJoin([]*Var{e}, Pat(e, PersonStatus, V("banned")))

	clause := notJoin.toClause()
	njc, ok := clause.(*query.NotJoinClause)
	if !ok {
		t.Fatalf("Expected *query.NotJoinClause, got %T", clause)
	}
	if len(njc.JoinVars) != 1 {
		t.Errorf("Expected 1 join var, got %d", len(njc.JoinVars))
	}
}

// TestOrClause tests OR clause
func TestOrClause(t *testing.T) {
	status := NewVar()

	orClause := Or(
		[]interface{}{Eq(status, V("active"))},
		[]interface{}{Eq(status, V("pending"))},
	)

	clause := orClause.toClause()
	oc, ok := clause.(*query.OrClause)
	if !ok {
		t.Fatalf("Expected *query.OrClause, got %T", clause)
	}
	if len(oc.Branches) != 2 {
		t.Errorf("Expected 2 branches, got %d", len(oc.Branches))
	}
}

// TestOrJoinClause tests OR-JOIN clause
func TestOrJoinClause(t *testing.T) {
	e := NewVar()
	name := NewVar()
	PersonNickname := Kw(":person/nickname")
	PersonName := Kw(":person/name")

	orJoin := OrJoin([]*Var{name},
		[]interface{}{Pat(e, PersonNickname, name)},
		[]interface{}{Pat(e, PersonName, name)},
	)

	clause := orJoin.toClause()
	ojc, ok := clause.(*query.OrJoinClause)
	if !ok {
		t.Fatalf("Expected *query.OrJoinClause, got %T", clause)
	}
	if len(ojc.JoinVars) != 1 {
		t.Errorf("Expected 1 join var, got %d", len(ojc.JoinVars))
	}
	if len(ojc.Branches) != 2 {
		t.Errorf("Expected 2 branches, got %d", len(ojc.Branches))
	}
}

// TestOrderSpecs tests ordering specifications
func TestOrderSpecs(t *testing.T) {
	name := NewVar()

	// Test Asc
	ascSpec := Asc(name)
	if ascSpec.direction != query.OrderAsc {
		t.Errorf("Expected OrderAsc, got %v", ascSpec.direction)
	}

	// Test Desc
	descSpec := Desc(name)
	if descSpec.direction != query.OrderDesc {
		t.Errorf("Expected OrderDesc, got %v", descSpec.direction)
	}
}

// TestQueryBuilder tests the main query builder
func TestQueryBuilder(t *testing.T) {
	e := NewVar()
	name := NewVar()
	age := NewVar()
	PersonName := Kw(":person/name")
	PersonAge := Kw(":person/age")

	q, err := Query().
		Find(name, age).
		Where(
			Pat(e, PersonName, name),
			Pat(e, PersonAge, age),
			Gt(age, V(21)),
		).
		OrderBy(Asc(name)).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Check find elements
	if len(q.Find) != 2 {
		t.Errorf("Expected 2 find elements, got %d", len(q.Find))
	}

	// Check where clauses
	if len(q.Where) != 3 {
		t.Errorf("Expected 3 where clauses, got %d", len(q.Where))
	}

	// Check order by
	if len(q.OrderBy) != 1 {
		t.Errorf("Expected 1 order by clause, got %d", len(q.OrderBy))
	}
}

// TestQueryBuilderWithInputs tests query builder with input parameters
func TestQueryBuilderWithInputs(t *testing.T) {
	e := NewVar()
	name := NewVar()
	age := NewVar()
	minAge := NewVar()
	PersonName := Kw(":person/name")
	PersonAge := Kw(":person/age")

	q, err := Query().
		Find(name, age).
		In(DB, Scalar(minAge)).
		Where(
			Pat(e, PersonName, name),
			Pat(e, PersonAge, age),
			Gte(age, minAge),
		).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Check inputs
	if len(q.In) != 2 {
		t.Errorf("Expected 2 input specs, got %d", len(q.In))
	}
}

// TestQueryBuilderWithAggregation tests query builder with aggregation
func TestQueryBuilderWithAggregation(t *testing.T) {
	e := NewVar()
	dept := NewVar()
	salary := NewVar()
	EmpDept := Kw(":employee/dept")
	EmpSalary := Kw(":employee/salary")

	q, err := Query().
		Find(dept, Sum(salary)).
		Where(
			Pat(e, EmpDept, dept),
			Pat(e, EmpSalary, salary),
		).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Check find elements include aggregation
	if len(q.Find) != 2 {
		t.Errorf("Expected 2 find elements, got %d", len(q.Find))
	}

	// Second element should be an aggregate
	if _, ok := q.Find[1].(query.FindAggregate); !ok {
		t.Errorf("Expected AggregateElement, got %T", q.Find[1])
	}
}

// TestQueryBuilderMustBuild tests MustBuild panics on error
func TestQueryBuilderMustBuild(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustBuild should panic on invalid query")
		}
	}()

	// Query without Find or Where should fail
	Query().MustBuild()
}

// TestQueryBuilderValidation tests build validation
func TestQueryBuilderValidation(t *testing.T) {
	// No find elements
	_, err := Query().Where(Eq(NewVar(), V(1))).Build()
	if err == nil {
		t.Error("Expected error for query without find elements")
	}

	// No where clauses
	_, err = Query().Find(NewVar()).Build()
	if err == nil {
		t.Error("Expected error for query without where clauses")
	}
}

// TestSubquery tests subquery builder
func TestSubquery(t *testing.T) {
	// Inner query: find max salary for a department
	dept := NewVar()
	maxSalary := NewVar()
	innerSalary := NewVar()
	innerE := NewVar()
	EmpDept := Kw(":employee/dept")
	EmpSalary := Kw(":employee/salary")

	innerQ := Query().
		Find(Max(innerSalary)).
		In(DB, Scalar(dept)).
		Where(
			Pat(innerE, EmpDept, dept),
			Pat(innerE, EmpSalary, innerSalary),
		)

	// Create subquery with binding
	sub := Subquery(innerQ, dept).BindTuple(maxSalary)

	clause := sub.toClause()
	sqp, ok := clause.(*query.SubqueryPattern)
	if !ok {
		t.Fatalf("Expected *query.SubqueryPattern, got %T", clause)
	}

	// Should have 1 input
	if len(sqp.Inputs) != 1 {
		t.Errorf("Expected 1 input, got %d", len(sqp.Inputs))
	}

	// Should have tuple binding
	if _, ok := sqp.Binding.(query.TupleBinding); !ok {
		t.Errorf("Expected TupleBinding, got %T", sqp.Binding)
	}
}

// TestSubqueryRelationBinding tests subquery with relation binding
func TestSubqueryRelationBinding(t *testing.T) {
	dept := NewVar()
	empName := NewVar()
	innerE := NewVar()
	EmpDept := Kw(":employee/dept")
	EmpName := Kw(":employee/name")

	innerQ := Query().
		Find(empName).
		In(DB, Scalar(dept)).
		Where(
			Pat(innerE, EmpDept, dept),
			Pat(innerE, EmpName, empName),
		)

	sub := Subquery(innerQ, dept).BindRelation(empName)

	clause := sub.toClause()
	sqp := clause.(*query.SubqueryPattern)

	if _, ok := sqp.Binding.(query.RelationBinding); !ok {
		t.Errorf("Expected RelationBinding, got %T", sqp.Binding)
	}
}

// TestSubqueryCollectionBinding tests subquery with collection binding
func TestSubqueryCollectionBinding(t *testing.T) {
	dept := NewVar()
	empName := NewVar()
	innerE := NewVar()
	EmpDept := Kw(":employee/dept")
	EmpName := Kw(":employee/name")

	innerQ := Query().
		Find(empName).
		In(DB, Scalar(dept)).
		Where(
			Pat(innerE, EmpDept, dept),
			Pat(innerE, EmpName, empName),
		)

	names := NewVar()
	sub := Subquery(innerQ, dept).BindCollection(names)

	clause := sub.toClause()
	sqp := clause.(*query.SubqueryPattern)

	if _, ok := sqp.Binding.(query.CollectionBinding); !ok {
		t.Errorf("Expected CollectionBinding, got %T", sqp.Binding)
	}
}

// TestVariableIdentityIsJoin tests that same variable creates join
func TestVariableIdentityIsJoin(t *testing.T) {
	// This is the key insight: using the same Go variable in multiple patterns
	// creates a join condition
	e := NewVar() // <-- This single variable
	name := NewVar()
	age := NewVar()
	PersonName := Kw(":person/name")
	PersonAge := Kw(":person/age")

	q, err := Query().
		Find(name, age).
		Where(
			Pat(e, PersonName, name), // e is used here
			Pat(e, PersonAge, age),   // and here - same variable = join!
		).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Both patterns should reference the same symbol
	dp1 := q.Where[0].(*query.DataPattern)
	dp2 := q.Where[1].(*query.DataPattern)

	// Get the entity variable from each pattern
	var1 := dp1.Elements[0].(query.Variable)
	var2 := dp2.Elements[0].(query.Variable)

	// They should have the same symbol name (the join condition)
	if var1.Name != var2.Name {
		t.Errorf("Variables should have same symbol for join: %s != %s", var1.Name, var2.Name)
	}
}

// TestPatternWithDatalogKeyword tests pattern with datalog.Keyword directly
func TestPatternWithDatalogKeyword(t *testing.T) {
	e := NewVar()
	name := NewVar()

	// Can use datalog.Keyword directly
	directKw := datalog.NewKeyword(":person/name")
	pat := Pat(e, directKw, name)

	clause := pat.toClause()
	dp := clause.(*query.DataPattern)

	// Should still work
	if len(dp.Elements) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(dp.Elements))
	}
}

// TestPatternWithDatalogIdentity tests pattern with datalog.Identity directly
func TestPatternWithDatalogIdentity(t *testing.T) {
	name := NewVar()
	PersonName := Kw(":person/name")

	// Can use datalog.Identity directly for entity
	entityID := datalog.NewIdentity("person-123")
	pat := Pat(entityID, PersonName, name)

	clause := pat.toClause()
	dp := clause.(*query.DataPattern)

	// First element should be a constant with the identity
	if _, ok := dp.Elements[0].(query.Constant); !ok {
		t.Errorf("Expected Constant, got %T", dp.Elements[0])
	}
}

// TestComplexQuery tests a realistic complex query
func TestComplexQuery(t *testing.T) {
	// Find employees who earn more than the average in their department
	e := NewVar()
	dept := NewVar()
	salary := NewVar()
	name := NewVar()
	avgSalary := NewVar()

	EmpName := Kw(":employee/name")
	EmpDept := Kw(":employee/dept")
	EmpSalary := Kw(":employee/salary")

	// Inner query to compute average salary per department
	innerE := NewVar()
	innerSalary := NewVar()
	innerDept := NewVar()

	innerQ := Query().
		Find(Avg(innerSalary)).
		In(DB, Scalar(innerDept)).
		Where(
			Pat(innerE, EmpDept, innerDept),
			Pat(innerE, EmpSalary, innerSalary),
		)

	// Main query
	q, err := Query().
		Find(name, dept, salary).
		Where(
			Pat(e, EmpName, name),
			Pat(e, EmpDept, dept),
			Pat(e, EmpSalary, salary),
			Subquery(innerQ, dept).BindTuple(avgSalary),
			Gt(salary, avgSalary),
		).
		OrderBy(Desc(salary)).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Should have 5 where clauses (3 patterns + 1 subquery + 1 predicate)
	if len(q.Where) != 5 {
		t.Errorf("Expected 5 where clauses, got %d", len(q.Where))
	}

	// Should have 3 find elements
	if len(q.Find) != 3 {
		t.Errorf("Expected 3 find elements, got %d", len(q.Find))
	}
}

// ========================================
// Comparison Binding Tests
// ========================================

// TestComparisonBindingAs tests all comparison operators with .As() binding
func TestComparisonBindingAs(t *testing.T) {
	a := NewVar()
	result := NewVar()

	tests := []struct {
		name string
		expr *Expression
		op   query.CompareOp
	}{
		{"Gt.As", Gt(a, V(0)).As(result), query.OpGT},
		{"Lt.As", Lt(a, V(100)).As(result), query.OpLT},
		{"Gte.As", Gte(a, V(21)).As(result), query.OpGTE},
		{"Lte.As", Lte(a, V(65)).As(result), query.OpLTE},
		{"Eq.As", Eq(a, V("active")).As(result), query.OpEQ},
		{"Ne.As", Ne(a, V("deleted")).As(result), query.OpNE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause := tt.expr.toClause()
			expr, ok := clause.(*query.Expression)
			if !ok {
				t.Fatalf("Expected *query.Expression, got %T", clause)
			}

			compFn, ok := expr.Function.(*query.ComparisonFunction)
			if !ok {
				t.Fatalf("Expected *query.ComparisonFunction, got %T", expr.Function)
			}

			if compFn.Comparison.Op != tt.op {
				t.Errorf("Expected op %v, got %v", tt.op, compFn.Comparison.Op)
			}

			// Check binding
			if binding, ok := expr.Binding.(query.Symbol); !ok || binding != result.Symbol() {
				t.Errorf("Expected binding %s, got %v", result.Symbol(), expr.Binding)
			}
		})
	}
}

// TestComparisonBindingTerms tests comparison binding with different term combinations
func TestComparisonBindingTerms(t *testing.T) {
	x := NewVar()
	y := NewVar()
	result := NewVar()

	// Var vs Constant
	expr1 := Gt(x, V(0)).As(result)
	clause1 := expr1.toClause().(*query.Expression)
	fn1 := clause1.Function.(*query.ComparisonFunction)
	if _, ok := fn1.Comparison.Left.(query.VariableTerm); !ok {
		t.Errorf("Expected VariableTerm for left, got %T", fn1.Comparison.Left)
	}
	if _, ok := fn1.Comparison.Right.(query.ConstantTerm); !ok {
		t.Errorf("Expected ConstantTerm for right, got %T", fn1.Comparison.Right)
	}

	// Constant vs Var
	expr2 := Gt(V(100), x).As(result)
	clause2 := expr2.toClause().(*query.Expression)
	fn2 := clause2.Function.(*query.ComparisonFunction)
	if _, ok := fn2.Comparison.Left.(query.ConstantTerm); !ok {
		t.Errorf("Expected ConstantTerm for left, got %T", fn2.Comparison.Left)
	}
	if _, ok := fn2.Comparison.Right.(query.VariableTerm); !ok {
		t.Errorf("Expected VariableTerm for right, got %T", fn2.Comparison.Right)
	}

	// Var vs Var
	expr3 := Gt(x, y).As(result)
	clause3 := expr3.toClause().(*query.Expression)
	fn3 := clause3.Function.(*query.ComparisonFunction)
	if _, ok := fn3.Comparison.Left.(query.VariableTerm); !ok {
		t.Errorf("Expected VariableTerm for left, got %T", fn3.Comparison.Left)
	}
	if _, ok := fn3.Comparison.Right.(query.VariableTerm); !ok {
		t.Errorf("Expected VariableTerm for right, got %T", fn3.Comparison.Right)
	}
}

// TestChainedComparisonBindingAs tests chained comparison with .As() binding
func TestChainedComparisonBindingAs(t *testing.T) {
	x := NewVar()
	result := NewVar()

	// Chained with OpLT
	chain := Chained(query.OpLT, V(0), x, V(100)).As(result)
	clause := chain.toClause()
	expr, ok := clause.(*query.Expression)
	if !ok {
		t.Fatalf("Expected *query.Expression, got %T", clause)
	}

	chainFn, ok := expr.Function.(*query.ChainedComparisonFunction)
	if !ok {
		t.Fatalf("Expected *query.ChainedComparisonFunction, got %T", expr.Function)
	}

	if chainFn.ChainedComparison.Op != query.OpLT {
		t.Errorf("Expected OpLT, got %v", chainFn.ChainedComparison.Op)
	}
	if len(chainFn.ChainedComparison.Terms) != 3 {
		t.Errorf("Expected 3 terms, got %d", len(chainFn.ChainedComparison.Terms))
	}
	if binding, ok := expr.Binding.(query.Symbol); !ok || binding != result.Symbol() {
		t.Errorf("Expected binding %s, got %v", result.Symbol(), expr.Binding)
	}
}

// TestRangeBindingAs tests Range convenience function with .As() binding
func TestRangeBindingAs(t *testing.T) {
	x := NewVar()
	inRange := NewVar()

	// Range: 0 < x < 100
	chain := Range(V(0), x, V(100)).As(inRange)
	clause := chain.toClause()
	expr := clause.(*query.Expression)
	chainFn := expr.Function.(*query.ChainedComparisonFunction)

	if chainFn.ChainedComparison.Op != query.OpLT {
		t.Errorf("Range should use OpLT, got %v", chainFn.ChainedComparison.Op)
	}
}

// TestRangeInclusiveBindingAs tests RangeInclusive convenience function with .As() binding
func TestRangeInclusiveBindingAs(t *testing.T) {
	rating := NewVar()
	valid := NewVar()

	// Range: 1 <= rating <= 5
	chain := RangeInclusive(V(1), rating, V(5)).As(valid)
	clause := chain.toClause()
	expr := clause.(*query.Expression)
	chainFn := expr.Function.(*query.ChainedComparisonFunction)

	if chainFn.ChainedComparison.Op != query.OpLTE {
		t.Errorf("RangeInclusive should use OpLTE, got %v", chainFn.ChainedComparison.Op)
	}
}

// TestComparisonDualUse tests that comparisons work both as predicate and expression
func TestComparisonDualUse(t *testing.T) {
	x := NewVar()
	flag := NewVar()

	// As predicate (filter)
	pred := Gt(x, V(0))
	predClause := pred.toClause()
	if _, ok := predClause.(*query.Comparison); !ok {
		t.Errorf("Without .As(), expected *query.Comparison, got %T", predClause)
	}

	// As expression (binding)
	expr := Gt(x, V(0)).As(flag)
	exprClause := expr.toClause()
	if _, ok := exprClause.(*query.Expression); !ok {
		t.Errorf("With .As(), expected *query.Expression, got %T", exprClause)
	}
}

// TestChainedComparisonDualUse tests that chained comparisons work both as predicate and expression
func TestChainedComparisonDualUse(t *testing.T) {
	x := NewVar()
	flag := NewVar()

	// As predicate (filter)
	pred := Range(V(0), x, V(100))
	predClause := pred.toClause()
	if _, ok := predClause.(*query.ChainedComparison); !ok {
		t.Errorf("Without .As(), expected *query.ChainedComparison, got %T", predClause)
	}

	// As expression (binding)
	expr := Range(V(0), x, V(100)).As(flag)
	exprClause := expr.toClause()
	if _, ok := exprClause.(*query.Expression); !ok {
		t.Errorf("With .As(), expected *query.Expression, got %T", exprClause)
	}
}

// ========================================
// Database Function Tests
// ========================================

// TestGetElse tests GetElse function builder
func TestGetElse(t *testing.T) {
	entity := NewVar()
	result := NewVar()
	PersonNickname := Kw(":person/nickname")

	// GetElse with string default
	expr := GetElse(entity, PersonNickname, "Anonymous").As(result)
	clause := expr.toClause()
	queryExpr, ok := clause.(*query.Expression)
	if !ok {
		t.Fatalf("Expected *query.Expression, got %T", clause)
	}

	getElse, ok := queryExpr.Function.(*query.GetElseFunction)
	if !ok {
		t.Fatalf("Expected *query.GetElseFunction, got %T", queryExpr.Function)
	}

	// Check entity is VariableTerm
	varTerm, ok := getElse.Entity.(query.VariableTerm)
	if !ok {
		t.Fatalf("Expected VariableTerm, got %T", getElse.Entity)
	}
	if varTerm.Symbol != entity.Symbol() {
		t.Errorf("Expected entity symbol %s, got %s", entity.Symbol(), varTerm.Symbol)
	}

	// Check attribute
	if getElse.Attr.String() != ":person/nickname" {
		t.Errorf("Expected attr :person/nickname, got %s", getElse.Attr.String())
	}

	// Check default
	if getElse.Default != "Anonymous" {
		t.Errorf("Expected default 'Anonymous', got %v", getElse.Default)
	}

	// Check binding
	if queryExpr.Binding != result.Symbol() {
		t.Errorf("Expected binding %s, got %s", result.Symbol(), queryExpr.Binding)
	}
}

// TestGetElseWithDifferentDefaults tests GetElse with various default value types
func TestGetElseWithDifferentDefaults(t *testing.T) {
	entity := NewVar()
	result := NewVar()
	attr := Kw(":test/attr")

	tests := []struct {
		name        string
		defaultVal  interface{}
		checkResult func(interface{}) bool
	}{
		{"string", "default", func(v interface{}) bool { return v == "default" }},
		{"int", int64(0), func(v interface{}) bool { return v == int64(0) }},
		{"float", 3.14, func(v interface{}) bool { return v == 3.14 }},
		{"bool", false, func(v interface{}) bool { return v == false }},
		{"nil", nil, func(v interface{}) bool { return v == nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := GetElse(entity, attr, tt.defaultVal).As(result)
			clause := expr.toClause().(*query.Expression)
			getElse := clause.Function.(*query.GetElseFunction)
			if !tt.checkResult(getElse.Default) {
				t.Errorf("Default value mismatch, got %v", getElse.Default)
			}
		})
	}
}

// TestMissingAsPredicate tests Missing as a filter predicate (without .As())
func TestMissingAsPredicate(t *testing.T) {
	entity := NewVar()
	PersonEmail := Kw(":person/email")

	missing := Missing(entity, PersonEmail)
	clause := missing.toClause()

	dbPred, ok := clause.(*query.DatabaseFunctionPredicate)
	if !ok {
		t.Fatalf("Expected *query.DatabaseFunctionPredicate, got %T", clause)
	}

	missingFn, ok := dbPred.Function.(*query.MissingFunction)
	if !ok {
		t.Fatalf("Expected *query.MissingFunction, got %T", dbPred.Function)
	}

	// Check entity
	varTerm := missingFn.Entity.(query.VariableTerm)
	if varTerm.Symbol != entity.Symbol() {
		t.Errorf("Expected entity %s, got %s", entity.Symbol(), varTerm.Symbol)
	}

	// Check attribute
	if missingFn.Attr.String() != ":person/email" {
		t.Errorf("Expected attr :person/email, got %s", missingFn.Attr.String())
	}
}

// TestMissingAsExpression tests Missing as an expression (with .As())
func TestMissingAsExpression(t *testing.T) {
	entity := NewVar()
	needsEmail := NewVar()
	PersonEmail := Kw(":person/email")

	missing := Missing(entity, PersonEmail).As(needsEmail)
	clause := missing.toClause()

	queryExpr, ok := clause.(*query.Expression)
	if !ok {
		t.Fatalf("Expected *query.Expression, got %T", clause)
	}

	missingFn, ok := queryExpr.Function.(*query.MissingFunction)
	if !ok {
		t.Fatalf("Expected *query.MissingFunction, got %T", queryExpr.Function)
	}

	// Check binding
	if queryExpr.Binding != needsEmail.Symbol() {
		t.Errorf("Expected binding %s, got %s", needsEmail.Symbol(), queryExpr.Binding)
	}

	// Check attribute
	if missingFn.Attr.String() != ":person/email" {
		t.Errorf("Expected attr :person/email, got %s", missingFn.Attr.String())
	}
}

// TestGetSome tests GetSome function builder
func TestGetSome(t *testing.T) {
	entity := NewVar()
	displayName := NewVar()
	PersonNickname := Kw(":person/nickname")
	PersonFullName := Kw(":person/fullname")
	PersonEmail := Kw(":person/email")

	getSome := GetSome(entity, PersonNickname, PersonFullName, PersonEmail).As(displayName)
	clause := getSome.toClause()

	queryExpr, ok := clause.(*query.Expression)
	if !ok {
		t.Fatalf("Expected *query.Expression, got %T", clause)
	}

	getSomeFn, ok := queryExpr.Function.(*query.GetSomeFunction)
	if !ok {
		t.Fatalf("Expected *query.GetSomeFunction, got %T", queryExpr.Function)
	}

	// Check entity
	varTerm := getSomeFn.Entity.(query.VariableTerm)
	if varTerm.Symbol != entity.Symbol() {
		t.Errorf("Expected entity %s, got %s", entity.Symbol(), varTerm.Symbol)
	}

	// Check attrs list
	if len(getSomeFn.Attrs) != 3 {
		t.Errorf("Expected 3 attrs, got %d", len(getSomeFn.Attrs))
	}
	if getSomeFn.Attrs[0].String() != ":person/nickname" {
		t.Errorf("Expected first attr :person/nickname, got %s", getSomeFn.Attrs[0].String())
	}
	if getSomeFn.Attrs[1].String() != ":person/fullname" {
		t.Errorf("Expected second attr :person/fullname, got %s", getSomeFn.Attrs[1].String())
	}
	if getSomeFn.Attrs[2].String() != ":person/email" {
		t.Errorf("Expected third attr :person/email, got %s", getSomeFn.Attrs[2].String())
	}

	// Check binding
	if queryExpr.Binding != displayName.Symbol() {
		t.Errorf("Expected binding %s, got %s", displayName.Symbol(), queryExpr.Binding)
	}
}

// TestGetSomeWithTwoAttrs tests GetSome with minimum required attributes
func TestGetSomeWithTwoAttrs(t *testing.T) {
	entity := NewVar()
	result := NewVar()
	AttrA := Kw(":attr/a")
	AttrB := Kw(":attr/b")

	getSome := GetSome(entity, AttrA, AttrB).As(result)
	clause := getSome.toClause().(*query.Expression)
	getSomeFn := clause.Function.(*query.GetSomeFunction)

	if len(getSomeFn.Attrs) != 2 {
		t.Errorf("Expected 2 attrs, got %d", len(getSomeFn.Attrs))
	}
}

// TestDatabaseFunctionInQuery tests using database functions in a complete query
func TestDatabaseFunctionInQuery(t *testing.T) {
	e := NewVar()
	name := NewVar()
	nickname := NewVar()
	PersonName := Kw(":person/name")
	PersonNickname := Kw(":person/nickname")

	q, err := Query().
		Find(name, nickname).
		Where(
			Pat(e, PersonName, name),
			GetElse(e, PersonNickname, "Anonymous").As(nickname),
		).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Should have 2 where clauses
	if len(q.Where) != 2 {
		t.Errorf("Expected 2 where clauses, got %d", len(q.Where))
	}

	// Second clause should be an expression with GetElseFunction
	expr, ok := q.Where[1].(*query.Expression)
	if !ok {
		t.Fatalf("Expected *query.Expression, got %T", q.Where[1])
	}
	if _, ok := expr.Function.(*query.GetElseFunction); !ok {
		t.Errorf("Expected GetElseFunction, got %T", expr.Function)
	}
}

// TestMissingPredicateInQuery tests using Missing as a predicate in a query
func TestMissingPredicateInQuery(t *testing.T) {
	e := NewVar()
	name := NewVar()
	PersonName := Kw(":person/name")
	PersonEmail := Kw(":person/email")

	q, err := Query().
		Find(name).
		Where(
			Pat(e, PersonName, name),
			Missing(e, PersonEmail),
		).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Should have 2 where clauses
	if len(q.Where) != 2 {
		t.Errorf("Expected 2 where clauses, got %d", len(q.Where))
	}

	// Second clause should be DatabaseFunctionPredicate
	if _, ok := q.Where[1].(*query.DatabaseFunctionPredicate); !ok {
		t.Errorf("Expected *query.DatabaseFunctionPredicate, got %T", q.Where[1])
	}
}

// TestComparisonBindingInQuery tests using comparison binding in a complete query
func TestComparisonBindingInQuery(t *testing.T) {
	e := NewVar()
	count := NewVar()
	hasItems := NewVar()
	ItemCount := Kw(":item/count")

	q, err := Query().
		Find(count, hasItems).
		Where(
			Pat(e, ItemCount, count),
			Gt(count, V(0)).As(hasItems),
		).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Should have 2 where clauses
	if len(q.Where) != 2 {
		t.Errorf("Expected 2 where clauses, got %d", len(q.Where))
	}

	// Second clause should be an expression with ComparisonFunction
	expr, ok := q.Where[1].(*query.Expression)
	if !ok {
		t.Fatalf("Expected *query.Expression, got %T", q.Where[1])
	}
	if _, ok := expr.Function.(*query.ComparisonFunction); !ok {
		t.Errorf("Expected ComparisonFunction, got %T", expr.Function)
	}
}
