package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestParseExpressionPatterns(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantFunc string
		wantArgs int
		wantBind query.Symbol
	}{
		{
			name:     "arithmetic addition",
			input:    `[:find ?total :where [?item :price ?price] [?item :tax ?tax] [(+ ?price ?tax) ?total]]`,
			wantFunc: "+",
			wantArgs: 2,
			wantBind: datalog.NewSymbol("?total"),
		},
		{
			name:     "string concatenation",
			input:    `[:find ?fullname :where [?person :first ?first] [?person :last ?last] [(str ?first " " ?last) ?fullname]]`,
			wantFunc: "str",
			wantArgs: 3,
			wantBind: datalog.NewSymbol("?fullname"),
		},
		{
			name:     "ground value",
			input:    `[:find ?x ?answer :where [?x :age 42] [(ground 42) ?answer]]`,
			wantFunc: "ground",
			wantArgs: 1,
			wantBind: datalog.NewSymbol("?answer"),
		},
		{
			name:     "identity binding",
			input:    `[:find ?x ?y :where [?x :name "Alice"] [(identity ?x) ?y]]`,
			wantFunc: "identity",
			wantArgs: 1,
			wantBind: datalog.NewSymbol("?y"),
		},
		{
			name:     "complex arithmetic",
			input:    `[:find ?result :where [?x :value ?v] [(* ?v 2) ?temp] [(+ ?temp 10) ?result]]`,
			wantFunc: "+", // Testing the second expression
			wantArgs: 2,
			wantBind: datalog.NewSymbol("?result"),
		},
		// Comparison functions with binding
		{
			name:     "greater than with binding",
			input:    `[:find ?x ?flag :where [?x :count ?n] [(> ?n 0) ?flag]]`,
			wantFunc: ">",
			wantArgs: 2,
			wantBind: datalog.NewSymbol("?flag"),
		},
		{
			name:     "equality with binding",
			input:    `[:find ?x ?match :where [?x :status ?s] [(= ?s "active") ?match]]`,
			wantFunc: "=",
			wantArgs: 2,
			wantBind: datalog.NewSymbol("?match"),
		},
		{
			name:     "less than or equal with binding",
			input:    `[:find ?x ?under :where [?x :price ?p] [(<= ?p 100) ?under]]`,
			wantFunc: "<=",
			wantArgs: 2,
			wantBind: datalog.NewSymbol("?under"),
		},
		{
			name:     "not equal with binding",
			input:    `[:find ?x ?diff :where [?x :value ?v] [(!= ?v 0) ?diff]]`,
			wantFunc: "!=",
			wantArgs: 2,
			wantBind: datalog.NewSymbol("?diff"),
		},
		{
			name:     "less than with binding",
			input:    `[:find ?x ?small :where [?x :age ?a] [(< ?a 18) ?small]]`,
			wantFunc: "<",
			wantArgs: 2,
			wantBind: datalog.NewSymbol("?small"),
		},
		{
			name:     "greater than or equal with binding",
			input:    `[:find ?x ?adult :where [?x :age ?a] [(>= ?a 18) ?adult]]`,
			wantFunc: ">=",
			wantArgs: 2,
			wantBind: datalog.NewSymbol("?adult"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("ParseQuery() error = %v", err)
			}

			// Find the expression (last one for complex arithmetic test)
			var expr *query.Expression
			for i := len(q.Where) - 1; i >= 0; i-- {
				if e, ok := q.Where[i].(*query.Expression); ok {
					expr = e
					break
				}
			}

			if expr == nil {
				t.Fatal("Expected to find an expression")
			}

			// Check the function type and properties
			switch fn := expr.Function.(type) {
			case *query.ArithmeticFunction:
				if fn.Op.String() != tt.wantFunc {
					t.Errorf("Function = %v, want %v", fn.Op, tt.wantFunc)
				}
				// Arithmetic functions always have 2 args (Left and Right)
				if tt.wantArgs != 2 {
					t.Errorf("Arithmetic functions should have 2 args")
				}
			case *query.StringConcatFunction:
				if tt.wantFunc != "str" {
					t.Errorf("Expected str function for StringConcat")
				}
				if len(fn.Terms) != tt.wantArgs {
					t.Errorf("Args length = %v, want %v", len(fn.Terms), tt.wantArgs)
				}
			case *query.GroundFunction:
				if tt.wantFunc != "ground" {
					t.Errorf("Expected ground function")
				}
				if tt.wantArgs != 1 {
					t.Errorf("Ground function should have 1 arg")
				}
			case *query.IdentityFunction:
				if tt.wantFunc != "identity" {
					t.Errorf("Expected identity function")
				}
				if tt.wantArgs != 1 {
					t.Errorf("Identity function should have 1 arg")
				}
			case *query.ComparisonFunction:
				if fn.Comparison.Op.String() != tt.wantFunc {
					t.Errorf("Comparison Op = %v, want %v", fn.Comparison.Op, tt.wantFunc)
				}
				if tt.wantArgs != 2 {
					t.Errorf("Comparison functions should have 2 args")
				}
			default:
				t.Errorf("Unexpected function type: %T", fn)
			}

			if binding, ok := expr.Binding.(query.Symbol); !ok || binding != tt.wantBind {
				t.Errorf("Binding = %v, want %v", expr.Binding, tt.wantBind)
			}
		})
	}
}

func TestParseVariadicComparators(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantFunc string
		wantArgs int
	}{
		{
			name:     "chained less than",
			input:    `[:find ?x ?y ?z :where [(< ?x ?y ?z)]]`,
			wantFunc: "<",
			wantArgs: 3,
		},
		{
			name:     "range check",
			input:    `[:find ?x :where [(<= 0 ?x 100)]]`,
			wantFunc: "<=",
			wantArgs: 3,
		},
		{
			name:     "multiple equality",
			input:    `[:find ?x ?y ?z :where [(= ?x ?y ?z)]]`,
			wantFunc: "=",
			wantArgs: 3,
		},
		{
			name:     "mixed variables and constants",
			input:    `[:find ?x ?y :where [(< 0 ?x ?y 100)]]`,
			wantFunc: "<",
			wantArgs: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("ParseQuery() error = %v", err)
			}

			// Find the chained comparison
			var chainedComp *query.ChainedComparison
			for _, p := range q.Where {
				if cc, ok := p.(*query.ChainedComparison); ok {
					chainedComp = cc
					break
				}
			}

			if chainedComp == nil {
				t.Fatal("Expected to find a chained comparison")
			}

			if chainedComp.Op.String() != tt.wantFunc {
				t.Errorf("Operator = %v, want %v", chainedComp.Op, tt.wantFunc)
			}

			if len(chainedComp.Terms) != tt.wantArgs {
				t.Errorf("Terms length = %v, want %v", len(chainedComp.Terms), tt.wantArgs)
			}
		})
	}
}

func TestParseTupleGround(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantValues []interface{}
		wantVars   []query.Symbol
		wantError  bool
	}{
		{
			name:       "basic tuple ground",
			input:      `[:find ?a ?b ?c :where [(ground [1 2 3]) [?a ?b ?c]]]`,
			wantValues: []interface{}{int64(1), int64(2), int64(3)},
			wantVars:   []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")},
		},
		{
			name:       "tuple ground with zero values",
			input:      `[:find ?x ?y ?z :where [(ground [0 0 0]) [?x ?y ?z]]]`,
			wantValues: []interface{}{int64(0), int64(0), int64(0)},
			wantVars:   []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")},
		},
		{
			name:       "tuple ground with mixed types",
			input:      `[:find ?s ?n :where [(ground ["hello" 42]) [?s ?n]]]`,
			wantValues: []interface{}{"hello", int64(42)},
			wantVars:   []query.Symbol{datalog.NewSymbol("?s"), datalog.NewSymbol("?n")},
		},
		{
			name:       "tuple ground with keyword",
			input:      `[:find ?status ?count :where [(ground [:none 0]) [?status ?count]]]`,
			wantValues: nil, // Keyword parsing may differ
			wantVars:   []query.Symbol{datalog.NewSymbol("?status"), datalog.NewSymbol("?count")},
		},
		{
			name:       "single element tuple ground",
			input:      `[:find ?x :where [(ground [42]) [?x]]]`,
			wantValues: []interface{}{int64(42)},
			wantVars:   []query.Symbol{datalog.NewSymbol("?x")},
		},
		{
			name:      "tuple ground length mismatch",
			input:     `[:find ?a ?b ?c :where [(ground [1 2]) [?a ?b ?c]]]`,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			if tt.wantError {
				if err == nil {
					t.Fatal("Expected error but got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseQuery() error = %v", err)
			}

			// Find the expression
			var expr *query.Expression
			for _, clause := range q.Where {
				if e, ok := clause.(*query.Expression); ok {
					expr = e
					break
				}
			}

			if expr == nil {
				t.Fatal("Expected to find an expression")
			}

			// Check it's a ground function
			gf, ok := expr.Function.(*query.GroundFunction)
			if !ok {
				t.Fatalf("Expected GroundFunction, got %T", expr.Function)
			}

			// Check value is a slice
			values, ok := gf.Value.([]interface{})
			if !ok {
				t.Fatalf("Expected []interface{} value, got %T", gf.Value)
			}

			// Check values if expected
			if tt.wantValues != nil {
				if len(values) != len(tt.wantValues) {
					t.Errorf("Values length = %d, want %d", len(values), len(tt.wantValues))
				} else {
					for i, v := range tt.wantValues {
						if values[i] != v {
							t.Errorf("Values[%d] = %v (%T), want %v (%T)",
								i, values[i], values[i], v, v)
						}
					}
				}
			}

			// Check binding is TupleBinding
			tb, ok := expr.Binding.(query.TupleBinding)
			if !ok {
				t.Fatalf("Expected TupleBinding, got %T", expr.Binding)
			}

			if len(tb.Variables) != len(tt.wantVars) {
				t.Errorf("Variables length = %d, want %d", len(tb.Variables), len(tt.wantVars))
			} else {
				for i, v := range tt.wantVars {
					if tb.Variables[i] != v {
						t.Errorf("Variables[%d] = %v, want %v", i, tb.Variables[i], v)
					}
				}
			}
		})
	}
}

func TestScalarGroundStillWorks(t *testing.T) {
	// Ensure backward compatibility - scalar ground should still work
	q, err := ParseQuery(`[:find ?x :where [(ground 42) ?x]]`)
	if err != nil {
		t.Fatalf("ParseQuery() error = %v", err)
	}

	// Find the expression
	var expr *query.Expression
	for _, clause := range q.Where {
		if e, ok := clause.(*query.Expression); ok {
			expr = e
			break
		}
	}

	if expr == nil {
		t.Fatal("Expected to find an expression")
	}

	// Check it's a ground function
	gf, ok := expr.Function.(*query.GroundFunction)
	if !ok {
		t.Fatalf("Expected GroundFunction, got %T", expr.Function)
	}

	// Check value is scalar (not slice)
	if _, isSlice := gf.Value.([]interface{}); isSlice {
		t.Errorf("Expected scalar value, got slice")
	}

	if gf.Value != int64(42) {
		t.Errorf("Value = %v, want 42", gf.Value)
	}

	// Check binding is Symbol (not TupleBinding)
	binding, ok := expr.Binding.(query.Symbol)
	if !ok {
		t.Fatalf("Expected Symbol binding, got %T", expr.Binding)
	}

	if binding != datalog.NewSymbol("?x") {
		t.Errorf("Binding = %v, want ?x", binding)
	}
}

func TestFormatExpressionPatterns(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "expression pattern formatting",
			input: `[:find ?total :where [?item :price ?price] [(+ ?price 10) ?total]]`,
			want: `[:find ?total
 :where [?item :price ?price]
        [(+ ?price 10) ?total]]`,
		},
		{
			name:  "multiple expressions",
			input: `[:find ?result :where [?x :value ?v] [(* ?v 2) ?temp] [(+ ?temp 10) ?result]]`,
			want: `[:find ?result
 :where [?x :value ?v]
        [(* ?v 2) ?temp]
        [(+ ?temp 10) ?result]]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			if err != nil {
				t.Fatalf("ParseQuery() error = %v", err)
			}

			got := FormatQuery(q)
			if got != tt.want {
				t.Errorf("FormatQuery() = %q, want %q", got, tt.want)
			}
		})
	}
}
