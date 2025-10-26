package parser

import (
	"testing"

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
			wantBind: "?total",
		},
		{
			name:     "string concatenation",
			input:    `[:find ?fullname :where [?person :first ?first] [?person :last ?last] [(str ?first " " ?last) ?fullname]]`,
			wantFunc: "str",
			wantArgs: 3,
			wantBind: "?fullname",
		},
		{
			name:     "ground value",
			input:    `[:find ?x ?answer :where [?x :age 42] [(ground 42) ?answer]]`,
			wantFunc: "ground",
			wantArgs: 1,
			wantBind: "?answer",
		},
		{
			name:     "identity binding",
			input:    `[:find ?x ?y :where [?x :name "Alice"] [(identity ?x) ?y]]`,
			wantFunc: "identity",
			wantArgs: 1,
			wantBind: "?y",
		},
		{
			name:     "complex arithmetic",
			input:    `[:find ?result :where [?x :value ?v] [(* ?v 2) ?temp] [(+ ?temp 10) ?result]]`,
			wantFunc: "+", // Testing the second expression
			wantArgs: 2,
			wantBind: "?result",
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
				if string(fn.Op) != tt.wantFunc {
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
			default:
				t.Errorf("Unexpected function type: %T", fn)
			}

			if expr.Binding != tt.wantBind {
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

			if string(chainedComp.Op) != tt.wantFunc {
				t.Errorf("Operator = %v, want %v", chainedComp.Op, tt.wantFunc)
			}

			if len(chainedComp.Terms) != tt.wantArgs {
				t.Errorf("Terms length = %v, want %v", len(chainedComp.Terms), tt.wantArgs)
			}
		})
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
