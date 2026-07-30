package qb

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The value domain admits int64, not int/int8/int16/int32: integer widths
// normalize to int64 at every engine boundary. The EDN parser produces int64
// natively; the query builder is the same boundary for Go callers, so every
// raw value it embeds in a query AST must come out int64. A Go int that
// survives into execution diverges from stored int64 data — joins and
// comparisons may still normalize internally, but aggregation folds skip
// non-int64 values, which is how a qb-built get-else default of 0 became a
// nil sum (BUG_BASELINE_ORDEFAULT_SUBQUERY_NIL_AGGREGATE.md).
func TestRawConstantsNormalizeToInt64(t *testing.T) {
	e := NewVar("e")
	x := NewVar("x")
	attr := Kw(":thing/count")

	requireInt64 := func(t *testing.T, label string, v interface{}, want int64) {
		t.Helper()
		got, ok := v.(int64)
		if !ok {
			t.Fatalf("%s: value is %T (%v), want int64", label, v, v)
		}
		if got != want {
			t.Fatalf("%s: value is %d, want %d", label, got, want)
		}
	}

	t.Run("pattern constant", func(t *testing.T) {
		p := Pat(e, attr, 7).toClause().(*query.DataPattern)
		c, ok := p.Elements[2].(query.Constant)
		if !ok {
			t.Fatalf("pattern value element is %T, want query.Constant", p.Elements[2])
		}
		requireInt64(t, "Pat value", c.Value, 7)
	})

	t.Run("comparison operand", func(t *testing.T) {
		cmp := Gt(x, 0).toClause().(*query.Comparison)
		term, ok := cmp.Right.(query.ConstantTerm)
		if !ok {
			t.Fatalf("comparison right is %T, want query.ConstantTerm", cmp.Right)
		}
		requireInt64(t, "Gt operand", term.Value, 0)
	})

	t.Run("chained comparison operand", func(t *testing.T) {
		chained := Range(0, x, 100).toClause().(*query.ChainedComparison)
		term, ok := chained.Terms[0].(query.ConstantTerm)
		if !ok {
			t.Fatalf("chained term is %T, want query.ConstantTerm", chained.Terms[0])
		}
		requireInt64(t, "Range min", term.Value, 0)
	})

	t.Run("arithmetic operand", func(t *testing.T) {
		expr := Add(x, 1).As(NewVar("sum")).toClause().(*query.Expression)
		fn, ok := expr.Function.(query.ArithmeticFunction)
		if !ok {
			t.Fatalf("function is %T, want query.ArithmeticFunction", expr.Function)
		}
		term, ok := fn.Args[1].(query.ConstantTerm)
		if !ok {
			t.Fatalf("arith arg is %T, want query.ConstantTerm", fn.Args[1])
		}
		requireInt64(t, "Add operand", term.Value, 1)
	})

	t.Run("ground scalar", func(t *testing.T) {
		expr := Ground(42).As(x).toClause().(*query.Expression)
		fn, ok := expr.Function.(*query.GroundFunction)
		if !ok {
			t.Fatalf("function is %T, want *query.GroundFunction", expr.Function)
		}
		requireInt64(t, "Ground value", fn.Value, 42)
	})

	t.Run("ground vector elements", func(t *testing.T) {
		expr := Ground([]interface{}{1, int32(2)}).As(x).toClause().(*query.Expression)
		fn := expr.Function.(*query.GroundFunction)
		elems, ok := fn.Value.([]interface{})
		if !ok {
			t.Fatalf("ground vector is %T, want []interface{}", fn.Value)
		}
		requireInt64(t, "Ground vector element 0", elems[0], 1)
		requireInt64(t, "Ground vector element 1", elems[1], 2)
	})

	t.Run("tuple ground elements", func(t *testing.T) {
		a, b := NewVar("a"), NewVar("b")
		expr := TupleGround(0, int16(5)).As(a, b).toClause().(*query.Expression)
		fn := expr.Function.(*query.GroundFunction)
		elems, ok := fn.Value.([]interface{})
		if !ok {
			t.Fatalf("tuple ground value is %T, want []interface{}", fn.Value)
		}
		requireInt64(t, "TupleGround element 0", elems[0], 0)
		requireInt64(t, "TupleGround element 1", elems[1], 5)
	})

	t.Run("get-else default", func(t *testing.T) {
		expr := GetElse(e, attr, 0).As(x).toClause().(*query.Expression)
		fn, ok := expr.Function.(*query.GetElseFunction)
		if !ok {
			t.Fatalf("function is %T, want *query.GetElseFunction", expr.Function)
		}
		requireInt64(t, "GetElse default", fn.Default, 0)
	})

	t.Run("val wrapper", func(t *testing.T) {
		requireInt64(t, "V value", V(8).Value(), 8)
	})

	t.Run("pull default", func(t *testing.T) {
		d := Default(attr, 0)
		requireInt64(t, "pull Default value", d.defaultValue, 0)
	})

	t.Run("non-integers pass through", func(t *testing.T) {
		expr := Ground("s").As(x).toClause().(*query.Expression)
		fn := expr.Function.(*query.GroundFunction)
		if got, ok := fn.Value.(string); !ok || got != "s" {
			t.Fatalf("Ground string changed to %T (%v)", fn.Value, fn.Value)
		}
		p := Pat(e, attr, 1.5).toClause().(*query.DataPattern)
		if got, ok := p.Elements[2].(query.Constant).Value.(float64); !ok || got != 1.5 {
			t.Fatalf("Pat float changed to %T", p.Elements[2].(query.Constant).Value)
		}
		if datalog.NormalizeValue(int64(9)) != int64(9) {
			t.Fatal("int64 must pass through NormalizeValue unchanged")
		}
	})
}
