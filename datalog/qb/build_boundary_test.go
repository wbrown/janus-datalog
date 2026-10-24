package qb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Build runs every static check the text admits over every inner query as
// over the outer query, and an inner query's refusal is Build's error, never
// a panic inside the fluent chain, so a builder-built query is accepted or
// refused as its rendering is at ParseQuery and at Prepare, with one message
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md goal 5 and §6).

var (
	pageNext = Kw(":page/next")
	pageKind = Kw(":page/kind")
)

func TestBuild_ReturnsAnInnerQuerysRefusal(t *testing.T) {
	t.Run("an inner invocation under an inner :in declaring no % is the rules-input refusal", func(t *testing.T) {
		x, y := NewVar("x"), NewVar("y")
		inner := Query().Find(y).In(DB, Scalar(x)).Where(Rule("follows", x, y))
		var outer *QueryBuilder
		require.NotPanics(t, func() {
			outer = Query().Find(y).In(DB, Rules, Scalar(x)).Where(Subquery(inner, x).BindCollection(y))
		})
		_, err := outer.Build()
		require.ErrorContains(t, err, "rule invocation (follows ?x ?y) requires % in :in")
	})

	t.Run("an inner not-join reading outside its header is the header refusal", func(t *testing.T) {
		x, y, e := NewVar("x"), NewVar("y"), NewVar("e")
		inner := Query().Find(y).In(DB, Scalar(x)).Where(Pat(x, pageNext, y), NotJoin([]*Var{x}, Gt(e, 1)))
		var outer *QueryBuilder
		require.NotPanics(t, func() {
			outer = Query().Find(y).In(DB, Scalar(x)).Where(Subquery(inner, x).BindCollection(y))
		})
		_, err := outer.Build()
		require.ErrorContains(t, err, "?e", "the refusal names the variable the body reads outside its header")
	})

	t.Run("an inner query declaring % once and invoking a rule builds", func(t *testing.T) {
		x, y := NewVar("x"), NewVar("y")
		inner := Query().Find(y).In(DB, Rules, Scalar(x)).Where(Rule("follows", x, y))
		_, err := Query().Find(y).In(DB, Rules, Scalar(x)).Where(Subquery(inner, x).BindCollection(y)).Build()
		require.NoError(t, err)
	})
}

// A branch of every form holds a subquery as it holds any clause, and an
// argument that is no clause is Build's error naming the form, the branch
// and the argument, never a panic inside the fluent chain.
func TestBuild_ReturnsABranchArgumentsRefusal(t *testing.T) {
	x, y, e := NewVar("x"), NewVar("y"), NewVar("e")
	inner := func() *QueryBuilder {
		return Query().Find(y).In(DB, Scalar(x)).Where(Pat(x, pageNext, y))
	}
	sub := func() *SubqueryBuilder { return Subquery(inner(), x).BindCollection(y) }

	forms := []struct {
		name string
		form interface{}
	}{
		{"or", Or().Branch(sub()).Branch(Pat(x, pageKind, y))},
		{"or-join", OrJoin(y).Require(x).Branch(sub()).Branch(Pat(x, pageKind, y))},
		{"or-default", OrDefault().Branch(sub()).Branch(Ground(int64(0)).As(y))},
		{"or-default-join", OrDefaultJoin(y).Required(x).Branch(sub()).Branch(Ground(int64(0)).As(y))},
		{"not", Not(sub())},
		{"not-join", NotJoin([]*Var{x}, sub())},
	}
	for _, tc := range forms {
		t.Run(tc.name+" holds a subquery", func(t *testing.T) {
			var b *QueryBuilder
			require.NotPanics(t, func() {
				b = Query().Find(e).Where(Pat(e, pageKind, x), tc.form)
			})
			_, err := b.Build()
			require.NoError(t, err)
		})
	}

	t.Run("an argument that is no clause is Build's error", func(t *testing.T) {
		var b *QueryBuilder
		require.NotPanics(t, func() {
			b = Query().Find(e).Where(Pat(e, pageKind, x), Or().Branch(Pat(x, pageNext, y)).Branch("not a clause"))
		})
		_, err := b.Build()
		require.ErrorContains(t, err, "Or: branch 2 argument 1: cannot convert string to Clause")
	})
}

func TestBuild_RefusesAnInnerLimitAndAnInnerPull(t *testing.T) {
	t.Run("an inner :limit is refused", func(t *testing.T) {
		x, y := NewVar("x"), NewVar("y")
		inner := Query().Find(y).In(DB, Scalar(x)).Where(Pat(x, pageNext, y)).Limit(1)
		_, err := Query().Find(y).In(DB, Scalar(x)).Where(Subquery(inner, x).BindCollection(y)).Build()
		require.ErrorContains(t, err, ":limit is not supported inside a subquery")
	})

	t.Run("an inner pull is refused", func(t *testing.T) {
		x, y := NewVar("x"), NewVar("y")
		inner := Query().Find(Pull(y, pageKind)).In(DB, Scalar(x)).Where(Pat(x, pageNext, y))
		_, err := Query().Find(y).In(DB, Scalar(x)).Where(Subquery(inner, x).BindCollection(y)).Build()
		require.ErrorContains(t, err, "(pull ...) is not supported inside a subquery find")
	})

	t.Run("an inner query with neither builds", func(t *testing.T) {
		x, y := NewVar("x"), NewVar("y")
		inner := Query().Find(y).In(DB, Scalar(x)).Where(Pat(x, pageNext, y))
		_, err := Query().Find(y).In(DB, Scalar(x)).Where(Subquery(inner, x).BindCollection(y)).Build()
		require.NoError(t, err)
	})
}

func TestBuild_RunsTheOrderByRule(t *testing.T) {
	t.Run("a sort key nothing binds is refused", func(t *testing.T) {
		e, k, z := NewVar("e"), NewVar("k"), NewVar("z")
		_, err := Query().Find(e).Where(Pat(e, pageKind, k)).OrderBy(Asc(z)).Build()
		require.ErrorContains(t, err, "order-by variable ?z is not bound in the query")
	})

	t.Run("a sort key under an aggregate that is no group key is refused", func(t *testing.T) {
		e, k := NewVar("e"), NewVar("k")
		_, err := Query().Find(Count(e)).Where(Pat(e, pageKind, k)).OrderBy(Asc(k)).Build()
		require.ErrorContains(t, err, "order-by variable ?k is not a group key")
	})

	t.Run("a sort key the :where binds builds", func(t *testing.T) {
		e, k := NewVar("e"), NewVar("k")
		_, err := Query().Find(e).Where(Pat(e, pageKind, k)).OrderBy(Asc(k)).Build()
		require.NoError(t, err)
	})
}
