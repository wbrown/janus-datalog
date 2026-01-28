package qb

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestSource(t *testing.T) {
	users := Source("$users")
	if users.Symbol() != query.Symbol("$users") {
		t.Errorf("expected Symbol $users, got %s", users.Symbol())
	}

	spec := users.toInputSpec()
	db, ok := spec.(query.DatabaseInput)
	if !ok {
		t.Fatalf("expected DatabaseInput, got %T", spec)
	}
	if db.Name != query.Symbol("$users") {
		t.Errorf("expected Name $users, got %s", db.Name)
	}
}

func TestSourceInQuery(t *testing.T) {
	users := Source("$users")
	e := NewVar("e")
	name := NewVar("name")

	q := Query().
		Find(name).
		In(users).
		Where(PatFrom(users, e, Kw(":user/name"), name)).
		MustBuild()

	if len(q.In) != 1 {
		t.Fatalf("expected 1 input spec, got %d", len(q.In))
	}
	db, ok := q.In[0].(query.DatabaseInput)
	if !ok {
		t.Fatalf("expected DatabaseInput, got %T", q.In[0])
	}
	if db.Name != query.Symbol("$users") {
		t.Errorf("expected Name $users, got %s", db.Name)
	}
}

func TestPatFrom(t *testing.T) {
	users := Source("$users")
	e := NewVar("e")
	name := NewVar("name")

	p := PatFrom(users, e, Kw(":user/name"), name)

	clause := p.toClause()
	pat, ok := clause.(*query.DataPattern)
	if !ok {
		t.Fatalf("expected *query.DataPattern, got %T", clause)
	}
	if pat.Source != query.Symbol("$users") {
		t.Errorf("expected Source $users, got %s", pat.Source)
	}
	if len(pat.Elements) != 3 {
		t.Errorf("expected 3 elements, got %d", len(pat.Elements))
	}
}

func TestPatFromInQuery(t *testing.T) {
	users := Source("$users")
	perms := Source("$perms")
	e := NewVar("e")
	name := NewVar("name")
	role := NewVar("role")

	q := Query().
		Find(name, role).
		In(users, perms).
		Where(
			PatFrom(users, e, Kw(":user/name"), name),
			PatFrom(perms, e, Kw(":perm/role"), role),
		).
		MustBuild()

	if len(q.In) != 2 {
		t.Fatalf("expected 2 input specs, got %d", len(q.In))
	}

	db1, ok := q.In[0].(query.DatabaseInput)
	if !ok {
		t.Fatalf("expected DatabaseInput for first input, got %T", q.In[0])
	}
	if db1.Name != query.Symbol("$users") {
		t.Errorf("expected $users, got %s", db1.Name)
	}

	db2, ok := q.In[1].(query.DatabaseInput)
	if !ok {
		t.Fatalf("expected DatabaseInput for second input, got %T", q.In[1])
	}
	if db2.Name != query.Symbol("$perms") {
		t.Errorf("expected $perms, got %s", db2.Name)
	}

	if len(q.Where) != 2 {
		t.Fatalf("expected 2 where clauses, got %d", len(q.Where))
	}

	pat1, ok := q.Where[0].(*query.DataPattern)
	if !ok {
		t.Fatalf("expected *query.DataPattern for first clause, got %T", q.Where[0])
	}
	if pat1.Source != query.Symbol("$users") {
		t.Errorf("expected $users source, got %s", pat1.Source)
	}

	pat2, ok := q.Where[1].(*query.DataPattern)
	if !ok {
		t.Fatalf("expected *query.DataPattern for second clause, got %T", q.Where[1])
	}
	if pat2.Source != query.Symbol("$perms") {
		t.Errorf("expected $perms source, got %s", pat2.Source)
	}
}

func TestPatWithoutSource(t *testing.T) {
	e := NewVar("e")
	name := NewVar("name")

	p := Pat(e, Kw(":user/name"), name)
	clause := p.toClause()
	pat, ok := clause.(*query.DataPattern)
	if !ok {
		t.Fatalf("expected *query.DataPattern, got %T", clause)
	}
	if pat.Source != "" {
		t.Errorf("expected empty Source for plain Pat, got %s", pat.Source)
	}
}

func TestSourceWithDBAndNamedSources(t *testing.T) {
	cache := Source("$cache")
	e := NewVar("e")
	name := NewVar("name")
	val := NewVar("val")

	q := Query().
		Find(name, val).
		In(DB, cache).
		Where(
			Pat(e, Kw(":entity/name"), name),
			PatFrom(cache, e, Kw(":cache/value"), val),
		).
		MustBuild()

	if len(q.In) != 2 {
		t.Fatalf("expected 2 input specs, got %d", len(q.In))
	}

	// First is default DB ($)
	db1, ok := q.In[0].(query.DatabaseInput)
	if !ok {
		t.Fatalf("expected DatabaseInput for DB, got %T", q.In[0])
	}
	if db1.Name != query.Symbol("$") {
		t.Errorf("expected $, got %s", db1.Name)
	}

	// Second is $cache
	db2, ok := q.In[1].(query.DatabaseInput)
	if !ok {
		t.Fatalf("expected DatabaseInput for $cache, got %T", q.In[1])
	}
	if db2.Name != query.Symbol("$cache") {
		t.Errorf("expected $cache, got %s", db2.Name)
	}

	// First where clause has no source (default $)
	pat1, ok := q.Where[0].(*query.DataPattern)
	if !ok {
		t.Fatalf("expected *query.DataPattern, got %T", q.Where[0])
	}
	if pat1.Source != "" {
		t.Errorf("expected empty source for default pattern, got %s", pat1.Source)
	}

	// Second where clause has $cache source
	pat2, ok := q.Where[1].(*query.DataPattern)
	if !ok {
		t.Fatalf("expected *query.DataPattern, got %T", q.Where[1])
	}
	if pat2.Source != query.Symbol("$cache") {
		t.Errorf("expected $cache source, got %s", pat2.Source)
	}
}

func TestSubqueryWithNamedSource(t *testing.T) {
	users := Source("$users")
	e := NewVar("e")
	name := NewVar("name")
	dept := NewVar("dept")

	innerQ := Query().
		Find(name).
		In(users, Scalar(dept)).
		Where(
			PatFrom(users, e, Kw(":user/name"), name),
			PatFrom(users, e, Kw(":user/dept"), dept),
		)

	sub := Subquery(innerQ, dept).BindRelation(name)
	clause := sub.toClause()

	sq, ok := clause.(*query.SubqueryPattern)
	if !ok {
		t.Fatalf("expected *query.SubqueryPattern, got %T", clause)
	}

	// First input should be the $users source reference, not $
	if len(sq.Inputs) < 1 {
		t.Fatal("expected at least 1 input in subquery pattern")
	}
	firstInput, ok := sq.Inputs[0].(query.Constant)
	if !ok {
		t.Fatalf("expected Constant for source input, got %T", sq.Inputs[0])
	}
	if firstInput.Value != query.Symbol("$users") {
		t.Errorf("expected $users source input, got %v", firstInput.Value)
	}
}
