package qb

import (
	"testing"
)

func TestBuilderLimit(t *testing.T) {
	e := NewVar("e")
	name := NewVar("name")

	q, err := Query().
		Find(e).
		Where(Pat(e, Kw(":person/name"), name)).
		Limit(10).
		Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if q.Limit == nil {
		t.Fatalf("expected Limit to be set, got nil")
	}
	if *q.Limit != 10 {
		t.Errorf("expected Limit 10, got %d", *q.Limit)
	}
}

func TestBuilderLimitZero(t *testing.T) {
	e := NewVar("e")
	name := NewVar("name")

	q, err := Query().
		Find(e).
		Where(Pat(e, Kw(":person/name"), name)).
		Limit(0).
		Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if q.Limit == nil || *q.Limit != 0 {
		t.Fatalf("expected Limit 0, got %v", q.Limit)
	}
}

func TestBuilderNoLimit(t *testing.T) {
	e := NewVar("e")
	name := NewVar("name")

	q, err := Query().
		Find(e).
		Where(Pat(e, Kw(":person/name"), name)).
		Build()
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if q.Limit != nil {
		t.Errorf("expected Limit nil when not set, got %d", *q.Limit)
	}
}

func TestBuilderNegativeLimitErrors(t *testing.T) {
	e := NewVar("e")
	name := NewVar("name")

	_, err := Query().
		Find(e).
		Where(Pat(e, Kw(":person/name"), name)).
		Limit(-1).
		Build()
	if err == nil {
		t.Error("expected Build() error for negative limit, got nil")
	}
}
