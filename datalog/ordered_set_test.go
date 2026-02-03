package datalog_test

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// Test OrderedSet[T] type unit tests

func TestOrderedSet_NewEmpty(t *testing.T) {
	s := datalog.NewOrderedSet[string]()
	if s.Len() != 0 {
		t.Errorf("expected empty set, got len=%d", s.Len())
	}
	if !s.IsEmpty() {
		t.Error("expected IsEmpty() to return true")
	}
}

func TestOrderedSet_Append(t *testing.T) {
	s := datalog.NewOrderedSet[string]()
	s.Append("a")
	s.Append("b")
	s.Append("c")

	if s.Len() != 3 {
		t.Errorf("expected len=3, got %d", s.Len())
	}

	// Verify order
	if s.Get(0) != "a" {
		t.Errorf("expected Get(0)='a', got %q", s.Get(0))
	}
	if s.Get(1) != "b" {
		t.Errorf("expected Get(1)='b', got %q", s.Get(1))
	}
	if s.Get(2) != "c" {
		t.Errorf("expected Get(2)='c', got %q", s.Get(2))
	}
}

func TestOrderedSet_AppendDuplicates(t *testing.T) {
	s := datalog.NewOrderedSet[string]()
	s.Append("a")
	s.Append("b")
	s.Append("a") // duplicate - should be no-op
	s.Append("c")
	s.Append("b") // duplicate - should be no-op

	if s.Len() != 3 {
		t.Errorf("expected len=3 (no duplicates), got %d", s.Len())
	}

	// Verify order is preserved (first occurrence wins)
	slice := s.Slice()
	expected := []string{"a", "b", "c"}
	for i, exp := range expected {
		if slice[i] != exp {
			t.Errorf("position %d: expected %q, got %q", i, exp, slice[i])
		}
	}
}

func TestOrderedSet_Contains(t *testing.T) {
	s := datalog.NewOrderedSet[string]()
	s.Append("a")
	s.Append("b")

	if !s.Contains("a") {
		t.Error("expected Contains('a') to be true")
	}
	if !s.Contains("b") {
		t.Error("expected Contains('b') to be true")
	}
	if s.Contains("c") {
		t.Error("expected Contains('c') to be false")
	}
}

func TestOrderedSet_Remove(t *testing.T) {
	s := datalog.NewOrderedSet[string]()
	s.Append("a")
	s.Append("b")
	s.Append("c")

	s.Remove("b")

	if s.Len() != 2 {
		t.Errorf("expected len=2 after remove, got %d", s.Len())
	}
	if s.Contains("b") {
		t.Error("expected 'b' to be removed")
	}

	// Verify order preserved
	slice := s.Slice()
	expected := []string{"a", "c"}
	for i, exp := range expected {
		if slice[i] != exp {
			t.Errorf("position %d: expected %q, got %q", i, exp, slice[i])
		}
	}
}

func TestOrderedSet_RemoveNonexistent(t *testing.T) {
	s := datalog.NewOrderedSet[string]()
	s.Append("a")
	s.Append("b")

	// Remove non-existent element should be no-op
	s.Remove("z")

	if s.Len() != 2 {
		t.Errorf("expected len=2 (unchanged), got %d", s.Len())
	}
}

func TestOrderedSet_Clear(t *testing.T) {
	s := datalog.NewOrderedSet[string]()
	s.Append("a")
	s.Append("b")
	s.Append("c")

	s.Clear()

	if s.Len() != 0 {
		t.Errorf("expected len=0 after clear, got %d", s.Len())
	}
	if !s.IsEmpty() {
		t.Error("expected IsEmpty() to be true after clear")
	}
}

func TestOrderedSet_Slice(t *testing.T) {
	s := datalog.NewOrderedSet[string]()
	s.Append("a")
	s.Append("b")
	s.Append("c")

	slice := s.Slice()

	// Verify it's a copy
	slice[0] = "modified"
	if s.Get(0) == "modified" {
		t.Error("Slice() should return a copy, not the original")
	}
}

func TestOrderedSet_OrderedSetFrom(t *testing.T) {
	values := []string{"a", "b", "c", "a", "d", "b"} // contains duplicates
	s := datalog.OrderedSetFrom(values)

	if s.Len() != 4 {
		t.Errorf("expected len=4 (unique values), got %d", s.Len())
	}

	// Verify order (first occurrence of each)
	expected := []string{"a", "b", "c", "d"}
	slice := s.Slice()
	for i, exp := range expected {
		if slice[i] != exp {
			t.Errorf("position %d: expected %q, got %q", i, exp, slice[i])
		}
	}
}

func TestOrderedSet_Int64(t *testing.T) {
	s := datalog.NewOrderedSet[int64]()
	s.Append(1)
	s.Append(2)
	s.Append(3)
	s.Append(1) // duplicate

	if s.Len() != 3 {
		t.Errorf("expected len=3, got %d", s.Len())
	}

	if !s.Contains(2) {
		t.Error("expected Contains(2) to be true")
	}
}

func TestOrderedSet_EmptyOperations(t *testing.T) {
	var s datalog.OrderedSet[string]

	// Operations on zero-value OrderedSet should not panic
	if s.Contains("a") {
		t.Error("zero-value Contains should return false")
	}

	s.Remove("a") // should be no-op, not panic

	if s.Slice() != nil {
		t.Error("zero-value Slice should return nil")
	}

	if !s.IsEmpty() {
		t.Error("zero-value IsEmpty should return true")
	}

	// Append should work on zero-value
	s.Append("a")
	if s.Len() != 1 {
		t.Errorf("expected len=1 after append to zero-value, got %d", s.Len())
	}
}

func TestOrderedSet_Union(t *testing.T) {
	a := datalog.OrderedSetFrom([]string{"a", "b", "c"})
	b := datalog.OrderedSetFrom([]string{"b", "c", "d", "e"})

	result := a.Union(b)

	// Should have all unique elements
	if result.Len() != 5 {
		t.Errorf("expected len=5, got %d", result.Len())
	}

	// Order: a's elements first, then b's new elements
	expected := []string{"a", "b", "c", "d", "e"}
	slice := result.Slice()
	for i, exp := range expected {
		if slice[i] != exp {
			t.Errorf("position %d: expected %q, got %q", i, exp, slice[i])
		}
	}
}

func TestOrderedSet_Union_Disjoint(t *testing.T) {
	a := datalog.OrderedSetFrom([]string{"a", "b"})
	b := datalog.OrderedSetFrom([]string{"c", "d"})

	result := a.Union(b)

	if result.Len() != 4 {
		t.Errorf("expected len=4, got %d", result.Len())
	}

	expected := []string{"a", "b", "c", "d"}
	slice := result.Slice()
	for i, exp := range expected {
		if slice[i] != exp {
			t.Errorf("position %d: expected %q, got %q", i, exp, slice[i])
		}
	}
}

func TestOrderedSet_Intersection(t *testing.T) {
	a := datalog.OrderedSetFrom([]string{"a", "b", "c", "d"})
	b := datalog.OrderedSetFrom([]string{"b", "c", "e"})

	result := a.Intersection(b)

	if result.Len() != 2 {
		t.Errorf("expected len=2, got %d", result.Len())
	}

	// Order preserved from a
	expected := []string{"b", "c"}
	slice := result.Slice()
	for i, exp := range expected {
		if slice[i] != exp {
			t.Errorf("position %d: expected %q, got %q", i, exp, slice[i])
		}
	}
}

func TestOrderedSet_Intersection_Empty(t *testing.T) {
	a := datalog.OrderedSetFrom([]string{"a", "b"})
	b := datalog.OrderedSetFrom([]string{"c", "d"})

	result := a.Intersection(b)

	if result.Len() != 0 {
		t.Errorf("expected empty intersection, got len=%d", result.Len())
	}
}

func TestOrderedSet_Difference(t *testing.T) {
	a := datalog.OrderedSetFrom([]string{"a", "b", "c", "d"})
	b := datalog.OrderedSetFrom([]string{"b", "d", "e"})

	result := a.Difference(b)

	if result.Len() != 2 {
		t.Errorf("expected len=2, got %d", result.Len())
	}

	// Elements in a but not in b
	expected := []string{"a", "c"}
	slice := result.Slice()
	for i, exp := range expected {
		if slice[i] != exp {
			t.Errorf("position %d: expected %q, got %q", i, exp, slice[i])
		}
	}
}

func TestOrderedSet_SymmetricDifference(t *testing.T) {
	a := datalog.OrderedSetFrom([]string{"a", "b", "c"})
	b := datalog.OrderedSetFrom([]string{"b", "c", "d", "e"})

	result := a.SymmetricDifference(b)

	if result.Len() != 3 {
		t.Errorf("expected len=3, got %d", result.Len())
	}

	// Elements unique to a first, then unique to b
	expected := []string{"a", "d", "e"}
	slice := result.Slice()
	for i, exp := range expected {
		if slice[i] != exp {
			t.Errorf("position %d: expected %q, got %q", i, exp, slice[i])
		}
	}
}

func TestOrderedSet_IsSubset(t *testing.T) {
	a := datalog.OrderedSetFrom([]string{"b", "c"})
	b := datalog.OrderedSetFrom([]string{"a", "b", "c", "d"})
	c := datalog.OrderedSetFrom([]string{"x", "y", "z"})

	if !a.IsSubset(b) {
		t.Error("expected {b,c} to be subset of {a,b,c,d}")
	}

	if a.IsSubset(c) {
		t.Error("expected {b,c} NOT to be subset of {x,y,z}")
	}

	// {a,b,c,d} is NOT a subset of {b,c}
	if b.IsSubset(a) {
		t.Error("expected {a,b,c,d} NOT to be subset of {b,c}")
	}

	// Every set is a subset of itself
	if !a.IsSubset(a) {
		t.Error("expected set to be subset of itself")
	}
}

func TestOrderedSet_IsSuperset(t *testing.T) {
	a := datalog.OrderedSetFrom([]string{"a", "b", "c", "d"})
	b := datalog.OrderedSetFrom([]string{"b", "c"})

	if !a.IsSuperset(b) {
		t.Error("expected {a,b,c,d} to be superset of {b,c}")
	}

	if b.IsSuperset(a) {
		t.Error("expected {b,c} NOT to be superset of {a,b,c,d}")
	}
}

func TestOrderedSet_Equals(t *testing.T) {
	a := datalog.OrderedSetFrom([]string{"a", "b", "c"})
	b := datalog.OrderedSetFrom([]string{"c", "b", "a"}) // different order
	c := datalog.OrderedSetFrom([]string{"a", "b"})
	d := datalog.OrderedSetFrom([]string{"a", "b", "d"})

	if !a.Equals(b) {
		t.Error("expected {a,b,c} to equal {c,b,a} (order-independent)")
	}

	if a.Equals(c) {
		t.Error("expected {a,b,c} NOT to equal {a,b} (different size)")
	}

	if a.Equals(d) {
		t.Error("expected {a,b,c} NOT to equal {a,b,d} (different elements)")
	}
}

func TestOrderedSet_EmptySetOperations(t *testing.T) {
	empty := datalog.NewOrderedSet[string]()
	nonempty := datalog.OrderedSetFrom([]string{"a", "b"})

	// Union with empty
	result := empty.Union(nonempty)
	if result.Len() != 2 {
		t.Errorf("empty.Union(nonempty) expected len=2, got %d", result.Len())
	}

	result = nonempty.Union(empty)
	if result.Len() != 2 {
		t.Errorf("nonempty.Union(empty) expected len=2, got %d", result.Len())
	}

	// Intersection with empty
	result = empty.Intersection(nonempty)
	if result.Len() != 0 {
		t.Errorf("empty.Intersection(nonempty) expected len=0, got %d", result.Len())
	}

	// Difference
	result = nonempty.Difference(empty)
	if result.Len() != 2 {
		t.Errorf("nonempty.Difference(empty) expected len=2, got %d", result.Len())
	}

	result = empty.Difference(nonempty)
	if result.Len() != 0 {
		t.Errorf("empty.Difference(nonempty) expected len=0, got %d", result.Len())
	}

	// Empty equals empty
	empty2 := datalog.NewOrderedSet[string]()
	if !empty.Equals(empty2) {
		t.Error("empty sets should be equal")
	}

	// Empty is subset of everything
	if !empty.IsSubset(nonempty) {
		t.Error("empty should be subset of nonempty")
	}

	if !empty.IsSubset(empty2) {
		t.Error("empty should be subset of empty")
	}
}
