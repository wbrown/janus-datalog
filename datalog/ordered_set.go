package datalog

// OrderedSet represents an ordered collection with unique elements.
// It preserves insertion order while ensuring no duplicate values.
//
// OrderedSet maps to Vector().UniqueElements(true) in schema (RGA-based ordered set).
// When used in struct tags, the reflect package will infer this schema.
//
// Unlike Vector (which allows duplicates), OrderedSet ensures each
// value appears at most once. Order is preserved from insertion.
//
// Example usage:
//
//	type Character struct {
//	    ID    datalog.Identity           `datalog:"-,id"`
//	    Prefs datalog.OrderedSet[string] `datalog:"prefs"`
//	}
type OrderedSet[T comparable] struct {
	Items []T            // Exported for reflection access
	seen  map[T]struct{} // Internal lookup map
}

// NewOrderedSet creates a new empty OrderedSet.
func NewOrderedSet[T comparable]() *OrderedSet[T] {
	return &OrderedSet[T]{
		Items: make([]T, 0),
		seen:  make(map[T]struct{}),
	}
}

// OrderedSetFrom creates an OrderedSet from a slice, preserving order and removing duplicates.
// The first occurrence of each value is kept.
func OrderedSetFrom[T comparable](values []T) *OrderedSet[T] {
	s := NewOrderedSet[T]()
	for _, v := range values {
		s.Append(v)
	}
	return s
}

// Append adds a value to the end of the ordered set if not already present.
// If the value already exists, this is a no-op.
func (s *OrderedSet[T]) Append(v T) {
	if s.seen == nil {
		s.seen = make(map[T]struct{})
	}
	if _, exists := s.seen[v]; exists {
		return // Already present
	}
	s.Items = append(s.Items, v)
	s.seen[v] = struct{}{}
}

// Contains returns true if the value is in the set.
// If the seen map is nil (e.g., when populated via reflection), falls back to linear search.
func (s *OrderedSet[T]) Contains(v T) bool {
	// Fast path: use the seen map if available
	if s.seen != nil {
		_, exists := s.seen[v]
		return exists
	}
	// Slow path: linear search through Items (for reflection-populated sets)
	for _, item := range s.Items {
		if item == v {
			return true
		}
	}
	return false
}

// Remove removes a value from the set.
// If the value is not present, this is a no-op.
func (s *OrderedSet[T]) Remove(v T) {
	if s.seen == nil {
		return
	}
	if _, exists := s.seen[v]; !exists {
		return
	}
	delete(s.seen, v)
	// Remove from items slice, preserving order
	for i, item := range s.Items {
		if item == v {
			s.Items = append(s.Items[:i], s.Items[i+1:]...)
			break
		}
	}
}

// Slice returns a copy of the ordered elements.
func (s *OrderedSet[T]) Slice() []T {
	if s.Items == nil {
		return nil
	}
	result := make([]T, len(s.Items))
	copy(result, s.Items)
	return result
}

// Len returns the number of elements in the set.
func (s *OrderedSet[T]) Len() int {
	return len(s.Items)
}

// Get returns the element at index i.
// Panics if i is out of range.
func (s *OrderedSet[T]) Get(i int) T {
	return s.Items[i]
}

// Clear removes all elements from the set.
func (s *OrderedSet[T]) Clear() {
	s.Items = s.Items[:0]
	s.seen = make(map[T]struct{})
}

// Union returns a new OrderedSet containing all elements from both sets.
// Elements from this set appear first (in order), followed by new elements from other.
func (s *OrderedSet[T]) Union(other *OrderedSet[T]) *OrderedSet[T] {
	result := NewOrderedSet[T]()
	for _, v := range s.Items {
		result.Append(v)
	}
	for _, v := range other.Items {
		result.Append(v) // No-op if already present
	}
	return result
}

// Intersection returns a new OrderedSet containing only elements present in both sets.
// Order is preserved from this set.
func (s *OrderedSet[T]) Intersection(other *OrderedSet[T]) *OrderedSet[T] {
	result := NewOrderedSet[T]()
	for _, v := range s.Items {
		if other.Contains(v) {
			result.Append(v)
		}
	}
	return result
}

// Difference returns a new OrderedSet containing elements in this set but not in other.
// Order is preserved from this set.
func (s *OrderedSet[T]) Difference(other *OrderedSet[T]) *OrderedSet[T] {
	result := NewOrderedSet[T]()
	for _, v := range s.Items {
		if !other.Contains(v) {
			result.Append(v)
		}
	}
	return result
}

// SymmetricDifference returns a new OrderedSet containing elements in exactly one of the sets.
// Elements unique to this set appear first, followed by elements unique to other.
func (s *OrderedSet[T]) SymmetricDifference(other *OrderedSet[T]) *OrderedSet[T] {
	result := NewOrderedSet[T]()
	// Elements in s but not in other
	for _, v := range s.Items {
		if !other.Contains(v) {
			result.Append(v)
		}
	}
	// Elements in other but not in s
	for _, v := range other.Items {
		if !s.Contains(v) {
			result.Append(v)
		}
	}
	return result
}

// IsSubset returns true if all elements of this set are in other.
func (s *OrderedSet[T]) IsSubset(other *OrderedSet[T]) bool {
	for _, v := range s.Items {
		if !other.Contains(v) {
			return false
		}
	}
	return true
}

// IsSuperset returns true if this set contains all elements of other.
func (s *OrderedSet[T]) IsSuperset(other *OrderedSet[T]) bool {
	return other.IsSubset(s)
}

// Equals returns true if both sets contain exactly the same elements.
// Order is not considered for equality.
func (s *OrderedSet[T]) Equals(other *OrderedSet[T]) bool {
	if s.Len() != other.Len() {
		return false
	}
	return s.IsSubset(other)
}
