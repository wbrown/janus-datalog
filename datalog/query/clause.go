package query

// Clause represents anything that can appear in a query's WHERE clause
type Clause interface {
	Pattern  // Embeds Pattern for String() method
	clause() // Private marker method
}

// Ensure our types implement Clause
func (*DataPattern) clause()       {}
func (*Comparison) clause()        {}
func (*ChainedComparison) clause() {}
func (*NotEqualPredicate) clause() {}
func (*GroundPredicate) clause()   {}
func (*MissingPredicate) clause()  {}
func (*Expression) clause()        {}
func (*Subquery) clause()          {}

// Expression wraps a Function with an optional binding variable
type Expression struct {
	Function Function // The function to evaluate
	Binding  Symbol   // Variable to bind result to (optional for equality checks)
}

func (e *Expression) String() string {
	// Functions format themselves as (fn ...), we add the brackets and binding
	return "[" + e.Function.String() + " " + e.Binding.String() + "]"
}

// Subquery represents a nested query with bindings
type Subquery struct {
	Query   *Query      // The nested query
	Inputs  []Symbol    // Input variables from outer query
	Binding interface{} // Can be Symbol (scalar), TupleBinding, or RelationBinding
}

func (s *Subquery) String() string {
	// Simplified string representation
	return "[(q ...) binding]"
}
