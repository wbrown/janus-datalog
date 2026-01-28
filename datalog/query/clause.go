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
func (*NotClause) clause()         {}
func (*NotJoinClause) clause()     {}
func (*OrClause) clause()          {}
func (*OrJoinClause) clause()      {}

// Expression wraps a Function with an optional binding
type Expression struct {
	Function Function    // The function to evaluate
	Binding  interface{} // Symbol (scalar) or TupleBinding (tuple)
}

func (e *Expression) String() string {
	// Functions format themselves as (fn ...), we add the brackets and binding
	var bindingStr string
	switch b := e.Binding.(type) {
	case Symbol:
		bindingStr = b.String()
	case TupleBinding:
		bindingStr = b.String()
	default:
		bindingStr = ""
	}
	return "[" + e.Function.String() + " " + bindingStr + "]"
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

// NotClause represents a negation clause: (not [clauses...])
// Filters tuples where the inner clauses match (anti-join)
type NotClause struct {
	Clauses []Clause // Inner clauses to negate
}

func (n *NotClause) String() string {
	result := "(not"
	for _, c := range n.Clauses {
		result += " " + c.String()
	}
	result += ")"
	return result
}

// NotJoinClause represents a not-join clause: (not-join [vars] [clauses...])
// Like NotClause but with explicit join variables
type NotJoinClause struct {
	JoinVars []Symbol // Variables to join on (must be bound before this clause)
	Clauses  []Clause // Inner clauses
}

func (n *NotJoinClause) String() string {
	result := "(not-join ["
	for i, v := range n.JoinVars {
		if i > 0 {
			result += " "
		}
		result += v.String()
	}
	result += "]"
	for _, c := range n.Clauses {
		result += " " + c.String()
	}
	result += ")"
	return result
}

// OrClause represents a disjunction: (or branch1 branch2 ...)
// Returns union of results from each branch
type OrClause struct {
	Branches [][]Clause // Each branch is a list of clauses
}

func (o *OrClause) String() string {
	result := "(or"
	for _, branch := range o.Branches {
		if len(branch) == 1 {
			result += " " + branch[0].String()
		} else {
			result += " (and"
			for _, c := range branch {
				result += " " + c.String()
			}
			result += ")"
		}
	}
	result += ")"
	return result
}

// OrJoinClause represents an or-join clause with explicit variable binding
// (or-join [vars] branch1 branch2 ...)
type OrJoinClause struct {
	JoinVars []Symbol   // Variables to expose from union
	Branches [][]Clause // Each branch is a list of clauses
}

// BranchHasExpressions checks if any clause in a branch is an expression type.
// This is used to determine whether an OR clause should use fallback semantics
// (Clojure-style first-non-empty-wins) vs union semantics (Datalog-style).
func BranchHasExpressions(branch []Clause) bool {
	for _, c := range branch {
		switch c.(type) {
		case *Expression, *Subquery, *SubqueryPattern:
			return true
		case *GroundPredicate:
			return true
		}
	}
	return false
}

// OrHasExpressions checks if any branch in an OR clause contains expressions.
func OrHasExpressions(branches [][]Clause) bool {
	for _, branch := range branches {
		if BranchHasExpressions(branch) {
			return true
		}
	}
	return false
}

func (o *OrJoinClause) String() string {
	result := "(or-join ["
	for i, v := range o.JoinVars {
		if i > 0 {
			result += " "
		}
		result += v.String()
	}
	result += "]"
	for _, branch := range o.Branches {
		if len(branch) == 1 {
			result += " " + branch[0].String()
		} else {
			result += " (and"
			for _, c := range branch {
				result += " " + c.String()
			}
			result += ")"
		}
	}
	result += ")"
	return result
}
