package query

// Clause represents anything that can appear in a query's WHERE clause
type Clause interface {
	Pattern  // Embeds Pattern for String() method
	clause() // Private marker method
}

// Ensure our types implement Clause
func (*DataPattern) clause()            {}
func (*Comparison) clause()             {}
func (*ChainedComparison) clause()      {}
func (*NotEqualPredicate) clause()      {}
func (*GroundPredicate) clause()        {}
func (*MissingPredicate) clause()       {}
func (*StrStartsWithPredicate) clause() {}
func (*Expression) clause()             {}
func (*NotClause) clause()              {}
func (*NotJoinClause) clause()          {}
func (*OrClause) clause()               {}
func (*OrJoinClause) clause()           {}
func (*OrDefaultClause) clause()        {}
func (*OrDefaultJoinClause) clause()    {}

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

// OrDefaultClause represents a disjunction with fallback semantics:
// (or-default branch1 branch2 ...)
// For each outer tuple, tries branches in order until one returns results.
// This is a janus-datalog extension for the "data with default" pattern.
type OrDefaultClause struct {
	Branches [][]Clause
}

func (o *OrDefaultClause) String() string {
	result := "(or-default"
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

// OrDefaultJoinClause represents an or-default-join with a declared interface:
//
//	(or-default-join [[?required ...] ?output ...] branch1 branch2 ...)
//	(or-default-join [?output ...] branch1 branch2 ...)   ; global fallback
//
// RequiredVars are the per-group correlation keys, bound by the enclosing
// query before the clause runs; empty means the fallback decision is global.
// OutputVars are bound by every branch and are the clause's provides. The
// header is the complete interface: branch locals never escape. or-default
// is non-monotone — the correlation keys change results, not just plans —
// so the quantification is declared syntax (Datomic's required-vars form),
// never inferred from branch structure or binding context. Validate
// enforces the declaration; the boundaries (parser, qb, executor) call it.
type OrDefaultJoinClause struct {
	RequiredVars []Symbol
	OutputVars   []Symbol
	Branches     [][]Clause
}

func (o *OrDefaultJoinClause) String() string {
	result := "(or-default-join ["
	if len(o.RequiredVars) > 0 {
		result += "["
		for i, v := range o.RequiredVars {
			if i > 0 {
				result += " "
			}
			result += v.String()
		}
		result += "]"
		if len(o.OutputVars) > 0 {
			result += " "
		}
	}
	for i, v := range o.OutputVars {
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
