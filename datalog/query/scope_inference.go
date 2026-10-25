package query

import "fmt"

// InferHeaders returns q with every or, not and or-default at any depth
// rewritten as the header form that states its interface in the text: an
// or-join, a not-join or an or-default-join. The header is computed from the
// text's occurrences under the rule set the query runs with. A variable
// inside the form that occurs in another clause of the form's list, in the
// enclosing form's header, or in the query's :in, :find or :order-by is
// shared; a variable occurring only inside one branch is local. A shared
// variable the form binds in every branch is an output and every other
// shared variable is required; a not binds nothing for its scope, so every
// shared variable of a not is required. A written or-join header is read
// strictly: a flat header variable some branch does not bind is an error
// naming the form, the branch and the variable, and a branch variable
// outside the header stays local. A form sharing no variable with its scope
// is an error naming the form. A
// subquery's inner query is a scope of its own. A form on the path to a
// rewritten form is copied, and every other clause is shared with q; a query
// of complete header forms is returned as it is.
func InferHeaders(q *Query, rules *RuleSet) (*Query, error) {
	bound := ConstantInputSymbols(q.In)
	for sym := range MultiValuedInputSymbols(q.In) {
		bound[sym] = true
	}
	occurs := make(map[Symbol]bool, len(bound)+len(q.Find))
	for sym := range bound {
		occurs[sym] = true
	}
	for _, element := range q.Find {
		switch f := element.(type) {
		case FindVariable:
			occurs[f.Symbol] = true
		case FindAggregate:
			occurs[f.Arg] = true
			if f.Predicate != nil {
				occurs[f.Predicate] = true
			}
		case FindPull:
			occurs[f.Variable] = true
		}
	}
	for _, clause := range q.OrderBy {
		occurs[clause.Variable] = true
	}
	where, changed, err := inferList(q.Where, occurs, rules)
	if err != nil {
		return nil, err
	}
	if !changed {
		return q, nil
	}
	return q.WithWhere(where), nil
}

// inferList infers the headers of the forms in list, a scope whose enclosing
// scope exposes occurs to it. The list comes back as it is when no clause
// changes.
func inferList(list []Clause, occurs map[Symbol]bool, rules *RuleSet) ([]Clause, bool, error) {
	var out []Clause
	for i, clause := range list {
		inferred, err := inferClause(list, i, occurs, rules)
		if err != nil {
			return nil, false, err
		}
		if inferred != clause && out == nil {
			out = make([]Clause, i, len(list))
			copy(out, list[:i])
		}
		if out != nil {
			out = append(out, inferred)
		}
	}
	if out == nil {
		return list, false, nil
	}
	return out, true, nil
}

// inferBranches infers each branch as a scope whose enclosing header exposes
// occurs. The branches come back as they are when none changes.
func inferBranches(branches [][]Clause, occurs map[Symbol]bool, rules *RuleSet) ([][]Clause, bool, error) {
	var out [][]Clause
	for i, branch := range branches {
		inferred, changed, err := inferList(branch, occurs, rules)
		if err != nil {
			return nil, false, err
		}
		if changed && out == nil {
			out = make([][]Clause, i, len(branches))
			copy(out, branches[:i])
		}
		if out != nil {
			out = append(out, inferred)
		}
	}
	if out == nil {
		return branches, false, nil
	}
	return out, true, nil
}

// inferClause infers the clause at i of list under the scope's occurrences,
// returning the clause itself when it is already a complete header form over
// inferred branches.
func inferClause(list []Clause, i int, occurs map[Symbol]bool, rules *RuleSet) (Clause, error) {
	switch c := list[i].(type) {
	case *OrClause:
		shared := sharedVariables(list, i, occurs, rules)
		var required, outputs []Symbol
		for _, v := range shared {
			if everyBranchBinds(c.Branches, v, rules) {
				outputs = append(outputs, v)
			} else {
				required = append(required, v)
			}
		}
		branches, _, err := branchesUnderHeader(c, required, outputs, c.Branches, rules)
		if err != nil {
			return nil, err
		}
		if len(shared) == 0 {
			return nil, sharesNothing(c)
		}
		return &OrJoinClause{RequiredVars: required, OutputVars: outputs, Branches: branches}, nil

	case *NotClause:
		shared := sharedVariables(list, i, occurs, rules)
		bodies, _, err := branchesUnderHeader(c, shared, nil, [][]Clause{c.Clauses}, rules)
		if err != nil {
			return nil, err
		}
		if len(shared) == 0 {
			return nil, sharesNothing(c)
		}
		return &NotJoinClause{RequiredVars: shared, Clauses: bodies[0]}, nil

	case *OrDefaultClause:
		shared := sharedVariables(list, i, occurs, rules)
		var required, outputs []Symbol
		for _, v := range shared {
			if everyBranchBinds(c.Branches, v, rules) {
				outputs = append(outputs, v)
			} else {
				required = append(required, v)
			}
		}
		branches, _, err := branchesUnderHeader(c, required, outputs, c.Branches, rules)
		if err != nil {
			return nil, err
		}
		if len(shared) == 0 {
			return nil, sharesNothing(c)
		}
		return &OrDefaultJoinClause{RequiredVars: required, OutputVars: outputs, Branches: branches}, nil

	case *OrJoinClause:
		branches, changed, err := branchesUnderHeader(c, c.RequiredVars, c.OutputVars, c.Branches, rules)
		if err != nil {
			return nil, err
		}
		if !changed {
			return c, nil
		}
		return &OrJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}, nil

	case *NotJoinClause:
		bodies, changed, err := branchesUnderHeader(c, c.RequiredVars, nil, [][]Clause{c.Clauses}, rules)
		if err != nil {
			return nil, err
		}
		for _, v := range c.RequiredVars {
			if !ContainsSymbol(FreeVariables(bodies[0], rules), v) {
				return nil, fmt.Errorf("%s: the header names %s, which the body neither binds nor reads; drop %s from the header or read it in the body", c, v, v)
			}
		}
		if !changed {
			return c, nil
		}
		return &NotJoinClause{RequiredVars: c.RequiredVars, Clauses: bodies[0]}, nil

	case *OrDefaultJoinClause:
		branches, changed, err := branchesUnderHeader(c, c.RequiredVars, c.OutputVars, c.Branches, rules)
		if err != nil {
			return nil, err
		}
		if !changed {
			return c, nil
		}
		return &OrDefaultJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}, nil

	case *SubqueryPattern:
		inner, err := InferHeaders(c.Query, rules)
		if err != nil {
			return nil, err
		}
		if inner == c.Query {
			return c, nil
		}
		return &SubqueryPattern{Query: inner, Inputs: c.Inputs, Binding: c.Binding}, nil
	}
	return list[i], nil
}

// branchesUnderHeader infers a form's branches under its header, each a
// scope whose enclosing header exposes the required run and the outputs, and
// reads the header strictly against them through checkHeader. Read with no
// rule set, an invocation provides every variable argument, so the check
// with none refuses a subset of what the check under the rule set refuses.
// The branches come back as they are when none changes.
func branchesUnderHeader(form Clause, required, outputs []Symbol, branches [][]Clause, rules *RuleSet) ([][]Clause, bool, error) {
	inferred, changed, err := inferBranches(branches, symbolSet(required, outputs), rules)
	if err != nil {
		return nil, false, err
	}
	if err := checkHeader(form, required, outputs, inferred, rules); err != nil {
		return nil, false, err
	}
	return inferred, changed, nil
}

// checkHeader reads a written header strictly against the form's inferred
// branches, under the rule set: every branch binds every output, and a
// variable a branch reads, which neither the branch nor the header binds, is
// an UnboundBranchInputError. The branch is named by its position counted
// from one.
func checkHeader(form Clause, required, outputs []Symbol, branches [][]Clause, rules *RuleSet) error {
	header := symbolSet(required, outputs)
	for b, branch := range branches {
		provided, correlates := branchInterface(branch, rules)
		for _, v := range outputs {
			if !ContainsSymbol(provided, v) {
				return fmt.Errorf("%s: branch %d does not bind %s, and every branch binds every output in the header; move %s into the required run", form, b+1, v, v)
			}
		}
		for _, v := range correlates {
			if !v.IsSource() && !ContainsSymbol(provided, v) && !header[v] {
				return &UnboundBranchInputError{Form: form, Branch: b + 1, Variable: v}
			}
		}
	}
	return nil
}

// UnboundBranchInputError reports a variable a branch's predicate, expression,
// subquery input or required position reads, which neither the branch nor
// the form's header binds. Branch is the branch's position counted from one.
type UnboundBranchInputError struct {
	Form     Clause
	Branch   int
	Variable Symbol
}

func (e *UnboundBranchInputError) Error() string {
	return fmt.Sprintf("%s: branch %d reads %s, which neither the branch nor the header binds; bind %s in the branch or name it in the header", e.Form, e.Branch, e.Variable, e.Variable)
}

// sharesNothing is the refusal of a form whose branches share no variable
// with the scope the form stands in.
func sharesNothing(form Clause) error {
	return fmt.Errorf("%s shares no variable with its enclosing scope; a nested form unifies with the enclosing query through at least one variable", form)
}

// sharedVariables returns, in first-appearance order over the form at i, the
// variables inside it that occur in another clause of list or in occurs.
func sharedVariables(list []Clause, i int, occurs map[Symbol]bool, rules *RuleSet) []Symbol {
	outside := make(map[Symbol]bool, len(occurs))
	for v := range occurs {
		outside[v] = true
	}
	for j, other := range list {
		if j == i {
			continue
		}
		for _, v := range occurrences(other, rules, nil) {
			outside[v] = true
		}
	}
	var shared []Symbol
	for _, v := range occurrences(list[i], rules, nil) {
		if outside[v] {
			shared = append(shared, v)
		}
	}
	return shared
}

// occurrences appends to out, each once in first-appearance order, the
// variables clause exposes to the scope it stands in: for an or, not or
// or-default, which has no header yet, every variable of its branches at any
// depth short of a nested header; for every other clause, what it provides
// and what it reads from the scope, which for a header form is its header
// and what its branches read without binding.
func occurrences(clause Clause, rules *RuleSet, out []Symbol) []Symbol {
	var branches [][]Clause
	switch c := clause.(type) {
	case *OrClause:
		branches = c.Branches
	case *OrDefaultClause:
		branches = c.Branches
	case *NotClause:
		branches = [][]Clause{c.Clauses}
	default:
		scope := ScopeOf(clause, rules)
		for _, v := range scope.Provides {
			if !v.IsSource() && !ContainsSymbol(out, v) {
				out = append(out, v)
			}
		}
		for _, v := range scope.Correlates {
			if !v.IsSource() && !ContainsSymbol(out, v) {
				out = append(out, v)
			}
		}
		return out
	}
	for _, branch := range branches {
		for _, inner := range branch {
			out = occurrences(inner, rules, out)
		}
	}
	return out
}

// binds reports whether clause binds v for the clauses after it: a clause
// providing v, or an or or or-default every branch of which binds v. A not
// binds nothing.
func binds(clause Clause, v Symbol, rules *RuleSet) bool {
	switch c := clause.(type) {
	case *OrClause:
		return everyBranchBinds(c.Branches, v, rules)
	case *OrDefaultClause:
		return everyBranchBinds(c.Branches, v, rules)
	case *NotClause:
		return false
	}
	return ContainsSymbol(ScopeOf(clause, rules).Provides, v)
}

// everyBranchBinds reports whether some clause of every branch binds v.
func everyBranchBinds(branches [][]Clause, v Symbol, rules *RuleSet) bool {
	for _, branch := range branches {
		if !otherClauseBinds(branch, -1, v, rules) {
			return false
		}
	}
	return len(branches) > 0
}

// otherClauseBinds reports whether a clause of list other than the one at i
// binds v.
func otherClauseBinds(list []Clause, i int, v Symbol, rules *RuleSet) bool {
	for j, clause := range list {
		if j != i && binds(clause, v, rules) {
			return true
		}
	}
	return false
}

// symbolSet is the set of every symbol in lists.
func symbolSet(lists ...[]Symbol) map[Symbol]bool {
	set := map[Symbol]bool{}
	for _, list := range lists {
		for _, v := range list {
			set[v] = true
		}
	}
	return set
}
