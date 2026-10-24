package query

import "github.com/wbrown/janus-datalog/datalog"

// UnifyPatterns returns q with every data pattern at any depth that names
// one variable at several positions written as the pattern over distinct
// variables, each later occurrence a UnifiedVariable, followed by the
// equality of each with the first: a variable written twice in a pattern
// names one value, and the equality selects the datoms whose positions
// agree, under every matcher and both planner paths. A pattern naming no
// variable twice, a form holding none and a query holding none come back as
// they are.
func UnifyPatterns(q *Query) *Query {
	where, changed := unifyList(q.Where)
	if !changed {
		return q
	}
	return q.WithWhere(where)
}

// unifyList rewrites the patterns of list, returning it as it is when none
// changes.
func unifyList(list []Clause) ([]Clause, bool) {
	var out []Clause
	for i, clause := range list {
		rewritten := unifyClause(clause)
		if len(rewritten) == 1 && rewritten[0] == clause {
			if out != nil {
				out = append(out, clause)
			}
			continue
		}
		if out == nil {
			out = make([]Clause, i, len(list)+len(rewritten))
			copy(out, list[:i])
		}
		out = append(out, rewritten...)
	}
	if out == nil {
		return list, false
	}
	return out, true
}

// unifyBranches rewrites the patterns of each branch, returning the branches
// as they are when none changes.
func unifyBranches(branches [][]Clause) ([][]Clause, bool) {
	var out [][]Clause
	for i, branch := range branches {
		rewritten, changed := unifyList(branch)
		if changed && out == nil {
			out = make([][]Clause, i, len(branches))
			copy(out, branches[:i])
		}
		if out != nil {
			out = append(out, rewritten)
		}
	}
	if out == nil {
		return branches, false
	}
	return out, true
}

// unifyClause is the clauses clause becomes: a pattern naming a variable
// twice with its equalities, a form over its rewritten branches, a subquery
// over its rewritten inner query, and any other clause itself.
func unifyClause(clause Clause) []Clause {
	switch c := clause.(type) {
	case *DataPattern:
		return unifyPattern(c)
	case *OrClause:
		if branches, changed := unifyBranches(c.Branches); changed {
			return []Clause{&OrClause{Branches: branches}}
		}
	case *NotClause:
		if bodies, changed := unifyBranches([][]Clause{c.Clauses}); changed {
			return []Clause{&NotClause{Clauses: bodies[0]}}
		}
	case *OrDefaultClause:
		if branches, changed := unifyBranches(c.Branches); changed {
			return []Clause{&OrDefaultClause{Branches: branches}}
		}
	case *OrJoinClause:
		if branches, changed := unifyBranches(c.Branches); changed {
			return []Clause{&OrJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}}
		}
	case *NotJoinClause:
		if bodies, changed := unifyBranches([][]Clause{c.Clauses}); changed {
			return []Clause{&NotJoinClause{RequiredVars: c.RequiredVars, Clauses: bodies[0]}}
		}
	case *OrDefaultJoinClause:
		if branches, changed := unifyBranches(c.Branches); changed {
			return []Clause{&OrDefaultJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}}
		}
	case *SubqueryPattern:
		if inner := UnifyPatterns(c.Query); inner != c.Query {
			return []Clause{&SubqueryPattern{Query: inner, Inputs: c.Inputs, Binding: c.Binding}}
		}
	}
	return []Clause{clause}
}

// unifyPattern is the pattern with each later occurrence of a variable
// written as its UnifiedVariable, followed by one equality per later
// occurrence, or the pattern itself when no variable occurs twice.
func unifyPattern(p *DataPattern) []Clause {
	positional := make([]Symbol, len(p.Elements))
	for i, element := range p.Elements {
		if v, ok := element.(Variable); ok {
			positional[i] = v.Name
		}
	}
	_, groups := SymbolGroups(positional)
	var elements []PatternElement
	var equalities []Clause
	for _, group := range groups {
		if len(group) == 1 {
			continue
		}
		if elements == nil {
			elements = append([]PatternElement(nil), p.Elements...)
		}
		first := positional[group[0]]
		for _, position := range group[1:] {
			unified := UnifiedVariable(first, position)
			elements[position] = Variable{Name: unified}
			equalities = append(equalities, &Comparison{Op: datalog.SymEQ, Left: VariableTerm{Symbol: first}, Right: VariableTerm{Symbol: unified}})
		}
	}
	if elements == nil {
		return []Clause{p}
	}
	return append([]Clause{&DataPattern{Source: p.Source, Elements: elements}}, equalities...)
}
