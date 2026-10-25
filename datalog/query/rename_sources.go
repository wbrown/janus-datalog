package query

import "github.com/wbrown/janus-datalog/datalog"

// CallSources is the substitution a call states about sources: each inner
// :in source, the $ of an omitted :in included, stands for the constant
// source the call passes at its place when that differs from the inner name,
// read once from the text for the planner's frame and the roster alike. A
// source passed under its own name, a variable argument, and a position that
// is no source name nothing.
func CallSources(call *SubqueryPattern) Substitution {
	var from, to []Symbol
	for position, at := range InputPositions(call.Query.In) {
		source, ok := at.Spec.(DatabaseInput)
		if !ok {
			continue
		}
		arg, ok := call.Inputs[position].(Constant)
		if !ok {
			continue
		}
		passed, ok := arg.Value.(Symbol)
		if !ok || passed == source.Name {
			continue
		}
		from = append(from, source.Name)
		to = append(to, passed)
	}
	return Substitution{from: from, to: to}
}

// RenameSources is a scope's clauses read against the sources its call
// names: every data pattern at any depth on a source the substitution names,
// nil standing for $, names the source standing for it, and every nested
// call passing such a source passes the one standing for it. A nested inner
// query is a scope of its own and keeps its sources. Clauses on no renamed
// source come back as they are.
func RenameSources(clauses []Clause, sources Substitution) []Clause {
	renamed, _ := renameSourcesList(clauses, sources)
	return renamed
}

// renameSourcesList renames the sources of list, returning it as it is when
// no clause changes.
func renameSourcesList(list []Clause, sources Substitution) ([]Clause, bool) {
	var out []Clause
	for i, clause := range list {
		renamed := renameSourcesClause(clause, sources)
		if renamed == clause {
			if out != nil {
				out = append(out, clause)
			}
			continue
		}
		if out == nil {
			out = make([]Clause, i, len(list))
			copy(out, list[:i])
		}
		out = append(out, renamed)
	}
	if out == nil {
		return list, false
	}
	return out, true
}

// renameSourcesBranches renames the sources of each branch, returning the
// branches as they are when none changes.
func renameSourcesBranches(branches [][]Clause, sources Substitution) ([][]Clause, bool) {
	var out [][]Clause
	for i, branch := range branches {
		renamed, changed := renameSourcesList(branch, sources)
		if changed && out == nil {
			out = make([][]Clause, i, len(branches))
			copy(out, branches[:i])
		}
		if out != nil {
			out = append(out, renamed)
		}
	}
	if out == nil {
		return branches, false
	}
	return out, true
}

// renameSourcesClause is the clause clause becomes: a pattern on a renamed
// source naming the source standing for it, a form over its renamed branches,
// a call passing a renamed source passing the one standing for it, and any
// other clause itself.
func renameSourcesClause(clause Clause, sources Substitution) Clause {
	switch c := clause.(type) {
	case *DataPattern:
		from := c.Source
		if from == nil {
			from = datalog.SymDollar
		}
		if to, ok := sources.Of(from); ok && to != from {
			return &DataPattern{Source: to, Elements: c.Elements}
		}
	case *OrClause:
		if branches, changed := renameSourcesBranches(c.Branches, sources); changed {
			return &OrClause{Branches: branches}
		}
	case *NotClause:
		if bodies, changed := renameSourcesBranches([][]Clause{c.Clauses}, sources); changed {
			return &NotClause{Clauses: bodies[0]}
		}
	case *OrDefaultClause:
		if branches, changed := renameSourcesBranches(c.Branches, sources); changed {
			return &OrDefaultClause{Branches: branches}
		}
	case *OrJoinClause:
		if branches, changed := renameSourcesBranches(c.Branches, sources); changed {
			return &OrJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}
		}
	case *NotJoinClause:
		if bodies, changed := renameSourcesBranches([][]Clause{c.Clauses}, sources); changed {
			return &NotJoinClause{RequiredVars: c.RequiredVars, Clauses: bodies[0]}
		}
	case *OrDefaultJoinClause:
		if branches, changed := renameSourcesBranches(c.Branches, sources); changed {
			return &OrDefaultJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}
		}
	case *SubqueryPattern:
		var inputs []PatternElement
		for i, input := range c.Inputs {
			constant, ok := input.(Constant)
			if !ok {
				continue
			}
			passed, ok := constant.Value.(Symbol)
			if !ok {
				continue
			}
			if to, ok := sources.Of(passed); ok && to != passed {
				if inputs == nil {
					inputs = append([]PatternElement(nil), c.Inputs...)
				}
				inputs[i] = Constant{Value: to}
			}
		}
		if inputs != nil {
			return &SubqueryPattern{Query: c.Query, Inputs: inputs, Binding: c.Binding}
		}
	}
	return clause
}
