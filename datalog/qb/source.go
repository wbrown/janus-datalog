package qb

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// sourceInput represents a named database source input (e.g., $users, $perms).
// It implements InputSpec for use in In() clauses and can be passed to PatFrom()
// to create source-qualified patterns.
type sourceInput struct {
	sym query.Symbol
}

// Source creates a named database source for multi-source queries.
// The name must start with "$" (e.g., "$users", "$perms").
//
// Use Source in the In() clause to declare the source, and with PatFrom()
// to create patterns that query against it.
//
// Example:
//
//	users := qb.Source("$users")
//	perms := qb.Source("$perms")
//	e, name, role := qb.NewVar("e"), qb.NewVar("name"), qb.NewVar("role")
//
//	q := qb.Query().
//	    Find(name, role).
//	    In(users, perms).
//	    Where(
//	        qb.PatFrom(users, e, qb.Kw(":user/name"), name),
//	        qb.PatFrom(perms, e, qb.Kw(":perm/role"), role),
//	    ).MustBuild()
func Source(name string) *sourceInput {
	return &sourceInput{sym: datalog.NewSymbol(name)}
}

// Symbol returns the underlying query.Symbol for this source.
func (s *sourceInput) Symbol() query.Symbol {
	return s.sym
}

// toInputSpec implements InputSpec, allowing Source to be used in In() clauses.
func (s *sourceInput) toInputSpec() query.InputSpec {
	return query.DatabaseInput{Name: s.sym}
}

// PatFrom creates a source-qualified data pattern.
// The pattern will be routed to the specified source during execution.
//
// Arguments after the source follow the same rules as Pat():
//   - *Var: a query variable
//   - Attr: a keyword attribute
//   - Val: a constant value wrapper
//   - blankElement: wildcard (from Blank())
//   - Raw values (string, int64, etc.): converted to constants
//
// Example:
//
//	users := qb.Source("$users")
//	e, name := qb.NewVar("e"), qb.NewVar("name")
//
//	qb.PatFrom(users, e, qb.Kw(":user/name"), name)
//	// Equivalent to parsed: [$users ?e :user/name ?name]
func PatFrom(source *sourceInput, args ...interface{}) *Pattern {
	if len(args) == 0 {
		panic("PatFrom requires at least one argument after source")
	}
	elements := make([]query.PatternElement, len(args))
	for i, arg := range args {
		elements[i] = toPatternElement(arg)
	}
	return &Pattern{source: source.sym, elements: elements}
}
