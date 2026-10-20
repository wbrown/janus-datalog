package query

import (
	"fmt"
	"strings"
)

// symbolList renders symbols separated by spaces, for error text.
func symbolList(symbols []Symbol) string {
	parts := make([]string, len(symbols))
	for i, s := range symbols {
		parts[i] = s.String()
	}
	return strings.Join(parts, " ")
}

// RuleArityError reports a predicate used at an arity its head does not
// have, by a later head of its own or by an invocation. Name is the
// predicate, Expected the arity its first head declares, Got the arity found.
type RuleArityError struct {
	Name     Symbol
	Expected int
	Got      int
}

func (e *RuleArityError) Error() string {
	return fmt.Sprintf("rule %s: expected %d arguments, got %d", e.Name, e.Expected, e.Got)
}

// UnsafeRuleError reports a rule with a symbol no positive body clause binds:
// a head symbol, or a symbol a predicate, an expression, a subquery input or
// an explicit-join header of the body consumes.
type UnsafeRuleError struct {
	Rule   Symbol
	Symbol Symbol
}

func (e *UnsafeRuleError) Error() string {
	return fmt.Sprintf("rule %s is unsafe: %s is bound by no positive body clause", e.Rule, e.Symbol)
}

// UnknownRuleError reports an invocation of a predicate the rule set does not
// define. Defined carries the predicates it does define.
type UnknownRuleError struct {
	Name    Symbol
	Defined []Symbol
}

func (e *UnknownRuleError) Error() string {
	return fmt.Sprintf("unknown rule %s: the rule set defines %s", e.Name, symbolList(e.Defined))
}

// UnstratifiableError reports predicates that depend on each other through
// negation, which no evaluation order satisfies.
type UnstratifiableError struct {
	Predicates []Symbol
}

func (e *UnstratifiableError) Error() string {
	return fmt.Sprintf("rule set is not stratifiable: %s depend on each other through negation", symbolList(e.Predicates))
}

// NestedRecursionError reports a rule invoking a predicate of its own stratum
// inside a form whose evaluation is not monotone: or-default,
// or-default-join or a subquery whose inner query aggregates, at any depth.
// Form names the innermost such form.
type NestedRecursionError struct {
	Rule      Symbol
	Predicate Symbol
	Form      string
}

func (e *NestedRecursionError) Error() string {
	return fmt.Sprintf("rule %s invokes %s, a predicate of its own stratum, inside %s: a recursive invocation stands at the top level of its body, in an or branch, in an or-join branch, or in a subquery whose inner query does not aggregate", e.Rule, e.Predicate, e.Form)
}

// RequiredBindingError reports an invocation that leaves a position its
// predicate requires bound free. Name is the predicate, Symbol the head
// variable at that position, and Rule the invoking rule, nil for the
// query's own invocation.
type RequiredBindingError struct {
	Name   Symbol
	Symbol Symbol
	Rule   Symbol
}

func (e *RequiredBindingError) Error() string {
	where := "the query"
	if e.Rule != nil {
		where = "rule " + e.Rule.String()
	}
	return fmt.Sprintf("rule %s requires %s bound, and the invocation in %s binds nothing there: bind it with a clause before the invocation", e.Name, e.Symbol, where)
}

// RuleIterationLimitError reports a stratum whose fixpoint ran past the
// iteration limit.
type RuleIterationLimitError struct {
	Predicates []Symbol
	Limit      int
	Reached    int
}

func (e *RuleIterationLimitError) Error() string {
	return fmt.Sprintf("evaluating %s exceeded the iteration limit of %d: iteration %d reached", symbolList(e.Predicates), e.Limit, e.Reached)
}

// DerivedTupleLimitError reports a rule set whose evaluation derived more
// tuples than the limit allows.
type DerivedTupleLimitError struct {
	Predicates []Symbol
	Limit      int
	Reached    int
}

func (e *DerivedTupleLimitError) Error() string {
	return fmt.Sprintf("evaluating %s exceeded the derived-tuple limit of %d: %d tuples derived", symbolList(e.Predicates), e.Limit, e.Reached)
}
