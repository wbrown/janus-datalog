package qb

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Rules is the % input: the rule set the query's invocations resolve against.
// The corresponding input value is a *query.RuleSet, from RuleSet or
// parser.ParseRules, or the text of a rule set.
//
// Example:
//
//	x, y := qb.NewVar("x"), qb.NewVar("y")
//	q := qb.Query().
//	    Find(x, y).
//	    In(qb.DB, qb.Rules).
//	    Where(qb.Rule("reach", x, y)).
//	    MustBuild()
//
//	db.Query(q, reach)  // reach is the *query.RuleSet
var Rules InputSpec = rulesInput{}

type rulesInput struct{}

func (rulesInput) toInputSpec() query.InputSpec {
	return query.RulesInput{}
}

// RuleClause is a rule invocation being constructed.
type RuleClause struct {
	name query.Symbol
	args []query.PatternElement
}

// Rule invokes the predicate name with args, one per head position: a *Var,
// Blank(), or a value, which becomes a constant.
//
// Example:
//
//	qb.Rule("reach", start, y)       // (reach ?start ?y)
//	qb.Rule("reach", qb.Blank(), y)  // (reach _ ?y)
func Rule(name string, args ...interface{}) *RuleClause {
	elements := make([]query.PatternElement, len(args))
	for i, arg := range args {
		elements[i] = toPatternElement(arg)
	}
	return &RuleClause{name: datalog.NewSymbol(name), args: elements}
}

func (r *RuleClause) toClause() query.Clause {
	return &query.RuleInvocation{Name: r.name, Args: r.args}
}

// RuleBuilder is one rule being defined: the predicate it derives, its head
// variables, the leading ones the caller binds, and its body clauses.
type RuleBuilder struct {
	name     query.Symbol
	head     []*Var
	required []*Var
	body     []interface{}
}

// DefineRule starts a rule deriving name over head. Several rules for one
// name are alternatives.
//
// Example:
//
//	a, b, c := qb.NewVar("a"), qb.NewVar("b"), qb.NewVar("c")
//	reach := qb.MustRuleSet(
//	    qb.DefineRule("reach", a, b).Where(qb.Pat(a, Edge, b)),
//	    qb.DefineRule("reach", a, c).Where(qb.Pat(a, Edge, b), qb.Rule("reach", b, c)),
//	)
func DefineRule(name string, head ...*Var) *RuleBuilder {
	return &RuleBuilder{name: datalog.NewSymbol(name), head: head}
}

// Require names the head variables the caller binds, the leading run of the
// head: Datomic's [(name [?x] ?y) ...]. A body may consume them without
// binding them.
//
// Example:
//
//	n, m := qb.NewVar("n"), qb.NewVar("m")
//	succ := qb.DefineRule("succ", n, m).Require(n).Where(qb.Add(n, int64(1)).As(m))
func (r *RuleBuilder) Require(vars ...*Var) *RuleBuilder {
	r.required = append(r.required, vars...)
	return r
}

// Where adds body clauses, the forms Query().Where accepts.
func (r *RuleBuilder) Where(clauses ...interface{}) *RuleBuilder {
	r.body = append(r.body, clauses...)
	return r
}

// rule converts the head, the required run and the body clauses into the
// query.Rule.
func (r *RuleBuilder) rule() (*query.Rule, error) {
	head := make([]query.Symbol, len(r.head))
	for i, v := range r.head {
		head[i] = v.Symbol()
	}
	var required []query.Symbol
	for _, v := range r.required {
		required = append(required, v.Symbol())
	}
	body := make([]query.Clause, len(r.body))
	for i, c := range r.body {
		clause, err := toClause(c)
		if err != nil {
			return nil, fmt.Errorf("rule %s: body clause %d: %w", r.name, i+1, err)
		}
		body[i] = clause
	}
	return &query.Rule{Name: r.name, Head: head, Required: required, Body: body}, nil
}

// RuleSet builds the rules into a rule set, validated and stratified by
// query.NewRuleSet.
func RuleSet(rules ...*RuleBuilder) (*query.RuleSet, error) {
	built := make([]*query.Rule, len(rules))
	for i, r := range rules {
		rule, err := r.rule()
		if err != nil {
			return nil, err
		}
		built[i] = rule
	}
	return query.NewRuleSet(built)
}

// MustRuleSet is RuleSet, panicking on error. For rule sets defined
// statically, where an error is a programming bug.
func MustRuleSet(rules ...*RuleBuilder) *query.RuleSet {
	rs, err := RuleSet(rules...)
	if err != nil {
		panic(err)
	}
	return rs
}
