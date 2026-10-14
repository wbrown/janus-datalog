package query

import (
	"maps"
	"strconv"

	"github.com/wbrown/janus-datalog/datalog"
)

// Magic is a rule set rewritten for one invocation with bound arguments.
// Rules derives Predicate, the invoked predicate adorned with the binding
// pattern, from Seed, the predicate the invocation's bound values fill, so
// evaluating Rules derives only the tuples those values reach.
type Magic struct {
	Rules     *RuleSet
	Predicate Symbol
	Seed      Symbol
}

// Magic rewrites the set for an invocation of name whose positions bound
// marks as bound, at least one of them. Each predicate the bindings reach
// gets a copy adorned with its binding pattern whose rules are guarded by a
// magic predicate holding the bindings the predicate is asked with. The
// bindings pass left to right through each body: a top-level invocation, or
// one inside an or, or-join, or-default or or-default-join form, is adorned
// by the constants it holds and the variables the head's bound positions and
// the top-level clauses before it provide, and its magic rule derives those
// bindings from the guard and those clauses. An invocation under a not or
// not-join form is adorned by the variables the head's bound positions and
// the top-level clauses before the first that invokes a predicate of the
// rule's own stratum provide, and its magic rule reads those clauses: a
// clause after that invocation is adorned under what it binds, so a magic
// rule reading it would depend on the adorned predicate of the stratum,
// which depends through the negation on the negated one. An invocation none
// of that binds keeps its name, and its predicate's original rules join the
// set unchanged; one that leaves a position its predicate requires bound
// free is RequiredBindingError. A subquery body is not entered. The
// rewritten set is built through the checks NewRuleSet runs, and a set that
// does not stratify is returned as that error: the decline.
func (rs *RuleSet) Magic(name Symbol, bound []bool) (*Magic, error) {
	w := &magicRewriter{
		rs:       rs,
		adorned:  map[Symbol]bool{},
		original: map[Symbol]bool{},
	}
	predicate := adornedSymbol(name, bound)
	seed := magicSymbol(name, bound)
	w.enqueue(name, bound)
	for len(w.worklist) > 0 {
		next := w.worklist[0]
		w.worklist = w.worklist[1:]
		if err := w.rewritePredicate(next.name, next.bound); err != nil {
			return nil, err
		}
	}
	rewritten, err := newRuleSet(w.rules, []seedPredicate{{name: seed, arity: countBound(bound)}})
	if err != nil {
		return nil, err
	}
	return &Magic{Rules: rewritten, Predicate: predicate, Seed: seed}, nil
}

func countBound(bound []bool) int {
	n := 0
	for _, b := range bound {
		if b {
			n++
		}
	}
	return n
}

// adornedPredicate is one predicate under one binding pattern.
type adornedPredicate struct {
	name  Symbol
	bound []bool
}

// magicRewriter accumulates the rewritten set: its rules in emission order,
// the adorned predicates already rewritten or queued, and the original
// predicates already included.
type magicRewriter struct {
	rs       *RuleSet
	rules    []*Rule
	adorned  map[Symbol]bool
	original map[Symbol]bool
	worklist []adornedPredicate
}

// passing is what passes into an invocation: the variables bound before it,
// and the clauses a magic rule reads to derive them, the guard first.
type passing struct {
	bound  map[Symbol]bool
	prefix []Clause
}

// within is what passes into a join form's branches: the header's variables
// the enclosing scope binds, over the same clauses, since inside the form
// only the header's variables are the enclosing scope's.
func (p passing) within(header []Symbol) passing {
	inside := make(map[Symbol]bool, len(header))
	for _, sym := range header {
		if p.bound[sym] {
			inside[sym] = true
		}
	}
	return passing{bound: inside, prefix: p.prefix}
}

// enqueue queues a predicate under a binding pattern once.
func (w *magicRewriter) enqueue(name Symbol, bound []bool) {
	key := adornedSymbol(name, bound)
	if w.adorned[key] {
		return
	}
	w.adorned[key] = true
	w.worklist = append(w.worklist, adornedPredicate{name: name, bound: bound})
}

// includeOriginal adds a predicate's own rules, and those of every predicate
// they invoke at any depth, unchanged.
func (w *magicRewriter) includeOriginal(name Symbol) {
	if w.original[name] {
		return
	}
	w.original[name] = true
	for _, rule := range w.rs.rules[name] {
		w.rules = append(w.rules, rule)
		for _, inv := range Invocations(rule.Body) {
			w.includeOriginal(inv.Name)
		}
	}
}

// rewritePredicate emits, for each rule of the predicate, the adorned rule
// guarded by the magic predicate over the bound head positions, then the
// magic rules its body's adorned invocations need. The adorned rule has no
// required run: its guard binds every position the predicate requires, so
// the fixpoint evaluates it whole.
func (w *magicRewriter) rewritePredicate(name Symbol, bound []bool) error {
	adorned := adornedSymbol(name, bound)
	magic := magicSymbol(name, bound)
	for _, rule := range w.rs.rules[name] {
		boundVars := map[Symbol]bool{}
		var guardArgs []PatternElement
		for i, head := range rule.Head {
			if bound[i] {
				guardArgs = append(guardArgs, Variable{Name: head})
				boundVars[head] = true
			}
		}
		prefix := []Clause{&RuleInvocation{Name: magic, Args: guardArgs}}
		body, magicRules, err := w.rewriteBody(rule, passing{bound: boundVars, prefix: prefix})
		if err != nil {
			return err
		}
		w.rules = append(w.rules, &Rule{Name: adorned, Head: rule.Head, Body: append(prefix, body...)})
		w.rules = append(w.rules, magicRules...)
	}
	return nil
}

// rewriteBody walks the rule's top-level clauses left to right. Each clause
// is rewritten under what passes into it. A clause a magic rule reads joins
// the prefix, and the variables it provides join the bound set for the
// clauses after it; the prefix a magic rule reads is the guard and those
// clauses before the one holding the invocation. A clause left out widens
// the bindings a magic rule derives and loses no tuple, since the adorned
// rule applies every clause. What passes into an invocation under a negation
// stops at the first clause that invokes a predicate of the rule's own
// stratum, so no magic rule depends through a negation on that stratum's
// adorned predicate.
func (w *magicRewriter) rewriteBody(rule *Rule, before passing) ([]Clause, []*Rule, error) {
	out := make([]Clause, len(rule.Body))
	var magicRules []*Rule
	negated := before
	sealed := false
	for i, clause := range rule.Body {
		if !sealed && w.invokesStratumOf(rule.Name, clause) {
			negated = passing{bound: maps.Clone(before.bound), prefix: before.prefix}
			sealed = true
		}
		rewritten, rules, err := w.rewriteClause(rule.Name, clause, before, negated)
		if err != nil {
			return nil, nil, err
		}
		out[i] = rewritten
		magicRules = append(magicRules, rules...)
		if !w.magicRuleReads(rewritten, before.bound) {
			continue
		}
		before.prefix = append(before.prefix[:len(before.prefix):len(before.prefix)], rewritten)
		for _, sym := range ScopeOf(rewritten, w.rs).Provides {
			before.bound[sym] = true
		}
		if !sealed {
			negated = before
		}
	}
	return out, magicRules, nil
}

// invokesStratumOf reports whether the clause invokes, at any depth, a
// predicate of the named rule's stratum.
func (w *magicRewriter) invokesStratumOf(rule Symbol, clause Clause) bool {
	for _, inv := range Invocations([]Clause{clause}) {
		if w.rs.sameStratum(rule, inv.Name) {
			return true
		}
	}
	return false
}

// magicRuleReads reports whether a magic rule reads clause: a monotone
// clause every mandatory correlate of which the bound set holds. A negation
// or a fallback form is never read, so no magic rule depends on a predicate
// through one; a clause reading a variable a later clause binds is not read,
// so every magic rule is safe.
func (w *magicRewriter) magicRuleReads(clause Clause, boundVars map[Symbol]bool) bool {
	switch clause.(type) {
	case *NotClause, *NotJoinClause, *OrDefaultClause, *OrDefaultJoinClause:
		return false
	}
	for _, sym := range ScopeOf(clause, w.rs).Mandatory {
		if !sym.IsSource() && !boundVars[sym] {
			return false
		}
	}
	return true
}

// rewriteClause adorns the invocations in one clause of the named rule: a
// positive invocation under what the clauses before pass, and one under a
// not or not-join form, at any depth under it, under what passes into a
// negation. A form on the path to an adorned invocation is copied; every
// other clause is shared. Inside a join form only the header's variables are
// the enclosing scope's, so what passes into its branches is filtered to the
// header.
func (w *magicRewriter) rewriteClause(rule Symbol, clause Clause, before, negated passing) (Clause, []*Rule, error) {
	switch c := clause.(type) {
	case *RuleInvocation:
		return w.adornInvocation(rule, c, before)
	case *NotClause:
		inner, rules, changed, err := w.rewriteNested(rule, c.Clauses, negated, negated)
		if err != nil || !changed {
			return c, nil, err
		}
		return &NotClause{Clauses: inner}, rules, nil
	case *NotJoinClause:
		inside := negated.within(c.JoinVars)
		inner, rules, changed, err := w.rewriteNested(rule, c.Clauses, inside, inside)
		if err != nil || !changed {
			return c, nil, err
		}
		return &NotJoinClause{JoinVars: c.JoinVars, Clauses: inner}, rules, nil
	case *OrClause:
		branches, rules, changed, err := w.rewriteBranches(rule, c.Branches, before, negated)
		if err != nil || !changed {
			return c, nil, err
		}
		return &OrClause{Branches: branches}, rules, nil
	case *OrJoinClause:
		branches, rules, changed, err := w.rewriteBranches(rule, c.Branches, before.within(c.JoinVars), negated.within(c.JoinVars))
		if err != nil || !changed {
			return c, nil, err
		}
		return &OrJoinClause{JoinVars: c.JoinVars, Branches: branches}, rules, nil
	case *OrDefaultClause:
		branches, rules, changed, err := w.rewriteBranches(rule, c.Branches, before, negated)
		if err != nil || !changed {
			return c, nil, err
		}
		return &OrDefaultClause{Branches: branches}, rules, nil
	case *OrDefaultJoinClause:
		header := append(append([]Symbol(nil), c.RequiredVars...), c.OutputVars...)
		branches, rules, changed, err := w.rewriteBranches(rule, c.Branches, before.within(header), negated.within(header))
		if err != nil || !changed {
			return c, nil, err
		}
		return &OrDefaultJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}, rules, nil
	case *SubqueryPattern:
		for _, inv := range Invocations(c.Query.Where) {
			w.includeOriginal(inv.Name)
		}
		return c, nil, nil
	default:
		return clause, nil, nil
	}
}

// rewriteNested rewrites a form's clause list and reports whether any
// clause changed, in which case the list is a new one.
func (w *magicRewriter) rewriteNested(rule Symbol, clauses []Clause, before, negated passing) ([]Clause, []*Rule, bool, error) {
	out := make([]Clause, len(clauses))
	var magicRules []*Rule
	changed := false
	for i, clause := range clauses {
		rewritten, rules, err := w.rewriteClause(rule, clause, before, negated)
		if err != nil {
			return nil, nil, false, err
		}
		out[i] = rewritten
		magicRules = append(magicRules, rules...)
		if rewritten != clause {
			changed = true
		}
	}
	return out, magicRules, changed, nil
}

// rewriteBranches rewrites each branch of a form, sharing the branches that
// hold no adorned invocation.
func (w *magicRewriter) rewriteBranches(rule Symbol, branches [][]Clause, before, negated passing) ([][]Clause, []*Rule, bool, error) {
	out := make([][]Clause, len(branches))
	var magicRules []*Rule
	changed := false
	for b, branch := range branches {
		rewritten, rules, branchChanged, err := w.rewriteNested(rule, branch, before, negated)
		if err != nil {
			return nil, nil, false, err
		}
		magicRules = append(magicRules, rules...)
		if branchChanged {
			out[b] = rewritten
			changed = true
		} else {
			out[b] = branch
		}
	}
	return out, magicRules, changed, nil
}

// adornInvocation adorns one invocation of the named rule under what passes
// into it: a position is bound when it holds a constant or a variable the
// bound set holds, a repeated variable at its first position only. A
// position the predicate requires bound that the adornment leaves free is
// RequiredBindingError. With no bound position the invocation stands and its
// predicate's original rules join the set. Otherwise the invocation names
// the adorned predicate, which is queued, and its magic rule derives the
// bound arguments from the prefix, a constant through a fresh variable a
// ground expression binds.
func (w *magicRewriter) adornInvocation(rule Symbol, inv *RuleInvocation, in passing) (Clause, []*Rule, error) {
	bound := make([]bool, len(inv.Args))
	seen := map[Symbol]bool{}
	for i, arg := range inv.Args {
		switch a := arg.(type) {
		case Constant, VectorConstant:
			bound[i] = true
		case Variable:
			if in.bound[a.Name] && !seen[a.Name] {
				bound[i] = true
			}
			seen[a.Name] = true
		}
	}
	if err := w.rs.CheckRequired(inv.Name, bound, rule); err != nil {
		return nil, nil, err
	}
	if countBound(bound) == 0 {
		w.includeOriginal(inv.Name)
		return inv, nil, nil
	}
	magic := magicSymbol(inv.Name, bound)
	var head []Symbol
	body := append([]Clause(nil), in.prefix...)
	for i, arg := range inv.Args {
		if !bound[i] {
			continue
		}
		switch a := arg.(type) {
		case Variable:
			head = append(head, a.Name)
		case Constant:
			fresh := datalog.NewSymbol("?" + magic.String() + "." + strconv.Itoa(i))
			head = append(head, fresh)
			body = append(body, &Expression{Function: &GroundFunction{Value: a.Value}, Binding: fresh})
		case VectorConstant:
			fresh := datalog.NewSymbol("?" + magic.String() + "." + strconv.Itoa(i))
			head = append(head, fresh)
			body = append(body, &Expression{Function: &GroundFunction{Value: a.Values}, Binding: fresh})
		}
	}
	w.enqueue(inv.Name, bound)
	adorned := &RuleInvocation{Name: adornedSymbol(inv.Name, bound), Args: inv.Args}
	return adorned, []*Rule{{Name: magic, Head: head, Body: body}}, nil
}
