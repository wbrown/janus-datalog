package query

import (
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
// one inside a not, not-join, or, or-join, or-default or or-default-join
// form, is adorned by the constants it holds and the variables the head's
// bound positions and the top-level clauses before it provide, and its magic
// rule derives those bindings from the guard and those clauses. An
// invocation none of that binds keeps its name, and its predicate's original
// rules join the set unchanged; one that leaves a position its predicate
// requires bound free is RequiredBindingError. A subquery body is not
// entered. The rewritten set is built through the checks NewRuleSet runs,
// and a set that does not stratify is returned as that error: the decline.
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
// magic rules its body's adorned invocations need. The adorned rule keeps
// the required run.
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
		body, magicRules, err := w.rewriteBody(rule, boundVars, prefix)
		if err != nil {
			return err
		}
		w.rules = append(w.rules, &Rule{Name: adorned, Head: rule.Head, Required: rule.Required, Body: append(prefix, body...)})
		w.rules = append(w.rules, magicRules...)
	}
	return nil
}

// rewriteBody walks the rule's top-level clauses left to right. Each clause
// is rewritten under the variables bound so far, and the clauses it provides
// join the bound set for the clauses after it; the prefix a magic rule reads
// is the guard and the rewritten clauses before the one holding the
// invocation.
func (w *magicRewriter) rewriteBody(rule *Rule, boundVars map[Symbol]bool, prefix []Clause) ([]Clause, []*Rule, error) {
	out := make([]Clause, len(rule.Body))
	var magicRules []*Rule
	for i, clause := range rule.Body {
		rewritten, rules, err := w.rewriteClause(rule.Name, clause, boundVars, prefix)
		if err != nil {
			return nil, nil, err
		}
		out[i] = rewritten
		magicRules = append(magicRules, rules...)
		prefix = append(prefix[:len(prefix):len(prefix)], rewritten)
		for _, sym := range ScopeOf(rewritten, w.rs).Provides {
			boundVars[sym] = true
		}
	}
	return out, magicRules, nil
}

// rewriteClause adorns the invocations in one clause of the named rule. A
// form on the path to an adorned invocation is copied; every other clause
// is shared. Inside a join form only the header's variables are the
// enclosing scope's, so the bound set entering its branches is filtered to
// the header.
func (w *magicRewriter) rewriteClause(rule Symbol, clause Clause, boundVars map[Symbol]bool, prefix []Clause) (Clause, []*Rule, error) {
	switch c := clause.(type) {
	case *RuleInvocation:
		return w.adornInvocation(rule, c, boundVars, prefix)
	case *NotClause:
		inner, rules, changed, err := w.rewriteNested(rule, c.Clauses, boundVars, prefix)
		if err != nil || !changed {
			return c, nil, err
		}
		return &NotClause{Clauses: inner}, rules, nil
	case *NotJoinClause:
		inner, rules, changed, err := w.rewriteNested(rule, c.Clauses, headerBound(boundVars, c.JoinVars), prefix)
		if err != nil || !changed {
			return c, nil, err
		}
		return &NotJoinClause{JoinVars: c.JoinVars, Clauses: inner}, rules, nil
	case *OrClause:
		branches, rules, changed, err := w.rewriteBranches(rule, c.Branches, boundVars, prefix)
		if err != nil || !changed {
			return c, nil, err
		}
		return &OrClause{Branches: branches}, rules, nil
	case *OrJoinClause:
		branches, rules, changed, err := w.rewriteBranches(rule, c.Branches, headerBound(boundVars, c.JoinVars), prefix)
		if err != nil || !changed {
			return c, nil, err
		}
		return &OrJoinClause{JoinVars: c.JoinVars, Branches: branches}, rules, nil
	case *OrDefaultClause:
		branches, rules, changed, err := w.rewriteBranches(rule, c.Branches, boundVars, prefix)
		if err != nil || !changed {
			return c, nil, err
		}
		return &OrDefaultClause{Branches: branches}, rules, nil
	case *OrDefaultJoinClause:
		header := append(append([]Symbol(nil), c.RequiredVars...), c.OutputVars...)
		branches, rules, changed, err := w.rewriteBranches(rule, c.Branches, headerBound(boundVars, header), prefix)
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

// headerBound is the bound set a join form's branches see: the variables of
// its header that the enclosing scope binds.
func headerBound(boundVars map[Symbol]bool, header []Symbol) map[Symbol]bool {
	inside := make(map[Symbol]bool, len(header))
	for _, sym := range header {
		if boundVars[sym] {
			inside[sym] = true
		}
	}
	return inside
}

// rewriteNested rewrites a form's clause list and reports whether any
// clause changed, in which case the list is a new one.
func (w *magicRewriter) rewriteNested(rule Symbol, clauses []Clause, boundVars map[Symbol]bool, prefix []Clause) ([]Clause, []*Rule, bool, error) {
	out := make([]Clause, len(clauses))
	var magicRules []*Rule
	changed := false
	for i, clause := range clauses {
		rewritten, rules, err := w.rewriteClause(rule, clause, boundVars, prefix)
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
func (w *magicRewriter) rewriteBranches(rule Symbol, branches [][]Clause, boundVars map[Symbol]bool, prefix []Clause) ([][]Clause, []*Rule, bool, error) {
	out := make([][]Clause, len(branches))
	var magicRules []*Rule
	changed := false
	for b, branch := range branches {
		rewritten, rules, branchChanged, err := w.rewriteNested(rule, branch, boundVars, prefix)
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

// adornInvocation adorns one invocation of the named rule: a position is
// bound when it holds a constant or a variable the bound set holds, a
// repeated variable at its first position only. A position the predicate
// requires bound that the adornment leaves free is RequiredBindingError.
// With no bound position the invocation stands and its predicate's original
// rules join the set. Otherwise the invocation names the adorned predicate,
// which is queued, and its magic rule derives the bound arguments from the
// prefix, a constant through a fresh variable a ground expression binds.
func (w *magicRewriter) adornInvocation(rule Symbol, inv *RuleInvocation, boundVars map[Symbol]bool, prefix []Clause) (Clause, []*Rule, error) {
	bound := make([]bool, len(inv.Args))
	seen := map[Symbol]bool{}
	for i, arg := range inv.Args {
		switch a := arg.(type) {
		case Constant, VectorConstant:
			bound[i] = true
		case Variable:
			if boundVars[a.Name] && !seen[a.Name] {
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
	body := append([]Clause(nil), prefix...)
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
