package planner

import (
	"errors"
	"maps"
	"strconv"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Magic is a rule set rewritten for one invocation with bound arguments.
// Rules derives Predicate, the invoked predicate adorned with the binding
// pattern, from Seed, the predicate the invocation's bound values fill, so
// evaluating Rules derives only the tuples those values reach.
type Magic struct {
	Rules     *query.RuleSet
	Predicate query.Symbol
	Seed      query.Symbol
}

// Rewrite rewrites the set for an invocation of name whose positions bound
// marks as bound, at least one of them. Each predicate the bindings reach
// gets a copy adorned with its binding pattern whose rules are guarded by a
// magic predicate holding the bindings the predicate is asked with. The
// bindings pass through each scope in the order the planner gives it, a
// body under its head's bound positions, a branch under its form's header,
// a not-join body under its header and a subquery's inner query under its
// data inputs, each planned through PlanScope under the set: a top-level
// invocation, or one inside an or or or-join form, is adorned by the
// constants it holds and the variables the head's bound positions and the
// planned clauses before it provide, and its magic rule derives those
// bindings from the guard and those clauses. An invocation under a
// non-monotone form, a not, not-join, or-default or or-default-join form or
// a subquery whose inner query aggregates, is adorned by the variables the
// head's bound positions and the planned clauses before the first that
// invokes a predicate of the rule's own stratum provide, and its magic rule
// reads those clauses: a clause after that invocation is adorned under what
// it binds, so a magic rule reading it would depend on the adorned predicate
// of the stratum, which depends through the form on the predicate inside
// it, a recursion through a non-monotone form. An invocation none of that
// binds, or one under a non-monotone form that it binds short of a position
// the predicate requires, keeps its name, and its predicate's original
// rules join the set unchanged; a positive invocation that leaves a
// position its predicate requires bound free is RequiredBindingError. A
// subquery is a scope of its own: its inner :in symbols stand for the
// call's arguments, an invocation inside the inner query is adorned by the
// arguments the clauses before the call bind and the inner clauses before
// it, and its magic rule reads the inner clauses as a subquery over the same
// call. Every rewritten scope holds its clauses in the planned order. The
// rewritten set is built through the checks NewRuleSet runs. A set the
// constructor refuses names sites under non-monotone forms whose adorned
// predicates lie in the rule's own stratum; those sites are left unadorned
// and the set walked again, until the constructor accepts it. Every round
// excludes a site the round before adorned, so the walk ends. The handler
// sees the plans the rewrite makes, as it sees the executor's.
func Rewrite(p QueryPlanner, rs *query.RuleSet, name query.Symbol, bound []bool, handler annotations.Handler) (*Magic, error) {
	predicate := query.AdornedSymbol(name, bound)
	seed := query.MagicSymbol(name, bound)
	excluded := map[nonMonotoneSite]bool{}
	for {
		w := &magicRewriter{
			rs:       rs,
			planner:  p,
			handler:  handler,
			adorned:  map[query.Symbol]bool{},
			original: map[query.Symbol]bool{},
			excluded: excluded,
		}
		w.enqueue(name, bound)
		for len(w.worklist) > 0 {
			next := w.worklist[0]
			w.worklist = w.worklist[1:]
			if err := w.rewritePredicate(next.name, next.bound); err != nil {
				return nil, err
			}
		}
		rewritten, err := query.NewSeededRuleSet(w.rules, []query.SeedPredicate{{Name: seed, Arity: query.CountBound(bound)}})
		if err == nil {
			return &Magic{Rules: rewritten, Predicate: predicate, Seed: seed}, nil
		}
		refused := w.refusedSites(err)
		if len(refused) == 0 {
			return nil, err
		}
		for _, site := range refused {
			excluded[site] = true
		}
	}
}

// refusedSites is the sites the walk adorned under non-monotone forms that
// the constructor's refusal names: under an UnstratifiableError, every site
// whose rule and adorned predicate both lie in the refused stratum; under a
// NestedRecursionError, every site in the rule it names adorning the
// predicate it names. Any other error names no site.
func (w *magicRewriter) refusedSites(err error) []nonMonotoneSite {
	var unstratifiable *query.UnstratifiableError
	var nested *query.NestedRecursionError
	var refused []nonMonotoneSite
	switch {
	case errors.As(err, &unstratifiable):
		for _, site := range w.sites {
			if query.ContainsSymbol(unstratifiable.Predicates, site.rule) && query.ContainsSymbol(unstratifiable.Predicates, site.target) {
				refused = append(refused, site)
			}
		}
	case errors.As(err, &nested):
		for _, site := range w.sites {
			if site.rule == nested.Rule && site.target == nested.Predicate {
				refused = append(refused, site)
			}
		}
	}
	return refused
}

// adornedPredicate is one predicate under one binding pattern.
type adornedPredicate struct {
	name  query.Symbol
	bound []bool
}

// magicRewriter accumulates the rewritten set: its rules in emission order,
// the adorned predicates already rewritten or queued, and the original
// predicates already included, under the planner that orders every scope it
// walks and the handler those plans report to.
// excluded holds the sites under non-monotone forms a refusal of the set
// named, left unadorned on the walk after it, and sites the sites the walk
// adorned under such forms, which a refusal is read against.
type magicRewriter struct {
	rs       *query.RuleSet
	planner  QueryPlanner
	handler  annotations.Handler
	rules    []*query.Rule
	adorned  map[query.Symbol]bool
	original map[query.Symbol]bool
	worklist []adornedPredicate
	excluded map[nonMonotoneSite]bool
	sites    []nonMonotoneSite
}

// nonMonotoneSite is an invocation under a non-monotone form, named by the
// adorned rule holding it and the adorned predicate adorning it would name.
type nonMonotoneSite struct {
	rule, target query.Symbol
}

// scopeCall is the call a subquery's inner query stands in: the inner
// query's inputs and the call's arguments, over which a magic rule of an
// invocation inside the inner query reads the inner clauses before it as a
// subquery of its own.
type scopeCall struct {
	in     []query.InputSpec
	inputs []query.PatternElement
}

// passing is what passes into an invocation: the variables bound before it,
// and the clauses a magic rule reads to derive them, the guard first. Inside
// a subquery it also carries the call the inner query stands in; scoped, the
// inner clauses before the invocation a magic rule reads, which the rule
// reads as a subquery over that call, since the inner query's variables are
// its own; names, the call's substitution, under which a magic rule's head
// names the argument a bound :in symbol stands for; and constants, the
// constant the call passes at an :in symbol, which a magic rule grounds as
// it grounds a constant argument of the invocation itself.
// adornedRule is the adorned rule the walk is rewriting, the rule a
// refusal names a site under. nonMonotone holds when the passing enters a
// non-monotone form, a negation, a fallback form or an aggregating inner
// query, where a site the sealed passing cannot bind as its predicate
// requires is left unadorned.
type passing struct {
	bound       map[query.Symbol]bool
	prefix      []query.Clause
	call        *scopeCall
	scoped      []query.Clause
	names       query.Substitution
	constants   map[query.Symbol]query.PatternElement
	adornedRule query.Symbol
	nonMonotone bool
}

// within is what passes into a join form's branches: the header's variables
// the enclosing scope binds, over the same clauses, since inside the form
// only the header's variables are the enclosing scope's.
func (p passing) within(header []query.Symbol) passing {
	inside := p
	inside.bound = make(map[query.Symbol]bool, len(header))
	for _, sym := range header {
		if p.bound[sym] {
			inside.bound[sym] = true
		}
	}
	return inside
}

// enqueue queues a predicate under a binding pattern once.
func (w *magicRewriter) enqueue(name query.Symbol, bound []bool) {
	key := query.AdornedSymbol(name, bound)
	if w.adorned[key] {
		return
	}
	w.adorned[key] = true
	w.worklist = append(w.worklist, adornedPredicate{name: name, bound: bound})
}

// includeOriginal adds a predicate's own rules, and those of every predicate
// they invoke at any depth, unchanged.
func (w *magicRewriter) includeOriginal(name query.Symbol) {
	if w.original[name] {
		return
	}
	w.original[name] = true
	for _, rule := range w.rs.Rules(name) {
		w.rules = append(w.rules, rule)
		for _, inv := range query.Invocations(rule.Body) {
			w.includeOriginal(inv.Name)
		}
	}
}

// planned is a scope's clauses in the planner's order: the scope planned
// through the planner under the set being rewritten, read as the rewrite
// reads a plan.
func (w *magicRewriter) planned(required []query.Symbol, find []query.FindElement, clauses []query.Clause) ([]query.Clause, error) {
	plan, err := w.planner.PlanScope(Scope{Required: required, Find: find, Clauses: clauses}, query.Bindings{Rules: w.rs}, w.handler)
	if err != nil {
		return nil, err
	}
	return plannedClauses(plan), nil
}

// rewritePredicate emits, for each rule of the predicate, the adorned rule
// guarded by the magic predicate over the bound head positions, then the
// magic rules its body's adorned invocations need. The body is planned as
// the scope it is, the bound head positions its required run and the free
// ones its find, and walked in that order. The adorned rule has no required
// run: its guard binds every position the predicate requires, so the
// fixpoint evaluates it whole.
func (w *magicRewriter) rewritePredicate(name query.Symbol, bound []bool) error {
	adorned := query.AdornedSymbol(name, bound)
	magic := query.MagicSymbol(name, bound)
	for _, rule := range w.rs.Rules(name) {
		boundVars := map[query.Symbol]bool{}
		var guardArgs []query.PatternElement
		var required []query.Symbol
		var find []query.FindElement
		for i, head := range rule.Head {
			if bound[i] {
				guardArgs = append(guardArgs, query.Variable{Name: head})
				boundVars[head] = true
				if !query.ContainsSymbol(required, head) {
					required = append(required, head)
				}
			} else if !query.ContainsSymbol(required, head) {
				find = append(find, query.FindVariable{Symbol: head})
			}
		}
		planned, err := w.planned(required, find, rule.Body)
		if err != nil {
			return err
		}
		prefix := []query.Clause{&query.RuleInvocation{Name: magic, Args: guardArgs}}
		body, magicRules, _, err := w.rewriteClauses(rule.Name, planned, passing{bound: boundVars, prefix: prefix, adornedRule: adorned})
		if err != nil {
			return err
		}
		w.rules = append(w.rules, &query.Rule{Name: adorned, Head: rule.Head, Body: append(prefix, body...)})
		w.rules = append(w.rules, magicRules...)
	}
	return nil
}

// rewriteClauses walks a scope's clauses left to right, a rule's body or a
// subquery's inner query, under what passes into the scope, and reports
// whether any clause changed. Each clause is rewritten under what passes
// into it. A clause a magic rule reads joins the clauses the rule reads, the
// prefix for a body and the scoped clauses inside a subquery, and the
// variables it provides join the bound set for the clauses after it; the
// prefix a magic rule reads is the guard and those clauses before the one
// holding the invocation. A clause left out widens the bindings a magic
// rule derives and loses no tuple, since the adorned rule applies every
// clause. What passes into an invocation under a non-monotone form stops at
// the first clause that invokes a predicate of the rule's own stratum, so
// no magic rule depends through such a form on that stratum's adorned
// predicate.
func (w *magicRewriter) rewriteClauses(rule query.Symbol, clauses []query.Clause, before passing) ([]query.Clause, []*query.Rule, bool, error) {
	out := make([]query.Clause, len(clauses))
	var magicRules []*query.Rule
	negated := before
	sealed := false
	changed := false
	for i, clause := range clauses {
		if !sealed && w.invokesStratumOf(rule, clause) {
			negated = before
			negated.bound = maps.Clone(before.bound)
			sealed = true
		}
		rewritten, rules, err := w.rewriteClause(rule, clause, before, negated)
		if err != nil {
			return nil, nil, false, err
		}
		out[i] = rewritten
		if rewritten != clause {
			changed = true
		}
		magicRules = append(magicRules, rules...)
		if !w.magicRuleReads(rewritten, before.bound) {
			continue
		}
		if before.call != nil {
			before.scoped = append(before.scoped[:len(before.scoped):len(before.scoped)], rewritten)
		} else {
			before.prefix = append(before.prefix[:len(before.prefix):len(before.prefix)], rewritten)
		}
		for _, sym := range query.ScopeOf(rewritten, w.rs).Provides {
			before.bound[sym] = true
		}
		if !sealed {
			negated = before
		}
	}
	return out, magicRules, changed, nil
}

// invokesStratumOf reports whether the clause invokes, at any depth, a
// predicate of the named rule's stratum.
func (w *magicRewriter) invokesStratumOf(rule query.Symbol, clause query.Clause) bool {
	for _, inv := range query.Invocations([]query.Clause{clause}) {
		if w.rs.SameStratum(rule, inv.Name) {
			return true
		}
	}
	return false
}

// magicRuleReads reports whether a magic rule reads clause: a monotone
// clause every correlate of which the bound set holds. A negation or a
// fallback form is never read, so no magic rule depends on a predicate
// through one; a clause reading a variable a later clause binds is not read,
// so every magic rule is safe.
func (w *magicRewriter) magicRuleReads(clause query.Clause, boundVars map[query.Symbol]bool) bool {
	switch clause.(type) {
	case *query.NotClause, *query.NotJoinClause, *query.OrDefaultClause, *query.OrDefaultJoinClause:
		return false
	}
	for _, sym := range query.ScopeOf(clause, w.rs).Correlates {
		if !sym.IsSource() && !boundVars[sym] {
			return false
		}
	}
	return true
}

// plannedBranches plans each branch of a header form as the scope it is: the
// form's required run its required run, the form's outputs its find as
// variables, and the branch's clauses as written, answered in the planner's
// order. A not-join's one body is planned the same way under its all-required
// header, with no outputs.
func (w *magicRewriter) plannedBranches(required, outputs []query.Symbol, branches [][]query.Clause) ([][]query.Clause, error) {
	find := make([]query.FindElement, len(outputs))
	for i, sym := range outputs {
		find[i] = query.FindVariable{Symbol: sym}
	}
	planned := make([][]query.Clause, len(branches))
	for b, branch := range branches {
		ordered, err := w.planned(required, find, branch)
		if err != nil {
			return nil, err
		}
		planned[b] = ordered
	}
	return planned, nil
}

// rewriteClause adorns the invocations in one clause of the named rule: a
// positive invocation under what the clauses before pass, and one under a
// non-monotone form, a not, not-join, or-default or or-default-join form or
// a subquery whose inner query aggregates, at any depth under it, under the
// passing sealed at the first invocation of the rule's own stratum, marked
// as entering such a form. A header form's branches are planned under its
// header before they are walked, so the clauses before an invocation inside
// a branch are the planner's; a headerless form is walked as written. A
// form on the path to an adorned invocation is copied; every other clause
// is shared. Inside a join form only the header's variables are the
// enclosing scope's, so what passes into its branches is filtered to the
// header.
func (w *magicRewriter) rewriteClause(rule query.Symbol, clause query.Clause, before, negated passing) (query.Clause, []*query.Rule, error) {
	under := negated
	under.nonMonotone = true
	switch c := clause.(type) {
	case *query.RuleInvocation:
		return w.adornInvocation(rule, c, before)
	case *query.NotClause:
		inner, rules, changed, err := w.rewriteNested(rule, c.Clauses, under, under)
		if err != nil || !changed {
			return c, nil, err
		}
		return &query.NotClause{Clauses: inner}, rules, nil
	case *query.NotJoinClause:
		planned, err := w.plannedBranches(c.RequiredVars, nil, [][]query.Clause{c.Clauses})
		if err != nil {
			return nil, nil, err
		}
		inside := under.within(c.RequiredVars)
		inner, rules, changed, err := w.rewriteNested(rule, planned[0], inside, inside)
		if err != nil || !changed {
			return c, nil, err
		}
		return &query.NotJoinClause{RequiredVars: c.RequiredVars, Clauses: inner}, rules, nil
	case *query.OrClause:
		branches, rules, changed, err := w.rewriteBranches(rule, c.Branches, before, negated)
		if err != nil || !changed {
			return c, nil, err
		}
		return &query.OrClause{Branches: branches}, rules, nil
	case *query.OrJoinClause:
		planned, err := w.plannedBranches(c.RequiredVars, c.OutputVars, c.Branches)
		if err != nil {
			return nil, nil, err
		}
		header := append(append([]query.Symbol(nil), c.RequiredVars...), c.OutputVars...)
		branches, rules, changed, err := w.rewriteBranches(rule, planned, before.within(header), negated.within(header))
		if err != nil || !changed {
			return c, nil, err
		}
		return &query.OrJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}, rules, nil
	case *query.OrDefaultClause:
		branches, rules, changed, err := w.rewriteBranches(rule, c.Branches, under, under)
		if err != nil || !changed {
			return c, nil, err
		}
		return &query.OrDefaultClause{Branches: branches}, rules, nil
	case *query.OrDefaultJoinClause:
		planned, err := w.plannedBranches(c.RequiredVars, c.OutputVars, c.Branches)
		if err != nil {
			return nil, nil, err
		}
		header := append(append([]query.Symbol(nil), c.RequiredVars...), c.OutputVars...)
		inside := under.within(header)
		branches, rules, changed, err := w.rewriteBranches(rule, planned, inside, inside)
		if err != nil || !changed {
			return c, nil, err
		}
		return &query.OrDefaultJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}, rules, nil
	case *query.SubqueryPattern:
		if c.Query.Aggregates() {
			return w.rewriteSubquery(rule, c, under)
		}
		return w.rewriteSubquery(rule, c, before)
	default:
		return clause, nil, nil
	}
}

// rewriteSubquery reads a subquery as a scope of the rewrite: each inner :in
// symbol stands for the argument at its place, so an inner symbol is bound
// when its argument is a constant or a variable the bound set holds. The
// inner query is planned under its data inputs as its required run and its
// find beyond them, and its clauses are walked in that order as a body's
// are, under what passes into the scope: the body's prefix, the call, the
// substitution of the bound :in symbols by the names the arguments they
// stand for carry in the magic rules, and the constants the call passes. A
// subquery holding no adorned invocation is shared; one on the path to an
// adorned invocation is copied over the inner query derived over the
// rewritten clauses.
func (w *magicRewriter) rewriteSubquery(rule query.Symbol, c *query.SubqueryPattern, before passing) (query.Clause, []*query.Rule, error) {
	inside := passing{
		bound:       map[query.Symbol]bool{},
		prefix:      before.prefix,
		call:        &scopeCall{in: c.Query.In, inputs: c.Inputs},
		constants:   map[query.Symbol]query.PatternElement{},
		adornedRule: before.adornedRule,
		nonMonotone: before.nonMonotone,
	}
	var from, to, required []query.Symbol
	for position, sym := range query.InputSymbols(c.Query.In) {
		if sym == nil {
			continue
		}
		if !query.ContainsSymbol(required, sym) {
			required = append(required, sym)
		}
		switch arg := c.Inputs[position].(type) {
		case query.Variable:
			if !before.bound[arg.Name] {
				continue
			}
			name := arg.Name
			if standsFor, ok := before.names.Of(arg.Name); ok {
				name = standsFor
			}
			from, to = append(from, sym), append(to, name)
			inside.bound[sym] = true
		case query.Constant, query.VectorConstant:
			inside.constants[sym] = arg
			inside.bound[sym] = true
		}
	}
	subst, err := query.SubstitutionOf(from, to)
	if err != nil {
		return nil, nil, err
	}
	inside.names = subst
	var find []query.FindElement
	for _, elem := range c.Query.Find {
		if v, ok := elem.(query.FindVariable); ok && query.ContainsSymbol(required, v.Symbol) {
			continue
		}
		find = append(find, elem)
	}
	planned, err := w.planned(required, find, c.Query.Where)
	if err != nil {
		return nil, nil, err
	}
	where, rules, changed, err := w.rewriteClauses(rule, planned, inside)
	if err != nil || !changed {
		return c, nil, err
	}
	return &query.SubqueryPattern{Query: c.Query.WithWhere(where), Inputs: c.Inputs, Binding: c.Binding}, rules, nil
}

// rewriteNested rewrites a form's clause list and reports whether any
// clause changed, in which case the list is a new one.
func (w *magicRewriter) rewriteNested(rule query.Symbol, clauses []query.Clause, before, negated passing) ([]query.Clause, []*query.Rule, bool, error) {
	out := make([]query.Clause, len(clauses))
	var magicRules []*query.Rule
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
func (w *magicRewriter) rewriteBranches(rule query.Symbol, branches [][]query.Clause, before, negated passing) ([][]query.Clause, []*query.Rule, bool, error) {
	out := make([][]query.Clause, len(branches))
	var magicRules []*query.Rule
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
// into it: a position is bound when it holds a constant, a variable the
// bound set holds, or a variable standing for a constant the call passes, a
// repeated variable at its first position only. A position the predicate
// requires bound that the adornment leaves free is RequiredBindingError at
// a positive site. With no bound position, with a required position free
// under a non-monotone form, or at a site under such a form that a refusal
// of the set excluded, the invocation stands and its predicate's original
// rules join the set. Otherwise the invocation names the adorned predicate,
// which is queued, a site under a non-monotone form is recorded for a
// refusal to name, and its magic rule derives the bound arguments from the
// prefix: a bound variable under the name standing for it, a constant
// through a fresh variable a ground expression binds, named for the magic
// predicate and the position, and, inside a subquery whose clauses before
// the invocation a magic rule reads, every bound variable through a subquery
// over the same call, finding those variables over those clauses and
// binding each to the name standing for it, an inner local's a fresh
// variable named the same way.
func (w *magicRewriter) adornInvocation(rule query.Symbol, inv *query.RuleInvocation, in passing) (query.Clause, []*query.Rule, error) {
	args := make([]query.PatternElement, len(inv.Args))
	for i, arg := range inv.Args {
		args[i] = arg
		if v, ok := arg.(query.Variable); ok && len(in.scoped) == 0 {
			if constant, passed := in.constants[v.Name]; passed {
				args[i] = constant
			}
		}
	}
	bound := make([]bool, len(args))
	seen := map[query.Symbol]bool{}
	for i, arg := range args {
		switch a := arg.(type) {
		case query.Constant, query.VectorConstant:
			bound[i] = true
		case query.Variable:
			if in.bound[a.Name] && !seen[a.Name] {
				bound[i] = true
			}
			seen[a.Name] = true
		}
	}
	if err := w.rs.CheckRequired(inv.Name, bound, rule); err != nil {
		if in.nonMonotone {
			w.includeOriginal(inv.Name)
			return inv, nil, nil
		}
		return nil, nil, err
	}
	if query.CountBound(bound) == 0 {
		w.includeOriginal(inv.Name)
		return inv, nil, nil
	}
	if in.nonMonotone {
		site := nonMonotoneSite{rule: in.adornedRule, target: query.AdornedSymbol(inv.Name, bound)}
		if w.excluded[site] {
			w.includeOriginal(inv.Name)
			return inv, nil, nil
		}
		w.sites = append(w.sites, site)
	}
	magic := query.MagicSymbol(inv.Name, bound)
	var head []query.Symbol
	body := append([]query.Clause(nil), in.prefix...)
	var found []query.FindElement
	var foundNames []query.Symbol
	for i, arg := range args {
		if !bound[i] {
			continue
		}
		fresh := datalog.NewSymbol("?" + magic.String() + "." + strconv.Itoa(i))
		switch a := arg.(type) {
		case query.Variable:
			name, named := in.names.Of(a.Name)
			switch {
			case named:
			case in.call != nil:
				name = fresh
			default:
				name = a.Name
			}
			head = append(head, name)
			if len(in.scoped) > 0 {
				found = append(found, query.FindVariable{Symbol: a.Name})
				foundNames = append(foundNames, name)
			}
		case query.Constant:
			head = append(head, fresh)
			body = append(body, &query.Expression{Function: &query.GroundFunction{Value: a.Value}, Binding: fresh})
		case query.VectorConstant:
			head = append(head, fresh)
			body = append(body, &query.Expression{Function: &query.GroundFunction{Value: a.Values}, Binding: fresh})
		}
	}
	if len(found) > 0 {
		body = append(body, &query.SubqueryPattern{
			Query:   &query.Query{Find: found, In: in.call.in, Where: in.scoped},
			Inputs:  in.call.inputs,
			Binding: query.RelationBinding{Variables: foundNames},
		})
	}
	w.enqueue(inv.Name, bound)
	adorned := &query.RuleInvocation{Name: query.AdornedSymbol(inv.Name, bound), Args: inv.Args}
	return adorned, []*query.Rule{{Name: magic, Head: head, Body: body}}, nil
}
