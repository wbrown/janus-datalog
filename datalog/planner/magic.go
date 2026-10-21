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
// marks as bound. Each predicate the bindings reach gets a copy adorned with
// its binding pattern whose rules are guarded by a magic predicate holding
// the bindings the predicate is asked with. A root no position of which is
// bound gets an unguarded copy and no seed, so its rules evaluate whole, and
// the predicates with required positions its bodies reach are adorned from
// the clauses before their invocations, one rewrite and one evaluation
// deriving the predicate (NESTED_SCOPES_AS_QUERIES.md §5.5). The
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
	var seed query.Symbol
	var seeds []query.SeedPredicate
	if query.CountBound(bound) > 0 {
		seed = query.MagicSymbol(name, bound)
		seeds = []query.SeedPredicate{{Name: seed, Arity: query.CountBound(bound)}}
	}
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
		rewritten, err := query.NewSeededRuleSet(w.rules, seeds)
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

// scopeLevel is one scope on the path from a rule's body to an invocation, a
// form's branch or a subquery's inner query, as a magic rule reads it: a
// subquery of its own whose :in is in and whose call passes inputs, over the
// clauses the walk has passed at this level, since the level's variables are
// its own and never unify with the enclosing scope's. For a branch, in and
// inputs carry the sources, the rule set and the header symbols the
// enclosing scope binds, and header holds the whole header, each symbol of
// which stands for the enclosing scope's symbol of the same name. For a
// subquery, in and inputs are the call's own, names is the call's
// substitution, under which a bound :in symbol stands for the argument's
// variable, and constants holds the constant the call passes at an :in
// symbol, which a magic rule grounds as it grounds a constant argument of
// the invocation itself.
type scopeLevel struct {
	in        []query.InputSpec
	inputs    []query.PatternElement
	clauses   []query.Clause
	header    []query.Symbol
	names     query.Substitution
	constants map[query.Symbol]query.PatternElement
}

// passing is what passes into an invocation: the variables bound before it,
// the body's clauses a magic rule reads to derive them, the guard first, and
// the scopes entered below the body on the path to the invocation, outermost
// first, each with the clauses the walk has passed inside it.
// adornedRule is the adorned rule the walk is rewriting, the rule a
// refusal names a site under. nonMonotone holds when the passing enters a
// non-monotone form, a negation, a fallback form or an aggregating inner
// query, where a site the sealed passing cannot bind as its predicate
// requires is left unadorned.
type passing struct {
	bound       map[query.Symbol]bool
	prefix      []query.Clause
	levels      []scopeLevel
	adornedRule query.Symbol
	nonMonotone bool
}

// within is what passes into a header form's branches: the header's
// variables the enclosing scope binds, over the same clauses, since inside
// the form only the header's variables are the enclosing scope's, with the
// branch entered as a scope a magic rule reads through a subquery over those
// variables.
func (p passing) within(header []query.Symbol) passing {
	inside := p
	inside.bound = make(map[query.Symbol]bool, len(header))
	level := scopeLevel{
		in:     []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}},
		inputs: []query.PatternElement{query.Constant{Value: datalog.SymDollar}, query.Constant{Value: datalog.NewSymbol("%")}},
		header: header,
	}
	for _, sym := range header {
		if p.bound[sym] {
			inside.bound[sym] = true
			level.in = append(level.in, query.ScalarInput{Symbol: sym})
			level.inputs = append(level.inputs, query.Variable{Name: sym})
		}
	}
	inside.levels = append(append([]scopeLevel(nil), p.levels...), level)
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
// fixpoint evaluates it whole. A root with no bound position has no guard:
// its rules evaluate whole as the original's would, and the predicates with
// required positions they reach are adorned from the clauses before their
// invocations (NESTED_SCOPES_AS_QUERIES.md §5.5).
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
		var prefix []query.Clause
		if len(guardArgs) > 0 {
			prefix = []query.Clause{&query.RuleInvocation{Name: magic, Args: guardArgs}}
		}
		into := passing{bound: boundVars, prefix: prefix, adornedRule: adorned}
		body, magicRules, _, err := w.rewriteClauses(rule.Name, planned, into, into, false)
		if err != nil {
			return err
		}
		w.rules = append(w.rules, &query.Rule{Name: adorned, Head: rule.Head, Body: append(prefix, body...)})
		w.rules = append(w.rules, magicRules...)
	}
	return nil
}

// rewriteClauses walks a scope's clauses left to right, a rule's body, a
// form's branch, a not-join's body or a subquery's inner query, under what
// passes into the scope, before, and the passing sealed at the first clause
// that invokes a predicate of the rule's own stratum, negated, which sealed
// says the walk has already met, and reports whether any clause changed.
// Each clause is rewritten under what passes into it. A clause a magic rule
// reads joins the clauses the rule reads, the body's prefix at the body and
// the innermost scope's clauses below it, an or-default-join as the union of
// its branches over its header, which binds what the form binds and holds
// every tuple it yields, and the variables it provides join the bound set
// for the clauses after it; the clauses a magic rule reads are the guard and
// those before the one holding the invocation. A clause left out widens the
// bindings a magic rule derives and loses no tuple, since the adorned rule
// applies every clause. What passes into an invocation under a non-monotone
// form stops at the first clause that invokes a predicate of the rule's own
// stratum, so no magic rule depends through such a form on that stratum's
// adorned predicate. The bound set is the walk's own copy, so the scopes a
// caller walks under one passing share nothing of what each binds.
func (w *magicRewriter) rewriteClauses(rule query.Symbol, clauses []query.Clause, before, negated passing, sealed bool) ([]query.Clause, []*query.Rule, bool, error) {
	out := make([]query.Clause, len(clauses))
	var magicRules []*query.Rule
	before.bound = maps.Clone(before.bound)
	changed := false
	for i, clause := range clauses {
		if !sealed && w.invokesStratumOf(rule, clause) {
			negated = before
			negated.bound = maps.Clone(before.bound)
			sealed = true
		}
		rewritten, rules, err := w.rewriteClause(rule, clause, before, negated, sealed)
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
		read := rewritten
		if fallback, ok := rewritten.(*query.OrDefaultJoinClause); ok {
			read = &query.OrJoinClause{RequiredVars: fallback.RequiredVars, OutputVars: fallback.OutputVars, Branches: fallback.Branches}
		}
		before = before.reading(read)
		for _, sym := range query.ScopeOf(rewritten, w.rs).Provides {
			before.bound[sym] = true
		}
		if !sealed {
			negated = before
		}
	}
	return out, magicRules, changed, nil
}

// reading is the passing with clause joined to the clauses a magic rule
// reads: the innermost scope's when the passing is inside one, the body's
// prefix otherwise, each list extended on a copy so a sibling scope shares
// nothing of it.
func (p passing) reading(clause query.Clause) passing {
	if n := len(p.levels); n > 0 {
		levels := append([]scopeLevel(nil), p.levels...)
		last := &levels[n-1]
		last.clauses = append(last.clauses[:len(last.clauses):len(last.clauses)], clause)
		p.levels = levels
		return p
	}
	p.prefix = append(p.prefix[:len(p.prefix):len(p.prefix)], clause)
	return p
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
// clause, or an or-default-join read as the union of its branches over its
// header, every correlate of which the bound set holds. A not-join is never
// read, so no magic rule depends on a predicate through one; a clause
// reading a variable a later clause binds is not read, so every magic rule
// is safe.
func (w *magicRewriter) magicRuleReads(clause query.Clause, boundVars map[query.Symbol]bool) bool {
	if _, negation := clause.(*query.NotJoinClause); negation {
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
// as entering such a form. A form's branches are planned under its header
// before they are walked, so the clauses before an invocation inside a
// branch are the planner's, and each branch is walked as a scope, the
// branch's clauses passing into the invocations after them. A form on the
// path to an adorned invocation is copied; every other clause is shared.
// Inside a form only the header's variables are the enclosing scope's, so
// what passes into its branches is filtered to the header. An or-join's
// branches walk under the passing and its sealed twin as the body does,
// sealed where the body already is; a non-monotone form's branches walk
// under the sealed passing alone, which holds nothing of the rule's stratum,
// so the walk inside starts unsealed. Every form the walk meets carries a
// header, since the bodies and inner queries it walks are inferred
// (NewRuleSet, InferHeaders).
func (w *magicRewriter) rewriteClause(rule query.Symbol, clause query.Clause, before, negated passing, sealed bool) (query.Clause, []*query.Rule, error) {
	under := negated
	under.nonMonotone = true
	switch c := clause.(type) {
	case *query.RuleInvocation:
		return w.adornInvocation(rule, c, before)
	case *query.NotJoinClause:
		planned, err := w.plannedBranches(c.RequiredVars, nil, [][]query.Clause{c.Clauses})
		if err != nil {
			return nil, nil, err
		}
		inside := under.within(c.RequiredVars)
		inner, rules, changed, err := w.rewriteClauses(rule, planned[0], inside, inside, false)
		if err != nil || !changed {
			return c, nil, err
		}
		return &query.NotJoinClause{RequiredVars: c.RequiredVars, Clauses: inner}, rules, nil
	case *query.OrJoinClause:
		planned, err := w.plannedBranches(c.RequiredVars, c.OutputVars, c.Branches)
		if err != nil {
			return nil, nil, err
		}
		header := append(append([]query.Symbol(nil), c.RequiredVars...), c.OutputVars...)
		branches, rules, changed, err := w.rewriteBranches(rule, planned, before.within(header), negated.within(header), sealed)
		if err != nil || !changed {
			return c, nil, err
		}
		return &query.OrJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}, rules, nil
	case *query.OrDefaultJoinClause:
		planned, err := w.plannedBranches(c.RequiredVars, c.OutputVars, c.Branches)
		if err != nil {
			return nil, nil, err
		}
		header := append(append([]query.Symbol(nil), c.RequiredVars...), c.OutputVars...)
		inside := under.within(header)
		branches, rules, changed, err := w.rewriteBranches(rule, planned, inside, inside, false)
		if err != nil || !changed {
			return c, nil, err
		}
		return &query.OrDefaultJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}, rules, nil
	case *query.SubqueryPattern:
		if c.Query.Aggregates() {
			return w.rewriteSubquery(rule, c, under, under, false)
		}
		return w.rewriteSubquery(rule, c, before, negated, sealed)
	default:
		return clause, nil, nil
	}
}

// rewriteSubquery reads a subquery as a scope of the rewrite: each inner :in
// symbol stands for the argument at its place, so an inner symbol is bound
// when its argument is a constant or a variable the bound set holds. The
// inner query is planned under its data inputs as its required run and its
// find beyond them, and its clauses are walked in that order as a body's
// are, under what passes into the scope and under its sealed twin, each
// entered through enteringSubquery. A subquery holding no adorned
// invocation is shared; one on the path to an adorned invocation is copied
// over the inner query derived over the rewritten clauses.
func (w *magicRewriter) rewriteSubquery(rule query.Symbol, c *query.SubqueryPattern, before, negated passing, sealed bool) (query.Clause, []*query.Rule, error) {
	inside, err := before.enteringSubquery(c)
	if err != nil {
		return nil, nil, err
	}
	insideNegated, err := negated.enteringSubquery(c)
	if err != nil {
		return nil, nil, err
	}
	var required []query.Symbol
	for _, sym := range query.InputSymbols(c.Query.In) {
		if sym != nil && !query.ContainsSymbol(required, sym) {
			required = append(required, sym)
		}
	}
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
	where, rules, changed, err := w.rewriteClauses(rule, planned, inside, insideNegated, sealed)
	if err != nil || !changed {
		return c, nil, err
	}
	return &query.SubqueryPattern{Query: c.Query.WithWhere(where), Inputs: c.Inputs, Binding: c.Binding}, rules, nil
}

// enteringSubquery is what passes into a subquery's inner query: the inner
// :in symbols whose arguments are constants or variables the passing binds,
// over the same body clauses, with the call pushed as a scope whose names
// substitute each bound :in symbol by its argument's variable and whose
// constants hold the constant each :in symbol stands for.
func (p passing) enteringSubquery(c *query.SubqueryPattern) (passing, error) {
	inside := p
	inside.bound = map[query.Symbol]bool{}
	level := scopeLevel{in: c.Query.In, inputs: c.Inputs, constants: map[query.Symbol]query.PatternElement{}}
	var from, to []query.Symbol
	for position, sym := range query.InputSymbols(c.Query.In) {
		if sym == nil {
			continue
		}
		switch arg := c.Inputs[position].(type) {
		case query.Variable:
			if !p.bound[arg.Name] {
				continue
			}
			from, to = append(from, sym), append(to, arg.Name)
			inside.bound[sym] = true
		case query.Constant, query.VectorConstant:
			level.constants[sym] = arg
			inside.bound[sym] = true
		}
	}
	names, err := query.SubstitutionOf(from, to)
	if err != nil {
		return passing{}, err
	}
	level.names = names
	inside.levels = append(append([]scopeLevel(nil), p.levels...), level)
	return inside, nil
}

// rewriteBranches rewrites each branch of a form as a scope, sharing the
// branches that hold no adorned invocation.
func (w *magicRewriter) rewriteBranches(rule query.Symbol, branches [][]query.Clause, before, negated passing, sealed bool) ([][]query.Clause, []*query.Rule, bool, error) {
	out := make([][]query.Clause, len(branches))
	var magicRules []*query.Rule
	changed := false
	for b, branch := range branches {
		rewritten, rules, branchChanged, err := w.rewriteClauses(rule, branch, before, negated, sealed)
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
// body's prefix and the scopes on the path: a constant through a fresh
// variable a ground expression binds, named for the magic predicate and
// the position; a variable bound in the body under its own name; and a
// variable bound inside a scope through that scope's read, a subquery over
// the scope's clauses finding the variable under its name there and binding
// it to the name standing for it one scope out, each read nested inside the
// next scope's out to the body. A read finds the bound variables the
// scope's clauses provide, the inner read among them. A bound variable the
// scope's inputs alone bind has no attribute in the read's result, so it
// passes through under the name standing for it outside: a header symbol
// under the enclosing scope's symbol of its name, a bound :in symbol under
// its argument's variable, and one standing for a constant the call passes
// as that constant, grounded at the body. A scope whose clauses provide
// none of the bound variables contributes no read, since a clause left out
// widens the magic set and loses no tuple. A scope's local stands for a
// fresh variable named for the magic predicate and the position, which the
// reads outside carry up unchanged.
func (w *magicRewriter) adornInvocation(rule query.Symbol, inv *query.RuleInvocation, in passing) (query.Clause, []*query.Rule, error) {
	args := append([]query.PatternElement(nil), inv.Args...)
	bound := make([]bool, len(args))
	names := make([]query.Symbol, len(args))
	seen := map[query.Symbol]bool{}
	for i, arg := range args {
		switch a := arg.(type) {
		case query.Constant, query.VectorConstant:
			bound[i] = true
		case query.Variable:
			names[i] = a.Name
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
	fresh := func(i int) query.Symbol { return datalog.NewSymbol("?" + magic.String() + "." + strconv.Itoa(i)) }
	var read query.Clause
	for k := len(in.levels) - 1; k >= 0; k-- {
		level := in.levels[k]
		where := level.clauses
		if read != nil {
			where = append(where[:len(where):len(where)], read)
		}
		provided := map[query.Symbol]bool{}
		for _, clause := range where {
			for _, sym := range query.ScopeOf(clause, w.rs).Provides {
				provided[sym] = true
			}
		}
		var find []query.FindElement
		var binding []query.Symbol
		for i, arg := range args {
			if _, variable := arg.(query.Variable); !variable || !bound[i] {
				continue
			}
			if constant, passed := level.constants[names[i]]; passed && !provided[names[i]] {
				args[i] = constant
				continue
			}
			outside, named := level.names.Of(names[i])
			if !named && query.ContainsSymbol(level.header, names[i]) {
				outside, named = names[i], true
			}
			if !named {
				outside = fresh(i)
			}
			if provided[names[i]] {
				find = append(find, query.FindVariable{Symbol: names[i]})
				binding = append(binding, outside)
			}
			names[i] = outside
		}
		read = nil
		if len(find) > 0 {
			read = &query.SubqueryPattern{
				Query:   &query.Query{Find: find, In: level.in, Where: where},
				Inputs:  level.inputs,
				Binding: query.RelationBinding{Variables: binding},
			}
		}
	}
	body := append([]query.Clause(nil), in.prefix...)
	for i, arg := range args {
		if !bound[i] {
			continue
		}
		switch a := arg.(type) {
		case query.Constant:
			names[i] = fresh(i)
			body = append(body, &query.Expression{Function: &query.GroundFunction{Value: a.Value}, Binding: names[i]})
		case query.VectorConstant:
			names[i] = fresh(i)
			body = append(body, &query.Expression{Function: &query.GroundFunction{Value: a.Values}, Binding: names[i]})
		}
	}
	if read != nil {
		body = append(body, read)
	}
	var head []query.Symbol
	for i := range args {
		if bound[i] {
			head = append(head, names[i])
		}
	}
	w.enqueue(inv.Name, bound)
	adorned := &query.RuleInvocation{Name: query.AdornedSymbol(inv.Name, bound), Args: inv.Args}
	return adorned, []*query.Rule{{Name: magic, Head: head, Body: body}}, nil
}
