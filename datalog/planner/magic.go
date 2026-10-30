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
// planned clauses before it that shared variables connect to them, or to a
// pattern with an entity constant, provide, and at a position the predicate
// requires by a variable any planned clause before it provides, and its
// magic rule derives those bindings from the guard, those clauses and the
// extent chains of the required positions
// (A_CLAUSE_OUTSIDE_THE_SEED_COMPONENT_BINDS_NOTHING_PROOF.md). An
// invocation under a non-monotone form, a not, not-join, or-default or
// or-default-join form or a subquery whose inner query aggregates, is
// adorned the same way by the head's bound positions and the planned
// clauses before the first that invokes a predicate of the rule's own
// stratum, and its magic rule reads those clauses: a clause after that
// invocation is adorned under what it binds, so a magic rule reading it
// would depend on the adorned predicate of the stratum, which depends
// through the form on the predicate inside it, a recursion through a
// non-monotone form. A magic rule reads no
// negation and no aggregating subquery, at any depth of what it reads, so
// no magic rule depends non-monotonically on any predicate
// (magicRuleReads). An invocation none of that binds keeps its name and its
// predicate's original rules, unless a rule of its predicate or of one its
// rules reach invokes a predicate with required positions, in which case it
// names the predicate adorned all free, at every level, so each required
// predicate is adorned from the clauses before its invocation. One under a
// non-monotone form that the passing binds short of a position the
// predicate requires keeps its name and the original rules; a positive
// invocation that leaves a position its predicate requires bound free is
// RequiredBindingError. A
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
// subquery of its own over the clauses the walk has passed at this level,
// since the level's variables are its own and never unify with the enclosing
// scope's. roots is the level's seed, the symbols it is given that the
// enclosing scope's bound set holds, and extentRoots the symbols it is given
// that the enclosing scope's extent set holds; which of them a read passes
// is decided at the invocation from the clauses the read carries
// (adornInvocation, passes). For a branch, given holds the symbols the
// branch is given (GivenSymbols), each of which stands for the enclosing
// scope's symbol of the same name; a header symbol the branch is not given,
// a fallback's output, is the branch's own. For a subquery, call is the call,
// whose :in and arguments a read restricts to the symbols it passes
// (passedInputs), names is the call's substitution, under which a bound :in
// symbol stands for the argument's variable, and constants holds the
// constant the call passes at an :in symbol, which a magic rule grounds as
// it grounds a constant argument of the invocation itself.
type scopeLevel struct {
	clauses     []query.Clause
	roots       map[query.Symbol]bool
	extentRoots map[query.Symbol]bool
	given       []query.Symbol
	call        *query.SubqueryPattern
	names       query.Substitution
	constants   map[query.Symbol]query.PatternElement
}

// passing is what passes into an invocation: the bound set, the variables
// of the seed and of the clauses before it that shared variables connect to
// the seed or to a pattern with an entity constant; the extent set, the
// symbols the other clauses before it provide and the extent roots the scope
// is given, less the bound set
// (A_CLAUSE_OUTSIDE_THE_SEED_COMPONENT_BINDS_NOTHING_PROOF.md §1); head,
// the body's seed, its bound head positions; the body's clauses a magic rule
// may read, the guard first; and the scopes entered below the body on the
// path to the invocation, outermost first, each with the clauses the walk
// has passed inside it. bound and extent are the innermost scope's.
// adornedRule is the adorned rule the walk is rewriting, the rule a
// refusal names a site under. nonMonotone holds when the passing enters a
// non-monotone form, a negation, a fallback form or an aggregating inner
// query, where a site the sealed passing cannot bind as its predicate
// requires is left unadorned.
type passing struct {
	bound       map[query.Symbol]bool
	extent      map[query.Symbol]bool
	head        map[query.Symbol]bool
	prefix      []query.Clause
	levels      []scopeLevel
	adornedRule query.Symbol
	nonMonotone bool
}

// within is what passes into a header form's branches: of the symbols the
// branches are given (GivenSymbols), those in the enclosing scope's bound
// set seed the branch and those in its extent set root the branch's extent
// set, over the same clauses, since inside the form only the given symbols
// are the enclosing scope's, with the branch entered as a scope a magic rule
// reads through a subquery over the given symbols its read carries.
func (p passing) within(given []query.Symbol) passing {
	inside := p
	inside.bound = make(map[query.Symbol]bool, len(given))
	inside.extent = make(map[query.Symbol]bool, len(given))
	level := scopeLevel{roots: map[query.Symbol]bool{}, extentRoots: map[query.Symbol]bool{}, given: given}
	for _, sym := range given {
		switch {
		case p.bound[sym]:
			inside.bound[sym] = true
			level.roots[sym] = true
		case p.extent[sym]:
			inside.extent[sym] = true
			level.extentRoots[sym] = true
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
// through the planner under the set being rewritten, with every invocation
// held, since the rewrite decides each site itself, read as the rewrite
// reads a plan.
func (w *magicRewriter) planned(required []query.Symbol, find []query.FindElement, clauses []query.Clause) ([]query.Clause, error) {
	plan, err := w.planner.PlanScope(Scope{Required: required, Find: find, Clauses: clauses}, query.Bindings{Rules: w.rs, Held: query.HeldEvery}, w.handler)
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
		into := passing{bound: boundVars, head: boundVars, prefix: prefix, adornedRule: adorned}
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
// may read, one every correlate of which the bound set or the extent set
// holds, joins the clauses the scope has read, the body's prefix at the body
// and the innermost scope's clauses below it, an or-default-join as the
// union of its branches over its header, which binds what the form binds
// and holds every tuple it yields, and the two sets are recomputed over the
// clauses read (split): the bound set is the seed and the variables of the
// clauses shared variables connect to it or to a pattern with an entity
// constant, in any order, and the extent set is what the other clauses
// provide. The clauses a magic rule reads are the guard and, of those before
// the one holding the invocation, the seed-connected ones and the extent
// chains the invocation needs (adornInvocation). A clause left out widens
// the bindings a magic rule derives and loses no tuple, since the adorned
// rule applies every clause. What passes into an invocation under a
// non-monotone form stops at the first clause that invokes a predicate of
// the rule's own stratum, so no magic rule depends through such a form on
// that stratum's adorned predicate. The two sets are the walk's own copies,
// so the scopes a caller walks under one passing share nothing of what each
// binds.
func (w *magicRewriter) rewriteClauses(rule query.Symbol, clauses []query.Clause, before, negated passing, sealed bool) ([]query.Clause, []*query.Rule, bool, error) {
	out := make([]query.Clause, len(clauses))
	var magicRules []*query.Rule
	before.bound = maps.Clone(before.bound)
	before.extent = maps.Clone(before.extent)
	changed := false
	for i, clause := range clauses {
		if !sealed && w.invokesStratumOf(rule, clause) {
			negated = before
			negated.bound = maps.Clone(before.bound)
			negated.extent = maps.Clone(before.extent)
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
		read, ok := w.magicRuleReads(rewritten, before.available())
		if !ok {
			continue
		}
		before = before.reading(read)
		before.bound, before.extent = w.split(before)
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

// available is the symbols the innermost scope holds before the current
// clause, the bound set and the extent set together: a clause a magic rule
// may read correlates on these alone.
func (p passing) available() map[query.Symbol]bool {
	all := make(map[query.Symbol]bool, len(p.bound)+len(p.extent))
	for sym := range p.bound {
		all[sym] = true
	}
	for sym := range p.extent {
		all[sym] = true
	}
	return all
}

// scope is the innermost scope the passing stands in: its seed, its extent
// roots and the clauses a magic rule may read in it so far, the last
// level's below the body and the body's own above it.
func (p passing) scope() (roots, extentRoots map[query.Symbol]bool, clauses []query.Clause) {
	if n := len(p.levels); n > 0 {
		level := p.levels[n-1]
		return level.roots, level.extentRoots, level.clauses
	}
	return p.head, nil, p.prefix
}

// split is the bound set and the extent set of the passing's innermost scope
// over the clauses it has read: the variables of the seed's connected
// component (seedOf), and the extent roots and the provides of the other
// clauses less them
// (A_CLAUSE_OUTSIDE_THE_SEED_COMPONENT_BINDS_NOTHING_PROOF.md §1).
func (w *magicRewriter) split(p passing) (bound, extent map[query.Symbol]bool) {
	roots, extentRoots, clauses := p.scope()
	bound = w.seedOf(roots, clauses)
	extent = map[query.Symbol]bool{}
	for sym := range extentRoots {
		if !bound[sym] {
			extent[sym] = true
		}
	}
	for _, clause := range clauses {
		if w.seedConnected(clause, bound) {
			continue
		}
		for _, sym := range query.ScopeOf(clause, w.rs).Provides {
			if !bound[sym] {
				extent[sym] = true
			}
		}
	}
	return bound, extent
}

// seedOf is the variables of a scope's seed component over the clauses
// read: the roots, and the variables of every clause shared variables
// connect to them or to a data pattern with an entity constant, in any
// order.
func (w *magicRewriter) seedOf(roots map[query.Symbol]bool, clauses []query.Clause) map[query.Symbol]bool {
	seed := make(map[query.Symbol]bool, len(roots))
	for sym := range roots {
		seed[sym] = true
	}
	for grown := true; grown; {
		grown = false
		for _, clause := range clauses {
			if !w.seedConnected(clause, seed) {
				continue
			}
			for _, sym := range query.FreeVariables([]query.Clause{clause}, w.rs) {
				if !sym.IsSource() && !seed[sym] {
					seed[sym] = true
					grown = true
				}
			}
		}
	}
	return seed
}

// seedConnected reports whether a clause lies in the seed component whose
// variables seed holds: it is a data pattern with a constant at the entity
// position, which names one entity, or a variable of it is in seed.
func (w *magicRewriter) seedConnected(clause query.Clause, seed map[query.Symbol]bool) bool {
	if entityConstant(clause) {
		return true
	}
	for _, sym := range query.FreeVariables([]query.Clause{clause}, w.rs) {
		if seed[sym] {
			return true
		}
	}
	return false
}

// entityConstant reports whether a clause is a data pattern with a constant
// at the entity position.
func entityConstant(clause query.Clause) bool {
	pattern, ok := clause.(*query.DataPattern)
	if !ok || len(pattern.Elements) == 0 {
		return false
	}
	switch pattern.Elements[0].(type) {
	case query.Constant, query.VectorConstant:
		return true
	}
	return false
}

// levelReads is the clauses a magic rule reads at a scope, in the walk's
// order, and the extent roots the read passes. The clauses are the seed
// component's and, for the symbols needed, their extent chains: the
// connected components, among the other clauses, that a needed symbol
// touches. The roots passed are the extent roots a clause read mentions and
// the needed symbols that are extent roots themselves; a read correlated on
// an extent root nothing it carries mentions would run once per value of
// it, the product inside the magic rule.
func (w *magicRewriter) levelReads(roots, extentRoots map[query.Symbol]bool, clauses []query.Clause, needed map[query.Symbol]bool) ([]query.Clause, map[query.Symbol]bool) {
	seed := w.seedOf(roots, clauses)
	reached := make(map[query.Symbol]bool, len(needed))
	for sym := range needed {
		reached[sym] = true
	}
	chained := make([]bool, len(clauses))
	for grown := true; grown; {
		grown = false
		for i, clause := range clauses {
			if chained[i] || w.seedConnected(clause, seed) {
				continue
			}
			vars := query.FreeVariables([]query.Clause{clause}, w.rs)
			touches := false
			for _, sym := range vars {
				if reached[sym] {
					touches = true
					break
				}
			}
			if !touches {
				continue
			}
			chained[i] = true
			grown = true
			for _, sym := range vars {
				reached[sym] = true
			}
		}
	}
	passed := map[query.Symbol]bool{}
	for sym := range needed {
		if extentRoots[sym] {
			passed[sym] = true
		}
	}
	var read []query.Clause
	for i, clause := range clauses {
		if !chained[i] && !w.seedConnected(clause, seed) {
			continue
		}
		read = append(read, clause)
		for _, sym := range query.FreeVariables([]query.Clause{clause}, w.rs) {
			if extentRoots[sym] {
				passed[sym] = true
			}
		}
	}
	return read, passed
}

// passes is a read's :in and arguments at the level over the symbols passed,
// the level's seed and the extent roots the read carries: for a call, its
// :in and arguments restricted to them (passedInputs); for a branch, the
// sources, the rule set and the given symbols among them, in the given
// order.
func (l scopeLevel) passes(passed map[query.Symbol]bool) ([]query.InputSpec, []query.PatternElement) {
	if l.call != nil {
		return passedInputs(l.call, passed)
	}
	in := []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}}
	inputs := []query.PatternElement{query.Constant{Value: datalog.SymDollar}, query.Constant{Value: datalog.NewSymbol("%")}}
	for _, sym := range l.given {
		if passed[sym] {
			in = append(in, query.ScalarInput{Symbol: sym})
			inputs = append(inputs, query.Variable{Name: sym})
		}
	}
	return in, inputs
}

// outside is the enclosing scope's name for a symbol the level is given:
// the argument's variable for a call's :in symbol, the symbol itself for a
// branch's given symbol.
func (l scopeLevel) outside(sym query.Symbol) query.Symbol {
	if name, named := l.names.Of(sym); named {
		return name
	}
	return sym
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

// magicRuleReads is clause as a magic rule reads it, and whether the rule
// reads it at all. A magic rule reads a monotone clause every correlate of
// which the bound set holds, so every magic rule is safe, and never a
// negation or an aggregating subquery, so no magic rule depends
// non-monotonically on any predicate (NON_MONOTONE_FORMS_PROOF.md §1). A
// header form is read as the union of its branches over its header
// (readUnion). A subquery whose inner query does not aggregate is read with
// its inner clauses read the same way under its :in symbols (readClauses),
// and is not read when they no longer bind every find variable. A clause
// left out widens the bindings a magic rule derives and loses no tuple,
// since the adorned rule applies every clause.
func (w *magicRewriter) magicRuleReads(clause query.Clause, bound map[query.Symbol]bool) (query.Clause, bool) {
	if _, negation := clause.(*query.NotJoinClause); negation {
		return nil, false
	}
	for _, sym := range query.ScopeOf(clause, w.rs).Correlates {
		if !sym.IsSource() && !bound[sym] {
			return nil, false
		}
	}
	switch c := clause.(type) {
	case *query.SubqueryPattern:
		if c.Query.Aggregates() {
			return nil, false
		}
		inner := map[query.Symbol]bool{}
		for _, sym := range query.InputSymbols(c.Query.In) {
			if sym != nil {
				inner[sym] = true
			}
		}
		where, provided := w.readClauses(c.Query.Where, inner)
		if len(where) == 0 {
			return nil, false
		}
		for _, elem := range c.Query.Find {
			if v, ok := elem.(query.FindVariable); ok && !provided[v.Symbol] && !inner[v.Symbol] {
				return nil, false
			}
		}
		return &query.SubqueryPattern{Query: c.Query.WithWhere(where), Inputs: c.Inputs, Binding: c.Binding}, true
	case *query.OrJoinClause:
		return w.readUnion(c.RequiredVars, c.OutputVars, c.Branches, bound)
	case *query.OrDefaultJoinClause:
		return w.readUnion(c.RequiredVars, c.OutputVars, c.Branches, bound)
	}
	return clause, true
}

// readUnion is the union of a header form's branches over its header as a
// magic rule reads it: each branch read as a scope under the header symbols
// the bound set holds, with every negation and aggregating subquery at any
// depth left out, so the union binds what the form binds and holds every
// tuple the form yields. A form a branch of which reads to nothing, or no
// longer binds every output, is not read.
func (w *magicRewriter) readUnion(required, outputs []query.Symbol, branches [][]query.Clause, bound map[query.Symbol]bool) (query.Clause, bool) {
	entering := map[query.Symbol]bool{}
	for _, sym := range required {
		entering[sym] = true
	}
	for _, sym := range outputs {
		if bound[sym] {
			entering[sym] = true
		}
	}
	read := make([][]query.Clause, len(branches))
	for b, branch := range branches {
		clauses, provided := w.readClauses(branch, entering)
		if len(clauses) == 0 {
			return nil, false
		}
		for _, sym := range outputs {
			if !provided[sym] {
				return nil, false
			}
		}
		read[b] = clauses
	}
	return &query.OrJoinClause{RequiredVars: required, OutputVars: outputs, Branches: read}, true
}

// readClauses is the clauses of a scope a magic rule reads, in order, each
// read under the bound set the read clauses before it extend, and the
// symbols the read clauses provide.
func (w *magicRewriter) readClauses(clauses []query.Clause, bound map[query.Symbol]bool) ([]query.Clause, map[query.Symbol]bool) {
	bound = maps.Clone(bound)
	provided := map[query.Symbol]bool{}
	var read []query.Clause
	for _, clause := range clauses {
		r, ok := w.magicRuleReads(clause, bound)
		if !ok {
			continue
		}
		read = append(read, r)
		for _, sym := range query.ScopeOf(r, w.rs).Provides {
			bound[sym] = true
			provided[sym] = true
		}
	}
	return read, provided
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
// What passes into a form's branches is what the executor gives them
// (GivenSymbols): a not-join's body and a fallback's branches are given the
// required run alone, since a fallback's outputs are joined after its
// decision and never bound into a branch, and a union's branches are given
// the required run with the outputs. An or-join's branches walk under the
// passing and its sealed twin as the body does, sealed where the body
// already is; a non-monotone form's branches walk under the sealed passing
// alone, which holds nothing of the rule's stratum, so the walk inside
// starts unsealed. Every form the walk meets carries a header, since the
// bodies and inner queries it walks are inferred (NewRuleSet,
// InferHeaders).
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
		given := GivenSymbols(c.RequiredVars, c.OutputVars, false)
		branches, rules, changed, err := w.rewriteBranches(rule, planned, before.within(given), negated.within(given), sealed)
		if err != nil || !changed {
			return c, nil, err
		}
		return &query.OrJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}, rules, nil
	case *query.OrDefaultJoinClause:
		planned, err := w.plannedBranches(c.RequiredVars, c.OutputVars, c.Branches)
		if err != nil {
			return nil, nil, err
		}
		inside := under.within(GivenSymbols(c.RequiredVars, c.OutputVars, true))
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
// :in symbols whose arguments are constants or variables in the enclosing
// bound set seed it, and those whose arguments are variables in the
// enclosing extent set root its extent set, over the same body clauses, with
// the call pushed as a scope, whose names substitute each such :in symbol by
// its argument's variable and whose constants hold the constant each :in
// symbol stands for; a read of the scope passes, of the call's inputs, the
// seed and the extent roots the read carries (passes).
func (p passing) enteringSubquery(c *query.SubqueryPattern) (passing, error) {
	inside := p
	inside.bound = map[query.Symbol]bool{}
	inside.extent = map[query.Symbol]bool{}
	level := scopeLevel{roots: map[query.Symbol]bool{}, extentRoots: map[query.Symbol]bool{}, call: c, constants: map[query.Symbol]query.PatternElement{}}
	var from, to []query.Symbol
	for position, sym := range query.InputSymbols(c.Query.In) {
		if sym == nil {
			continue
		}
		switch arg := c.Inputs[position].(type) {
		case query.Variable:
			switch {
			case p.bound[arg.Name]:
				inside.bound[sym] = true
				level.roots[sym] = true
			case p.extent[arg.Name]:
				inside.extent[sym] = true
				level.extentRoots[sym] = true
			default:
				continue
			}
			from, to = append(from, sym), append(to, arg.Name)
		case query.Constant, query.VectorConstant:
			level.constants[sym] = arg
			inside.bound[sym] = true
			level.roots[sym] = true
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

// passedInputs is a call's :in and arguments restricted to the sources, the
// rule set and the inputs bound marks, in call order: an entry of one symbol
// is kept or left out whole, and a tuple or relation entry is kept over its
// bound symbols. An omitted :in is the one $ the call passes.
func passedInputs(c *query.SubqueryPattern, bound map[query.Symbol]bool) ([]query.InputSpec, []query.PatternElement) {
	if len(c.Query.In) == 0 {
		return nil, c.Inputs
	}
	var in []query.InputSpec
	var inputs []query.PatternElement
	at := 0
	keep := func(sym query.Symbol) bool {
		kept := bound[sym]
		if kept {
			inputs = append(inputs, c.Inputs[at])
		}
		at++
		return kept
	}
	for _, spec := range c.Query.In {
		switch entry := spec.(type) {
		case query.ScalarInput:
			if keep(entry.Symbol) {
				in = append(in, spec)
			}
		case query.CollectionInput:
			if keep(entry.Symbol) {
				in = append(in, spec)
			}
		case query.TupleInput:
			var symbols []query.Symbol
			for _, sym := range entry.Symbols {
				if keep(sym) {
					symbols = append(symbols, sym)
				}
			}
			if len(symbols) > 0 {
				in = append(in, query.TupleInput{Symbols: symbols})
			}
		case query.RelationInput:
			var symbols []query.Symbol
			for _, sym := range entry.Symbols {
				if keep(sym) {
					symbols = append(symbols, sym)
				}
			}
			if len(symbols) > 0 {
				in = append(in, query.RelationInput{Symbols: symbols})
			}
		default:
			in = append(in, spec)
			inputs = append(inputs, c.Inputs[at])
			at++
		}
	}
	return in, inputs
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

// excludedSite reports whether a refusal of the set has excluded the site
// the invocation stands at under a non-monotone form, and records the site
// for a refusal to name otherwise. At a site under no such form it reports
// false and records nothing.
func (w *magicRewriter) excludedSite(in passing, inv *query.RuleInvocation, bound []bool) bool {
	if !in.nonMonotone {
		return false
	}
	site := nonMonotoneSite{rule: in.adornedRule, target: query.AdornedSymbol(inv.Name, bound)}
	if w.excluded[site] {
		return true
	}
	w.sites = append(w.sites, site)
	return false
}

// adornInvocation adorns one invocation of the named rule under what passes
// into it: a position is bound when it holds a constant, a variable the
// bound set holds, a variable the extent set holds at a position the
// predicate requires, or a variable standing for a constant the call
// passes, a repeated variable at its first position only
// (A_CLAUSE_OUTSIDE_THE_SEED_COMPONENT_BINDS_NOTHING_PROOF.md T2). A
// position the predicate requires bound that the adornment leaves free is
// RequiredBindingError at a positive site. With a required position free
// under a non-monotone form, or at a site under such a form that a refusal
// of the set excluded, the invocation stands and its predicate's original
// rules join the set. With no bound position, the invocation names the
// predicate adorned all free, with no magic rule, when a rule of the
// predicate or of one its rules reach invokes a predicate with required
// positions (RuleSet.ReachesRequired), so that predicate is adorned from
// the clauses before its invocation and derived in the rewritten set at
// every level; otherwise the invocation stands and the original rules join
// the set. Every site under a non-monotone form the walk names adorned, all
// free or at a bound position, is recorded for a refusal to name
// (excludedSite). At a bound position the invocation names the adorned
// predicate, which is queued, and its magic rule derives the bound
// arguments from the body's clauses and the scopes on the path, each read
// through levelReads: the scope's seed-connected clauses and the extent
// chains of the symbols the read needs, a variable bound from the extent
// set at the innermost scope and the extent roots a read inside passes up
// under their names outside. A constant is derived through a fresh variable
// a ground expression binds, named for the magic predicate and the
// position; a variable bound in the body under its own name; and a variable
// bound inside a scope through that scope's read, a subquery over the
// clauses the scope reads finding the variable under its name there and
// binding it to the name standing for it one scope out, each read nested
// inside the next scope's out to the body. A read finds the bound variables
// the clauses it carries provide, the inner read among them, and passes the
// scope's seed and the extent roots those clauses mention (passes). A bound
// variable the scope's inputs alone bind has no attribute in the read's
// result, so it passes through under the name standing for it outside: a
// symbol the branch is given under the enclosing scope's symbol of its
// name, a bound :in symbol under its argument's variable, and one standing
// for a constant the call passes as that constant, grounded at the body. A
// scope whose read carries no clause providing a bound variable contributes
// no read, since a clause left out widens the magic set and loses no tuple.
// A scope's local, a fallback's output the branch provides among them,
// stands for a fresh variable named for the magic predicate and the
// position, which the reads outside carry up unchanged.
func (w *magicRewriter) adornInvocation(rule query.Symbol, inv *query.RuleInvocation, in passing) (query.Clause, []*query.Rule, error) {
	args := append([]query.PatternElement(nil), inv.Args...)
	bound := make([]bool, len(args))
	names := make([]query.Symbol, len(args))
	required := w.rs.Required(inv.Name)
	needed := map[query.Symbol]bool{}
	seen := map[query.Symbol]bool{}
	for i, arg := range args {
		switch a := arg.(type) {
		case query.Constant, query.VectorConstant:
			bound[i] = true
		case query.Variable:
			names[i] = a.Name
			switch {
			case seen[a.Name]:
			case in.bound[a.Name]:
				bound[i] = true
			case in.extent[a.Name] && i < len(required) && required[i]:
				bound[i] = true
				needed[a.Name] = true
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
	if query.CountBound(bound) == 0 && !w.rs.ReachesRequired(inv.Name) {
		w.includeOriginal(inv.Name)
		return inv, nil, nil
	}
	if w.excludedSite(in, inv, bound) {
		w.includeOriginal(inv.Name)
		return inv, nil, nil
	}
	if query.CountBound(bound) == 0 {
		w.enqueue(inv.Name, bound)
		return &query.RuleInvocation{Name: query.AdornedSymbol(inv.Name, bound), Args: inv.Args}, nil, nil
	}
	magic := query.MagicSymbol(inv.Name, bound)
	fresh := func(i int) query.Symbol { return datalog.NewSymbol("?" + magic.String() + "." + strconv.Itoa(i)) }
	var read query.Clause
	for k := len(in.levels) - 1; k >= 0; k-- {
		level := in.levels[k]
		where, passed := w.levelReads(level.roots, level.extentRoots, level.clauses, needed)
		if read != nil {
			where = append(where, read)
		}
		provided := map[query.Symbol]bool{}
		for _, clause := range where {
			for _, sym := range query.ScopeOf(clause, w.rs).Provides {
				provided[sym] = true
			}
		}
		for sym := range level.roots {
			passed[sym] = true
		}
		levelIn, inputs := level.passes(passed)
		needed = map[query.Symbol]bool{}
		for sym := range level.extentRoots {
			if passed[sym] {
				needed[level.outside(sym)] = true
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
			if !named && query.ContainsSymbol(level.given, names[i]) {
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
				Query:   &query.Query{Find: find, In: levelIn, Where: where},
				Inputs:  inputs,
				Binding: query.RelationBinding{Variables: binding},
			}
		}
	}
	body, _ := w.levelReads(in.head, nil, in.prefix, needed)
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
