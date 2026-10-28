package executor

import (
	"context"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// Context carries the per-query execution state that is not configuration: the
// scope's environment, the scan registry, the rule set with the relations
// derived from it, the rule evaluations a form execution continues across its
// windows, and the cancellation the caller gave.
//
// It does not carry the annotation handler. The handler is registered on the
// options (planner.PlannerOptions.Handler, derived into ExecutorOptions), which
// every executor, matcher, and relation is constructed with — so the sites that
// emit already hold it, and a second copy here could only disagree with the
// first.
//
// Not safe for concurrent use — the lazily created scanRegistry is mutable per
// query; forkContext derives a context with a scan registry of its own.
type Context struct {
	scanRegistry *ScanRegistry
	env          Relation

	// run is the required run of the scope whose plan runs under this
	// context, which the scope's query reads under query.ScopeRequired
	// through Derived; nil outside a scope's run.
	run Relation

	// rules is the rule set the query's invocations resolve against, nil when
	// the query passed none.
	rules *query.RuleSet

	// derived holds, per predicate symbol, the relation the fixpoint holds
	// for it: a predicate's relation over its canonical symbols, and, under
	// the predicate's delta symbol, the tuples the last iteration added. An
	// invocation reads the relation held under the symbol it invokes.
	derived map[query.Symbol]Relation

	// continued holds the rule evaluations the enclosing form execution
	// continues across its windows, by the invocation clause each evaluates,
	// nil outside a form execution, where an invocation's evaluation runs its
	// one window and completes.
	continued *ruleContinuations

	// held says which invocations of a scope planned under this context read
	// a relation the context holds, which a scope's plan is served under.
	held query.Held

	// cancellation is the context the caller gave to cancel the query, nil
	// when none was given.
	cancellation context.Context
}

// NewContext returns a context for one query scope.
func NewContext() *Context {
	return &Context{}
}

// forkContext returns an independent Context sharing everything but the scan
// registry, which is mutable per query.
func forkContext(ctx *Context) *Context {
	if ctx == nil {
		return nil
	}
	return &Context{
		env:          ctx.env,
		run:          ctx.run,
		rules:        ctx.rules,
		derived:      ctx.derived,
		continued:    ctx.continued,
		held:         ctx.held,
		cancellation: ctx.cancellation,
	}
}

// WithHeld derives a context whose scopes are planned with held as the
// invocations that read a relation the context holds.
func (c *Context) WithHeld(held query.Held) *Context {
	next := c.WithEnvironment(c.env)
	next.held = held
	return next
}

// Held returns which invocations of a scope planned under this context read
// a relation the context holds.
func (c *Context) Held() query.Held {
	return c.held
}

// WithRules derives a context whose invocations resolve against rules.
func (c *Context) WithRules(rules *query.RuleSet) *Context {
	derived := c.WithEnvironment(c.env)
	derived.rules = rules
	return derived
}

// WithCancellation derives a context whose query stops with cancellation's
// error once it is done: at entry, before any work, and a fixpoint at its
// next iteration.
func (c *Context) WithCancellation(cancellation context.Context) *Context {
	derived := c.WithEnvironment(c.env)
	derived.cancellation = cancellation
	return derived
}

// Cancellation returns the context the caller gave to cancel the query, nil
// when none was given.
func (c *Context) Cancellation() context.Context {
	return c.cancellation
}

// cancelled reports the cancellation's error once it is done, and nil while
// the query runs on or when no cancellation was given.
func (c *Context) cancelled() error {
	if c.cancellation == nil {
		return nil
	}
	return c.cancellation.Err()
}

// Rules returns the rule set the query passed, nil when it passed none.
func (c *Context) Rules() *query.RuleSet {
	return c.rules
}

// WithDerived derives a context carrying the relations the fixpoint holds,
// by predicate symbol, replacing the map.
func (c *Context) WithDerived(derived map[query.Symbol]Relation) *Context {
	next := c.WithEnvironment(c.env)
	next.derived = derived
	return next
}

// WithRequiredRun derives the context a scope's plan runs under: it holds
// run as the scope's required run, under query.ScopeRequired, beside every
// relation this context holds, and carries no environment, since a query
// input reaches a scope only through the header.
func (c *Context) WithRequiredRun(run Relation) *Context {
	next := c.WithEnvironment(nil)
	next.run = run
	return next
}

// Derived returns the relation held under a predicate symbol and whether the
// scope holds one: the scope's required run under query.ScopeRequired, and
// the fixpoint's relation under a predicate's symbol or its delta symbol.
func (c *Context) Derived(name query.Symbol) (Relation, bool) {
	if name == query.ScopeRequired && c.run != nil {
		return c.run, true
	}
	rel, ok := c.derived[name]
	return rel, ok
}

// WithContinuations derives a context whose invocations continue the
// evaluations continued holds, by clause, across the windows of the form
// execution that owns them.
func (c *Context) WithContinuations(continued *ruleContinuations) *Context {
	next := c.WithEnvironment(c.env)
	next.continued = continued
	return next
}

// ScanRegistry returns the per-query scan registry for sharing unbound scan
// results across subqueries, initializing it lazily.
func (c *Context) ScanRegistry() *ScanRegistry {
	if c.scanRegistry == nil {
		c.scanRegistry = NewScanRegistry()
	}
	return c.scanRegistry
}

// Environment returns the executing query scope's environment: one single-tuple
// relation over its single-valued :in parameters (scalar and tuple inputs) — the
// scope's joint parameter binding. The environment is ambient — visible in every
// clause scope of the query it parameterizes, including or-branch bodies, and
// never subject to branch locality or alpha-renaming — and it reaches consumers
// by join, never as a bindings map. Multi-valued inputs (collection, relation)
// are data, not environment; exactly one tuple is the structural invariant
// separating the two. Nil when the scope has no bound inputs.
func (c *Context) Environment() Relation {
	return c.env
}

// WithEnvironment derives a context for a query scope with the given
// environment, sharing the scan registry and the scope's required run.
// Query-scope boundaries (top-level plan binding, subquery entry) derive here
// so an inner scope never captures an outer scope's environment.
func (c *Context) WithEnvironment(env Relation) *Context {
	return &Context{
		scanRegistry: c.ScanRegistry(),
		env:          env,
		run:          c.run,
		rules:        c.rules,
		derived:      c.derived,
		continued:    c.continued,
		held:         c.held,
		cancellation: c.cancellation,
	}
}
