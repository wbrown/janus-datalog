package executor

// Context carries the per-query execution state that is not configuration: the
// scope's environment and the scan registry.
//
// It does not carry the annotation handler. The handler is registered on the
// options (planner.PlannerOptions.Handler, derived into ExecutorOptions), which
// every executor, matcher, and relation is constructed with — so the sites that
// emit already hold it, and a second copy here could only disagree with the
// first.
//
// Not safe for concurrent use — the lazily created scanRegistry is mutable per
// query. Parallel workers each take their own via forkContext.
type Context struct {
	scanRegistry *ScanRegistry
	env          Relation
}

// NewContext returns a context for one query scope.
func NewContext() *Context {
	return &Context{}
}

// forkContext returns an independent Context for a concurrent worker, sharing
// only the environment. The scan registry is not shared: it is mutable per query.
func forkContext(ctx *Context) *Context {
	if ctx == nil {
		return nil
	}
	return &Context{env: ctx.env}
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
// environment, sharing the scan registry. Query-scope boundaries (top-level plan
// binding, subquery entry) derive here so an inner scope never captures an outer
// scope's environment.
func (c *Context) WithEnvironment(env Relation) *Context {
	return &Context{
		scanRegistry: c.ScanRegistry(),
		env:          env,
	}
}
