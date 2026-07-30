package planner

import (
	"fmt"
	"strings"

	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// AlgebraExplanation is the record of what planning did to a query, as
// values: the relational algebra the query compiles to, every rewrite
// decision the optimization passes made, the optimized tree, the Datalog it
// decompiles back to, and the physical plan. It answers "what is the algebra
// of this query" and "what algebraic transforms happened" without event
// scraping — the annotation events remain the streaming observability view
// of the same decisions.
type AlgebraExplanation struct {
	// Original is the query as resolved, before any rewriting.
	Original *query.Query
	// OptimizerEnabled reports whether this planner runs the algebra
	// optimizer; when false the explanation carries the compiled algebra
	// only, and Rewritten equals Original.
	OptimizerEnabled bool
	// Compiled is the relational algebra the query compiles to, before any
	// pass runs. It is populated regardless of OptimizerEnabled — the
	// algebra of a query is a fact about the query. Nil only when
	// compilation fails while the optimizer is disabled; CompileError then
	// carries the reason (a database with the optimizer off never runs the
	// compile, so its queries do not depend on it succeeding).
	Compiled *algebra.Node
	// CompileError is the compile failure when the optimizer is disabled
	// and the algebra view is unavailable; empty otherwise.
	CompileError string
	// Rewrites is every decision the passes and bridge transforms made, in
	// occurrence order: considered, applied, or declined with the failed
	// precondition. Nil when the optimizer is disabled.
	Rewrites []algebra.RewriteRecord
	// Optimized is the final tree after all passes and the bridge's
	// projection insertion — the tree that decompiles to Rewritten. Nil
	// when the optimizer is disabled.
	Optimized *algebra.Node
	// Rewritten is the Datalog the optimized tree decompiles to — the query
	// the physical planner actually phases. Equals Original when the
	// optimizer is disabled.
	Rewritten *query.Query
	// Plan is the physical plan for Rewritten, exactly as this planner's
	// options produce it.
	Plan *RealizedPlan
}

// String renders the explanation: the original query, the compiled algebra,
// each rewrite decision, the optimized algebra, the rewritten Datalog, and
// the physical plan.
func (e *AlgebraExplanation) String() string {
	var sb strings.Builder
	sb.WriteString("Algebra Explanation:\n")
	if e.Original != nil {
		sb.WriteString(fmt.Sprintf("Original:\n%s\n", e.Original.String()))
	}
	if e.Compiled != nil {
		sb.WriteString(fmt.Sprintf("Compiled algebra:\n%s\n", e.Compiled.String()))
	} else if e.CompileError != "" {
		sb.WriteString(fmt.Sprintf("Compiled algebra: unavailable (%s)\n", e.CompileError))
	}
	if !e.OptimizerEnabled {
		sb.WriteString("Optimizer: disabled — no rewrites\n")
	} else {
		sb.WriteString(fmt.Sprintf("Rewrites (%d):\n", len(e.Rewrites)))
		for _, r := range e.Rewrites {
			switch r.Action {
			// %v on Subject: the record carries the node the decision was made
			// on, and this is where it is rendered.
			case algebra.RewriteDeclined:
				sb.WriteString(fmt.Sprintf("  [%s] %s: %s — %v\n", r.Pass, r.Action, r.Reason, r.Subject))
			default:
				sb.WriteString(fmt.Sprintf("  [%s] %s: %v\n", r.Pass, r.Action, r.Subject))
			}
		}
		if e.Optimized != nil {
			sb.WriteString(fmt.Sprintf("Optimized algebra:\n%s\n", e.Optimized.String()))
		}
		if e.Rewritten != nil {
			sb.WriteString(fmt.Sprintf("Rewritten:\n%s\n", e.Rewritten.String()))
		}
	}
	if e.Plan != nil {
		sb.WriteString(e.Plan.String())
	}
	return sb.String()
}

// ExplainPlan plans the query without executing it and returns the full
// algebra explanation alongside the physical plan. It bypasses the plan
// cache — provenance requires actually running the bridge — and never
// stores its plan, so explaining a query does not perturb cached execution.
func (p *ClauseBasedPlanner) ExplainPlan(q *query.Query) (*AlgebraExplanation, error) {
	expl := &AlgebraExplanation{
		Original:         q,
		OptimizerEnabled: p.options.EnableAlgebraOptimizer,
	}

	if p.options.EnableAlgebraOptimizer {
		plan, err := p.planWithBindings(q, nil, nil, expl)
		if err != nil {
			return nil, err
		}
		expl.Plan = plan
		return expl, nil
	}

	// Optimizer disabled: plan exactly as this database would, and compile
	// the algebra view separately — the algebra of a query is a fact about
	// the query, but this database's queries do not depend on the compile
	// succeeding, so a failure reports instead of erroring.
	plan, err := p.planWithBindings(q, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	expl.Plan = plan
	expl.Rewritten = q
	if compiled, cerr := algebra.Compile(q); cerr != nil {
		expl.CompileError = cerr.Error()
	} else {
		expl.Compiled = compiled
	}
	return expl, nil
}
