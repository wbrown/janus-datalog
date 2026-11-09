package planner

import (
	"fmt"
	"strings"

	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// IndexType names the index orderings the planner may report in its
// explain output. Mirrors the storage-layer enum of the same name but
// defined separately to avoid a planner → storage import cycle; the
// integer values are independent of storage's and must not be
// cross-cast. selectIndexForMask does not yet emit EATV or AETV — they
// are included here so the set of reportable indices stays aligned
// with what storage actually maintains.
type IndexType uint8

const (
	EAVT IndexType = iota // Entity-Attribute-Value-Tx
	EATV                  // Entity-Attribute-Tx-Value  (cardinality-one: first entry wins)
	AEVT                  // Attribute-Entity-Value-Tx
	AETV                  // Attribute-Entity-Tx-Value  (A-primary CRDT: first entry wins)
	AVET                  // Attribute-Value-Entity-Tx
	VAET                  // Value-Attribute-Entity-Tx
	TAEV                  // Tx-Attribute-Entity-Value  (clock recovery / audit log)
)

// PatternPlan represents a planned pattern with index selection
type PatternPlan struct {
	Pattern     query.Pattern         // Original pattern
	Index       IndexType             // Selected index
	BoundMask   BoundMask             // Which elements are bound
	Selectivity int                   // Estimated selectivity (lower = more selective)
	Bindings    map[query.Symbol]bool // Variables that will be bound after execution
}

// PredicatePlanType represents the type of predicate plan
type PredicatePlanType uint8

const (
	PredicateEquality PredicatePlanType = iota
	PredicateComparison
	PredicateTimeExtraction
	PredicateChainedComparison
	PredicateNotEqual
	PredicateGround
	PredicateMissing
	PredicateFunction
	PredicateUnknown
)

// String returns the string representation of PredicatePlanType
func (t PredicatePlanType) String() string {
	switch t {
	case PredicateEquality:
		return "equality"
	case PredicateComparison:
		return "comparison"
	case PredicateTimeExtraction:
		return "time_extraction"
	case PredicateChainedComparison:
		return "chained_comparison"
	case PredicateNotEqual:
		return "not_equal"
	case PredicateGround:
		return "ground"
	case PredicateMissing:
		return "missing"
	case PredicateFunction:
		return "function"
	default:
		return "unknown"
	}
}

// PredicatePlan represents a planned predicate
type PredicatePlan struct {
	Predicate    query.Predicate   // The predicate interface
	RequiredVars []query.Symbol    // All variables required for evaluation
	Type         PredicatePlanType // Type of predicate plan
}

// ExpressionPlan represents a planned expression to evaluate in a phase
type ExpressionPlan struct {
	Expression *query.Expression // Use the new Expression type
	Inputs     []query.Symbol    // Symbols this expression needs
	Output     interface{}       // The expression's binding: a Symbol, a TupleBinding, a CollectionBinding or a RelationBinding
	IsEquality bool              // True if this is an equality check (no binding)
}

// SubqueryPlan represents a planned subquery to execute in a phase
type SubqueryPlan struct {
	Subquery *query.SubqueryPattern // The subquery pattern
	Inputs   []query.Symbol         // Symbols this subquery needs from outer query
}

// InvocationPlan is a planned rule invocation: the clause and the variable
// arguments available before it, which the derived relation joins on.
type InvocationPlan struct {
	Invocation *query.RuleInvocation
	Bound      []query.Symbol
}

// BoundMask indicates which elements of a pattern are bound
type BoundMask struct {
	E     bool // Entity bound
	A     bool // Attribute bound
	V     bool // Value bound
	T     bool // Transaction/time bound
	Added bool // The fifth position, whether the datom asserts, bound
}

// Statistics tracks query statistics for optimization
type Statistics struct {
	AttributeCardinality map[string]int // Estimated distinct values per attribute
	EntityCount          int            // Total number of entities
}

// PlannerOptions configures both the query planner and executor
type PlannerOptions struct {
	// Planner options
	Cache *PlanCache // Shared query plan cache (optional)

	// Subquery / algebra optimization
	EnableAlgebraOptimizer     bool // Enable relational algebra IR optimization (decorrelation, predicate pushdown)
	EnableScanSharing          bool // Share unbound scan results across subqueries via LazySeq (default: true)
	EnableEntityPrefetch       bool // Prefetch a fallback window's entities into the EA cache before its branches are built (default: false)
	EnableAttributeFetchFusion bool // Fuse same-entity [?e :const-attr ?fresh] fetches into a per-tuple binding instead of match+join (default: true)

	// CollectRosters holds with each plan the rosters of what its query names
	// on each source (RealizedPlan.Rosters), which the unknown-attribute
	// check at the executor's entry reads when a source the plan runs over
	// refuses attributes its schema does not declare. An executor sets it
	// from whether a source its matcher routes to refuses, so a plan made for
	// sources that refuse nothing carries none and costs no walk (default:
	// false).
	CollectRosters bool

	// MatchAnswersLookup is the sources' answer to whether a match of an
	// attribute on the source a query names answers, per entity, the one
	// value the source's lookup answers. The get-else scan rewrite applies
	// only where it answers true, a plan carries every answer its rewrites
	// read (RealizedPlan.SourceAnswers), and the plan cache serves a plan
	// only where the requesting planner answers each alike. An executor sets
	// it from its matcher's CanFuseAttributeFetch. Nil answers false for
	// every source, so a planner given none declines every get-else rewrite.
	MatchAnswersLookup algebra.MatchAnswersLookup

	// Executor streaming options - control memory vs performance tradeoffs
	EnableTrueStreaming     bool // Avoid auto-materialization of StreamingRelation (default: true)
	EnableSymmetricHashJoin bool // Use symmetric hash join for stream-to-stream joins (default: false)

	// Executor join/aggregation options
	EnableStreamingJoins       bool // Return StreamingRelation from joins instead of materializing
	EnableStreamingAggregation bool // Enable streaming aggregation (default: true)

	// Rule evaluation bounds, each an error naming the predicates and the
	// count reached when exceeded. Zero is unbounded (default: 0).
	MaxRuleIterations int // Fixpoint iterations per stratum
	MaxDerivedTuples  int // Tuples one rule set derives

	// The window a form's branches run over
	// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.2). Every subject,
	// sized or streaming, is read into windows of one tuple, then
	// ScopeWindowGrowth times the last, up to ScopeWindowCap tuples. Zero
	// takes the executor's default.
	ScopeWindowCap    int
	ScopeWindowGrowth int

	// Handler receives annotation events from planning and execution. Nil is
	// annotations-off and is the default. Registered here, on the options the
	// caller already builds, so it reaches the executor and every matcher and
	// relation constructed under them — there is nothing to attach afterwards.
	Handler annotations.Handler
}

func indexName(idx IndexType) string {
	switch idx {
	case EAVT:
		return "EAVT"
	case EATV:
		return "EATV"
	case AEVT:
		return "AEVT"
	case AETV:
		return "AETV"
	case AVET:
		return "AVET"
	case VAET:
		return "VAET"
	case TAEV:
		return "TAEV"
	default:
		return fmt.Sprintf("Unknown(%d)", idx)
	}
}

// RealizedPhase is the clean interchange format between planner and executor.
// It contains a Datalog query fragment instead of subdivided operation types.
type RealizedPhase struct {
	Query     *query.Query   // Datalog query fragment for this phase
	Available []query.Symbol // Symbols available from previous phases
	Provides  []query.Symbol // Symbols this phase provides
	Keep      []query.Symbol // Symbols to keep for next phase

	// The analysis of each clause of Query, in clause order, which Plan fills
	// for every phase. Explain renders it, and the executor's scan sharing
	// reads each pattern's BoundMask.
	Patterns    []PatternPlan    // Pattern plans with index selection and selectivity
	Expressions []ExpressionPlan // Expression plans with inputs/outputs
	Predicates  []PredicatePlan  // Predicate plans with classification
	Subqueries  []SubqueryPlan   // Subquery plans with nested queries
	Invocations []InvocationPlan // Rule invocations with their bound arguments
}

// RealizedPlan is the output of the planner in the realized format.
// The executor operates on RealizedPlan directly.
type RealizedPlan struct {
	Query  *query.Query    // The prepared query the plan was made from
	Phases []RealizedPhase // Phases as Datalog query fragments

	// Rosters is what the prepared query names (query.CollectRosters),
	// collected at planning under PlannerOptions.CollectRosters and held with
	// the plan, so a plan the cache serves names its text once; the
	// vocabulary check at the executor's entry reads it after planning. Nil
	// for a plan made without the option.
	Rosters *query.Rosters

	// Evaluations is what each invocation the plan's phases derive at its
	// clause evaluates, by the clause, planned with the plan; the executor's
	// evaluation reads it here.
	Evaluations map[*query.RuleInvocation]*InvocationEvaluation

	// Frames is the frames each form of the plan's phases runs, by the form's
	// clause, in branch order: an or-join's and an or-default-join's
	// branches, a not-join's body and a subquery's inner query.
	Frames map[query.Clause][]FramePlan

	// Windows is the set of the plan's clauses read in windows of a group
	// their scope produces: each data pattern and invocation sharing a
	// variable with a clause's result, or with a join of one with a group. A
	// clause the set does not hold binds through the groups before it.
	Windows map[query.Clause]bool

	// SourceAnswers is each question the get-else scan rewrite asked the
	// sources while this plan, the body plans its evaluations embed, and the
	// scope plans each evaluation's magic-sets rewrite read were made, with
	// the answer each got. The plan cache serves the plan only where the
	// requesting planner's sources answer every one alike.
	SourceAnswers []SourceAnswer
}

// FramePlan is one frame a form runs: its plan, the one the cache serves the
// executor's framing of the form, and whether its branch reads nothing of
// what it is given (BranchFrame.ReadsNothing). The form runs a frame whose
// branch reads nothing once, and every other frame once per window of its
// subject.
type FramePlan struct {
	Plan         *RealizedPlan
	ReadsNothing bool
}

// String returns a human-readable representation of a RealizedPhase
func (rp *RealizedPhase) String() string {
	return rp.render(nil)
}

// render renders the phase, each invocation followed by what it evaluates as
// evaluations, the plan's (RealizedPlan.Evaluations), holds it. An
// invocation that evaluates nothing at its clause has no entry.
func (rp *RealizedPhase) render(evaluations map[*query.RuleInvocation]*InvocationEvaluation) string {
	var sb strings.Builder

	sb.WriteString("Query:\n")
	writeIndented(&sb, rp.Query.String(), "  ")

	if len(rp.Available) > 0 {
		sb.WriteString(fmt.Sprintf("Available: %v\n", rp.Available))
	}
	sb.WriteString(fmt.Sprintf("Provides: %v\n", rp.Provides))
	if len(rp.Keep) > 0 {
		sb.WriteString(fmt.Sprintf("Keep: %v\n", rp.Keep))
	}

	// Explain fields - only shown when populated
	if len(rp.Patterns) > 0 {
		sb.WriteString("Patterns:\n")
		for _, pat := range rp.Patterns {
			sb.WriteString(fmt.Sprintf("  %s [%s index, selectivity=%d]\n",
				pat.Pattern.String(), indexName(pat.Index), pat.Selectivity))
			if pat.BoundMask.E || pat.BoundMask.A || pat.BoundMask.V || pat.BoundMask.T || pat.BoundMask.Added {
				sb.WriteString(fmt.Sprintf("    Bound: E=%v A=%v V=%v T=%v Added=%v\n",
					pat.BoundMask.E, pat.BoundMask.A, pat.BoundMask.V, pat.BoundMask.T, pat.BoundMask.Added))
			}
			if len(pat.Bindings) > 0 {
				sb.WriteString(fmt.Sprintf("    Binds: %v\n", pat.Bindings))
			}
		}
	}

	if len(rp.Predicates) > 0 {
		sb.WriteString("Predicates:\n")
		for _, pred := range rp.Predicates {
			sb.WriteString(fmt.Sprintf("  %s [%s]\n", pred.Predicate.String(), pred.Type.String()))
		}
	}

	if len(rp.Expressions) > 0 {
		sb.WriteString("Expressions:\n")
		for _, expr := range rp.Expressions {
			if expr.IsEquality {
				sb.WriteString(fmt.Sprintf("  %s (equality filter)\n", expr.Expression.String()))
			} else {
				sb.WriteString(fmt.Sprintf("  %s\n", expr.Expression.String()))
			}
			if len(expr.Inputs) > 0 {
				sb.WriteString(fmt.Sprintf("    Inputs: %v\n", expr.Inputs))
			}
		}
	}

	if len(rp.Subqueries) > 0 {
		sb.WriteString("Subqueries:\n")
		for _, subq := range rp.Subqueries {
			sb.WriteString(fmt.Sprintf("  %s\n", subq.Subquery.String()))
			if len(subq.Inputs) > 0 {
				sb.WriteString(fmt.Sprintf("    Inputs: %v\n", subq.Inputs))
			}
		}
	}

	if len(rp.Invocations) > 0 {
		sb.WriteString("Invocations:\n")
		for _, inv := range rp.Invocations {
			sb.WriteString(fmt.Sprintf("  %s\n", inv.Invocation.String()))
			if len(inv.Bound) > 0 {
				sb.WriteString(fmt.Sprintf("    Bound: %v\n", inv.Bound))
			}
			if evaluation, ok := evaluations[inv.Invocation]; ok {
				writeIndented(&sb, evaluation.String(), "    ")
			}
		}
	}

	return sb.String()
}

// String returns a human-readable representation of a RealizedPlan
func (rpl *RealizedPlan) String() string {
	var sb strings.Builder
	sb.WriteString("Realized Query Plan:\n")
	sb.WriteString(fmt.Sprintf("  Phases: %d\n\n", len(rpl.Phases)))

	// Show original user query
	sb.WriteString("Original Query:\n")
	writeIndented(&sb, rpl.Query.String(), "  ")

	for i := range rpl.Phases {
		sb.WriteString(fmt.Sprintf("\nPhase %d:\n", i+1))
		writeIndented(&sb, rpl.Phases[i].render(rpl.Evaluations), "  ")
	}

	return sb.String()
}

// String renders what an invocation evaluates: a magic-sets rewrite deriving
// the adorned predicate, or the predicate under the rule set as given, then
// each stratum's predicates and every body the stratum runs, by kind (plain,
// delta, lower), with the predicate a copy is marked on and the body's plan,
// whose own invocations render their evaluations the same way.
func (e *InvocationEvaluation) String() string {
	var sb strings.Builder
	if e.Rewritten {
		sb.WriteString(fmt.Sprintf("Evaluates: magic-sets rewrite deriving %s\n", e.Magic.Predicate))
	} else {
		sb.WriteString(fmt.Sprintf("Evaluates: %s, under the rule set as given\n", e.Magic.Predicate))
	}
	for _, stratum := range e.Strata {
		sb.WriteString(fmt.Sprintf("Stratum %v:\n", stratum.Stratum.Predicates))
		for _, kind := range []struct {
			name   string
			bodies []PlannedBody
		}{
			{"plain", stratum.Bodies.Plain},
			{"delta", stratum.Bodies.Delta},
			{"lower", stratum.Bodies.Lower},
		} {
			for _, body := range kind.bodies {
				header := kind.name + " body"
				if body.Marked != nil {
					header += ", marked " + body.Marked.String()
				}
				sb.WriteString(fmt.Sprintf("  %s: %s\n", header, body.Rule.String()))
				writeIndented(&sb, body.Plan.String(), "    ")
			}
		}
	}
	return sb.String()
}

// writeIndented writes each non-empty line of text to sb under prefix, the
// form in which one rendering nests another.
func writeIndented(sb *strings.Builder, text, prefix string) {
	for _, line := range strings.Split(text, "\n") {
		if line != "" {
			sb.WriteString(prefix + line + "\n")
		}
	}
}
