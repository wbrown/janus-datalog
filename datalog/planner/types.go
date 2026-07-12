package planner

import (
	"fmt"
	"strings"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// IndexType names the index orderings the planner may report in its
// explain output. Mirrors the storage-layer enum of the same name but
// defined separately to avoid a planner → storage import cycle; the
// integer values are independent of storage's and must not be
// cross-cast. selectIndexForMask does not yet emit EATV or AETV — they
// are included here so the set of reportable indices stays aligned
// with what storage actually maintains (seven indices since the
// CRDT-Tx migration).
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
	Output     interface{}       // Symbol (scalar) or TupleBinding (tuple)
	IsEquality bool              // True if this is an equality check (no binding)
}

// SubqueryPlan represents a planned subquery to execute in a phase
type SubqueryPlan struct {
	Subquery *query.SubqueryPattern // The subquery pattern
	Inputs   []query.Symbol         // Symbols this subquery needs from outer query
}

// BoundMask indicates which elements of a pattern are bound
type BoundMask struct {
	E bool // Entity bound
	A bool // Attribute bound
	V bool // Value bound
	T bool // Transaction/time bound
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
	EnableScanSharing          bool // Share unbound scan results across subqueries via LazySeq (default: false)
	EnableEntityPrefetch       bool // Warm EA cache after first DataPattern via PrefetchEntities (default: false)
	EnableAttributeFetchFusion bool // Fuse same-entity [?e :const-attr ?fresh] fetches into per-tuple column attach instead of match+join (default: true)

	// Executor streaming options - control memory vs performance tradeoffs
	EnableIteratorComposition bool // Use composed iterators for lazy evaluation (default: true)
	EnableTrueStreaming       bool // Avoid auto-materialization of StreamingRelation (default: true)
	EnableSymmetricHashJoin   bool // Use symmetric hash join for stream-to-stream joins (default: false)

	// Executor parallel execution options
	EnableParallelSubqueries bool // Execute subqueries in parallel (default: true)
	MaxSubqueryWorkers       int  // Maximum parallel workers for subqueries (0 = 4)

	// Executor join/aggregation options
	EnableStreamingJoins       bool // Return StreamingRelation from joins instead of materializing
	EnableStreamingAggregation bool // Enable streaming aggregation (default: true)

	// Storage join strategy options
	IndexNestedLoopThreshold int // Threshold for choosing IndexNestedLoop vs HashJoinScan (default: 0)
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

	// Explain fields - populated for detailed plan output, nil during normal execution
	Patterns    []PatternPlan    // Pattern plans with index selection and selectivity
	Expressions []ExpressionPlan // Expression plans with inputs/outputs
	Predicates  []PredicatePlan  // Predicate plans with classification
	Subqueries  []SubqueryPlan   // Subquery plans with nested queries
}

// RealizedPlan is the output of the planner in the realized format.
// The executor operates on RealizedPlan directly.
type RealizedPlan struct {
	Query  *query.Query    // Original user query
	Phases []RealizedPhase // Phases as Datalog query fragments
}

// String returns a human-readable representation of a RealizedPhase
func (rp *RealizedPhase) String() string {
	var sb strings.Builder

	sb.WriteString("Query:\n")
	// Use the Query's built-in String() which formats Datalog nicely
	// Indent each line of the query output
	queryStr := rp.Query.String()
	lines := strings.Split(queryStr, "\n")
	for _, line := range lines {
		if line != "" {
			sb.WriteString("  " + line + "\n")
		}
	}

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
			if pat.BoundMask.E || pat.BoundMask.A || pat.BoundMask.V || pat.BoundMask.T {
				sb.WriteString(fmt.Sprintf("    Bound: E=%v A=%v V=%v T=%v\n",
					pat.BoundMask.E, pat.BoundMask.A, pat.BoundMask.V, pat.BoundMask.T))
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

	return sb.String()
}

// String returns a human-readable representation of a RealizedPlan
func (rpl *RealizedPlan) String() string {
	var sb strings.Builder
	sb.WriteString("Realized Query Plan:\n")
	sb.WriteString(fmt.Sprintf("  Phases: %d\n\n", len(rpl.Phases)))

	// Show original user query
	sb.WriteString("Original Query:\n")
	queryStr := rpl.Query.String()
	lines := strings.Split(queryStr, "\n")
	for _, line := range lines {
		if line != "" {
			sb.WriteString("  " + line + "\n")
		}
	}

	for i, phase := range rpl.Phases {
		sb.WriteString(fmt.Sprintf("\nPhase %d:\n", i+1))
		// Indent the phase string
		phaseStr := phase.String()
		lines := strings.Split(phaseStr, "\n")
		for _, line := range lines {
			if line != "" {
				sb.WriteString("  " + line + "\n")
			}
		}
	}

	return sb.String()
}
