package planner

import (
	"fmt"
	"github.com/wbrown/janus-datalog/datalog/query"
	"strings"
)

// IndexType represents different index orderings (copied to avoid circular import)
type IndexType uint8

const (
	EAVT IndexType = iota // Entity-Attribute-Value-Tx
	AEVT                  // Attribute-Entity-Value-Tx
	AVET                  // Attribute-Value-Entity-Tx
	VAET                  // Value-Attribute-Entity-Tx
	TAEV                  // Tx-Attribute-Entity-Value
)

// QueryPlan represents an optimized execution plan for a query
type QueryPlan struct {
	Query    *query.Query
	Phases   []Phase
	Metadata map[string]interface{} // Query-level metadata (e.g., time range constraints)
}

// Phase represents a phase of query execution
type Phase struct {
	Patterns               []PatternPlan              // Patterns to execute in this phase
	Predicates             []PredicatePlan            // Predicates to apply after patterns
	JoinPredicates         []JoinPredicate            // Equality predicates to push into join
	Expressions            []ExpressionPlan           // Expressions to evaluate in this phase
	Subqueries             []SubqueryPlan             // Subqueries to execute in this phase
	DecorrelatedSubqueries []DecorrelatedSubqueryPlan // Decorrelated subquery groups
	Available              []query.Symbol             // Symbols available from previous phases (including bindings)
	Provides               []query.Symbol             // Symbols this phase provides
	Keep                   []query.Symbol             // Symbols to keep for later phases
	Find                   []query.FindElement        // Find clause elements (preserves aggregates)
	Metadata               map[string]interface{}     // Phase metadata (e.g., decorrelation analysis)
}

// combineTimeExtractions merges time extraction expressions with predicates
// Example: [(day ?t) ?d] + [(= ?d 20)] -> time extraction constraint
func (p *Phase) combineTimeExtractions() {
	// Map output variables to time extraction expressions
	timeExtractionOutputs := make(map[query.Symbol]string)      // variable -> time field (day, month, etc.)
	timeExtractionInputs := make(map[query.Symbol]query.Symbol) // output var -> input var

	// Check expressions for time extraction functions
	for _, exprPlan := range p.Expressions {
		if exprPlan.Expression != nil {
			// Check if this is a time extraction function
			if tef, ok := exprPlan.Expression.Function.(*query.TimeExtractionFunction); ok {
				// This is a time extraction expression
				if exprPlan.Output != "" {
					timeExtractionOutputs[exprPlan.Output] = tef.Field
					// The input is typically the first argument
					if len(exprPlan.Inputs) > 0 {
						timeExtractionInputs[exprPlan.Output] = exprPlan.Inputs[0]
					}
				}
			}
		}
	}

	// Now process predicates
	var result []PredicatePlan

	for _, pred := range p.Predicates {
		// Check if this is an equality or comparison predicate on a time extraction output
		if pred.Type == PredicateEquality && len(pred.RequiredVars) == 2 {
			// Variable equality - these are filters between two variables
			// Don't try to optimize these as time extraction predicates
			// Examples: [(= ?year ?year-open)], [(= ?month ?month-close)]
			result = append(result, pred)

		} else if (pred.Type == PredicateEquality || pred.Type == PredicateComparison) && pred.Variable != "" {
			// Single variable with constant - check if it's a time extraction output
			if timeField, found := timeExtractionOutputs[pred.Variable]; found {
				// Create a time extraction predicate
				inputVar := timeExtractionInputs[pred.Variable]
				operator := pred.Operator
				if pred.Type == PredicateEquality {
					operator = query.OpEQ
				}
				result = append(result, PredicatePlan{
					Type:         PredicateTimeExtraction,
					Variable:     inputVar,
					TimeField:    timeField,
					Value:        pred.Value,
					Operator:     operator,
					RequiredVars: []query.Symbol{inputVar},
					Predicate:    pred.Predicate, // Keep original for reference
				})
			} else {
				result = append(result, pred)
			}
		} else {
			result = append(result, pred)
		}
	}

	p.Predicates = result
}

// ConstraintType represents the type of storage constraint
type ConstraintType uint8

const (
	ConstraintEquality ConstraintType = iota
	ConstraintRange
	ConstraintTimeExtraction
)

// String returns the string representation of ConstraintType
func (t ConstraintType) String() string {
	switch t {
	case ConstraintEquality:
		return "equality"
	case ConstraintRange:
		return "range"
	case ConstraintTimeExtraction:
		return "time_extraction"
	default:
		return "unknown"
	}
}

// StorageConstraint represents a constraint that can be evaluated at storage level
type StorageConstraint struct {
	Type      ConstraintType  // Type of constraint
	Attribute string          // The attribute to constrain
	Value     interface{}     // The value or range
	Operator  query.CompareOp // For comparisons: OpEQ, OpLT, OpGT, OpLTE, OpGTE
	TimeField string          // For time extraction: "year", "month", "day", etc.
}

// PatternPlan represents a planned pattern with index selection
type PatternPlan struct {
	Pattern            query.Pattern          // Original pattern
	Index              IndexType              // Selected index
	BoundMask          BoundMask              // Which elements are bound
	Selectivity        int                    // Estimated selectivity (lower = more selective)
	Bindings           map[query.Symbol]bool  // Variables that will be bound after execution
	PushablePredicates []PredicatePlan        // Predicates that can be pushed to storage for this pattern
	Metadata           map[string]interface{} // Additional metadata (storage constraints, etc.)
}

// ApplyConstraints analyzes predicates and applies relevant constraints to this pattern
func (pp *PatternPlan) ApplyConstraints(predicates []PredicatePlan, phase Phase) {
	var constraints []StorageConstraint

	for _, pred := range predicates {
		if constraint := pp.toConstraint(pred, phase); constraint != nil {
			constraints = append(constraints, *constraint)
		}
	}

	// Add constraints to metadata
	if len(constraints) > 0 {
		if pp.Metadata == nil {
			pp.Metadata = make(map[string]interface{})
		}
		pp.Metadata["storage_constraints"] = constraints
	}
}

// toConstraint converts a predicate to a storage constraint if applicable to this pattern
func (pp *PatternPlan) toConstraint(pred PredicatePlan, phase Phase) *StorageConstraint {
	dp, ok := pp.Pattern.(*query.DataPattern)
	if !ok {
		return nil
	}

	// Check if this is a time extraction predicate
	if pred.Type == PredicateTimeExtraction {
		// Check if this pattern provides the time variable (pred.Variable contains the time var)
		for i, elem := range dp.Elements {
			if v, ok := elem.(query.Variable); ok && query.Symbol(v.Name) == pred.Variable {
				// This pattern provides the time variable
				if i == 2 { // Value position - the time is in the value
					// Get the attribute
					if attrElem, ok := dp.Elements[1].(query.Constant); ok {
						attrStr := fmt.Sprintf("%v", attrElem.Value)
						// Check if this is a time attribute
						if attrStr == ":price/time" || attrStr == ":bar/time" {
							return &StorageConstraint{
								Type:      ConstraintTimeExtraction,
								Attribute: attrStr,
								Value:     pred.Value,
								TimeField: pred.TimeField,
								Operator:  pred.Operator,
							}
						}
					}
				}
			}
		}
	}

	// Check if this is a value comparison predicate
	if pred.Type == PredicateComparison {
		// Check if this pattern has the variable in value position
		if len(dp.Elements) > 2 {
			if v, ok := dp.Elements[2].(query.Variable); ok {
				if query.Symbol(v.Name) == pred.Variable {
					// This pattern provides the variable in value position
					// Get the attribute
					if attrElem, ok := dp.Elements[1].(query.Constant); ok {
						return &StorageConstraint{
							Type:      ConstraintRange,
							Attribute: fmt.Sprintf("%v", attrElem.Value),
							Value:     pred.Value,
							Operator:  pred.Operator,
						}
					}
				}
			}
		}
	}

	// Check for equality predicates on the value position
	if pred.Type == PredicateEquality && len(dp.Elements) > 2 && dp.Elements[2] != nil {
		if v, ok := dp.Elements[2].(query.Variable); ok {
			if query.Symbol(v.Name) == pred.Variable {
				// Pattern's value variable has an equality constraint
				if attrElem, ok := dp.Elements[1].(query.Constant); ok {
					return &StorageConstraint{
						Type:      ConstraintEquality,
						Attribute: fmt.Sprintf("%v", attrElem.Value),
						Value:     pred.Value,
						Operator:  query.OpEQ,
					}
				}
			}
		}
	}

	return nil
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
	CanPushDown  bool              // Can be pushed to storage layer

	// Metadata for predicate pushdown optimization
	Variable  query.Symbol           // Main variable (if applicable)
	Value     interface{}            // Constant value (if applicable)
	Operator  query.CompareOp        // Operator (OpEQ, OpLT, OpGT, etc.)
	TimeField string                 // For time extraction predicates
	Metadata  map[string]interface{} // Additional metadata (e.g., optimized_by_constraint)
}

// ExpressionPlan represents a planned expression to evaluate in a phase
type ExpressionPlan struct {
	Expression *query.Expression      // Use the new Expression type
	Inputs     []query.Symbol         // Symbols this expression needs
	Output     query.Symbol           // The binding it produces
	IsEquality bool                   // True if this is an equality check (no binding)
	Metadata   map[string]interface{} // Additional metadata (e.g., optimized_by_constraint)
}

// JoinPredicate represents an equality predicate that can be pushed into a join
type JoinPredicate struct {
	Predicate   query.Predicate // The predicate (should be Comparison with OpEQ)
	LeftSymbol  query.Symbol    // Symbol from previous phase result
	RightSymbol query.Symbol    // Symbol from current phase
}

// PredicateType classifies predicates for optimization
type PredicateType int

const (
	PredicateTypeUnknown         PredicateType = iota
	PredicateTypeIntraPhase                    // Can be evaluated within a phase
	PredicateTypeInterPhase                    // Needs symbols from multiple phases
	PredicateTypeStoragePushable               // Can be pushed to storage
	PredicateTypeJoinCondition                 // Can be used as join condition
)

// SubqueryPlan represents a planned subquery to execute in a phase
type SubqueryPlan struct {
	Subquery     *query.SubqueryPattern // The subquery pattern
	Inputs       []query.Symbol         // Symbols this subquery needs from outer query
	NestedPlan   *QueryPlan             // The planned nested query
	Decorrelated bool                   // True if this subquery is part of a decorrelated group
}

// DecorrelatedSubqueryPlan represents a group of subqueries optimized together
type DecorrelatedSubqueryPlan struct {
	OriginalSubqueries []int             // Indices in Phase.Subqueries
	FilterGroups       []FilterGroup     // Groups of subqueries by filter
	MergedPlans        []*QueryPlan      // One plan per filter group
	CorrelationKeys    []query.Symbol    // Keys to join on from outer query (e.g., ?year, ?month, ?day, ?hour)
	GroupingVars       [][]query.Symbol  // Actual grouping variables in merged queries (per filter group)
	ColumnMapping      map[int]ResultMap // Original subquery -> result columns

	// Metadata for annotations (captured at plan time, reported at execution time)
	SignatureHash     string // Hash of the correlation signature
	TotalSubqueries   int    // Total subqueries considered for this group
	DecorrelatedCount int    // How many were actually decorrelated
}

// ResultMap maps original subquery to columns in merged result
type ResultMap struct {
	FilterGroupIdx int            // Which merged query produced this result
	ColumnIndices  []int          // Which columns in that result
	BindingVars    []query.Symbol // Variable names from the binding form
}

// FilterGroup represents subqueries with the same filter predicates
type FilterGroup struct {
	CommonPatterns     []query.Pattern   // Patterns shared by all subqueries
	FilterPredicates   []query.Predicate // Distinguishing filter predicates
	AccessedAttributes []string          // Attributes accessed by subqueries (for grouping)
	Subqueries         []int             // Indices of subqueries in this group
	AggFunctions       map[int][]string  // SubqIdx -> aggregate functions
}

// CorrelationSignature identifies subqueries that can be decorrelated together
type CorrelationSignature struct {
	BasePatterns    []PatternFingerprint // Simplified pattern structure
	CorrelationVars []query.Symbol       // Input variables from :in clause
	IsAggregate     bool                 // Must have aggregation functions
}

// PatternFingerprint is a simplified representation of patterns for matching
type PatternFingerprint struct {
	Attributes []string       // Attributes accessed (e.g., ":price/high")
	Bound      []query.Symbol // Which variables are bound
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
	// Planner architecture selection
	UseClauseBasedPlanner bool // Use new clause-based planner instead of old phase-based planner (default: false)

	// Planner options
	EnableDynamicReordering             bool       // Enable dynamic join reordering (1-3μs overhead, can prevent cross-products - should be enabled)
	EnablePredicatePushdown             bool       // Early predicate filtering during pattern matching (not true storage pushdown)
	EnableConditionalAggregateRewriting bool       // DISABLED: Returns empty results (bug). Rewrite correlated aggregates as conditional aggregates
	EnableSubqueryDecorrelation         bool       // Enable Selinger-style subquery decorrelation optimization
	EnableParallelDecorrelation         bool       // Execute decorrelated merged queries in parallel (requires EnableSubqueryDecorrelation)
	EnableCSE                           bool       // Enable Common Subexpression Elimination for decorrelated subqueries
	EnableSemanticRewriting             bool       // Rewrite predicates for efficiency (e.g., year(t)=2025 → time range constraint)
	UseStreamingSubqueryUnion           bool       // Use streaming union for subquery results instead of materializing all (default: true)
	UseComponentizedSubquery            bool       // Use component-based subquery execution (strategy selector, batcher, worker pool)
	MaxPhases                           int        // Maximum phases to generate (0 = unlimited)
	EnableFineGrainedPhases             bool       // Use fine-grained phase creation to avoid cross-products
	Cache                               *PlanCache // Shared query plan cache (optional)

	// Executor streaming options - control memory vs performance tradeoffs
	EnableIteratorComposition bool // Use composed iterators for lazy evaluation (default: true)
	EnableTrueStreaming       bool // Avoid auto-materialization of StreamingRelation (default: true)
	EnableSymmetricHashJoin   bool // Use symmetric hash join for stream-to-stream joins (default: false)

	// Executor parallel execution options
	EnableParallelSubqueries bool // Execute subqueries in parallel (default: true)
	MaxSubqueryWorkers       int  // Maximum parallel workers for subqueries (0 = runtime.NumCPU())

	// Executor join/aggregation options
	EnableStreamingJoins            bool // Return StreamingRelation from joins instead of materializing
	EnableStreamingAggregation      bool // Enable streaming aggregation (default: true)
	EnableStreamingAggregationDebug bool // Debug logging for streaming aggregation (default: false)
	EnableDebugLogging              bool // Enable debug logging for joins (default: false)
}

// String returns a human-readable representation of the query plan
func (qp *QueryPlan) String() string {
	var sb strings.Builder
	sb.WriteString("Query Plan:\n")
	sb.WriteString(fmt.Sprintf("  Find: %v\n", qp.Query.Find))
	sb.WriteString(fmt.Sprintf("  Phases: %d\n", len(qp.Phases)))

	for i, phase := range qp.Phases {
		sb.WriteString(fmt.Sprintf("\nPhase %d:\n", i+1))
		sb.WriteString(phase.String())
	}

	return sb.String()
}

// String returns a human-readable representation of a phase
func (p *Phase) String() string {
	var sb strings.Builder

	if len(p.Available) > 0 {
		sb.WriteString(fmt.Sprintf("  Available: %v\n", p.Available))
	}

	if len(p.Patterns) > 0 {
		sb.WriteString("  Patterns:\n")
		for _, pat := range p.Patterns {
			sb.WriteString(fmt.Sprintf("    %s [%s index, selectivity=%d]\n",
				pat.Pattern.String(), indexName(pat.Index), pat.Selectivity))
			if pat.BoundMask.E || pat.BoundMask.A || pat.BoundMask.V || pat.BoundMask.T {
				sb.WriteString(fmt.Sprintf("      Bound: E=%v A=%v V=%v T=%v\n",
					pat.BoundMask.E, pat.BoundMask.A, pat.BoundMask.V, pat.BoundMask.T))
			}
			if len(pat.Bindings) > 0 {
				sb.WriteString(fmt.Sprintf("      Binds: %v\n", pat.Bindings))
			}
		}
	}

	if len(p.Predicates) > 0 {
		sb.WriteString("  Predicates:\n")
		for _, pred := range p.Predicates {
			sb.WriteString(fmt.Sprintf("    %s\n", pred.Predicate.String()))
		}
	}

	if len(p.Expressions) > 0 {
		sb.WriteString("  Expressions:\n")
		for _, expr := range p.Expressions {
			if expr.IsEquality {
				sb.WriteString(fmt.Sprintf("    %s (equality filter)\n", expr.Expression.String()))
			} else {
				sb.WriteString(fmt.Sprintf("    %s\n", expr.Expression.String()))
			}
			if len(expr.Inputs) > 0 {
				sb.WriteString(fmt.Sprintf("      Inputs: %v\n", expr.Inputs))
			}
		}
	}

	if len(p.Subqueries) > 0 {
		sb.WriteString("  Subqueries:\n")
		for _, subq := range p.Subqueries {
			sb.WriteString(fmt.Sprintf("    %s\n", subq.Subquery.String()))
			if len(subq.Inputs) > 0 {
				sb.WriteString(fmt.Sprintf("      Inputs: %v\n", subq.Inputs))
			}
			// Show the nested query plan
			if subq.NestedPlan != nil {
				sb.WriteString("      Nested Plan:\n")
				nestedPlanStr := subq.NestedPlan.String()
				// Indent each line of the nested plan
				lines := strings.Split(nestedPlanStr, "\n")
				for _, line := range lines {
					if line != "" {
						sb.WriteString("        " + line + "\n")
					}
				}
			}
		}
	}

	if len(p.Provides) > 0 {
		sb.WriteString(fmt.Sprintf("  Provides: %v\n", p.Provides))
	}

	if len(p.Keep) > 0 {
		sb.WriteString(fmt.Sprintf("  Keep: %v\n", p.Keep))
	}

	return sb.String()
}

func indexName(idx IndexType) string {
	switch idx {
	case EAVT:
		return "EAVT"
	case AEVT:
		return "AEVT"
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
	Query     *query.Query           // Datalog query fragment for this phase
	Available []query.Symbol         // Symbols available from previous phases
	Provides  []query.Symbol         // Symbols this phase provides
	Keep      []query.Symbol         // Symbols to keep for next phase
	Metadata  map[string]interface{} // Phase metadata (decorrelation hints, etc.)
}

// RealizedPlan is the output of the planner in the realized format.
// The executor operates on RealizedPlan instead of QueryPlan.
type RealizedPlan struct {
	Query  *query.Query     // Original user query
	Phases []RealizedPhase  // Phases as Datalog query fragments
}

// Realize converts a QueryPlan (with Phase structures) into a RealizedPlan
// (with Query fragments). This is the interchange format between planner and executor.
//
// The realized queries preserve EXACT execution order from the current executor:
//   1. Patterns (pattern matching)
//   2. Expressions (function evaluation)
//   3. Predicates (filtering)
//   4. Subqueries (nested query execution)
//
// This ensures identical results for validation during migration.
func (qp *QueryPlan) Realize() *RealizedPlan {
	realizedPhases := make([]RealizedPhase, len(qp.Phases))
	for i, phase := range qp.Phases {
		isLastPhase := (i == len(qp.Phases)-1)
		// For phases after the first, get previous phase's Keep for :in clause
		var prevKeep []query.Symbol
		if i > 0 {
			prevKeep = qp.Phases[i-1].Keep
		}
		realizedPhases[i] = realizePhase(phase, isLastPhase, prevKeep)
	}
	return &RealizedPlan{
		Query:  qp.Query,
		Phases: realizedPhases,
	}
}

// reconstructPredicatesFromConstraints rebuilds predicates from storage constraints.
// When predicates are pushed to storage, they're removed from phase.Predicates but stored
// as storage constraints in pattern metadata. This function reconstructs them for the
// realized query to ensure semantic completeness.
func reconstructPredicatesFromConstraints(phase Phase) []query.Clause {
	var predicates []query.Clause

	// Build a map of time extraction expression outputs
	// For time extraction constraints, we need to know which variable represents the extraction
	timeExtractionVars := make(map[string]map[string]query.Symbol) // timeField -> attribute -> output variable
	for _, expr := range phase.Expressions {
		if expr.Expression != nil && expr.Output != "" {
			if tef, ok := expr.Expression.Function.(*query.TimeExtractionFunction); ok {
				// Found time extraction expression: [(day ?t) ?d]
				// We need to map field="day" + input var ?t -> output var ?d
				if len(expr.Inputs) > 0 {
					inputVar := expr.Inputs[0]
					if timeExtractionVars[tef.Field] == nil {
						timeExtractionVars[tef.Field] = make(map[string]query.Symbol)
					}
					timeExtractionVars[tef.Field][string(inputVar)] = expr.Output
				}
			}
		}
	}

	// Collect all constraints from patterns
	for _, pattern := range phase.Patterns {
		if pattern.Metadata == nil {
			continue
		}

		constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint)
		if !ok {
			continue
		}

		for _, constraint := range constraints {
			switch constraint.Type {
			case ConstraintEquality:
				// Reconstruct equality predicate: [(= ?var value)]
				// The variable comes from the pattern's value position
				if dp, ok := pattern.Pattern.(*query.DataPattern); ok && len(dp.Elements) >= 3 {
					if v, ok := dp.Elements[2].(query.Variable); ok {
						predicates = append(predicates, &query.Comparison{
							Left:  query.VariableTerm{Symbol: query.Symbol(v.Name)},
							Right: query.ConstantTerm{Value: constraint.Value},
							Op:    query.OpEQ,
						})
					}
				}

			case ConstraintRange:
				// Reconstruct range predicate: [(> ?var value)] or similar
				if dp, ok := pattern.Pattern.(*query.DataPattern); ok && len(dp.Elements) >= 3 {
					if v, ok := dp.Elements[2].(query.Variable); ok {
						predicates = append(predicates, &query.Comparison{
							Left:  query.VariableTerm{Symbol: query.Symbol(v.Name)},
							Right: query.ConstantTerm{Value: constraint.Value},
							Op:    constraint.Operator,
						})
					}
				}

			case ConstraintTimeExtraction:
				// Reconstruct time extraction predicate: [(= ?d 20)]
				// We need to find the output variable from the time extraction expression
				// The constraint has TimeField (e.g., "day") and we need the pattern's time variable
				if dp, ok := pattern.Pattern.(*query.DataPattern); ok && len(dp.Elements) >= 3 {
					if v, ok := dp.Elements[2].(query.Variable); ok {
						// v is the time variable from the pattern (e.g., ?t)
						// Look up the corresponding extraction output variable (e.g., ?d)
						if fieldMap, found := timeExtractionVars[constraint.TimeField]; found {
							if outputVar, found := fieldMap[string(v.Name)]; found {
								// Found it! Create predicate: [(op ?d value)]
								predicates = append(predicates, &query.Comparison{
									Left:  query.VariableTerm{Symbol: outputVar},
									Right: query.ConstantTerm{Value: constraint.Value},
									Op:    constraint.Operator,
								})
							}
						}
					}
				}
			}
		}
	}

	return predicates
}

// realizePhase converts a Phase (with 7 operation types) into a RealizedPhase
// (with a single Query containing Clause list).
//
// The resulting Query is independently executable:
// - :in clause documents inputs from previous phase's Keep
// - :find clause outputs this phase's Keep (or final :find for last phase)
// - :where clause contains all operations in execution order
//
// prevKeep is the previous phase's Keep symbols (for :in clause), nil for first phase.
func realizePhase(phase Phase, isLastPhase bool, prevKeep []query.Symbol) RealizedPhase {
	var where []query.Clause

	// CRITICAL: Preserve EXACT execution order from current executor!
	// This ensures identical results for validation.
	//
	// Current executor executes in this order:
	//   1. Patterns (pattern matching)
	//   2. Expressions (function evaluation)
	//   3. Predicates (filtering)
	//   4. Subqueries (nested query execution)

	// 1. Add patterns (in order)
	for _, pp := range phase.Patterns {
		// Pattern is stored as query.Pattern interface, but we need query.Clause
		// DataPattern implements both Pattern and Clause, so type assert
		if dp, ok := pp.Pattern.(*query.DataPattern); ok {
			where = append(where, dp)
		}
	}

	// 2. Add expressions (in order)
	for _, ep := range phase.Expressions {
		where = append(where, ep.Expression)
	}

	// 3. Add predicates (in order)
	for _, pred := range phase.Predicates {
		where = append(where, pred.Predicate)
	}

	// 3a. Reconstruct predicates from storage constraints (for pushed predicates)
	// These predicates were removed from phase.Predicates after being pushed to storage,
	// but need to appear in the realized query for semantic completeness.
	reconstructedPredicates := reconstructPredicatesFromConstraints(phase)
	where = append(where, reconstructedPredicates...)

	// 3b. Add join predicates (optimization hints that are also predicates)
	for _, jp := range phase.JoinPredicates {
		where = append(where, jp.Predicate)
	}

	// 4. Add subqueries (in order)
	// Note: We include all subqueries, even those marked as Decorrelated.
	// The Metadata will contain decorrelation hints for optimization.
	for _, sq := range phase.Subqueries {
		where = append(where, sq.Subquery)
	}

	// Build :find clause
	// - Last phase: Use Phase.Find (preserves aggregates from original query)
	// - Intermediate phases: Reconstruct from Keep (what actually passes forward)
	var find []query.FindElement
	if isLastPhase {
		// Last phase uses original query's :find (with aggregates)
		find = phase.Find
	} else {
		// Intermediate phase: output only what's in Keep (what passes to next phase)
		for _, sym := range phase.Keep {
			find = append(find, query.FindVariable{Symbol: sym})
		}
	}

	// Build :in clause from previous phase's Keep (what was actually passed forward)
	// First phase has no :in, subsequent phases receive previous Keep
	var in []query.InputSpec
	if len(prevKeep) > 0 {
		in = append(in, query.DatabaseInput{})
		in = append(in, query.RelationInput{Symbols: prevKeep})
	}

	return RealizedPhase{
		Query: &query.Query{
			Find:  find,
			In:    in,
			Where: where,
		},
		Available: phase.Available,
		Provides:  phase.Provides,
		Keep:      phase.Keep,
		Metadata:  phase.Metadata,
	}
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
