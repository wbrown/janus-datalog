// Package algebra provides a relational algebra IR for Datalog query optimization.
//
// Datalog clauses compile to algebra nodes, algebraic equivalence rules
// are applied as EBNF transform passes, then the optimized tree is executed.
// This enables principled optimizations like lateral join decorrelation
// without special-casing individual clause patterns.
package algebra

import (
	"fmt"
	"strings"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Rule name constants for parse.Node representation.
// These are the strings used in parse.Node.Rule when the algebra tree
// is adapted for the EBNF transform framework.
const (
	RuleScan        = "Scan"
	RuleSelect      = "Select"
	RuleProject     = "Project"
	RuleMap         = "Map"
	RuleJoin        = "Join"
	RuleAntiJoin    = "AntiJoin"
	RuleUnion       = "Union"
	RuleLateralJoin = "LateralJoin"
	RuleAggregate   = "Aggregate"
	RuleConstant    = "Constant"
)

// JoinKind distinguishes inner joins from left outer joins.
type JoinKind int

const (
	InnerJoin    JoinKind = iota
	LeftOuterJoin // OR-fallback semantics: preserve left side, fill defaults
)

func (k JoinKind) String() string {
	switch k {
	case InnerJoin:
		return "Inner"
	case LeftOuterJoin:
		return "LeftOuter"
	default:
		return fmt.Sprintf("JoinKind(%d)", int(k))
	}
}

// Node is a relational algebra operator. Each variant corresponds to
// one of the Rule* constants and carries its operator-specific data.
// Children are other Nodes forming the expression tree.
type Node struct {
	Op       string  // One of the Rule* constants
	Children []*Node // Operator inputs (0 for leaves, 1-2 for unary/binary)
	Data     OpData  // Operator-specific payload
}

// Symbols returns the output symbols this node produces.
func (n *Node) Symbols() []query.Symbol {
	return n.Data.OutputSymbols()
}

// String returns a human-readable representation of the algebra tree.
func (n *Node) String() string {
	return formatNode(n, 0)
}

func formatNode(n *Node, indent int) string {
	prefix := strings.Repeat("  ", indent)
	s := fmt.Sprintf("%s%s(%s)", prefix, n.Op, n.Data)
	for _, child := range n.Children {
		s += "\n" + formatNode(child, indent+1)
	}
	return s
}

// OpData is the interface for operator-specific data.
type OpData interface {
	OutputSymbols() []query.Symbol
	String() string
}

// Scan reads from storage via pattern matching.
// Compiled from query.DataPattern.
type Scan struct {
	Source   query.Symbol       // "$" or named source
	Pattern *query.DataPattern  // Original pattern for executor
	Output  []query.Symbol      // Symbols this scan produces
}

func (s *Scan) OutputSymbols() []query.Symbol { return s.Output }
func (s *Scan) String() string {
	if s.Pattern == nil {
		return "subquery-scan"
	}
	return s.Pattern.String()
}

// Select filters tuples by a predicate. σ_p(R)
// Compiled from Comparison, ChainedComparison, predicates.
type Select struct {
	Predicate query.Predicate
	Required  []query.Symbol // Symbols the predicate references
	Output    []query.Symbol // Same as child's output (passthrough)
}

func (s *Select) OutputSymbols() []query.Symbol { return s.Output }
func (s *Select) String() string {
	return fmt.Sprintf("σ(%s)", s.Predicate.String())
}

// Project reduces to specified symbols. π_S(R)
type Project struct {
	Symbols []query.Symbol
}

func (p *Project) OutputSymbols() []query.Symbol { return p.Symbols }
func (p *Project) String() string {
	syms := make([]string, len(p.Symbols))
	for i, s := range p.Symbols {
		syms[i] = s.String()
	}
	return fmt.Sprintf("π(%s)", strings.Join(syms, ", "))
}

// Map extends each tuple with a computed value. R + f(R) → R'
// Compiled from query.Expression.
type Map struct {
	Expression *query.Expression // The function + binding
	Required   []query.Symbol    // Input symbols needed
	Output     []query.Symbol    // Full output (child symbols + new binding symbols)
}

func (m *Map) OutputSymbols() []query.Symbol { return m.Output }
func (m *Map) String() string {
	return m.Expression.String()
}

// Join combines two relations. R ⋈ S
type Join struct {
	Kind          JoinKind         // Inner or LeftOuter
	JoinSymbols   []query.Symbol   // Symbols to join on (empty = natural join on shared symbols)
	Output        []query.Symbol   // Combined output symbols
	DefaultValues []interface{}    // For LeftOuterJoin: fill these when right side has no match
	DefaultAttr   *datalog.Keyword // For get-else rewrite: the attribute, enabling typed defaults
}

func (j *Join) OutputSymbols() []query.Symbol { return j.Output }
func (j *Join) String() string {
	syms := make([]string, len(j.JoinSymbols))
	for i, s := range j.JoinSymbols {
		syms[i] = s.String()
	}
	if len(syms) > 0 {
		return fmt.Sprintf("%s on [%s]", j.Kind, strings.Join(syms, ", "))
	}
	return j.Kind.String()
}

// AntiJoin returns tuples from left with no match in right. R ▷ S
// Compiled from query.NotClause / query.NotJoinClause.
type AntiJoin struct {
	JoinSymbols    []query.Symbol // Variables to anti-join on
	Output         []query.Symbol // Same as left child's output
	ExplicitJoin   bool           // True if compiled from NotJoinClause (user specified join vars)
}

func (a *AntiJoin) OutputSymbols() []query.Symbol { return a.Output }
func (a *AntiJoin) String() string {
	syms := make([]string, len(a.JoinSymbols))
	for i, s := range a.JoinSymbols {
		syms[i] = s.String()
	}
	return fmt.Sprintf("▷ on [%s]", strings.Join(syms, ", "))
}

// Union combines branches. R ∪ S
// Compiled from query.OrClause or query.OrJoinClause with union semantics.
type Union struct {
	Output   []query.Symbol // Shared output symbols across branches
	JoinVars []query.Symbol // Explicit join variables from or-join (nil for plain or)
}

func (u *Union) OutputSymbols() []query.Symbol { return u.Output }
func (u *Union) String() string {
	if len(u.JoinVars) > 0 {
		syms := make([]string, len(u.JoinVars))
		for i, s := range u.JoinVars {
			syms[i] = s.String()
		}
		return fmt.Sprintf("∪_join(%s)", strings.Join(syms, ", "))
	}
	return "∪"
}

// LateralJoin is a correlated subquery. R ⋈_L S(r.x)
// THE target for decorrelation: LateralJoin → Join + Aggregate.
type LateralJoin struct {
	CorrelationVars []query.Symbol    // Variables passed from outer to inner
	InnerQuery      *query.Query      // The nested query to execute per outer tuple
	Binding         interface{}       // query.Symbol, query.TupleBinding, etc.
	Output          []query.Symbol    // Combined output (outer + binding symbols)
	DefaultValues   []interface{}     // Fallback values when inner produces no results (from OR-fallback ground)
}

func (l *LateralJoin) OutputSymbols() []query.Symbol { return l.Output }
func (l *LateralJoin) String() string {
	vars := make([]string, len(l.CorrelationVars))
	for i, v := range l.CorrelationVars {
		vars[i] = v.String()
	}
	hasDefaults := ""
	if len(l.DefaultValues) > 0 {
		hasDefaults = " +defaults"
	}
	return fmt.Sprintf("⋈_L(%s)%s", strings.Join(vars, ", "), hasDefaults)
}

// Aggregate groups and aggregates. γ_{keys}(aggs)(R)
// Created by decorrelation transform, or from :find aggregates.
type Aggregate struct {
	GroupBy   []query.Symbol        // Grouping variables
	Functions []query.FindAggregate // Aggregate functions (count, sum, etc.)
	Output    []query.Symbol        // GroupBy keys + aggregate result symbols
}

func (a *Aggregate) OutputSymbols() []query.Symbol { return a.Output }
func (a *Aggregate) String() string {
	keys := make([]string, len(a.GroupBy))
	for i, k := range a.GroupBy {
		keys[i] = k.String()
	}
	return fmt.Sprintf("γ(%s)", strings.Join(keys, ", "))
}

// Constant injects literal values. Compiled from query.GroundPredicate.
type Constant struct {
	Symbols []query.Symbol
	Values  []interface{}
}

func (c *Constant) OutputSymbols() []query.Symbol { return c.Symbols }
func (c *Constant) String() string {
	return fmt.Sprintf("const(%v)", c.Values)
}
