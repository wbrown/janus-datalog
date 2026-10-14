package query

import (
	"fmt"
	"strings"
	"sync"

	"github.com/wbrown/janus-datalog/datalog"
)

// RulesInput is the % input: the rule set the query's invocations resolve
// against. It consumes one input value and binds no symbol.
type RulesInput struct{}

func (RulesInput) isInputSpec()   {}
func (RulesInput) String() string { return "%" }

// RuleInvocation is a clause invoking a predicate of the rule set, (name
// args...). An argument is a variable, a constant or a blank. The predicate's
// derived relation, projected onto the arguments, joins the enclosing clause
// scope as a subquery's relation binding does. Its scope, the positions the
// predicate requires bound among them, is read from the rule set the query
// runs with (ScopeOf). An occurrence that reads a stratum's last delta
// invokes the predicate's delta symbol (DeltaSymbol).
type RuleInvocation struct {
	Name Symbol
	Args []PatternElement
}

func (*RuleInvocation) clause() {}

// String renders the invocation as the parser reads it.
func (r *RuleInvocation) String() string {
	var sb strings.Builder
	sb.WriteString("(")
	sb.WriteString(r.Name.String())
	for _, arg := range r.Args {
		sb.WriteString(" ")
		sb.WriteString(arg.String())
	}
	sb.WriteString(")")
	return sb.String()
}

// Variables returns the invocation's variable arguments, each once, in
// argument order.
func (r *RuleInvocation) Variables() []Symbol {
	var vars []Symbol
	for _, arg := range r.Args {
		if v, ok := arg.(Variable); ok && !ContainsSymbol(vars, v.Name) {
			vars = append(vars, v.Name)
		}
	}
	return vars
}

// Rule is one way to derive a predicate's tuples: each binding of the head
// variables the body admits is one tuple of the predicate. Required is the
// leading run of head variables the caller binds, Datomic's [(name [?x] ?y)
// ...]; a body may consume them without binding them.
type Rule struct {
	Name     Symbol
	Head     []Symbol
	Required []Symbol
	Body     []Clause
}

// String renders the rule as ParseRules reads it, [(name head...) body...],
// the required run as its vector.
func (r *Rule) String() string {
	var sb strings.Builder
	sb.WriteString("[(")
	sb.WriteString(r.Name.String())
	if len(r.Required) > 0 {
		sb.WriteString(" [")
		for i, head := range r.Required {
			if i > 0 {
				sb.WriteString(" ")
			}
			sb.WriteString(head.String())
		}
		sb.WriteString("]")
	}
	for _, head := range r.Head[len(r.Required):] {
		sb.WriteString(" ")
		sb.WriteString(head.String())
	}
	sb.WriteString(")")
	for _, clause := range r.Body {
		sb.WriteString(" ")
		sb.WriteString(clause.String())
	}
	sb.WriteString("]")
	return sb.String()
}

// Stratum is one strongly connected component of the predicate dependency
// graph. Recursive holds when a predicate in it invokes a predicate in it.
type Stratum struct {
	Predicates []Symbol
	Recursive  bool
}

// RuleSet is a validated, stratified set of rules. NewRuleSet is the one
// constructor; a RuleSet is never copied by value.
type RuleSet struct {
	order    []Symbol
	rules    map[Symbol][]*Rule
	arity    map[Symbol]int
	required map[Symbol][]bool
	strata   []Stratum

	// attributePositions marks, per predicate and head position, whether a
	// body pattern on the default source reads the head symbol there as its
	// attribute, directly or through a body invocation (AttributePositions).
	attributePositions map[Symbol][]bool

	// attributes is the roster DefaultSourceAttributes returns, collected on
	// its first call. A rule set is immutable and serves every query that
	// passes it, so the roster is collected once.
	attributesOnce sync.Once
	attributes     []datalog.Keyword
}

// clauseForms are the symbols the parser reads as a clause form at the head
// of a list clause, so no predicate is named for one: a list headed by it
// never parses as an invocation.
var clauseForms = []Symbol{
	datalog.NewSymbol("not"),
	datalog.NewSymbol("not-join"),
	datalog.NewSymbol("or"),
	datalog.NewSymbol("or-join"),
	datalog.NewSymbol("or-default"),
	datalog.NewSymbol("or-default-join"),
	datalog.NewSymbol("and"),
}

// ClauseForm reports whether name heads a clause form.
func ClauseForm(name Symbol) bool { return ContainsSymbol(clauseForms, name) }

// NewRuleSet validates and stratifies rules. No predicate is named in a
// namespace the engine mints into, for a clause form, or as a variable; a
// head names at least one variable; the heads of one predicate agree on
// arity and are distinct variables; every rule is safe; every invocation
// names a defined predicate at its arity; no predicates depend on each other
// through negation; a predicate invoked by a rule of its own stratum is
// invoked at the top level of the body, in an or branch, or in an or-join
// branch; and a predicate with required positions is recursive only with
// predicates that have them.
func NewRuleSet(rules []*Rule) (*RuleSet, error) {
	for _, rule := range rules {
		switch {
		case engineNamespace(rule.Name):
			return nil, fmt.Errorf("rule %s: the namespace %s is the engine's, which names the adorned, magic and delta predicates it mints there", rule.Name, rule.Name.Namespace())
		case ClauseForm(rule.Name):
			return nil, fmt.Errorf("rule %s: %s heads a clause form, so a list headed by it never parses as an invocation", rule.Name, rule.Name)
		case rule.Name.IsVariable():
			return nil, fmt.Errorf("rule %s: a predicate's name is not a variable", rule.Name)
		case len(rule.Head) == 0:
			return nil, fmt.Errorf("rule %s: a head names at least one variable", rule.Name)
		}
	}
	return newRuleSet(rules, nil)
}

// seedPredicate is a predicate declared by arity with no rules of its own:
// the relation a caller fills, the magic predicate of a rewritten set.
type seedPredicate struct {
	name  Symbol
	arity int
}

// newRuleSet builds a set whose seed predicates, declared in the order
// given, are the relations a caller fills (Magic). A seed is a stratum of
// its own unless a rule derives into it.
func newRuleSet(rules []*Rule, seeds []seedPredicate) (*RuleSet, error) {
	rs := &RuleSet{rules: map[Symbol][]*Rule{}, arity: map[Symbol]int{}, required: map[Symbol][]bool{}}
	for _, seed := range seeds {
		rs.order = append(rs.order, seed.name)
		rs.arity[seed.name] = seed.arity
	}
	for _, rule := range rules {
		if rule.Name == nil {
			return nil, fmt.Errorf("rule with no name")
		}
		if arity, seen := rs.arity[rule.Name]; !seen {
			rs.order = append(rs.order, rule.Name)
			rs.arity[rule.Name] = len(rule.Head)
		} else if arity != len(rule.Head) {
			return nil, &RuleArityError{Name: rule.Name, Expected: arity, Got: len(rule.Head)}
		}
		for i, head := range rule.Head {
			if head == nil || !head.IsVariable() {
				return nil, fmt.Errorf("rule %s: head position %d holds %v, not a variable", rule.Name, i, head)
			}
			if ContainsSymbol(rule.Head[:i], head) {
				return nil, fmt.Errorf("rule %s: head variable %s is repeated", rule.Name, head)
			}
		}
		required, err := requiredRun(rule)
		if err != nil {
			return nil, err
		}
		if earlier, seen := rs.required[rule.Name]; !seen {
			rs.required[rule.Name] = required
		} else if !equalMasks(earlier, required) {
			return nil, fmt.Errorf("rule %s: its required run %v differs from an earlier rule's of the same predicate", rule.Name, rule.Required)
		}
	}
	for _, rule := range rules {
		rs.rules[rule.Name] = append(rs.rules[rule.Name], rule)
	}
	edges, err := rs.dependencies(rules)
	if err != nil {
		return nil, err
	}
	for _, rule := range rules {
		if err := rs.checkRuleSafety(rule); err != nil {
			return nil, err
		}
	}
	strata, component, err := rs.stratify(edges)
	if err != nil {
		return nil, err
	}
	for _, edge := range edges {
		if edge.form != "" && component[edge.from] == component[edge.to] {
			return nil, &NestedRecursionError{Rule: edge.from, Predicate: edge.to, Form: edge.form}
		}
	}
	// A predicate with required positions is evaluated at its invocation,
	// through the rewrite, never whole; one recursive with a predicate the
	// fixpoint evaluates whole would be drawn into that evaluation without
	// its bindings.
	for _, stratum := range strata {
		if !stratum.Recursive {
			continue
		}
		var requiring, free Symbol
		for _, p := range stratum.Predicates {
			if countBound(rs.required[p]) > 0 {
				if requiring == nil {
					requiring = p
				}
			} else if free == nil {
				free = p
			}
		}
		if requiring != nil && free != nil {
			return nil, fmt.Errorf("rule %s: a predicate with required positions is recursive with %s, which has none; a predicate requiring bindings is evaluated at its invocation, never whole with its stratum", requiring, free)
		}
	}
	rs.strata = strata
	rs.attributePositions = attributePositionsOf(rs, rules)
	return rs, nil
}

// attributePositionsOf computes, to a fixpoint over the rules, the head
// positions each predicate reads as an attribute: a body pattern on the
// default source whose attribute is the head symbol, or a body invocation
// passing the head symbol at a position the invoked predicate reads as one.
// The walk stops at a subquery body, whose inputs are bound when it runs and
// checked then. A seed predicate, with no rules, reads none.
func attributePositionsOf(rs *RuleSet, rules []*Rule) map[Symbol][]bool {
	positions := make(map[Symbol][]bool, len(rs.order))
	for _, name := range rs.order {
		positions[name] = make([]bool, rs.arity[name])
	}
	for changed := true; changed; {
		changed = false
		for _, rule := range rules {
			mask := positions[rule.Name]
			mark := func(sym Symbol) {
				if i := SymbolIndex(rule.Head, sym); i >= 0 && !mask[i] {
					mask[i] = true
					changed = true
				}
			}
			WalkClauses(rule.Body, func(clause Clause) bool {
				switch c := clause.(type) {
				case *DataPattern:
					if c.Source != nil && c.Source != datalog.SymDollar {
						return true
					}
					if a, ok := c.GetA().(Variable); ok {
						mark(a.Name)
					}
				case *RuleInvocation:
					invoked := positions[c.Name]
					for j, arg := range c.Args {
						if v, ok := arg.(Variable); ok && j < len(invoked) && invoked[j] {
							mark(v.Name)
						}
					}
				}
				return true
			})
		}
	}
	return positions
}

// Predicates returns the defined predicates in first-appearance order.
func (rs *RuleSet) Predicates() []Symbol { return rs.order }

// Rules returns the rules of a predicate in declaration order, nil for a
// predicate the set does not define.
func (rs *RuleSet) Rules(name Symbol) []*Rule { return rs.rules[name] }

// Arity returns a predicate's arity and whether the set defines it.
func (rs *RuleSet) Arity(name Symbol) (int, bool) {
	arity, ok := rs.arity[name]
	return arity, ok
}

// Required returns, per position, whether the predicate requires the caller
// to bind it; nil for a predicate the set does not define.
func (rs *RuleSet) Required(name Symbol) []bool {
	return rs.required[name]
}

// CheckRequired reports an invocation of name that leaves a position the
// predicate requires bound free, bound marking the positions the invocation
// binds. rule is the invoking rule, nil for the query's own invocation.
func (rs *RuleSet) CheckRequired(name Symbol, bound []bool, rule Symbol) error {
	for i, required := range rs.required[name] {
		if required && !bound[i] {
			return &RequiredBindingError{Name: name, Symbol: rs.rules[name][0].Head[i], Rule: rule}
		}
	}
	return nil
}

// requiredRun checks that a rule's required run leads its head and returns
// the per-position mask.
func requiredRun(rule *Rule) ([]bool, error) {
	if len(rule.Required) > len(rule.Head) {
		return nil, fmt.Errorf("rule %s: the required run %v is longer than the head", rule.Name, rule.Required)
	}
	for i, sym := range rule.Required {
		if rule.Head[i] != sym {
			return nil, fmt.Errorf("rule %s: required variable %s does not lead the head: a required run is the head's first variables", rule.Name, sym)
		}
	}
	mask := make([]bool, len(rule.Head))
	for i := range rule.Required {
		mask[i] = true
	}
	return mask, nil
}

func equalMasks(a, b []bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Strata returns the strata in evaluation order: every predicate a stratum's
// bodies invoke is in that stratum or an earlier one.
func (rs *RuleSet) Strata() []Stratum { return rs.strata }

// sameStratum reports whether two predicates are in one stratum.
func (rs *RuleSet) sameStratum(a, b Symbol) bool {
	for _, stratum := range rs.strata {
		if ContainsSymbol(stratum.Predicates, a) {
			return ContainsSymbol(stratum.Predicates, b)
		}
	}
	return false
}

// CheckInvocation reports an invocation of a predicate the set does not
// define, or one at the wrong arity.
func (rs *RuleSet) CheckInvocation(inv *RuleInvocation) error {
	arity, defined := rs.arity[inv.Name]
	if !defined {
		return &UnknownRuleError{Name: inv.Name, Defined: rs.order}
	}
	if len(inv.Args) != arity {
		return &RuleArityError{Name: inv.Name, Expected: arity, Got: len(inv.Args)}
	}
	return nil
}

// DefaultSourceAttributes returns every attribute the rule bodies name on
// the default source, in order of first appearance and each once, collected
// as Query.DefaultSourceAttributes collects a query's under the rule set
// itself, then the keywords the bodies' invocations pass at attribute
// positions (InvocationAttributes).
func (rs *RuleSet) DefaultSourceAttributes() []datalog.Keyword {
	rs.attributesOnce.Do(func() {
		roster := newAttributeRoster()
		for _, name := range rs.order {
			for _, rule := range rs.rules[name] {
				roster.addClauses(rule.Body, false, rs)
			}
		}
		for _, attr := range rs.InvocationAttributes(roster.invocations) {
			roster.add(attr)
		}
		rs.attributes = roster.order
	})
	return rs.attributes
}

// AttributePositions reports, per head position of a predicate, whether a
// body pattern on the default source reads the head symbol at that position
// as its attribute: in the predicate's own bodies at any depth short of a
// subquery body, or through a body invocation in the bodies of the predicate
// invoked. A keyword an invocation passes at such a position names an
// attribute (InvocationAttributes). Nil for a predicate the set does not
// define.
func (rs *RuleSet) AttributePositions(name Symbol) []bool {
	return rs.attributePositions[name]
}

// InvocationAttributes returns, from a roster of invocation constants, the
// keywords passed at the positions the invoked predicate reads as an
// attribute, in order and each once. A keyword at a value position, or
// passed to a predicate the set does not define, names nothing.
func (rs *RuleSet) InvocationAttributes(constants []InvocationConstant) []datalog.Keyword {
	if len(constants) == 0 {
		return nil
	}
	roster := newAttributeRoster()
	for _, c := range constants {
		if mask := rs.attributePositions[c.Name]; c.Position < len(mask) && mask[c.Position] {
			roster.add(c.Attr)
		}
	}
	return roster.order
}

// checkRuleSafety reports the first symbol neither the rule's positive body
// clauses nor its required run binds: a head symbol, or a mandatory
// correlate of a body clause, an invocation's required positions among
// them, read from the set's own declarations. A source symbol is execution
// context, never a binding.
func (rs *RuleSet) checkRuleSafety(rule *Rule) error {
	provided := map[Symbol]bool{}
	for _, sym := range rule.Required {
		provided[sym] = true
	}
	for _, clause := range rule.Body {
		for _, sym := range ScopeOf(clause, rs).Provides {
			provided[sym] = true
		}
	}
	for _, head := range rule.Head {
		if !provided[head] {
			return &UnsafeRuleError{Rule: rule.Name, Symbol: head}
		}
	}
	for _, clause := range rule.Body {
		for _, sym := range ScopeOf(clause, rs).Mandatory {
			if sym.IsSource() || provided[sym] {
				continue
			}
			return &UnsafeRuleError{Rule: rule.Name, Symbol: sym}
		}
	}
	return nil
}

// invocationEdge is one dependency: the rule named from invokes to. negative
// holds when the invocation sits under a not or not-join at any depth, and
// form names the innermost or-default, or-default-join or subquery enclosing
// it, "" when none does.
type invocationEdge struct {
	from, to Symbol
	negative bool
	form     string
}

// RulesUse returns the first clause in clauses that uses the query's % in
// the query's own scope, nil when none does: a rule invocation at any depth
// short of a subquery body, or a subquery call passing % through. A subquery
// body's invocations use the inner query's %, which its own :in declares.
func RulesUse(clauses []Clause) Clause {
	var use Clause
	WalkClauses(clauses, func(clause Clause) bool {
		switch c := clause.(type) {
		case *RuleInvocation:
			use = c
		case *SubqueryPattern:
			for _, input := range c.Inputs {
				if constant, ok := input.(Constant); ok && constant.Value == datalog.SymRules {
					use = c
				}
			}
		}
		return use == nil
	})
	return use
}

// Invocations returns every rule invocation in clauses at any depth, in
// clause order, subquery bodies included.
func Invocations(clauses []Clause) []*RuleInvocation {
	var found []*RuleInvocation
	forInvocations(clauses, false, "", func(inv *RuleInvocation, _ bool, _ string) {
		found = append(found, inv)
	})
	return found
}

// forInvocations visits every invocation in clauses at any depth with
// whether it sits under a negation and the innermost non-monotone form
// enclosing it. It descends into subquery bodies, which WalkClauses leaves
// to its visitors: a body's invocation is a dependency wherever it sits.
func forInvocations(clauses []Clause, negative bool, form string, visit func(inv *RuleInvocation, negative bool, form string)) {
	for _, clause := range clauses {
		switch c := clause.(type) {
		case *RuleInvocation:
			visit(c, negative, form)
		case *NotClause:
			forInvocations(c.Clauses, true, form, visit)
		case *NotJoinClause:
			forInvocations(c.Clauses, true, form, visit)
		case *OrClause:
			for _, branch := range c.Branches {
				forInvocations(branch, negative, form, visit)
			}
		case *OrJoinClause:
			for _, branch := range c.Branches {
				forInvocations(branch, negative, form, visit)
			}
		case *OrDefaultClause:
			for _, branch := range c.Branches {
				forInvocations(branch, negative, "or-default", visit)
			}
		case *OrDefaultJoinClause:
			for _, branch := range c.Branches {
				forInvocations(branch, negative, "or-default-join", visit)
			}
		case *SubqueryPattern:
			forInvocations(c.Query.Where, negative, "subquery", visit)
		}
	}
}

// dependencies collects every invocation edge of the rules, checking each
// invocation against the defined predicates and their arities.
func (rs *RuleSet) dependencies(rules []*Rule) ([]invocationEdge, error) {
	var edges []invocationEdge
	var err error
	for _, rule := range rules {
		forInvocations(rule.Body, false, "", func(inv *RuleInvocation, negative bool, form string) {
			if err != nil {
				return
			}
			if err = rs.CheckInvocation(inv); err != nil {
				return
			}
			edges = append(edges, invocationEdge{from: rule.Name, to: inv.Name, negative: negative, form: form})
		})
		if err != nil {
			return nil, err
		}
	}
	return edges, nil
}

// stratify finds the strongly connected components of the dependency graph
// with Tarjan's algorithm, which emits a component after every component it
// depends on, so the components are the strata in evaluation order. A
// component holding a negative edge is refused. It returns the strata and
// each predicate's stratum index.
func (rs *RuleSet) stratify(edges []invocationEdge) ([]Stratum, map[Symbol]int, error) {
	successors := map[Symbol][]Symbol{}
	for _, edge := range edges {
		if !ContainsSymbol(successors[edge.from], edge.to) {
			successors[edge.from] = append(successors[edge.from], edge.to)
		}
	}

	index := map[Symbol]int{}
	low := map[Symbol]int{}
	onStack := map[Symbol]bool{}
	var stack []Symbol
	var components [][]Symbol
	next := 0
	var visit func(v Symbol)
	visit = func(v Symbol) {
		index[v] = next
		low[v] = next
		next++
		stack = append(stack, v)
		onStack[v] = true
		for _, w := range successors[v] {
			if _, seen := index[w]; !seen {
				visit(w)
				if low[w] < low[v] {
					low[v] = low[w]
				}
			} else if onStack[w] && index[w] < low[v] {
				low[v] = index[w]
			}
		}
		if low[v] != index[v] {
			return
		}
		var component []Symbol
		for {
			w := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			onStack[w] = false
			component = append(component, w)
			if w == v {
				break
			}
		}
		components = append(components, component)
	}
	for _, predicate := range rs.order {
		if _, seen := index[predicate]; !seen {
			visit(predicate)
		}
	}

	component := map[Symbol]int{}
	strata := make([]Stratum, len(components))
	for i, members := range components {
		// The component's predicates in declaration order, so the stratum
		// renders the same whatever order the search reached them in.
		var ordered []Symbol
		for _, predicate := range rs.order {
			if ContainsSymbol(members, predicate) {
				ordered = append(ordered, predicate)
				component[predicate] = i
			}
		}
		strata[i] = Stratum{Predicates: ordered}
	}
	for _, edge := range edges {
		if component[edge.from] != component[edge.to] {
			continue
		}
		if edge.negative {
			return nil, nil, &UnstratifiableError{Predicates: strata[component[edge.from]].Predicates}
		}
		strata[component[edge.from]].Recursive = true
	}
	return strata, component, nil
}
