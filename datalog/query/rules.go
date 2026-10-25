package query

import (
	"errors"
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

	// heads holds each predicate's head (Head): its first rule's, or the
	// minted per-position variables of a seed predicate no rule derives.
	heads map[Symbol][]Symbol

	// rosters is what the bodies name on each source (SourceAttributes),
	// collected on its first call. A rule set is immutable and serves every
	// query that passes it, so the rosters are collected once.
	rostersOnce sync.Once
	rosters     *Rosters
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
// namespace the engine mints into, for a clause form, or as a variable; no
// variable of a head or a body lies in a namespace the engine mints into,
// checked before the inference and the unification write the engine's own
// into the body; a head names at least one variable; the heads of one
// predicate agree on arity and are distinct variables; every rule is safe;
// every invocation names a defined predicate at its arity; no predicates
// depend on each other through negation; and a predicate invoked by a rule of
// its own stratum is invoked at the top level of the body, in an or branch,
// in an or-join branch, or in a subquery whose inner query does not
// aggregate.
func NewRuleSet(rules []*Rule) (*RuleSet, error) {
	for _, rule := range rules {
		switch {
		case engineNamespace(rule.Name):
			return nil, fmt.Errorf("rule %s: the namespace %s is the engine's, which mints the adorned, magic, delta, scope and unify names there", rule.Name, rule.Name.Namespace())
		case ClauseForm(rule.Name):
			return nil, fmt.Errorf("rule %s: %s heads a clause form, so a list headed by it never parses as an invocation", rule.Name, rule.Name)
		case rule.Name.IsVariable():
			return nil, fmt.Errorf("rule %s: a predicate's name is not a variable", rule.Name)
		case len(rule.Head) == 0:
			return nil, fmt.Errorf("rule %s: a head names at least one variable", rule.Name)
		}
		for _, head := range rule.Head {
			if engineNamespace(head) {
				return nil, &EngineNamespaceError{Variable: head, Rule: rule.Name}
			}
		}
		if v := engineVariableIn(rule.Body, nil); v != nil {
			return nil, &EngineNamespaceError{Variable: v, Rule: rule.Name}
		}
	}
	return NewSeededRuleSet(rules, nil)
}

// Head is the symbols a predicate's relation carries, which an invocation's
// arguments unify with by position: the first rule's head for a predicate
// with rules, the minted per-position variables for a seed predicate no rule
// derives, and the predicate's own head under its delta symbol. The second
// value is false for a name the set does not declare.
func (rs *RuleSet) Head(name Symbol) ([]Symbol, bool) {
	head, ok := rs.heads[name]
	return head, ok
}

// declareHead records a predicate's head under its symbol and under its
// delta symbol, whose relation is over the same head.
func (rs *RuleSet) declareHead(name Symbol, head []Symbol) {
	rs.heads[name] = head
	rs.heads[DeltaSymbol(name)] = head
}

// SeedPredicate is a predicate declared by arity with no rules of its own:
// the relation a caller fills, the magic predicate of a rewritten set.
type SeedPredicate struct {
	Name  Symbol
	Arity int
}

// NewSeededRuleSet builds a set whose seed predicates, declared in the order
// given, are the relations a caller fills: the magic predicates of a set the
// planner's rewrite builds. A seed is a stratum of its own unless a rule
// derives into it.
func NewSeededRuleSet(rules []*Rule, seeds []SeedPredicate) (*RuleSet, error) {
	rs := &RuleSet{rules: map[Symbol][]*Rule{}, arity: map[Symbol]int{}, required: map[Symbol][]bool{}, heads: map[Symbol][]Symbol{}}
	for _, seed := range seeds {
		rs.order = append(rs.order, seed.Name)
		rs.arity[seed.Name] = seed.Arity
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
		if _, derived := rs.heads[rule.Name]; !derived {
			rs.declareHead(rule.Name, rule.Head)
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
	for _, seed := range seeds {
		if _, derived := rs.heads[seed.Name]; !derived {
			rs.declareHead(seed.Name, seedHead(seed.Name, seed.Arity))
		}
	}
	inferred := make([]*Rule, len(rules))
	for i, rule := range rules {
		body, err := rs.inferBody(rule)
		if err != nil {
			return nil, err
		}
		inferred[i] = body
		rs.rules[rule.Name] = append(rs.rules[rule.Name], body)
	}
	rules = inferred
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
		if component[edge.from] != component[edge.to] {
			continue
		}
		if form, ok := edge.nonMonotone(); ok {
			return nil, &NestedRecursionError{Rule: edge.from, Predicate: edge.to, Form: form}
		}
	}
	// A recursive stratum may hold a predicate with required positions beside
	// one with none: a free invocation of the latter is rewritten with its
	// head all free, so the former is adorned from the clauses before each of
	// its invocations and derived in the rewritten set with its bindings
	// (NESTED_SCOPES_AS_QUERIES.md §5.5).
	rs.strata = strata
	return rs, nil
}

// CountBound is the number of positions a binding pattern marks as bound.
func CountBound(bound []bool) int {
	n := 0
	for _, b := range bound {
		if b {
			n++
		}
	}
	return n
}

// inferBody returns rule with its body's static shapes walked as at the text
// boundaries, its forms inferred under the set and its patterns unified, as
// a query finding the head with the required run as its inputs: the rule
// itself when its body is header forms over distinct pattern variables
// already, otherwise a copy holding the prepared body. A shape the walk
// refuses is an error naming the rule, and a branch input the inference
// finds nothing binds is an UnsafeRuleError naming the rule and the
// variable.
func (rs *RuleSet) inferBody(rule *Rule) (*Rule, error) {
	if err := ValidateStaticClauseShapes(rule.Body); err != nil {
		return nil, fmt.Errorf("rule %s: %w", rule.Name, err)
	}
	scope := &Query{In: []InputSpec{DatabaseInput{Name: datalog.SymDollar}, RulesInput{}}, Where: rule.Body}
	for _, head := range rule.Head {
		scope.Find = append(scope.Find, FindVariable{Symbol: head})
	}
	for _, sym := range rule.Required {
		scope.In = append(scope.In, ScalarInput{Symbol: sym})
	}
	inferred, err := InferHeaders(scope, rs)
	if err != nil {
		var unbound *UnboundBranchInputError
		if errors.As(err, &unbound) {
			return nil, &UnsafeRuleError{Rule: rule.Name, Symbol: unbound.Variable}
		}
		return nil, fmt.Errorf("rule %s: %w", rule.Name, err)
	}
	inferred = UnifyPatterns(inferred)
	if inferred == scope {
		return rule, nil
	}
	return &Rule{Name: rule.Name, Head: rule.Head, Required: rule.Required, Body: inferred.Where}, nil
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

// ReachesRequired reports whether a rule of name, or of a predicate its rules
// invoke at any depth, invokes a predicate with required positions. A free
// invocation of such a predicate is rewritten with its head all free, so each
// required predicate is adorned from the clauses before its invocation and
// derived in the rewritten set (NESTED_SCOPES_AS_QUERIES.md §5.5).
func (rs *RuleSet) ReachesRequired(name Symbol) bool {
	visited := map[Symbol]bool{}
	worklist := []Symbol{name}
	for len(worklist) > 0 {
		p := worklist[0]
		worklist = worklist[1:]
		if visited[p] {
			continue
		}
		visited[p] = true
		for _, rule := range rs.rules[p] {
			for _, inv := range Invocations(rule.Body) {
				if CountBound(rs.required[inv.Name]) > 0 {
					return true
				}
				worklist = append(worklist, inv.Name)
			}
		}
	}
	return false
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

// SameStratum reports whether two predicates are in one stratum.
func (rs *RuleSet) SameStratum(a, b Symbol) bool {
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

// SourceAttributes returns every attribute the rule bodies name on the
// source, in order of first appearance and each once, collected once over
// every body as Rosters.SourceAttributes collects a query's: the keyword
// constants in the attribute position of the bodies' patterns on the source,
// at any depth, an inner query's under the sources its call passes.
func (rs *RuleSet) SourceAttributes(source Symbol) []datalog.Keyword {
	rs.rostersOnce.Do(func() {
		rosters := newRosters()
		for _, name := range rs.order {
			for _, rule := range rs.rules[name] {
				rosters.addClauses(rule.Body)
			}
		}
		rs.rosters = rosters
	})
	return rs.rosters.SourceAttributes(source)
}

// checkRuleSafety reports the first symbol neither the rule's positive body
// clauses nor its required run binds: a head symbol, or a correlate of a
// body clause, an invocation's required positions among them, read from the
// set's own declarations. It reads the inferred body (inferBody), whose
// nested forms are header forms, so a form's correlates are its required
// run. A source symbol is execution context, never a binding.
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
		for _, sym := range ScopeOf(clause, rs).Correlates {
			if sym.IsSource() || provided[sym] {
				continue
			}
			return &UnsafeRuleError{Rule: rule.Name, Symbol: sym}
		}
	}
	return nil
}

// pathForm is one form enclosing an invocation: the form's name, and for a
// subquery body whether its inner query aggregates.
type pathForm struct {
	name        string
	aggregating bool
}

// invocationEdge is one dependency: the rule named from invokes to, under
// path, the forms enclosing the invocation from the body's top level inward,
// each at any depth: not, not-join, or-default, or-default-join and
// subquery.
type invocationEdge struct {
	from, to Symbol
	path     []pathForm
}

// negative reports whether the invocation sits under a not or not-join.
func (e invocationEdge) negative() bool {
	for _, form := range e.path {
		if form.name == "not" || form.name == "not-join" {
			return true
		}
	}
	return false
}

// nonMonotone is the innermost form on the path whose evaluation is not
// monotone, a fallback form or an aggregating inner query, and whether the
// path holds one. A subquery whose inner query does not aggregate joins its
// result with the scope over the combinations, monotone as a branch is.
func (e invocationEdge) nonMonotone() (string, bool) {
	for i := len(e.path) - 1; i >= 0; i-- {
		form := e.path[i]
		switch form.name {
		case "or-default", "or-default-join":
			return form.name, true
		case "subquery":
			if form.aggregating {
				return form.name, true
			}
		}
	}
	return "", false
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
	forInvocations(clauses, nil, func(inv *RuleInvocation, _ []pathForm) {
		found = append(found, inv)
	})
	return found
}

// forInvocations visits every invocation in clauses at any depth with the
// forms enclosing it, outermost first, path being the forms enclosing
// clauses. It descends into subquery bodies, which WalkClauses leaves to its
// visitors: a body's invocation is a dependency wherever it sits.
func forInvocations(clauses []Clause, path []pathForm, visit func(inv *RuleInvocation, path []pathForm)) {
	under := func(name string, aggregating bool) []pathForm {
		return append(path[:len(path):len(path)], pathForm{name: name, aggregating: aggregating})
	}
	for _, clause := range clauses {
		switch c := clause.(type) {
		case *RuleInvocation:
			visit(c, path)
		case *NotClause:
			forInvocations(c.Clauses, under("not", false), visit)
		case *NotJoinClause:
			forInvocations(c.Clauses, under("not-join", false), visit)
		case *OrClause:
			for _, branch := range c.Branches {
				forInvocations(branch, path, visit)
			}
		case *OrJoinClause:
			for _, branch := range c.Branches {
				forInvocations(branch, path, visit)
			}
		case *OrDefaultClause:
			for _, branch := range c.Branches {
				forInvocations(branch, under("or-default", false), visit)
			}
		case *OrDefaultJoinClause:
			for _, branch := range c.Branches {
				forInvocations(branch, under("or-default-join", false), visit)
			}
		case *SubqueryPattern:
			forInvocations(c.Query.Where, under("subquery", c.Query.Aggregates()), visit)
		}
	}
}

// dependencies collects every invocation edge of the rules, checking each
// invocation against the defined predicates and their arities.
func (rs *RuleSet) dependencies(rules []*Rule) ([]invocationEdge, error) {
	var edges []invocationEdge
	var err error
	for _, rule := range rules {
		forInvocations(rule.Body, nil, func(inv *RuleInvocation, path []pathForm) {
			if err != nil {
				return
			}
			if err = rs.CheckInvocation(inv); err != nil {
				return
			}
			edges = append(edges, invocationEdge{from: rule.Name, to: inv.Name, path: path})
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
		if edge.negative() {
			return nil, nil, &UnstratifiableError{Predicates: strata[component[edge.from]].Predicates}
		}
		strata[component[edge.from]].Recursive = true
	}
	return strata, component, nil
}
