package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// A query's structural identity. Hash folds the typed parts of a query into
// one word and Equal compares two queries part for part. A symbol or keyword
// enters by its interned pointer and a constant by its value hash, as a
// tuple key's values do (datalog.HashValue); each node enters under a tag
// naming its kind and each sequence under its length, so two structures fold
// alike only when they are alike part for part, and a collision is settled
// by Equal. The plan cache keys a plan on the hash and compares the parts on
// a hit. A rule set folds its rules the same way (RuleSet.Hash).

// structuralKind tags a node of a query's structure in its hash.
type structuralKind uint64

const (
	kindQuery structuralKind = iota + 1
	kindFindVariable
	kindFindAggregate
	kindFindPull
	kindDatabaseInput
	kindScalarInput
	kindCollectionInput
	kindTupleInput
	kindRelationInput
	kindRulesInput
	kindDataPattern
	kindComparison
	kindChainedComparison
	kindNotEqual
	kindGround
	kindMissing
	kindStrStartsWith
	kindFunctionPredicate
	kindDatabaseFunctionPredicate
	kindTxRange
	kindExpression
	kindNot
	kindNotJoin
	kindOr
	kindOrJoin
	kindOrDefault
	kindOrDefaultJoin
	kindSubquery
	kindRuleInvocation
	kindVariable
	kindConstant
	kindBlank
	kindVectorConstant
	kindVariableTerm
	kindConstantTerm
	kindNoBinding
	kindSymbolBinding
	kindTupleBinding
	kindCollectionBinding
	kindScalarBinding
	kindRelationBinding
	kindArithmetic
	kindStringConcat
	kindTimeExtraction
	kindGroundFunction
	kindIdentityFunction
	kindComparisonFunction
	kindChainedComparisonFunction
	kindAndFunction
	kindCustomFunction
	kindNth
	kindFirst
	kindLast
	kindLength
	kindContains
	kindIndexOf
	kindSubvec
	kindEnumerate
	kindGetElse
	kindMissingFunction
	kindGetSome
	kindNoPull
	kindPullAttribute
	kindPullWildcard
	kindPullMap
	kindPullLimit
	kindPullDefault
	kindNoLimit
	kindLimit
	kindRuleSet
	kindRule
)

// structuralHash is an FNV-1a fold of words, the fold HashValues makes of a
// tuple's values.
type structuralHash uint64

func newStructuralHash() structuralHash { return 14695981039346656037 }

func (h *structuralHash) word(w uint64) { *h = structuralHash((uint64(*h) ^ w) * 1099511628211) }

func (h *structuralHash) kind(k structuralKind) { h.word(uint64(k)) }

func (h *structuralHash) count(n int) { h.word(uint64(n)) }

func (h *structuralHash) flag(b bool) {
	if b {
		h.word(1)
		return
	}
	h.word(0)
}

func (h *structuralHash) symbol(s Symbol) { h.word(datalog.HashValue(s)) }

func (h *structuralHash) symbols(syms []Symbol) {
	h.count(len(syms))
	for _, s := range syms {
		h.symbol(s)
	}
}

func (h *structuralHash) keyword(k datalog.Keyword) { h.word(datalog.HashValue(k)) }

func (h *structuralHash) value(v interface{}) { h.word(datalog.HashValue(v)) }

// Hash is the query's structural hash: its find elements, inputs, clauses,
// ordering, limit and scalar marker, each under its kind.
func (q *Query) Hash() uint64 {
	h := newStructuralHash()
	hashQuery(&h, q)
	return uint64(h)
}

func hashQuery(h *structuralHash, q *Query) {
	h.kind(kindQuery)
	h.count(len(q.Find))
	for _, elem := range q.Find {
		hashFind(h, elem)
	}
	h.count(len(q.In))
	for _, input := range q.In {
		hashInput(h, input)
	}
	hashClauses(h, q.Where)
	h.count(len(q.OrderBy))
	for _, order := range q.OrderBy {
		h.symbol(order.Variable)
		h.flag(order.Descending)
	}
	if q.Limit == nil {
		h.kind(kindNoLimit)
	} else {
		h.kind(kindLimit)
		h.word(uint64(*q.Limit))
	}
	h.flag(q.ScalarReturn)
}

// Equal reports whether two queries are alike part for part: the same find
// elements, inputs, clauses, ordering, limit and scalar marker, a symbol or
// keyword the same interned pointer and a constant an equal value.
func (q *Query) Equal(other *Query) bool {
	if q == other {
		return true
	}
	if len(q.Find) != len(other.Find) || len(q.In) != len(other.In) || len(q.OrderBy) != len(other.OrderBy) || q.ScalarReturn != other.ScalarReturn {
		return false
	}
	for i := range q.Find {
		if !equalFind(q.Find[i], other.Find[i]) {
			return false
		}
	}
	for i := range q.In {
		if !equalInput(q.In[i], other.In[i]) {
			return false
		}
	}
	if !equalClauses(q.Where, other.Where) {
		return false
	}
	for i := range q.OrderBy {
		if q.OrderBy[i] != other.OrderBy[i] {
			return false
		}
	}
	if (q.Limit == nil) != (other.Limit == nil) {
		return false
	}
	return q.Limit == nil || *q.Limit == *other.Limit
}

// Hash is the set's structural hash, computed at construction: the set's
// predicates in order, each by its name, its arity and its rules.
func (rs *RuleSet) Hash() uint64 { return rs.hash }

func hashRuleSet(rs *RuleSet) uint64 {
	h := newStructuralHash()
	h.kind(kindRuleSet)
	h.count(len(rs.order))
	for _, name := range rs.order {
		h.symbol(name)
		h.count(rs.arity[name])
		rules := rs.rules[name]
		h.count(len(rules))
		for _, rule := range rules {
			hashRule(&h, rule)
		}
	}
	return uint64(h)
}

// Equal reports whether two sets are alike predicate for predicate, in the
// set's order, each at the same arity with the same rules.
func (rs *RuleSet) Equal(other *RuleSet) bool {
	if rs == other {
		return true
	}
	if len(rs.order) != len(other.order) {
		return false
	}
	for i, name := range rs.order {
		if name != other.order[i] || rs.arity[name] != other.arity[name] {
			return false
		}
		rules, others := rs.rules[name], other.rules[name]
		if len(rules) != len(others) {
			return false
		}
		for j := range rules {
			if !equalRule(rules[j], others[j]) {
				return false
			}
		}
	}
	return true
}

func hashRule(h *structuralHash, rule *Rule) {
	h.kind(kindRule)
	h.symbol(rule.Name)
	h.symbols(rule.Head)
	h.symbols(rule.Required)
	hashClauses(h, rule.Body)
}

func equalRule(a, b *Rule) bool {
	return a.Name == b.Name && equalSymbols(a.Head, b.Head) && equalSymbols(a.Required, b.Required) && equalClauses(a.Body, b.Body)
}

func hashFind(h *structuralHash, elem FindElement) {
	switch f := elem.(type) {
	case FindVariable:
		h.kind(kindFindVariable)
		h.symbol(f.Symbol)
	case FindAggregate:
		h.kind(kindFindAggregate)
		h.symbol(f.Function)
		h.symbol(f.Arg)
		h.symbol(f.Predicate)
	case FindPull:
		h.kind(kindFindPull)
		h.symbol(f.Variable)
		hashPull(h, f.Pattern)
	default:
		panic(fmt.Sprintf("query hash: %T is not a find element kind", elem))
	}
}

func equalFind(a, b FindElement) bool {
	switch x := a.(type) {
	case FindVariable:
		y, ok := b.(FindVariable)
		return ok && x.Symbol == y.Symbol
	case FindAggregate:
		y, ok := b.(FindAggregate)
		return ok && x.Function == y.Function && x.Arg == y.Arg && x.Predicate == y.Predicate
	case FindPull:
		y, ok := b.(FindPull)
		return ok && x.Variable == y.Variable && equalPull(x.Pattern, y.Pattern)
	}
	panic(fmt.Sprintf("query equality: %T is not a find element kind", a))
}

func hashInput(h *structuralHash, input InputSpec) {
	switch in := input.(type) {
	case DatabaseInput:
		h.kind(kindDatabaseInput)
		h.symbol(in.Name)
	case ScalarInput:
		h.kind(kindScalarInput)
		h.symbol(in.Symbol)
	case CollectionInput:
		h.kind(kindCollectionInput)
		h.symbol(in.Symbol)
	case TupleInput:
		h.kind(kindTupleInput)
		h.symbols(in.Symbols)
	case RelationInput:
		h.kind(kindRelationInput)
		h.symbols(in.Symbols)
	case RulesInput:
		h.kind(kindRulesInput)
	default:
		panic(fmt.Sprintf("query hash: %T is not an input kind", input))
	}
}

func equalInput(a, b InputSpec) bool {
	switch x := a.(type) {
	case DatabaseInput:
		y, ok := b.(DatabaseInput)
		return ok && x.Name == y.Name
	case ScalarInput:
		y, ok := b.(ScalarInput)
		return ok && x.Symbol == y.Symbol
	case CollectionInput:
		y, ok := b.(CollectionInput)
		return ok && x.Symbol == y.Symbol
	case TupleInput:
		y, ok := b.(TupleInput)
		return ok && equalSymbols(x.Symbols, y.Symbols)
	case RelationInput:
		y, ok := b.(RelationInput)
		return ok && equalSymbols(x.Symbols, y.Symbols)
	case RulesInput:
		_, ok := b.(RulesInput)
		return ok
	}
	panic(fmt.Sprintf("query equality: %T is not an input kind", a))
}

func hashClauses(h *structuralHash, clauses []Clause) {
	h.count(len(clauses))
	for _, clause := range clauses {
		hashClause(h, clause)
	}
}

func equalClauses(a, b []Clause) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equalClause(a[i], b[i]) {
			return false
		}
	}
	return true
}

func hashBranches(h *structuralHash, branches [][]Clause) {
	h.count(len(branches))
	for _, branch := range branches {
		hashClauses(h, branch)
	}
}

func equalBranches(a, b [][]Clause) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equalClauses(a[i], b[i]) {
			return false
		}
	}
	return true
}

func hashClause(h *structuralHash, clause Clause) {
	switch c := clause.(type) {
	case *DataPattern:
		h.kind(kindDataPattern)
		h.symbol(c.Source)
		hashElements(h, c.Elements)
	case *Comparison:
		h.kind(kindComparison)
		hashComparison(h, c)
	case *ChainedComparison:
		h.kind(kindChainedComparison)
		hashChainedComparison(h, c)
	case *NotEqualPredicate:
		h.kind(kindNotEqual)
		hashComparison(h, &c.Comparison)
	case *GroundPredicate:
		h.kind(kindGround)
		h.symbols(c.Variables)
	case *MissingPredicate:
		h.kind(kindMissing)
		h.symbols(c.Variables)
	case *StrStartsWithPredicate:
		h.kind(kindStrStartsWith)
		hashTerm(h, c.Value)
		hashTerm(h, c.Prefix)
	case *FunctionPredicate:
		h.kind(kindFunctionPredicate)
		h.value(c.Fn)
		hashElements(h, c.Args)
	case *DatabaseFunctionPredicate:
		h.kind(kindDatabaseFunctionPredicate)
		hashDatabaseFunction(h, c.Function)
	case *TxRangePredicate:
		h.kind(kindTxRange)
		h.symbol(c.TxVar)
		h.word(c.Low)
		h.word(c.High)
	case *Expression:
		h.kind(kindExpression)
		hashFunction(h, c.Function)
		hashBinding(h, c.Binding)
	case *NotClause:
		h.kind(kindNot)
		hashClauses(h, c.Clauses)
	case *NotJoinClause:
		h.kind(kindNotJoin)
		h.symbols(c.RequiredVars)
		hashClauses(h, c.Clauses)
	case *OrClause:
		h.kind(kindOr)
		hashBranches(h, c.Branches)
	case *OrJoinClause:
		h.kind(kindOrJoin)
		h.symbols(c.RequiredVars)
		h.symbols(c.OutputVars)
		hashBranches(h, c.Branches)
	case *OrDefaultClause:
		h.kind(kindOrDefault)
		hashBranches(h, c.Branches)
	case *OrDefaultJoinClause:
		h.kind(kindOrDefaultJoin)
		h.symbols(c.RequiredVars)
		h.symbols(c.OutputVars)
		hashBranches(h, c.Branches)
	case *SubqueryPattern:
		h.kind(kindSubquery)
		hashQuery(h, c.Query)
		hashElements(h, c.Inputs)
		hashBindingForm(h, c.Binding)
	case *RuleInvocation:
		h.kind(kindRuleInvocation)
		h.symbol(c.Name)
		hashElements(h, c.Args)
	default:
		panic(fmt.Sprintf("query hash: %T is not a clause kind", clause))
	}
}

func equalClause(a, b Clause) bool {
	switch x := a.(type) {
	case *DataPattern:
		y, ok := b.(*DataPattern)
		return ok && x.Source == y.Source && equalElements(x.Elements, y.Elements)
	case *Comparison:
		y, ok := b.(*Comparison)
		return ok && equalComparison(x, y)
	case *ChainedComparison:
		y, ok := b.(*ChainedComparison)
		return ok && equalChainedComparison(x, y)
	case *NotEqualPredicate:
		y, ok := b.(*NotEqualPredicate)
		return ok && equalComparison(&x.Comparison, &y.Comparison)
	case *GroundPredicate:
		y, ok := b.(*GroundPredicate)
		return ok && equalSymbols(x.Variables, y.Variables)
	case *MissingPredicate:
		y, ok := b.(*MissingPredicate)
		return ok && equalSymbols(x.Variables, y.Variables)
	case *StrStartsWithPredicate:
		y, ok := b.(*StrStartsWithPredicate)
		return ok && equalTerm(x.Value, y.Value) && equalTerm(x.Prefix, y.Prefix)
	case *FunctionPredicate:
		y, ok := b.(*FunctionPredicate)
		return ok && x.Fn == y.Fn && equalElements(x.Args, y.Args)
	case *DatabaseFunctionPredicate:
		y, ok := b.(*DatabaseFunctionPredicate)
		return ok && equalDatabaseFunction(x.Function, y.Function)
	case *TxRangePredicate:
		y, ok := b.(*TxRangePredicate)
		return ok && x.TxVar == y.TxVar && x.Low == y.Low && x.High == y.High
	case *Expression:
		y, ok := b.(*Expression)
		return ok && equalFunction(x.Function, y.Function) && equalBinding(x.Binding, y.Binding)
	case *NotClause:
		y, ok := b.(*NotClause)
		return ok && equalClauses(x.Clauses, y.Clauses)
	case *NotJoinClause:
		y, ok := b.(*NotJoinClause)
		return ok && equalSymbols(x.RequiredVars, y.RequiredVars) && equalClauses(x.Clauses, y.Clauses)
	case *OrClause:
		y, ok := b.(*OrClause)
		return ok && equalBranches(x.Branches, y.Branches)
	case *OrJoinClause:
		y, ok := b.(*OrJoinClause)
		return ok && equalSymbols(x.RequiredVars, y.RequiredVars) && equalSymbols(x.OutputVars, y.OutputVars) && equalBranches(x.Branches, y.Branches)
	case *OrDefaultClause:
		y, ok := b.(*OrDefaultClause)
		return ok && equalBranches(x.Branches, y.Branches)
	case *OrDefaultJoinClause:
		y, ok := b.(*OrDefaultJoinClause)
		return ok && equalSymbols(x.RequiredVars, y.RequiredVars) && equalSymbols(x.OutputVars, y.OutputVars) && equalBranches(x.Branches, y.Branches)
	case *SubqueryPattern:
		y, ok := b.(*SubqueryPattern)
		return ok && x.Query.Equal(y.Query) && equalElements(x.Inputs, y.Inputs) && equalBindingForm(x.Binding, y.Binding)
	case *RuleInvocation:
		y, ok := b.(*RuleInvocation)
		return ok && x.Name == y.Name && equalElements(x.Args, y.Args)
	}
	panic(fmt.Sprintf("query equality: %T is not a clause kind", a))
}

func hashComparison(h *structuralHash, c *Comparison) {
	h.symbol(c.Op)
	hashTerm(h, c.Left)
	hashTerm(h, c.Right)
}

func equalComparison(a, b *Comparison) bool {
	return a.Op == b.Op && equalTerm(a.Left, b.Left) && equalTerm(a.Right, b.Right)
}

func hashChainedComparison(h *structuralHash, c *ChainedComparison) {
	h.symbol(c.Op)
	hashTerms(h, c.Terms)
}

func equalChainedComparison(a, b *ChainedComparison) bool {
	return a.Op == b.Op && equalTerms(a.Terms, b.Terms)
}

func hashElements(h *structuralHash, elements []PatternElement) {
	h.count(len(elements))
	for _, elem := range elements {
		hashElement(h, elem)
	}
}

func equalElements(a, b []PatternElement) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equalElement(a[i], b[i]) {
			return false
		}
	}
	return true
}

func hashElement(h *structuralHash, elem PatternElement) {
	switch e := elem.(type) {
	case Variable:
		h.kind(kindVariable)
		h.symbol(e.Name)
	case Constant:
		h.kind(kindConstant)
		h.value(e.Value)
	case Blank:
		h.kind(kindBlank)
	case VectorConstant:
		h.kind(kindVectorConstant)
		h.count(len(e.Values))
		for _, value := range e.Values {
			h.value(value)
		}
	default:
		panic(fmt.Sprintf("query hash: %T is not a pattern element kind", elem))
	}
}

func equalElement(a, b PatternElement) bool {
	switch x := a.(type) {
	case Variable:
		y, ok := b.(Variable)
		return ok && x.Name == y.Name
	case Constant:
		y, ok := b.(Constant)
		return ok && datalog.ValuesEqual(x.Value, y.Value)
	case Blank:
		_, ok := b.(Blank)
		return ok
	case VectorConstant:
		y, ok := b.(VectorConstant)
		return ok && equalValues(x.Values, y.Values)
	}
	panic(fmt.Sprintf("query equality: %T is not a pattern element kind", a))
}

func equalValues(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !datalog.ValuesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// termValue is the term as a value, whether the caller holds it as a value or
// as a pointer.
func termValue(term Term) Term {
	switch t := term.(type) {
	case *VariableTerm:
		return *t
	case *ConstantTerm:
		return *t
	}
	return term
}

func hashTerms(h *structuralHash, terms []Term) {
	h.count(len(terms))
	for _, term := range terms {
		hashTerm(h, term)
	}
}

func equalTerms(a, b []Term) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equalTerm(a[i], b[i]) {
			return false
		}
	}
	return true
}

func hashTerm(h *structuralHash, term Term) {
	switch t := termValue(term).(type) {
	case VariableTerm:
		h.kind(kindVariableTerm)
		h.symbol(t.Symbol)
	case ConstantTerm:
		h.kind(kindConstantTerm)
		hashTermValue(h, t.Value)
	default:
		panic(fmt.Sprintf("query hash: %T is not a term kind", term))
	}
}

func equalTerm(a, b Term) bool {
	switch x := termValue(a).(type) {
	case VariableTerm:
		y, ok := termValue(b).(VariableTerm)
		return ok && x.Symbol == y.Symbol
	case ConstantTerm:
		y, ok := termValue(b).(ConstantTerm)
		return ok && equalTermValue(x.Value, y.Value)
	}
	panic(fmt.Sprintf("query equality: %T is not a term kind", a))
}

// hashTermValue folds a constant term's value. The parser wraps a vector
// literal or a blank in a function's argument as the term's value itself, so
// such a value is the pattern element it is and folds as one; every other
// value is a value of the domain.
func hashTermValue(h *structuralHash, value interface{}) {
	if elem, ok := value.(PatternElement); ok {
		hashElement(h, elem)
		return
	}
	h.kind(kindConstant)
	h.value(value)
}

func equalTermValue(a, b interface{}) bool {
	x, aElem := a.(PatternElement)
	y, bElem := b.(PatternElement)
	if aElem || bElem {
		return aElem && bElem && equalElement(x, y)
	}
	return datalog.ValuesEqual(a, b)
}

// functionValue is the function as a value, whether the caller holds it as a
// value or as a pointer; a database function is a pointer and stays one.
func functionValue(fn Function) Function {
	switch f := fn.(type) {
	case *ArithmeticFunction:
		return *f
	case *StringConcatFunction:
		return *f
	case *TimeExtractionFunction:
		return *f
	case *GroundFunction:
		return *f
	case *IdentityFunction:
		return *f
	case *ComparisonFunction:
		return *f
	case *ChainedComparisonFunction:
		return *f
	case *AndFunction:
		return *f
	case *CustomFunction:
		return *f
	case *NthFunction:
		return *f
	case *FirstFunction:
		return *f
	case *LastFunction:
		return *f
	case *LengthFunction:
		return *f
	case *ContainsFunction:
		return *f
	case *IndexOfFunction:
		return *f
	case *SubvecFunction:
		return *f
	case *EnumerateFunction:
		return *f
	}
	return fn
}

func hashFunction(h *structuralHash, fn Function) {
	switch f := functionValue(fn).(type) {
	case ArithmeticFunction:
		h.kind(kindArithmetic)
		h.symbol(f.Op)
		hashTerms(h, f.Args)
	case StringConcatFunction:
		h.kind(kindStringConcat)
		hashTerms(h, f.Terms)
	case TimeExtractionFunction:
		h.kind(kindTimeExtraction)
		h.symbol(f.Field)
		hashTerm(h, f.TimeTerm)
	case GroundFunction:
		h.kind(kindGroundFunction)
		h.value(f.Value)
	case IdentityFunction:
		h.kind(kindIdentityFunction)
		hashTerm(h, f.Arg)
	case ComparisonFunction:
		h.kind(kindComparisonFunction)
		hashComparison(h, f.Comparison)
	case ChainedComparisonFunction:
		h.kind(kindChainedComparisonFunction)
		hashChainedComparison(h, f.ChainedComparison)
	case AndFunction:
		h.kind(kindAndFunction)
		h.symbols(f.Terms)
	case CustomFunction:
		h.kind(kindCustomFunction)
		h.value(f.Fn)
		hashTerms(h, f.Args)
	case NthFunction:
		h.kind(kindNth)
		hashTerm(h, f.VecTerm)
		hashTerm(h, f.IndexTerm)
	case FirstFunction:
		h.kind(kindFirst)
		hashTerm(h, f.VecTerm)
	case LastFunction:
		h.kind(kindLast)
		hashTerm(h, f.VecTerm)
	case LengthFunction:
		h.kind(kindLength)
		hashTerm(h, f.VecTerm)
	case ContainsFunction:
		h.kind(kindContains)
		hashTerm(h, f.VecTerm)
		hashTerm(h, f.ValueTerm)
	case IndexOfFunction:
		h.kind(kindIndexOf)
		hashTerm(h, f.VecTerm)
		hashTerm(h, f.ValueTerm)
	case SubvecFunction:
		h.kind(kindSubvec)
		hashTerm(h, f.VecTerm)
		hashTerm(h, f.StartTerm)
		hashTerm(h, f.EndTerm)
	case EnumerateFunction:
		h.kind(kindEnumerate)
		hashTerm(h, f.VecTerm)
	case *GetElseFunction:
		hashDatabaseFunction(h, f)
	case *MissingFunction:
		hashDatabaseFunction(h, f)
	case *GetSomeFunction:
		hashDatabaseFunction(h, f)
	default:
		panic(fmt.Sprintf("query hash: %T is not a function kind", fn))
	}
}

func equalFunction(a, b Function) bool {
	switch x := functionValue(a).(type) {
	case ArithmeticFunction:
		y, ok := functionValue(b).(ArithmeticFunction)
		return ok && x.Op == y.Op && equalTerms(x.Args, y.Args)
	case StringConcatFunction:
		y, ok := functionValue(b).(StringConcatFunction)
		return ok && equalTerms(x.Terms, y.Terms)
	case TimeExtractionFunction:
		y, ok := functionValue(b).(TimeExtractionFunction)
		return ok && x.Field == y.Field && equalTerm(x.TimeTerm, y.TimeTerm)
	case GroundFunction:
		y, ok := functionValue(b).(GroundFunction)
		return ok && datalog.ValuesEqual(x.Value, y.Value)
	case IdentityFunction:
		y, ok := functionValue(b).(IdentityFunction)
		return ok && equalTerm(x.Arg, y.Arg)
	case ComparisonFunction:
		y, ok := functionValue(b).(ComparisonFunction)
		return ok && equalComparison(x.Comparison, y.Comparison)
	case ChainedComparisonFunction:
		y, ok := functionValue(b).(ChainedComparisonFunction)
		return ok && equalChainedComparison(x.ChainedComparison, y.ChainedComparison)
	case AndFunction:
		y, ok := functionValue(b).(AndFunction)
		return ok && equalSymbols(x.Terms, y.Terms)
	case CustomFunction:
		y, ok := functionValue(b).(CustomFunction)
		return ok && x.Fn == y.Fn && equalTerms(x.Args, y.Args)
	case NthFunction:
		y, ok := functionValue(b).(NthFunction)
		return ok && equalTerm(x.VecTerm, y.VecTerm) && equalTerm(x.IndexTerm, y.IndexTerm)
	case FirstFunction:
		y, ok := functionValue(b).(FirstFunction)
		return ok && equalTerm(x.VecTerm, y.VecTerm)
	case LastFunction:
		y, ok := functionValue(b).(LastFunction)
		return ok && equalTerm(x.VecTerm, y.VecTerm)
	case LengthFunction:
		y, ok := functionValue(b).(LengthFunction)
		return ok && equalTerm(x.VecTerm, y.VecTerm)
	case ContainsFunction:
		y, ok := functionValue(b).(ContainsFunction)
		return ok && equalTerm(x.VecTerm, y.VecTerm) && equalTerm(x.ValueTerm, y.ValueTerm)
	case IndexOfFunction:
		y, ok := functionValue(b).(IndexOfFunction)
		return ok && equalTerm(x.VecTerm, y.VecTerm) && equalTerm(x.ValueTerm, y.ValueTerm)
	case SubvecFunction:
		y, ok := functionValue(b).(SubvecFunction)
		return ok && equalTerm(x.VecTerm, y.VecTerm) && equalTerm(x.StartTerm, y.StartTerm) && equalTerm(x.EndTerm, y.EndTerm)
	case EnumerateFunction:
		y, ok := functionValue(b).(EnumerateFunction)
		return ok && equalTerm(x.VecTerm, y.VecTerm)
	case *GetElseFunction:
		return equalDatabaseFunction(x, b)
	case *MissingFunction:
		return equalDatabaseFunction(x, b)
	case *GetSomeFunction:
		return equalDatabaseFunction(x, b)
	}
	panic(fmt.Sprintf("query equality: %T is not a function kind", a))
}

func hashDatabaseFunction(h *structuralHash, fn DatabaseFunction) {
	switch f := fn.(type) {
	case *GetElseFunction:
		h.kind(kindGetElse)
		hashTerm(h, f.Entity)
		h.keyword(f.Attr)
		h.value(f.Default)
	case *MissingFunction:
		h.kind(kindMissingFunction)
		hashTerm(h, f.Entity)
		h.keyword(f.Attr)
	case *GetSomeFunction:
		h.kind(kindGetSome)
		hashTerm(h, f.Entity)
		h.count(len(f.Attrs))
		for _, attr := range f.Attrs {
			h.keyword(attr)
		}
	default:
		panic(fmt.Sprintf("query hash: %T is not a database function kind", fn))
	}
}

// equalDatabaseFunction compares a database function with a function of any
// kind: the other is equal when it is the same kind over the same parts.
func equalDatabaseFunction(a DatabaseFunction, b Pattern) bool {
	switch x := a.(type) {
	case *GetElseFunction:
		y, ok := b.(*GetElseFunction)
		return ok && equalTerm(x.Entity, y.Entity) && x.Attr == y.Attr && datalog.ValuesEqual(x.Default, y.Default)
	case *MissingFunction:
		y, ok := b.(*MissingFunction)
		return ok && equalTerm(x.Entity, y.Entity) && x.Attr == y.Attr
	case *GetSomeFunction:
		y, ok := b.(*GetSomeFunction)
		return ok && equalTerm(x.Entity, y.Entity) && equalKeywords(x.Attrs, y.Attrs)
	}
	panic(fmt.Sprintf("query equality: %T is not a database function kind", a))
}

func hashBinding(h *structuralHash, binding interface{}) {
	switch b := binding.(type) {
	case nil:
		h.kind(kindNoBinding)
	case Symbol:
		h.kind(kindSymbolBinding)
		h.symbol(b)
	case TupleBinding:
		h.kind(kindTupleBinding)
		h.symbols(b.Variables)
	case CollectionBinding:
		h.kind(kindCollectionBinding)
		h.symbol(b.Variable)
	case RelationBinding:
		h.kind(kindRelationBinding)
		h.symbols(b.Variables)
	default:
		panic(fmt.Sprintf("query hash: %T is not an expression binding kind", binding))
	}
}

func equalBinding(a, b interface{}) bool {
	switch x := a.(type) {
	case nil:
		return b == nil
	case Symbol:
		y, ok := b.(Symbol)
		return ok && x == y
	case TupleBinding:
		y, ok := b.(TupleBinding)
		return ok && equalSymbols(x.Variables, y.Variables)
	case CollectionBinding:
		y, ok := b.(CollectionBinding)
		return ok && x.Variable == y.Variable
	case RelationBinding:
		y, ok := b.(RelationBinding)
		return ok && equalSymbols(x.Variables, y.Variables)
	}
	panic(fmt.Sprintf("query equality: %T is not an expression binding kind", a))
}

func hashBindingForm(h *structuralHash, form BindingForm) {
	switch b := form.(type) {
	case CollectionBinding:
		h.kind(kindCollectionBinding)
		h.symbol(b.Variable)
	case ScalarBinding:
		h.kind(kindScalarBinding)
		h.symbol(b.Variable)
	case RelationBinding:
		h.kind(kindRelationBinding)
		h.symbols(b.Variables)
	default:
		panic(fmt.Sprintf("query hash: %T is not a subquery binding kind", form))
	}
}

func equalBindingForm(a, b BindingForm) bool {
	switch x := a.(type) {
	case CollectionBinding:
		y, ok := b.(CollectionBinding)
		return ok && x.Variable == y.Variable
	case ScalarBinding:
		y, ok := b.(ScalarBinding)
		return ok && x.Variable == y.Variable
	case RelationBinding:
		y, ok := b.(RelationBinding)
		return ok && equalSymbols(x.Variables, y.Variables)
	}
	panic(fmt.Sprintf("query equality: %T is not a subquery binding kind", a))
}

func hashPull(h *structuralHash, pattern *PullPattern) {
	if pattern == nil {
		h.kind(kindNoPull)
		return
	}
	h.count(len(pattern.Specs))
	for _, spec := range pattern.Specs {
		switch s := spec.(type) {
		case *PullAttribute:
			h.kind(kindPullAttribute)
			h.keyword(s.Attr)
		case *PullWildcard:
			h.kind(kindPullWildcard)
		case *PullMapSpec:
			h.kind(kindPullMap)
			h.keyword(s.Attr)
			hashPull(h, s.Pattern)
		case *PullLimitExpr:
			h.kind(kindPullLimit)
			h.keyword(s.Attr)
			h.word(uint64(s.Limit))
		case *PullDefaultExpr:
			h.kind(kindPullDefault)
			h.keyword(s.Attr)
			h.value(s.Default)
		default:
			panic(fmt.Sprintf("query hash: %T is not a pull specification kind", spec))
		}
	}
}

func equalPull(a, b *PullPattern) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil || len(a.Specs) != len(b.Specs) {
		return false
	}
	for i := range a.Specs {
		if !equalPullSpec(a.Specs[i], b.Specs[i]) {
			return false
		}
	}
	return true
}

func equalPullSpec(a, b PullAttrSpec) bool {
	switch x := a.(type) {
	case *PullAttribute:
		y, ok := b.(*PullAttribute)
		return ok && x.Attr == y.Attr
	case *PullWildcard:
		_, ok := b.(*PullWildcard)
		return ok
	case *PullMapSpec:
		y, ok := b.(*PullMapSpec)
		return ok && x.Attr == y.Attr && equalPull(x.Pattern, y.Pattern)
	case *PullLimitExpr:
		y, ok := b.(*PullLimitExpr)
		return ok && x.Attr == y.Attr && x.Limit == y.Limit
	case *PullDefaultExpr:
		y, ok := b.(*PullDefaultExpr)
		return ok && x.Attr == y.Attr && datalog.ValuesEqual(x.Default, y.Default)
	}
	panic(fmt.Sprintf("query equality: %T is not a pull specification kind", a))
}

func equalSymbols(a, b []Symbol) bool {
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

func equalKeywords(a, b []datalog.Keyword) bool {
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
