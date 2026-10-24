package query

import (
	"strconv"
	"strings"

	"github.com/wbrown/janus-datalog/datalog"
)

// The engine mints predicates beside a rule set's own, each a symbol in a
// namespace of the engine's: a predicate adorned with a binding pattern, the
// magic predicate holding the bindings an adorned predicate is asked with,
// the delta of a predicate under semi-naive evaluation, and scope/required,
// the required run a nested scope runs over. The kind leads the namespace,
// the predicate's own namespace follows it, and the name is the predicate's,
// so the derivation reads off the symbol and the text of a body that invokes
// it is Datalog the parser reads. NewRuleSet refuses a rule set that declares
// a predicate in one of these namespaces.
const (
	adornedNamespace = "adorned"
	magicNamespace   = "magic"
	deltaNamespace   = "delta"
	scopeNamespace   = "scope"
)

// mint names the predicate derived from name under kind: kind leads the
// namespace, name's own namespace follows it, and the name is name's.
func mint(kind string, name Symbol) Symbol {
	ns := kind
	if own := name.Namespace(); own != "" {
		ns += "." + own
	}
	return datalog.NewSymbol(ns + "/" + name.Name())
}

// adornment renders a binding pattern, b for a bound position and f for a
// free one.
func adornment(bound []bool) string {
	var sb strings.Builder
	for _, b := range bound {
		if b {
			sb.WriteByte('b')
		} else {
			sb.WriteByte('f')
		}
	}
	return sb.String()
}

// AdornedSymbol names a predicate under a binding pattern: adorned.bf/reach.
func AdornedSymbol(name Symbol, bound []bool) Symbol {
	return mint(adornedNamespace+"."+adornment(bound), name)
}

// MagicSymbol names the magic predicate of an adorned one: magic.bf/reach.
func MagicSymbol(name Symbol, bound []bool) Symbol {
	return mint(magicNamespace+"."+adornment(bound), name)
}

// DeltaSymbol names the delta of a predicate: the tuples the last iteration
// of its stratum added, which a body's occurrence of the predicate reads
// under semi-naive evaluation. The delta of reach is delta/reach, and the
// delta of the adorned predicate adorned.bf/reach is delta.adorned.bf/reach.
func DeltaSymbol(name Symbol) Symbol {
	return mint(deltaNamespace, name)
}

// seedHead is the head of a seed predicate no rule derives: one minted
// variable per position, ?magic.bf/reach.0 and so on.
func seedHead(name Symbol, arity int) []Symbol {
	head := make([]Symbol, arity)
	for i := range head {
		head[i] = datalog.NewSymbol("?" + name.String() + "." + strconv.Itoa(i))
	}
	return head
}

// ScopeRequired names the predicate a nested scope's required run enters
// its query under: the executor holds the projection of the enclosing scope
// onto the required run under it, and the scope's query opens with its
// invocation over the required run.
var ScopeRequired = mint(scopeNamespace, datalog.NewSymbol("required"))

// ScopeInput names the variable a nested scope's required run carries a
// collection input's whole value under, by the input's call position:
// ?scope/input.2. The scope's clauses open by binding the input's members
// from it, so the members join the body as a group and the inner find
// aggregates over them per argument value.
func ScopeInput(position int) Symbol {
	return datalog.NewSymbol("?" + scopeNamespace + "/input." + strconv.Itoa(position))
}

// unifyNamespace leads the namespace of the variables a data pattern's
// later occurrences of one variable are written as.
const unifyNamespace = "unify"

// UnifiedVariable names the variable a data pattern's later occurrence of
// sym at position is written as, ?unify/x.2 for ?x at the value position,
// the variable's own namespace following unify when it has one. The pattern
// then states the equality of sym with it, so the two positions agree.
func UnifiedVariable(sym Symbol, position int) Symbol {
	ns := "?" + unifyNamespace
	name := sym.Name()
	if own := sym.Namespace(); own != "" {
		ns += "." + strings.TrimPrefix(own, "?")
	} else {
		name = strings.TrimPrefix(name, "?")
	}
	return datalog.NewSymbol(ns + "/" + name + "." + strconv.Itoa(position))
}

// SeedRestricted reports whether a predicate's relation holds only what a
// seed reaches: the magic and adorned predicates the rewrite mints, their
// deltas, and scope/required, the tuples a nested scope runs over. A body's
// invocation of one reads a relation the seed restricts, so the planner
// schedules it before the patterns.
func SeedRestricted(name Symbol) bool {
	kind, rest, _ := strings.Cut(name.Namespace(), ".")
	if kind == deltaNamespace {
		kind, _, _ = strings.Cut(rest, ".")
	}
	return kind == adornedNamespace || kind == magicNamespace || kind == scopeNamespace
}

// engineNamespace reports whether a predicate name lies in a namespace the
// engine mints into: its namespace up to the first dot is one of the kinds.
func engineNamespace(name Symbol) bool {
	ns := name.Namespace()
	if i := strings.Index(ns, "."); i >= 0 {
		ns = ns[:i]
	}
	return ns == adornedNamespace || ns == magicNamespace || ns == deltaNamespace || ns == scopeNamespace
}
