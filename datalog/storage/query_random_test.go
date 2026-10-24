package storage

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// The engine against a reference it does not compute, on queries without
// rules: patterns, ground, identity, != and count subqueries at the top
// level, and or, or-default, not, not-join and or-join holding the same, over
// random datoms of six or of forty entities and the mark their commit writes,
// the :db/txInstant datom a pattern with a variable attribute matches. The
// reference plans each scope from the text alone and then searches the
// datoms in the planned order, under the language's scope rules:
//
//   - A not-join and an or-join share their header with the enclosing scope
//     and nothing else.
//   - A subquery's inner query reads the input its call passes and nothing
//     else of the enclosing scope. Its tuple binding binds the count when the
//     count is above zero and binds nothing when it is zero.
//   - A form without a header, or, or-default or not, shares every variable
//     that occurs elsewhere in its enclosing scope: in another clause of the
//     same list, in the enclosing form's shared set, or in :find. A variable
//     inside a nested header form occurs outside it only through what that
//     form shares.
//   - A shared variable the form binds in every branch is its output. A
//     branch computes it from the form's required variables alone, and the
//     scope joins on it. Every other shared variable is required: bound
//     before the form runs by the clause that binds it.
//   - A scope is ordered by readiness: repeatedly the first clause whose
//     inputs are bound, a clause that is not a form before one that is.
//
// A query is refused when a not-join header variable is not mentioned by its
// body, an or-join header variable is not bound by some branch, a form
// shares no variable with its scope, an identity or != input or a form's
// shared variable is bound by no clause and not by the form itself, a :find
// variable is bound by no clause, or a scope has no order in which every
// clause can run.
//
// A query may declare one or two :in entries, a scalar, a collection, a
// tuple or a relation, at most one of them a relation, a tuple entry naming
// one symbol twice some of the time. The reference reads every input as a
// relation over its symbols the body joins, whatever its form, a symbol
// named twice selecting the tuples whose values agree. A query whose find
// holds an aggregate is not computed by the reference: it is checked against
// the engine's answer for the same text with each input spelled as a ground
// clause of the same form in the body, since the forms are spellings of one
// relation and the aggregate groups by the find over the whole join.
//
// A query may hold a ground of a vector into ?coll, empty some of the time,
// and a counting subquery whose :in collection is passed ?coll: the count is
// over the entities whose value under the attribute is any member, as one
// count, and a count of zero binds nothing.
//
// A constant under :r is an integer or a float, and the store holds both, so
// a query reads one type and not the other, and a vector ground of integers
// has a twin of strings that no :r value matches. A query holding either may
// be followed by its respelled twin, the same text with the constant
// respelled, on the same store: the twin's rows are its own type's alone,
// and a plan served to the twin under the original's text shows as the
// original's rows.
//
// Each seed draws its datoms and its queries from one source, so a seed and
// query number name the same query on every run. The engine runs each query
// in every mode of the matrix, in the written order, the reverse, a shuffle
// and the written order again, and must give the reference's rows, or refuse
// the queries the reference refuses, and never panic.

// shapeTerm is a variable, a constant, or the blank.
type shapeTerm struct {
	v   string // the variable's name, "" for a constant or the blank
	c   string // the constant's text, "_" for the blank
	val interface{}
}

func (t shapeTerm) String() string {
	if t.v != "" {
		return t.v
	}
	return t.c
}

// shapeClause is one clause of a generated query: a pattern, a ground, an
// identity, a != predicate, a count subquery, an input's tuples, or a form
// over branches with an optional header. A subq's terms are its input
// variable, its binding variable and the attribute its inner query counts
// under; the inner query's own variable ?s is no term, since it is local to
// the inner query. A groundvec's terms are a vector constant and the
// variable it binds; a subqcoll's are the variable holding the collection
// its inner query is passed, its binding variable and the attribute its
// inner query counts under. An in's terms are the input's symbols, its rows
// the input's tuples, one value per symbol, and its form the spelling the
// input has in the :in; it renders as a ground clause binding the same
// tuples under the same form.
type shapeClause struct {
	kind     string // pat, ground, ident, neq, subq, groundvec, subqcoll, in, or, ordef, not, notjoin, orjoin
	t        []shapeTerm
	branches [][]shapeClause
	header   []string
	rows     [][]shapeTerm
	form     string // an in's form: scalar, coll, tuple or rel
}

// rowText renders a row's values, each by its constant's text.
func rowText(row []shapeTerm) string {
	parts := make([]string, len(row))
	for i, t := range row {
		parts[i] = t.String()
	}
	return strings.Join(parts, " ")
}

func (c shapeClause) String() string {
	switch c.kind {
	case "pat":
		return fmt.Sprintf("[%s %s %s]", c.t[0], c.t[1], c.t[2])
	case "ground":
		return fmt.Sprintf("[(ground %s) %s]", c.t[0], c.t[1])
	case "ident":
		return fmt.Sprintf("[(identity %s) %s]", c.t[0], c.t[1])
	case "neq":
		return fmt.Sprintf("[(!= %s %s)]", c.t[0], c.t[1])
	case "subq":
		return fmt.Sprintf("[(q [:find (count ?s) :in $ %s :where [?s %s %s]] $ %s) [[%s]]]", c.t[0], c.t[2], c.t[0], c.t[0], c.t[1])
	case "groundvec":
		return fmt.Sprintf("[(ground %s) %s]", c.t[0], c.t[1])
	case "subqcoll":
		return fmt.Sprintf("[(q [:find (count ?s) :in $ [?x ...] :where [?s %s ?x]] $ %s) [[%s]]]", c.t[2], c.t[0], c.t[1])
	case "in":
		syms := make([]string, len(c.t))
		for i, t := range c.t {
			syms[i] = t.String()
		}
		rows := make([]string, len(c.rows))
		for i, row := range c.rows {
			rows[i] = rowText(row)
		}
		switch c.form {
		case "scalar":
			return fmt.Sprintf("[(ground %s) %s]", rows[0], syms[0])
		case "coll":
			return fmt.Sprintf("[(ground [%s]) [%s ...]]", strings.Join(rows, " "), syms[0])
		case "tuple":
			return fmt.Sprintf("[(ground [%s]) [%s]]", rows[0], strings.Join(syms, " "))
		}
		for i, row := range rows {
			rows[i] = "[" + row + "]"
		}
		return fmt.Sprintf("[(ground [%s]) [[%s] ...]]", strings.Join(rows, " "), strings.Join(syms, " "))
	}
	var b strings.Builder
	switch c.kind {
	case "or":
		b.WriteString("(or")
	case "ordef":
		b.WriteString("(or-default")
	case "not":
		b.WriteString("(not")
	case "notjoin":
		b.WriteString("(not-join [" + strings.Join(c.header, " ") + "]")
	case "orjoin":
		b.WriteString("(or-join [" + strings.Join(c.header, " ") + "]")
	}
	for _, br := range c.branches {
		if c.kind == "not" || c.kind == "notjoin" {
			for _, cl := range br {
				b.WriteString(" " + cl.String())
			}
			continue
		}
		if len(br) == 1 {
			b.WriteString(" " + br[0].String())
		} else {
			b.WriteString(" (and")
			for _, cl := range br {
				b.WriteString(" " + cl.String())
			}
			b.WriteString(")")
		}
	}
	b.WriteString(")")
	return b.String()
}

// vars adds every variable the clause mentions, at any depth, to out.
func (c shapeClause) vars(out map[string]bool) {
	for _, t := range c.t {
		if t.v != "" {
			out[t.v] = true
		}
	}
	for _, br := range c.branches {
		for _, cl := range br {
			cl.vars(out)
		}
	}
	for _, h := range c.header {
		out[h] = true
	}
}

// exposed adds to out the variables the clause exposes to the list it stands
// in: a pattern's, ground's, identity's or != predicate's variables; for an
// or, or-default or not, what its branches expose; for an or-join or
// not-join, its header, since a branch variable outside the header is local
// to the branch.
func (c shapeClause) exposed(out map[string]bool) {
	switch c.kind {
	case "orjoin", "notjoin":
		for _, h := range c.header {
			out[h] = true
		}
		return
	}
	for _, t := range c.t {
		if t.v != "" {
			out[t.v] = true
		}
	}
	for _, br := range c.branches {
		for _, cl := range br {
			cl.exposed(out)
		}
	}
}

func isShapeForm(c shapeClause) bool {
	switch c.kind {
	case "pat", "ground", "ident", "neq", "subq", "groundvec", "subqcoll", "in":
		return false
	}
	return true
}

// shapeInput is one :in entry: the form it is spelled in, scalar, coll,
// tuple or rel, its symbols, and its tuples, one value per symbol. A scalar
// or tuple entry holds one tuple; a coll or rel entry holds any number.
type shapeInput struct {
	form string
	syms []string
	rows [][]shapeTerm
}

// spec is the entry's text in the :in.
func (in shapeInput) spec() string {
	switch in.form {
	case "scalar":
		return in.syms[0]
	case "coll":
		return "[" + in.syms[0] + " ...]"
	case "tuple":
		return "[" + strings.Join(in.syms, " ") + "]"
	}
	return "[[" + strings.Join(in.syms, " ") + "] ...]"
}

// rowValues is a row's values.
func rowValues(row []shapeTerm) []interface{} {
	out := make([]interface{}, len(row))
	for i, t := range row {
		out[i] = t.val
	}
	return out
}

// value is the Go value the entry is given: the value for a scalar, the
// values for a coll, the tuple for a tuple, and the tuples for a rel.
func (in shapeInput) value() interface{} {
	switch in.form {
	case "scalar":
		return in.rows[0][0].val
	case "coll":
		out := make([]interface{}, len(in.rows))
		for i, row := range in.rows {
			out[i] = row[0].val
		}
		return out
	case "tuple":
		return rowValues(in.rows[0])
	}
	out := make([]interface{}, len(in.rows))
	for i, row := range in.rows {
		out[i] = rowValues(row)
	}
	return out
}

// ground is the entry as a clause of the body binding the same tuples under
// the same form.
func (in shapeInput) ground() shapeClause {
	c := shapeClause{kind: "in", form: in.form, rows: in.rows}
	for _, s := range in.syms {
		c.t = append(c.t, shapeVar(s))
	}
	return c
}

type shapeQuery struct {
	find  []string
	ins   []shapeInput
	where []shapeClause
}

// text renders the query with its inputs in the :in and its clauses in the
// given order.
func (q shapeQuery) text(order []int) string {
	in := ""
	if len(q.ins) > 0 {
		specs := []string{"$"}
		for _, i := range q.ins {
			specs = append(specs, i.spec())
		}
		in = " :in " + strings.Join(specs, " ")
	}
	parts := make([]string, 0, len(order))
	for _, i := range order {
		parts = append(parts, q.where[i].String())
	}
	return fmt.Sprintf("[:find %s%s :where %s]", strings.Join(q.find, " "), in, strings.Join(parts, " "))
}

// groundText renders the query with no :in, each input as a ground clause
// of its own form before the clauses in the given order.
func (q shapeQuery) groundText(order []int) string {
	parts := make([]string, 0, len(q.ins)+len(order))
	for _, in := range q.ins {
		parts = append(parts, in.ground().String())
	}
	for _, i := range order {
		parts = append(parts, q.where[i].String())
	}
	return fmt.Sprintf("[:find %s :where %s]", strings.Join(q.find, " "), strings.Join(parts, " "))
}

// values is the Go value of each input, in the :in's order.
func (q shapeQuery) values() []interface{} {
	out := make([]interface{}, len(q.ins))
	for i, in := range q.ins {
		out[i] = in.value()
	}
	return out
}

// aggregates reports whether the find holds an aggregate.
func (q shapeQuery) aggregates() bool {
	for _, f := range q.find {
		if strings.HasPrefix(f, "(") {
			return true
		}
	}
	return false
}

// repeatsASymbol reports whether an input names one symbol twice.
func (q shapeQuery) repeatsASymbol() bool {
	for _, in := range q.ins {
		seen := map[string]bool{}
		for _, s := range in.syms {
			if seen[s] {
				return true
			}
			seen[s] = true
		}
	}
	return false
}

// shapeDatom is one generated datom.
type shapeDatom struct{ e, a, v interface{} }

// shapeData is the datoms of one seed and the names of its entities.
type shapeData struct {
	ents    []datalog.Identity
	entName map[string]string
	datoms  []shapeDatom
}

// randomDatoms draws n entities and their datoms: for each entity, under :p
// and :q zero or one ref to a random entity, under :r two times in three a
// number below three, a float one time in four and otherwise an integer, and
// under :k one of :p, :q or :r half the time.
func randomDatoms(r *rand.Rand, n int) shapeData {
	data := shapeData{entName: map[string]string{}}
	for i := 0; i < n; i++ {
		id := datalog.NewIdentity(fmt.Sprintf("e%d", i))
		data.ents = append(data.ents, id)
		data.entName[id.String()] = fmt.Sprintf("e%d", i)
	}
	seen := map[string]bool{}
	add := func(d shapeDatom) {
		k := fmt.Sprintf("%v %v %T:%v", d.e, d.a, d.v, d.v)
		if !seen[k] {
			seen[k] = true
			data.datoms = append(data.datoms, d)
		}
	}
	for _, e := range data.ents {
		for _, a := range []string{":p", ":q"} {
			cnt := r.Intn(2)
			for j := 0; j < cnt; j++ {
				add(shapeDatom{e, datalog.NewKeyword(a), data.ents[r.Intn(n)]})
			}
		}
		if r.Intn(3) > 0 {
			if r.Intn(4) == 0 {
				add(shapeDatom{e, datalog.NewKeyword(":r"), float64(r.Intn(3))})
			} else {
				add(shapeDatom{e, datalog.NewKeyword(":r"), int64(r.Intn(3))})
			}
		}
		if r.Intn(2) == 0 {
			kws := []string{":p", ":q", ":r"}
			add(shapeDatom{e, datalog.NewKeyword(":k"), datalog.NewKeyword(kws[r.Intn(3)])})
		}
	}
	return data
}

var shapeEntityVars = []string{"?e", "?x", "?y", "?w"}

func pickString(r *rand.Rand, xs []string) string { return xs[r.Intn(len(xs))] }

func shapeVar(s string) shapeTerm { return shapeTerm{v: s} }

func shapeKeyword(s string) shapeTerm { return shapeTerm{c: s, val: datalog.NewKeyword(s)} }

// randomPattern draws a pattern: an entity variable from evars, an attribute
// keyword or, when allowed, the variable ?a one time in four, and a value fit
// to the attribute: under :r an integer three times in eight, a float one
// time in eight and ?n otherwise, a keyword or ?a under :k, the blank one
// time in six, and otherwise an entity variable other than the entity's.
func randomPattern(r *rand.Rand, evars []string, allowAttrVar bool) shapeClause {
	e := shapeVar(pickString(r, evars))
	var a shapeTerm
	switch {
	case allowAttrVar && r.Intn(4) == 0:
		a = shapeVar("?a")
	default:
		a = shapeKeyword(pickString(r, []string{":p", ":q", ":p", ":q", ":r", ":k"}))
	}
	var v shapeTerm
	switch {
	case a.v == "" && a.c == ":r":
		switch r.Intn(8) {
		case 0, 1, 2:
			n := int64(r.Intn(3))
			v = shapeTerm{c: strconv.FormatInt(n, 10), val: n}
		case 3:
			v = floatTerm(float64(r.Intn(3)))
		default:
			v = shapeVar("?n")
		}
	case a.v == "" && a.c == ":k":
		if r.Intn(3) == 0 {
			v = shapeKeyword(pickString(r, []string{":p", ":q", ":r"}))
		} else {
			v = shapeVar("?a")
		}
	case r.Intn(6) == 0:
		v = shapeTerm{c: "_"}
	default:
		v = shapeVar(pickString(r, evars))
		for tries := 0; v.v == e.v && tries < 20; tries++ {
			v = shapeVar(pickString(r, evars))
		}
		if v.v == e.v {
			v = shapeTerm{c: "_"}
		}
	}
	return shapeClause{kind: "pat", t: []shapeTerm{e, a, v}}
}

// randomCountSubquery draws a count subquery: its input an entity variable
// from evars, its binding ?c, counting the entities that refer to the input
// under :p or :q.
func randomCountSubquery(r *rand.Rand, evars []string) shapeClause {
	return shapeClause{kind: "subq", t: []shapeTerm{shapeVar(pickString(r, evars)), shapeVar("?c"), shapeKeyword(pickString(r, []string{":p", ":q"}))}}
}

// randomBranch draws one or two clauses: a ground of a keyword into ?a one
// time in nine, an identity between two distinct variables one time in nine,
// a count subquery one time in nine, one time in nine a cycle of two
// identities between two distinct variables, each binding the one the other
// reads, so that the branch provides every symbol it reads, and otherwise a
// pattern.
func randomBranch(r *rand.Rand, evars []string) []shapeClause {
	n := 1 + r.Intn(2)
	var out []shapeClause
	for i := 0; i < n; i++ {
		switch r.Intn(9) {
		case 0:
			out = append(out, shapeClause{kind: "ground", t: []shapeTerm{shapeKeyword(pickString(r, []string{":p", ":q", ":r"})), shapeVar("?a")}})
		case 1:
			x, y := pickString(r, evars), pickString(r, evars)
			for x == y {
				y = pickString(r, evars)
			}
			out = append(out, shapeClause{kind: "ident", t: []shapeTerm{shapeVar(x), shapeVar(y)}})
		case 2:
			out = append(out, randomCountSubquery(r, evars))
		case 3:
			x, y := pickString(r, evars), pickString(r, evars)
			for x == y {
				y = pickString(r, evars)
			}
			out = append(out, shapeClause{kind: "ident", t: []shapeTerm{shapeVar(x), shapeVar(y)}})
			out = append(out, shapeClause{kind: "ident", t: []shapeTerm{shapeVar(y), shapeVar(x)}})
		default:
			out = append(out, randomPattern(r, evars, true))
		}
	}
	return out
}

// randomForm draws a form over the top-level variables and the locals ?l and
// ?m: an or of two or three branches one time in three, an or-default of
// two, a not, a not-join whose header is a random subset of the body's
// non-local variables, or an or-join whose header is a random subset of the
// variables every branch mentions. An empty header becomes [?e]. One
// or-default in three is one of the two shapes whose first branch has a
// result, or whose default fires, by a clause beside its subquery or its
// ground: a count subquery followed by a predicate on its count, against a
// ground; or a count subquery against a ground guarded by a not.
func randomForm(r *rand.Rand, top []string) shapeClause {
	locals := []string{"?l", "?m"}
	evars := append(append([]string{}, top...), locals...)
	switch r.Intn(6) {
	case 0, 1:
		nb := 2 + r.Intn(2)
		c := shapeClause{kind: "or"}
		for i := 0; i < nb; i++ {
			c.branches = append(c.branches, randomBranch(r, evars))
		}
		return c
	case 2:
		c := shapeClause{kind: "ordef"}
		zero := shapeTerm{c: "0", val: int64(0)}
		switch r.Intn(3) {
		case 0:
			sub := randomCountSubquery(r, evars)
			one := shapeTerm{c: "1", val: int64(1)}
			c.branches = append(c.branches, []shapeClause{sub, {kind: "neq", t: []shapeTerm{sub.t[1], one}}})
			c.branches = append(c.branches, []shapeClause{{kind: "ground", t: []shapeTerm{zero, sub.t[1]}}})
		case 1:
			sub := randomCountSubquery(r, evars)
			guard := shapeClause{kind: "not", branches: [][]shapeClause{{{kind: "pat", t: []shapeTerm{sub.t[0], shapeKeyword(":k"), shapeKeyword(":p")}}}}}
			c.branches = append(c.branches, []shapeClause{sub})
			c.branches = append(c.branches, []shapeClause{{kind: "ground", t: []shapeTerm{zero, sub.t[1]}}, guard})
		default:
			for i := 0; i < 2; i++ {
				c.branches = append(c.branches, randomBranch(r, evars))
			}
		}
		return c
	case 3:
		return shapeClause{kind: "not", branches: [][]shapeClause{randomBranch(r, evars)}}
	case 4:
		body := randomBranch(r, evars)
		vs := map[string]bool{}
		for _, cl := range body {
			cl.vars(vs)
		}
		var h []string
		for _, v := range sortedKeys(vs) {
			if v != "?l" && v != "?m" && r.Intn(3) > 0 {
				h = append(h, v)
			}
		}
		sort.Strings(h)
		if len(h) == 0 {
			h = []string{"?e"}
		}
		return shapeClause{kind: "notjoin", header: h, branches: [][]shapeClause{body}}
	default:
		c := shapeClause{kind: "orjoin"}
		for i := 0; i < 2; i++ {
			c.branches = append(c.branches, randomBranch(r, evars))
		}
		common := map[string]int{}
		for _, br := range c.branches {
			vs := map[string]bool{}
			for _, cl := range br {
				cl.vars(vs)
			}
			for v := range vs {
				common[v]++
			}
		}
		cks := map[string]bool{}
		for v := range common {
			cks[v] = true
		}
		var h []string
		for _, v := range sortedKeys(cks) {
			if common[v] == len(c.branches) && r.Intn(4) > 0 {
				h = append(h, v)
			}
		}
		sort.Strings(h)
		if len(h) == 0 {
			h = []string{"?e"}
		}
		c.header = h
		return c
	}
}

// entityTerm is the constant naming the i-th entity of data, written as the
// #id literal of the seed the entity was made from.
func entityTerm(data shapeData, i int) shapeTerm {
	return shapeTerm{c: fmt.Sprintf("#id %q", data.entName[data.ents[i].String()]), val: data.ents[i]}
}

// intTerm is an integer constant.
func intTerm(n int64) shapeTerm {
	return shapeTerm{c: strconv.FormatInt(n, 10), val: n}
}

// vectorTerm is a vector constant over the members.
func vectorTerm(members []shapeTerm) shapeTerm {
	return shapeTerm{c: "[" + rowText(members) + "]", val: rowValues(members)}
}

// randomVectorGround draws a ground of a vector into ?coll, the members a
// counting subquery under attr is passed as its collection: empty one time
// in six, and otherwise one to three members, entities under :p and :q and
// integers below three under :r.
func randomVectorGround(r *rand.Rand, data shapeData, attr string) shapeClause {
	var members []shapeTerm
	if r.Intn(6) > 0 {
		for i := 1 + r.Intn(3); i > 0; i-- {
			if attr == ":r" {
				members = append(members, intTerm(int64(r.Intn(3))))
			} else {
				members = append(members, entityTerm(data, r.Intn(len(data.ents))))
			}
		}
	}
	return shapeClause{kind: "groundvec", t: []shapeTerm{vectorTerm(members), shapeVar("?coll")}}
}

// randomInput draws one :in entry: a scalar, a coll of one to three values,
// a tuple over two symbols or, when allowed, a rel of one to three tuples
// over two distinct symbols. A symbol is an entity variable holding
// entities or ?n holding integers. A tuple entry names one symbol twice one
// time in four, and its two values then agree half the time.
func randomInput(r *rand.Rand, data shapeData, allowRel bool) shapeInput {
	forms := []string{"scalar", "coll", "tuple"}
	if allowRel {
		forms = append(forms, "rel")
	}
	in := shapeInput{form: pickString(r, forms)}
	inputVars := []string{"?e", "?x", "?y", "?w", "?n"}
	nsyms := 1
	if in.form == "tuple" || in.form == "rel" {
		nsyms = 2
	}
	for i := 0; i < nsyms; i++ {
		in.syms = append(in.syms, pickString(r, inputVars))
	}
	switch {
	case in.form == "rel":
		for in.syms[1] == in.syms[0] {
			in.syms[1] = pickString(r, inputVars)
		}
	case in.form == "tuple" && r.Intn(4) == 0:
		in.syms[1] = in.syms[0]
	}
	repeated := nsyms == 2 && in.syms[0] == in.syms[1]
	nrows := 1
	if in.form == "coll" || in.form == "rel" {
		nrows = 1 + r.Intn(3)
	}
	for i := 0; i < nrows; i++ {
		var row []shapeTerm
		for _, s := range in.syms {
			if s == "?n" {
				row = append(row, intTerm(int64(r.Intn(3))))
			} else {
				row = append(row, entityTerm(data, r.Intn(len(data.ents))))
			}
		}
		if repeated && r.Intn(2) == 0 {
			row[1] = row[0]
		}
		in.rows = append(in.rows, row)
	}
	return in
}

// randomQuery draws, one time in three, one or two inputs, at most one of
// them a rel; one to three top-level patterns, a ground of a keyword into ?a
// one time in three, a count subquery over a bound entity variable one time
// in four, one or two forms, a != between the first two bound variables one
// time in five, and a :find of the first bound variable and, half the time,
// one other, and, half the time a top-level clause binds ?c, ?c. A query
// with inputs naming no symbol twice takes, half the time, an aggregate find
// instead: a count of a bound variable, alone or beside another bound
// variable as its group key.
func randomQuery(r *rand.Rand, data shapeData) shapeQuery {
	var q shapeQuery
	used := []string{"?e"}
	if r.Intn(3) == 0 {
		n := 1
		if r.Intn(4) == 0 {
			n = 2
		}
		allowRel := true
		for i := 0; i < n; i++ {
			in := randomInput(r, data, allowRel)
			if in.form == "rel" {
				allowRel = false
			}
			q.ins = append(q.ins, in)
			for _, s := range in.syms {
				if !containsString(used, s) && s != "?n" {
					used = append(used, s)
				}
			}
		}
	}
	npos := 1 + r.Intn(3)
	for i := 0; i < npos; i++ {
		p := randomPattern(r, append(used, pickString(r, shapeEntityVars)), r.Intn(3) == 0)
		q.where = append(q.where, p)
		vs := map[string]bool{}
		p.vars(vs)
		for _, v := range sortedKeys(vs) {
			if !containsString(used, v) && v != "?a" && v != "?n" {
				used = append(used, v)
			}
		}
	}
	if r.Intn(3) == 0 {
		q.where = append(q.where, shapeClause{kind: "ground", t: []shapeTerm{shapeKeyword(pickString(r, []string{":p", ":q", ":r"})), shapeVar("?a")}})
	}
	if r.Intn(4) == 0 {
		q.where = append(q.where, randomCountSubquery(r, used))
	}
	if r.Intn(4) == 0 {
		attr := pickString(r, []string{":p", ":q", ":r"})
		q.where = append(q.where, randomVectorGround(r, data, attr),
			shapeClause{kind: "subqcoll", t: []shapeTerm{shapeVar("?coll"), shapeVar("?c"), shapeKeyword(attr)}})
	}
	nf := 1 + r.Intn(2)
	for i := 0; i < nf; i++ {
		q.where = append(q.where, randomForm(r, append(used, pickString(r, shapeEntityVars))))
	}
	if r.Intn(5) == 0 && len(used) > 1 {
		q.where = append(q.where, shapeClause{kind: "neq", t: []shapeTerm{shapeVar(used[0]), shapeVar(used[1])}})
	}
	q.find = []string{used[0]}
	if len(used) > 1 && r.Intn(2) == 0 {
		q.find = append(q.find, used[1+r.Intn(len(used)-1)])
	}
	if branchBinds(q.where, "?c") && r.Intn(2) == 0 {
		q.find = append(q.find, "?c")
	}
	if len(q.ins) > 0 && !q.repeatsASymbol() && r.Intn(2) == 0 {
		counted := pickString(r, used)
		q.find = []string{"(count " + counted + ")"}
		if len(used) > 1 && r.Intn(2) == 0 {
			key := pickString(r, used)
			for key == counted {
				key = pickString(r, used)
			}
			q.find = []string{key, "(count " + counted + ")"}
		}
	}
	return q
}

// floatTerm is a float constant, written with its point so the text names
// its type: 1.0, never 1.
func floatTerm(f float64) shapeTerm {
	return shapeTerm{c: strconv.FormatFloat(f, 'f', 1, 64), val: f}
}

// respelledTwin is q with every integer constant under :r respelled as the
// float of the same magnitude and every vector ground of integers respelled
// as the strings of their digits, and false when q holds neither. The twin
// matches the store's float values alone and no :r value as a string, so
// its rows are the original's only where the engine reads the twin's text
// as the original's.
func respelledTwin(q shapeQuery) (shapeQuery, bool) {
	changed := false
	var twinClause func(c shapeClause) shapeClause
	twinClause = func(c shapeClause) shapeClause {
		out := shapeClause{kind: c.kind, t: append([]shapeTerm{}, c.t...), header: append([]string{}, c.header...)}
		if c.kind == "pat" && c.t[1].c == ":r" && c.t[2].v == "" {
			if n, ok := c.t[2].val.(int64); ok {
				out.t[2] = floatTerm(float64(n))
				changed = true
			}
		}
		if c.kind == "groundvec" {
			members := c.t[0].val.([]interface{})
			var strs []shapeTerm
			for _, member := range members {
				if n, ok := member.(int64); ok {
					digits := strconv.FormatInt(n, 10)
					strs = append(strs, shapeTerm{c: strconv.Quote(digits), val: digits})
				}
			}
			if len(strs) > 0 && len(strs) == len(members) {
				out.t[0] = vectorTerm(strs)
				changed = true
			}
		}
		for _, br := range c.branches {
			var twinBranch []shapeClause
			for _, cl := range br {
				twinBranch = append(twinBranch, twinClause(cl))
			}
			out.branches = append(out.branches, twinBranch)
		}
		return out
	}
	twin := shapeQuery{find: append([]string{}, q.find...), ins: q.ins}
	for _, c := range q.where {
		twin.where = append(twin.where, twinClause(c))
	}
	return twin, changed
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// errShapeRefused marks a query the reference refuses.
var errShapeRefused = errors.New("refused")

// outsideVars is every variable the clauses of list other than skip expose,
// plus extra.
func outsideVars(list []shapeClause, skip int, extra []string) map[string]bool {
	out := map[string]bool{}
	for i, c := range list {
		if i != skip {
			c.exposed(out)
		}
	}
	for _, v := range extra {
		out[v] = true
	}
	return out
}

// sharedVars is what the form at i shares with its enclosing scope, sorted:
// a not-join's or an or-join's header; otherwise every variable it exposes
// that occurs outside it.
func sharedVars(list []shapeClause, i int, extra []string) []string {
	c := list[i]
	sh := map[string]bool{}
	switch c.kind {
	case "notjoin", "orjoin":
		for _, h := range c.header {
			sh[h] = true
		}
	default:
		out := outsideVars(list, i, extra)
		vs := map[string]bool{}
		c.exposed(vs)
		for v := range vs {
			if out[v] {
				sh[v] = true
			}
		}
	}
	return sortedKeys(sh)
}

// bindsVar reports whether the clause binds v: a pattern or an input
// mentioning it, a ground, identity or subquery into it, or a form whose
// every branch binds it, within the header for an or-join. A != and a not or
// not-join bind nothing.
func bindsVar(c shapeClause, v string) bool {
	switch c.kind {
	case "pat", "in":
		for _, t := range c.t {
			if t.v == v {
				return true
			}
		}
	case "ground", "ident", "subq", "groundvec", "subqcoll":
		return c.t[1].v == v
	case "or", "ordef", "orjoin":
		if c.kind == "orjoin" && !containsString(c.header, v) {
			return false
		}
		for _, br := range c.branches {
			if !branchBinds(br, v) {
				return false
			}
		}
		return true
	}
	return false
}

// branchBinds reports whether some clause of the branch binds v.
func branchBinds(branch []shapeClause, v string) bool {
	for _, cl := range branch {
		if bindsVar(cl, v) {
			return true
		}
	}
	return false
}

// scopePlan is one scope ordered for evaluation: the clause indices in the
// order they run, what each form shares with the scope, and a plan per
// branch of each form.
type scopePlan struct {
	order    []int
	shared   map[int][]string
	branches map[int][]*scopePlan
}

// planScope orders list from the text alone. avail is what the enclosing
// scope has bound when the list runs, and extra the variables the enclosing
// scope shares with it. The plan refuses the query on the conditions the
// file's comment lists.
func planScope(list []shapeClause, avail map[string]bool, extra []string) (*scopePlan, error) {
	plan := &scopePlan{shared: map[int][]string{}, branches: map[int][]*scopePlan{}}
	for i, c := range list {
		if !isShapeForm(c) {
			continue
		}
		if c.kind == "notjoin" {
			mentioned := map[string]bool{}
			for _, cl := range c.branches[0] {
				cl.vars(mentioned)
			}
			for _, h := range c.header {
				if !mentioned[h] {
					return nil, fmt.Errorf("%w: not-join header %s is not mentioned by the body of %s", errShapeRefused, h, c)
				}
			}
		}
		if c.kind == "orjoin" {
			for b, br := range c.branches {
				for _, h := range c.header {
					if !branchBinds(br, h) {
						return nil, fmt.Errorf("%w: %s: branch %d does not bind %s", errShapeRefused, c, b+1, h)
					}
				}
			}
		}
		plan.shared[i] = sharedVars(list, i, extra)
		if len(plan.shared[i]) == 0 {
			return nil, fmt.Errorf("%w: %s shares no variable with its enclosing scope", errShapeRefused, c)
		}
	}
	bound := map[string]bool{}
	for v := range avail {
		bound[v] = true
	}
	placed := make([]bool, len(list))
	for len(plan.order) < len(list) {
		next := -1
		for i, c := range list {
			if placed[i] {
				continue
			}
			ok, err := clauseCanRun(list, placed, i, bound, plan.shared[i])
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			if next == -1 || (isShapeForm(list[next]) && !isShapeForm(c)) {
				next = i
			}
		}
		if next == -1 {
			var stuck []string
			for i := range list {
				if !placed[i] {
					stuck = append(stuck, list[i].String())
				}
			}
			return nil, fmt.Errorf("%w: no order runs %s", errShapeRefused, strings.Join(stuck, " "))
		}
		placed[next] = true
		plan.order = append(plan.order, next)
		c := list[next]
		if isShapeForm(c) {
			// A branch runs with the form's required variables bound; an
			// output is computed by the branch, never read from the scope.
			inner := map[string]bool{}
			for _, v := range plan.shared[next] {
				if bound[v] && !bindsVar(c, v) {
					inner[v] = true
				}
			}
			for _, br := range c.branches {
				bp, err := planScope(br, inner, plan.shared[next])
				if err != nil {
					return nil, err
				}
				plan.branches[next] = append(plan.branches[next], bp)
			}
		}
		for _, v := range clauseProvides(c, plan.shared[next]) {
			bound[v] = true
		}
	}
	return plan, nil
}

// clauseCanRun reports whether the clause at i can run next: a pattern or
// ground always; an identity, a subquery or a != once its inputs are bound;
// a form once every shared variable it must take from the scope is bound. A
// shared variable the form binds in every branch is its output and waits on
// nothing. An input or a shared variable no clause binds, and the form does
// not bind, refuses the query.
func clauseCanRun(list []shapeClause, placed []bool, i int, bound map[string]bool, shared []string) (bool, error) {
	c := list[i]
	pendingBinder := func(v string) bool {
		for j, o := range list {
			if j != i && !placed[j] && bindsVar(o, v) {
				return true
			}
		}
		return false
	}
	inputReady := func(t shapeTerm) (bool, error) {
		if t.v == "" || bound[t.v] {
			return true, nil
		}
		if pendingBinder(t.v) {
			return false, nil
		}
		return false, fmt.Errorf("%w: %s is bound by no clause before %s", errShapeRefused, t.v, c)
	}
	switch c.kind {
	case "pat", "ground", "groundvec", "in":
		return true, nil
	case "ident", "subq", "subqcoll":
		return inputReady(c.t[0])
	case "neq":
		if ready, err := inputReady(c.t[0]); err != nil || !ready {
			return ready, err
		}
		return inputReady(c.t[1])
	}
	for _, v := range shared {
		if bound[v] {
			continue
		}
		output := bindsVar(c, v)
		if output {
			continue
		}
		if pendingBinder(v) {
			return false, nil
		}
		if !output {
			return false, fmt.Errorf("%w: %s is bound by no clause before %s", errShapeRefused, v, c)
		}
	}
	return true, nil
}

// clauseProvides is what the clause binds for the clauses after it: a
// pattern's or an input's variables, a ground's, identity's or subquery's
// output, and a form's shared variables it binds in every branch.
func clauseProvides(c shapeClause, shared []string) []string {
	switch c.kind {
	case "pat", "in":
		var out []string
		for _, t := range c.t {
			if t.v != "" {
				out = append(out, t.v)
			}
		}
		return out
	case "ground", "ident", "subq", "groundvec", "subqcoll":
		return []string{c.t[1].v}
	case "or", "ordef", "orjoin":
		var out []string
		for _, v := range shared {
			if bindsVar(c, v) {
				out = append(out, v)
			}
		}
		return out
	}
	return nil
}

// shapeEnv is a binding of variables to values.
type shapeEnv map[string]interface{}

func (e shapeEnv) copy() shapeEnv {
	n := make(shapeEnv, len(e))
	for k, v := range e {
		n[k] = v
	}
	return n
}

func (e shapeEnv) key() string {
	ks := make([]string, 0, len(e))
	for k := range e {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	var b strings.Builder
	for _, k := range ks {
		fmt.Fprintf(&b, "%s=%T:%v;", k, e[k], e[k])
	}
	return b.String()
}

func valuesEqual(a, b interface{}) bool {
	return fmt.Sprint(a) == fmt.Sprint(b) && fmt.Sprintf("%T", a) == fmt.Sprintf("%T", b)
}

// shapeSolver enumerates the bindings of a planned scope over the datoms,
// indexed by entity and by value so a pattern with a bound entity or value
// reads only the datoms that can match it. The index keys on the value
// itself: an identity or a keyword is interned, so one value is one key, and
// an integer and a float of one magnitude are two keys, as they are two
// values.
type shapeSolver struct {
	datoms []shapeDatom
	byE    map[interface{}][]shapeDatom
	byV    map[interface{}][]shapeDatom
}

func newShapeSolver(datoms []shapeDatom) *shapeSolver {
	s := &shapeSolver{datoms: datoms, byE: map[interface{}][]shapeDatom{}, byV: map[interface{}][]shapeDatom{}}
	for _, d := range datoms {
		s.byE[d.e] = append(s.byE[d.e], d)
		s.byV[d.v] = append(s.byV[d.v], d)
	}
	return s
}

// candidates is the datoms a pattern can match under σ: those of its entity
// when σ binds it, else those of its value when σ binds it or the pattern
// states it, else every datom.
func (s *shapeSolver) candidates(c shapeClause, σ shapeEnv) []shapeDatom {
	if t := c.t[0]; t.v != "" {
		if val, ok := σ[t.v]; ok {
			return s.byE[val]
		}
	}
	if t := c.t[2]; t.v != "" {
		if val, ok := σ[t.v]; ok {
			return s.byV[val]
		}
	} else if t.c != "_" {
		return s.byV[t.val]
	}
	return s.datoms
}

// evalScope enumerates every extension of σ satisfying the clauses of list
// from position k of the plan on and calls emit with each; emit returns
// false to stop, and evalScope returns false once stopped.
func (s *shapeSolver) evalScope(plan *scopePlan, list []shapeClause, k int, σ shapeEnv, emit func(shapeEnv) bool) bool {
	if k == len(plan.order) {
		return emit(σ)
	}
	i := plan.order[k]
	c := list[i]
	step := func(n shapeEnv) bool { return s.evalScope(plan, list, k+1, n, emit) }
	unify := func(n shapeEnv, t shapeTerm, val interface{}) bool {
		if t.c == "_" {
			return true
		}
		if t.v == "" {
			return valuesEqual(t.val, val)
		}
		if cur, ok := n[t.v]; ok {
			return valuesEqual(cur, val)
		}
		n[t.v] = val
		return true
	}
	// innerEnv is the branch's starting binding: the form's required
	// variables σ binds. An output, a shared variable the form binds in every
	// branch, is computed by the branch alone. merged is σ joined with the
	// outputs a branch bound, false when an output σ already binds differs.
	innerEnv := func() shapeEnv {
		inner := shapeEnv{}
		for _, v := range plan.shared[i] {
			if bindsVar(c, v) {
				continue
			}
			if val, ok := σ[v]; ok {
				inner[v] = val
			}
		}
		return inner
	}
	merged := func(r shapeEnv) (shapeEnv, bool) {
		n := σ.copy()
		for _, v := range plan.shared[i] {
			val, ok := r[v]
			if !ok {
				continue
			}
			if cur, bound := n[v]; bound {
				if !valuesEqual(cur, val) {
					return nil, false
				}
				continue
			}
			n[v] = val
		}
		return n, true
	}
	// termValue is a term's value under σ: a constant's own, a variable's
	// binding.
	termValue := func(t shapeTerm) interface{} {
		if t.v == "" {
			return t.val
		}
		return σ[t.v]
	}
	switch c.kind {
	case "pat":
		for _, d := range s.candidates(c, σ) {
			n := σ.copy()
			if unify(n, c.t[0], d.e) && unify(n, c.t[1], d.a) && unify(n, c.t[2], d.v) {
				if !step(n) {
					return false
				}
			}
		}
		return true
	case "in":
		for _, row := range c.rows {
			n := σ.copy()
			unified := true
			for i, t := range c.t {
				if !unify(n, t, row[i].val) {
					unified = false
					break
				}
			}
			if unified && !step(n) {
				return false
			}
		}
		return true
	case "ground":
		n := σ.copy()
		if unify(n, c.t[1], c.t[0].val) {
			return step(n)
		}
		return true
	case "ident":
		n := σ.copy()
		if unify(n, c.t[1], σ[c.t[0].v]) {
			return step(n)
		}
		return true
	case "subq":
		// The inner query counts the entities that refer to the input under
		// the attribute. The input is an entity variable, bound to an identity,
		// a keyword or a number. A count of zero binds nothing, so the clause
		// has no solution; otherwise the count unifies with the binding
		// variable.
		count := 0
		for _, d := range s.byV[σ[c.t[0].v]] {
			if valuesEqual(d.a, c.t[2].val) {
				count++
			}
		}
		if count == 0 {
			return true
		}
		n := σ.copy()
		if unify(n, c.t[1], int64(count)) {
			return step(n)
		}
		return true
	case "groundvec":
		n := σ.copy()
		if unify(n, c.t[1], c.t[0].val) {
			return step(n)
		}
		return true
	case "subqcoll":
		// The inner query counts the entities whose value under the attribute
		// is a member of the collection, each distinct member once; a member
		// is an identity or an integer. A count of zero binds nothing, so the
		// clause has no solution; otherwise the count unifies with the binding
		// variable.
		count := 0
		seen := map[interface{}]bool{}
		for _, member := range σ[c.t[0].v].([]interface{}) {
			if seen[member] {
				continue
			}
			seen[member] = true
			for _, d := range s.byV[member] {
				if valuesEqual(d.a, c.t[2].val) {
					count++
				}
			}
		}
		if count == 0 {
			return true
		}
		n := σ.copy()
		if unify(n, c.t[1], int64(count)) {
			return step(n)
		}
		return true
	case "neq":
		if !valuesEqual(termValue(c.t[0]), termValue(c.t[1])) {
			return step(σ.copy())
		}
		return true
	case "not", "notjoin":
		found := false
		s.evalScope(plan.branches[i][0], c.branches[0], 0, innerEnv(), func(shapeEnv) bool { found = true; return false })
		if !found {
			return step(σ.copy())
		}
		return true
	case "or", "orjoin":
		results := map[string]shapeEnv{}
		var order []string
		for b, br := range c.branches {
			s.evalScope(plan.branches[i][b], br, 0, innerEnv(), func(r shapeEnv) bool {
				n, ok := merged(r)
				if !ok {
					return true
				}
				key := n.key()
				if _, ok := results[key]; !ok {
					results[key] = n
					order = append(order, key)
				}
				return true
			})
		}
		for _, key := range order {
			if !step(results[key]) {
				return false
			}
		}
		return true
	case "ordef":
		// The fallback is decided on a branch's own rows, before they join
		// the scope on the outputs.
		for b, br := range c.branches {
			var sols []shapeEnv
			seen := map[string]bool{}
			s.evalScope(plan.branches[i][b], br, 0, innerEnv(), func(r shapeEnv) bool {
				if key := r.key(); !seen[key] {
					seen[key] = true
					sols = append(sols, r)
				}
				return true
			})
			if len(sols) > 0 {
				emitted := map[string]bool{}
				for _, r := range sols {
					n, ok := merged(r)
					if !ok {
						continue
					}
					key := n.key()
					if emitted[key] {
						continue
					}
					emitted[key] = true
					if !step(n) {
						return false
					}
				}
				return true
			}
		}
		return true
	}
	panic(fmt.Sprintf("BUG: unknown shape clause kind %q", c.kind))
}

// renderValue renders a result value as the reference and the engine runs
// are compared: an entity by its generated name, anything else by fmt.
func (data shapeData) renderValue(v interface{}) string {
	if id, ok := v.(datalog.Identity); ok {
		if n, ok := data.entName[id.String()]; ok {
			return n
		}
	}
	return fmt.Sprint(v)
}

// referenceRows plans q and returns the sorted, deduplicated rows the
// reference computes for it over data, or errShapeRefused. The reference
// reads each input as a clause of the top-level scope, before the body: a
// relation over the input's symbols the body joins, whatever its form.
func referenceRows(data shapeData, q shapeQuery) ([]string, error) {
	list := make([]shapeClause, 0, len(q.ins)+len(q.where))
	for _, in := range q.ins {
		list = append(list, in.ground())
	}
	list = append(list, q.where...)
	plan, err := planScope(list, map[string]bool{}, q.find)
	if err != nil {
		return nil, err
	}
	for _, f := range q.find {
		bound := false
		for _, c := range list {
			if bindsVar(c, f) {
				bound = true
			}
		}
		if !bound {
			return nil, fmt.Errorf("%w: :find variable %s is bound by no clause", errShapeRefused, f)
		}
	}
	s := newShapeSolver(data.datoms)
	set := map[string]bool{}
	s.evalScope(plan, list, 0, shapeEnv{}, func(σ shapeEnv) bool {
		parts := make([]string, 0, len(q.find))
		for _, f := range q.find {
			parts = append(parts, data.renderValue(σ[f]))
		}
		set[strings.Join(parts, ",")] = true
		return true
	})
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

// engineRows runs text on d with the given inputs and returns the sorted,
// deduplicated rows, or the error's text, or "PANIC: ..." for a panic.
func engineRows(d *Database, data shapeData, text string, inputs ...interface{}) (rows []string, msg string) {
	defer func() {
		if p := recover(); p != nil {
			rows, msg = nil, fmt.Sprintf("PANIC: %v", p)
		}
	}()
	rel, err := d.Query(text, inputs...)
	if err != nil {
		return nil, err.Error()
	}
	set := map[string]bool{}
	err = executor.ForEach(rel, func(t executor.Tuple) error {
		parts := make([]string, 0, len(t))
		for _, v := range t {
			parts = append(parts, data.renderValue(v))
		}
		set[strings.Join(parts, ",")] = true
		return nil
	})
	if err != nil {
		return nil, err.Error()
	}
	rows = make([]string, 0, len(set))
	for k := range set {
		rows = append(rows, k)
	}
	sort.Strings(rows)
	return rows, ""
}

// writeShapeData writes a seed's datoms in one transaction and returns the
// datoms the store then holds: the seed's and the commit's mark, the
// :db/txInstant datom Commit writes on the transaction's entity, read back
// through the store, since a pattern with a variable attribute matches it.
func writeShapeData(t testing.TB, d *Database, data shapeData) shapeData {
	t.Helper()
	tx := d.NewTransaction()
	for _, dt := range data.datoms {
		if err := tx.Add(dt.e.(datalog.Identity), dt.a.(datalog.Keyword), dt.v); err != nil {
			t.Fatalf("writing %v: %v", dt, err)
		}
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("committing seed data: %v", err)
	}
	stored := shapeData{ents: data.ents, entName: data.entName, datoms: append([]shapeDatom{}, data.datoms...)}
	txInstant := datalog.NewKeyword(":db/txInstant")
	marks, err := d.Query(fmt.Sprintf("[:find ?t ?v :where [?t %s ?v]]", txInstant))
	if err != nil {
		t.Fatalf("reading the commit mark: %v", err)
	}
	err = executor.ForEach(marks, func(tuple executor.Tuple) error {
		stored.datoms = append(stored.datoms, shapeDatom{tuple[0], txInstant, tuple[1]})
		return nil
	})
	if err != nil {
		t.Fatalf("reading the commit mark: %v", err)
	}
	return stored
}

// shapeOrders is the clause orders each query runs in: the written order, the
// reverse, a shuffle drawn from the seed's source, and the written order
// again, which the plan cache serves.
func shapeOrders(r *rand.Rand, n int) map[string][]int {
	written := make([]int, n)
	for i := range written {
		written[i] = i
	}
	reverse := make([]int, n)
	for i := range written {
		reverse[i] = written[n-1-i]
	}
	shuffled := append([]int{}, written...)
	r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	return map[string][]int{"written": written, "reverse": reverse, "shuffled": shuffled, "again": written}
}

var shapeOrderNames = []string{"written", "reverse", "shuffled", "again"}

// queryDifferences runs q on d in each order and returns one line per
// difference from what it must answer. A query whose find holds no aggregate
// is checked against the reference in its input form and, when no input
// names a symbol twice, in its ground form, which the reference reads the
// same. A query whose find holds an aggregate is checked against the
// engine's own answer for its ground form: the forms are spellings of one
// relation the body joins, and the aggregate groups by the find over the
// whole join. The error is the reference's own failure, never a refusal.
func queryDifferences(d *Database, stored shapeData, q shapeQuery, orders map[string][]int) ([]string, error) {
	var diffs []string
	if q.aggregates() {
		for _, name := range shapeOrderNames {
			rows, msg := engineRows(d, stored, q.text(orders[name]), q.values()...)
			ground, groundMsg := engineRows(d, stored, q.groundText(orders[name]))
			switch {
			case strings.HasPrefix(msg, "PANIC") || strings.HasPrefix(groundMsg, "PANIC"):
				diffs = append(diffs, fmt.Sprintf("%s: %q, ground form %q", name, msg, groundMsg))
			case (msg == "") != (groundMsg == ""):
				diffs = append(diffs, fmt.Sprintf("%s: error %q, ground form error %q", name, msg, groundMsg))
			case msg == "" && !equalStrings(rows, ground):
				diffs = append(diffs, fmt.Sprintf("%s: rows %v, ground form rows %v", name, rows, ground))
			}
		}
		return diffs, nil
	}
	ref, refused := referenceRows(stored, q)
	if refused != nil && !errors.Is(refused, errShapeRefused) {
		return nil, fmt.Errorf("the reference failed: %w", refused)
	}
	compare := func(label string, rows []string, msg string) {
		switch {
		case strings.HasPrefix(msg, "PANIC"):
			diffs = append(diffs, fmt.Sprintf("%s: %s", label, msg))
		case refused != nil && msg == "":
			diffs = append(diffs, fmt.Sprintf("%s: rows %v, reference refuses: %v", label, rows, refused))
		case refused == nil && msg != "":
			diffs = append(diffs, fmt.Sprintf("%s: error %q, reference rows %v", label, msg, ref))
		case refused == nil && !equalStrings(rows, ref):
			diffs = append(diffs, fmt.Sprintf("%s: rows %v, reference rows %v", label, rows, ref))
		}
	}
	for _, name := range shapeOrderNames {
		rows, msg := engineRows(d, stored, q.text(orders[name]), q.values()...)
		compare(name, rows, msg)
		if len(q.ins) > 0 && !q.repeatsASymbol() {
			rows, msg = engineRows(d, stored, q.groundText(orders[name]))
			compare(name+" as grounds", rows, msg)
		}
	}
	return diffs, nil
}

// Every generated query gives, in every mode and in every order, the rows the
// reference computes over the datoms that mode's store holds, and a query the
// reference refuses is refused. The failure for a query names its seed, its
// number, its text, with its inputs as grounds where it has any, and for each
// order and mode what the engine gave against what it must give.
// shapeRuns is the entity counts the test draws its datoms at, each with the
// seeds it runs: six entities over many seeds, and forty, where a query's
// answer spans more of the store and a branch's run holds more tuples, over
// fewer.
var shapeRuns = []struct{ entities, seeds int }{{6, 120}, {40, 20}}

func TestQueryShapesOnRandomGraphsMatchTheReference(t *testing.T) {
	const perSeed = 25
	type generated struct {
		data    shapeData
		queries []shapeQuery
		orders  []map[string][]int
	}
	for _, run := range shapeRuns {
		byseed := make([]generated, run.seeds)
		for seed := range byseed {
			r := rand.New(rand.NewSource(int64(seed)))
			g := generated{data: randomDatoms(r, run.entities)}
			for len(g.queries) < perSeed {
				q := randomQuery(r, g.data)
				g.queries = append(g.queries, q)
				g.orders = append(g.orders, shapeOrders(r, len(q.where)))
				// A query holding an integer constant under :r or a vector
				// ground of integers is followed, half the time, by its
				// respelled twin, which runs on the same store after it.
				if twin, ok := respelledTwin(q); ok && len(g.queries) < perSeed && r.Intn(2) == 0 {
					g.queries = append(g.queries, twin)
					g.orders = append(g.orders, shapeOrders(r, len(twin.where)))
				}
			}
			byseed[seed] = g
		}

		t.Run(fmt.Sprintf("%d_entities", run.entities), func(t *testing.T) {
			for _, mode := range optimizerModes {
				t.Run(mode.name, func(t *testing.T) {
					differing := 0
					for seed, g := range byseed {
						d := createOptimizerModeDB(t, mode, DatabaseOptions{})
						stored := writeShapeData(t, d, g.data)
						for qi, q := range g.queries {
							diffs, err := queryDifferences(d, stored, q, g.orders[qi])
							if err != nil {
								t.Fatalf("seed %d query %d: %v", seed, qi, err)
							}
							if len(diffs) > 0 {
								differing++
								text := q.text(g.orders[qi]["written"])
								if len(q.ins) > 0 {
									text += "\n  as grounds: " + q.groundText(g.orders[qi]["written"])
								}
								t.Errorf("seed %d query %d: %s\n  %s", seed, qi, text, strings.Join(diffs, "\n  "))
							}
						}
						if err := d.Close(); err != nil {
							t.Fatalf("closing seed %d: %v", seed, err)
						}
					}
					if differing > 0 {
						t.Errorf("%d of %d queries differ from the reference", differing, run.seeds*perSeed)
					}
				})
			}
		})
	}
}

func equalStrings(a, b []string) bool {
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
