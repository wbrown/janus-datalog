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
// random datoms of six entities and the mark their commit writes, the
// :db/txInstant datom a pattern with a variable attribute matches. The
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
// identity, a != predicate, a count subquery, or a form over branches with
// an optional header. A subq's terms are its input variable, its binding
// variable and the attribute its inner query counts under; the inner
// query's own variable ?s is no term, since it is local to the inner query.
type shapeClause struct {
	kind     string // pat, ground, ident, neq, subq, or, ordef, not, notjoin, orjoin
	t        []shapeTerm
	branches [][]shapeClause
	header   []string
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
	case "pat", "ground", "ident", "neq", "subq":
		return false
	}
	return true
}

type shapeQuery struct {
	find  []string
	where []shapeClause
}

// text renders the query with its clauses in the given order.
func (q shapeQuery) text(order []int) string {
	parts := make([]string, 0, len(order))
	for _, i := range order {
		parts = append(parts, q.where[i].String())
	}
	return fmt.Sprintf("[:find %s :where %s]", strings.Join(q.find, " "), strings.Join(parts, " "))
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
// and :q zero or one ref to a random entity, under :r an integer below three
// two times in three, and under :k one of :p, :q or :r half the time.
func randomDatoms(r *rand.Rand, n int) shapeData {
	data := shapeData{entName: map[string]string{}}
	for i := 0; i < n; i++ {
		id := datalog.NewIdentity(fmt.Sprintf("e%d", i))
		data.ents = append(data.ents, id)
		data.entName[id.String()] = fmt.Sprintf("e%d", i)
	}
	seen := map[string]bool{}
	add := func(d shapeDatom) {
		k := fmt.Sprint(d.e, d.a, d.v)
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
			add(shapeDatom{e, datalog.NewKeyword(":r"), int64(r.Intn(3))})
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
// to the attribute: an integer or ?n under :r, a keyword or ?a under :k, the
// blank one time in six, and otherwise an entity variable other than the
// entity's.
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
		if r.Intn(2) == 0 {
			n := int64(r.Intn(3))
			v = shapeTerm{c: strconv.FormatInt(n, 10), val: n}
		} else {
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
// time in eight, an identity between two distinct variables one time in
// eight, a count subquery one time in eight, and otherwise a pattern.
func randomBranch(r *rand.Rand, evars []string) []shapeClause {
	n := 1 + r.Intn(2)
	var out []shapeClause
	for i := 0; i < n; i++ {
		switch r.Intn(8) {
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

// randomQuery draws one to three top-level patterns, a ground of a keyword
// into ?a one time in three, a count subquery over a bound entity variable
// one time in four, one or two forms, a != between the first two bound
// variables one time in five, and a :find of the first bound variable and,
// half the time, one other, and, half the time a top-level clause binds ?c,
// ?c.
func randomQuery(r *rand.Rand) shapeQuery {
	var q shapeQuery
	npos := 1 + r.Intn(3)
	used := []string{"?e"}
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
	return q
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

// bindsVar reports whether the clause binds v: a pattern mentioning it, a
// ground, identity or subquery into it, or a form whose every branch binds
// it, within the header for an or-join. A != and a not or not-join bind
// nothing.
func bindsVar(c shapeClause, v string) bool {
	switch c.kind {
	case "pat":
		for _, t := range c.t {
			if t.v == v {
				return true
			}
		}
	case "ground", "ident", "subq":
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
						return nil, fmt.Errorf("%w: %s: branch %d does not bind %s", errShapeRefused, c, b, h)
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
	case "pat", "ground":
		return true, nil
	case "ident", "subq":
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
// pattern's variables, a ground's, identity's or subquery's output, and a
// form's shared variables it binds in every branch.
func clauseProvides(c shapeClause, shared []string) []string {
	switch c.kind {
	case "pat":
		var out []string
		for _, t := range c.t {
			if t.v != "" {
				out = append(out, t.v)
			}
		}
		return out
	case "ground", "ident", "subq":
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

// shapeSolver enumerates the bindings of a planned scope over the datoms.
type shapeSolver struct {
	datoms []shapeDatom
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
		for _, d := range s.datoms {
			n := σ.copy()
			if unify(n, c.t[0], d.e) && unify(n, c.t[1], d.a) && unify(n, c.t[2], d.v) {
				if !step(n) {
					return false
				}
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
		// the attribute. A count of zero binds nothing, so the clause has no
		// solution; otherwise the count unifies with the binding variable.
		count := 0
		for _, d := range s.datoms {
			if valuesEqual(d.a, c.t[2].val) && valuesEqual(d.v, σ[c.t[0].v]) {
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
// reference computes for it over data, or errShapeRefused.
func referenceRows(data shapeData, q shapeQuery) ([]string, error) {
	plan, err := planScope(q.where, map[string]bool{}, q.find)
	if err != nil {
		return nil, err
	}
	for _, f := range q.find {
		bound := false
		for _, c := range q.where {
			if bindsVar(c, f) {
				bound = true
			}
		}
		if !bound {
			return nil, fmt.Errorf("%w: :find variable %s is bound by no clause", errShapeRefused, f)
		}
	}
	s := &shapeSolver{datoms: data.datoms}
	set := map[string]bool{}
	s.evalScope(plan, q.where, 0, shapeEnv{}, func(σ shapeEnv) bool {
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

// engineRows runs text on d and returns the sorted, deduplicated rows, or
// the error's text, or "PANIC: ..." for a panic.
func engineRows(d *Database, data shapeData, text string) (rows []string, msg string) {
	defer func() {
		if p := recover(); p != nil {
			rows, msg = nil, fmt.Sprintf("PANIC: %v", p)
		}
	}()
	rel, err := d.Query(text)
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

// Every generated query gives, in every mode and in every order, the rows the
// reference computes over the datoms that mode's store holds, and a query the
// reference refuses is refused. The failure for a query names its seed, its
// number, its text, and for each order and mode what the engine gave against
// what the reference gave.
func TestQueryShapesOnRandomGraphsMatchTheReference(t *testing.T) {
	const seeds, perSeed, entities = 120, 25, 6
	type generated struct {
		data    shapeData
		queries []shapeQuery
		orders  []map[string][]int
	}
	byseed := make([]generated, seeds)
	for seed := range byseed {
		r := rand.New(rand.NewSource(int64(seed)))
		g := generated{data: randomDatoms(r, entities)}
		for qi := 0; qi < perSeed; qi++ {
			q := randomQuery(r)
			g.queries = append(g.queries, q)
			g.orders = append(g.orders, shapeOrders(r, len(q.where)))
		}
		byseed[seed] = g
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			differing := 0
			for seed, g := range byseed {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				stored := writeShapeData(t, d, g.data)
				for qi, q := range g.queries {
					ref, refused := referenceRows(stored, q)
					if refused != nil && !errors.Is(refused, errShapeRefused) {
						t.Fatalf("seed %d query %d: the reference failed: %v", seed, qi, refused)
					}
					var diffs []string
					for _, name := range shapeOrderNames {
						text := q.text(g.orders[qi][name])
						rows, msg := engineRows(d, stored, text)
						switch {
						case strings.HasPrefix(msg, "PANIC"):
							diffs = append(diffs, fmt.Sprintf("%s: %s", name, msg))
						case refused != nil && msg == "":
							diffs = append(diffs, fmt.Sprintf("%s: rows %v, reference refuses: %v", name, rows, refused))
						case refused == nil && msg != "":
							diffs = append(diffs, fmt.Sprintf("%s: error %q, reference rows %v", name, msg, ref))
						case refused == nil && !equalStrings(rows, ref):
							diffs = append(diffs, fmt.Sprintf("%s: rows %v, reference rows %v", name, rows, ref))
						}
					}
					if len(diffs) > 0 {
						differing++
						t.Errorf("seed %d query %d: %s\n  %s", seed, qi, q.text(g.orders[qi]["written"]), strings.Join(diffs, "\n  "))
					}
				}
				if err := d.Close(); err != nil {
					t.Fatalf("closing seed %d: %v", seed, err)
				}
			}
			if differing > 0 {
				t.Errorf("%d of %d queries differ from the reference", differing, seeds*perSeed)
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
