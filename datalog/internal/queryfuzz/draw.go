package queryfuzz

import (
	"math/rand"
	"strconv"
	"strings"
)

// Rules is the rule set a drawn query may invoke: r, recursive over
// :a and :b, and s, whose first position is required.
const Rules = `[[(r ?x ?y) [?x :a ?y]]
 [(r ?x ?y) [?x :b ?z] (r ?z ?y)]
 [(s [?x] ?y) [?x :c ?y]]]`

var vars = []string{"?e", "?f", "?v", "?w", "?k", "?j"}

// Seeds are the seeds the tests draw from, and PerSeed the texts each draws.
var Seeds = []int64{1, 2, 3, 4}

const PerSeed = 750

// Draw draws query text from one source.
type Draw struct {
	r *rand.Rand
}

// NewDraw is a draw from the source seed names. Two draws from one seed
// render the same texts in the same order.
func NewDraw(seed int64) *Draw {
	return &Draw{r: rand.New(rand.NewSource(seed))}
}

func (d *Draw) pick(xs []string) string { return xs[d.r.Intn(len(xs))] }

// constant draws one constant: an integer, a float of integral, of
// fractional or of infinite magnitude, a string with an escape, a keyword, a
// boolean, an instant, bytes, a transaction value or a vector.
func (d *Draw) constant() string {
	switch d.r.Intn(13) {
	case 0:
		return strconv.Itoa(d.r.Intn(5))
	case 1:
		return d.pick([]string{"1.5", "-2.25", "3.125e10"})
	case 2:
		return d.pick([]string{"1.0", "2.0", "-3.0"})
	case 3:
		return d.pick([]string{`"s"`, `"a\"b"`, `"x\\y"`, `"tab\t"`, `"nl\n"`})
	case 4:
		return d.pick([]string{":k", ":ns/kw", ":a.b/c-d"})
	case 5:
		return d.pick([]string{"true", "false"})
	case 6:
		return `#inst "2021-03-04T05:06:07.5Z"`
	case 7:
		return `#bytes "abcdefgh"`
	case 8:
		return `[1 2]`
	case 9:
		return d.pick([]string{"##Inf", "##-Inf"})
	case 10:
		return `#eid [3 1]`
	default:
		return strconv.Itoa(d.r.Intn(100))
	}
}

// term is a constant one time in three and a variable otherwise.
func (d *Draw) term() string {
	if d.r.Intn(3) == 0 {
		return d.constant()
	}
	return d.pick(vars)
}

// pattern draws a data pattern: a source one time in eight, an entity
// variable or the blank, an attribute, a value term or the blank, and the
// transaction position one time in ten.
func (d *Draw) pattern() string {
	src := ""
	if d.r.Intn(8) == 0 {
		src = "$ "
	}
	a := d.pick([]string{":a", ":b", ":c"})
	e := d.pick([]string{"?e", "?f", "_"})
	v := d.term()
	if d.r.Intn(6) == 0 {
		v = "_"
	}
	if d.r.Intn(10) == 0 {
		return "[" + src + e + " " + a + " " + v + " ?tx]"
	}
	return "[" + src + e + " " + a + " " + v + "]"
}

// predicate draws a comparison, a string predicate, a chained comparison, a
// database function, a transaction range or an equality.
func (d *Draw) predicate() string {
	switch d.r.Intn(7) {
	case 0:
		return "[(" + d.pick([]string{"<", ">", "<=", ">=", "=", "!=", "not="}) + " " + d.pick(vars) + " " + d.term() + ")]"
	case 1:
		return `[(str/starts-with? ` + d.pick(vars) + ` "a")]`
	case 2:
		return "[(< " + d.pick(vars) + " " + d.pick(vars) + " " + d.pick(vars) + ")]"
	case 3:
		return "[(missing? $ " + d.pick([]string{"?e", "?f"}) + " :c)]"
	case 4:
		return "[(tx-between ?tx 1 5)]"
	case 5:
		return "[(> " + d.pick(vars) + " 1.5)]"
	default:
		return "[(= " + d.pick(vars) + " " + d.pick(vars) + ")]"
	}
}

// expression draws an expression under a scalar, collection, tuple or
// relation binding, or a database function.
func (d *Draw) expression() string {
	out := d.pick(vars)
	switch d.r.Intn(8) {
	case 0:
		return "[(+ " + d.pick(vars) + " " + d.term() + ") " + out + "]"
	case 1:
		return "[(str " + d.pick(vars) + ` "x") ` + out + "]"
	case 2:
		return "[(ground " + d.constant() + ") " + out + "]"
	case 3:
		return "[(ground [1 2 3]) [" + out + " ...]]"
	case 4:
		return "[(ground [1 2]) [" + d.pick(vars) + " " + d.pick(vars) + "]]"
	case 5:
		return "[(ground [[1 2] [3 4]]) [[" + d.pick(vars) + " " + d.pick(vars) + "]" + d.pick([]string{"", " ..."}) + "]]"
	case 6:
		return "[(get-else $ " + d.pick([]string{"?e", "?f"}) + " :b " + d.constant() + ") " + out + "]"
	default:
		return "[(identity " + d.pick(vars) + ") " + out + "]"
	}
}

// inSpec draws one :in entry and names its symbols.
func (d *Draw) inSpec() (string, []string) {
	switch d.r.Intn(4) {
	case 0:
		x := d.pick(vars)
		return x, []string{x}
	case 1:
		x := d.pick(vars)
		return "[" + x + " ...]", []string{x}
	case 2:
		x, y := d.pick(vars), d.pick(vars)
		return "[" + x + " " + y + "]", []string{x, y}
	default:
		x, y := d.pick(vars), d.pick(vars)
		return "[[" + x + " " + y + "]" + d.pick([]string{"", " ..."}) + "]", []string{x, y}
	}
}

// subquery draws a subquery clause: an inner query with one :in entry, one
// or two clauses, a plain or an aggregate find, rules one time in four, and
// a binding under the scalar, collection, tuple or relation form.
func (d *Draw) subquery(depth int) string {
	spec, syms := d.inSpec()
	var body []string
	for i := 0; i < 1+d.r.Intn(2); i++ {
		body = append(body, d.clause(depth+1))
	}
	find := d.pick(vars)
	find2 := ""
	if d.r.Intn(4) == 0 {
		find = "(max " + find + ")"
	} else if d.r.Intn(3) == 0 {
		find2 = " " + d.pick(vars)
	}
	rulesIn, rulesArg := "", ""
	if d.r.Intn(4) == 0 {
		rulesIn, rulesArg = " %", " %"
	}
	var args []string
	for range syms {
		args = append(args, d.term())
	}
	inner := "[:find " + find + find2 + " :in $" + rulesIn + " " + spec + " :where " + strings.Join(body, " ") + "]"
	n := 1
	if find2 != "" {
		n = 2
	}
	var bound []string
	for i := 0; i < n; i++ {
		bound = append(bound, d.pick(vars))
	}
	var bind string
	switch {
	case n == 1 && d.r.Intn(3) == 0:
		bind = bound[0]
	case n == 1 && d.r.Intn(2) == 0:
		bind = "[" + bound[0] + " ...]"
	default:
		bind = "[[" + strings.Join(bound, " ") + "]" + d.pick([]string{"", " ..."}) + "]"
	}
	return "[(q " + inner + " $" + rulesArg + " " + strings.Join(args, " ") + ") " + bind + "]"
}

// branch draws one or two clauses, under and when two or half the time.
func (d *Draw) branch(depth int) string {
	n := 1 + d.r.Intn(2)
	var clauses []string
	for i := 0; i < n; i++ {
		clauses = append(clauses, d.clause(depth+1))
	}
	if n == 1 && d.r.Intn(2) == 0 {
		return clauses[0]
	}
	return "(and " + strings.Join(clauses, " ") + ")"
}

// header draws an or-join or or-default-join header: required symbols in a
// nested vector when any, then output symbols, ?e alone when none.
func (d *Draw) header() string {
	var required, outputs []string
	for _, v := range vars {
		switch d.r.Intn(4) {
		case 0:
			required = append(required, v)
		case 1:
			outputs = append(outputs, v)
		}
	}
	if len(required) == 0 && len(outputs) == 0 {
		outputs = []string{"?e"}
	}
	if len(required) == 0 {
		return "[" + strings.Join(outputs, " ") + "]"
	}
	s := "[[" + strings.Join(required, " ") + "]"
	if len(outputs) > 0 {
		s += " " + strings.Join(outputs, " ")
	}
	return s + "]"
}

// clause draws one clause; past depth two, only a clause holding no nested
// form.
func (d *Draw) clause(depth int) string {
	k := d.r.Intn(14)
	if depth >= 2 && k >= 6 {
		k = d.r.Intn(6)
	}
	switch k {
	case 0, 1, 2:
		return d.pattern()
	case 3:
		return d.predicate()
	case 4:
		return d.expression()
	case 5:
		return "(r " + d.pick(vars) + " " + d.term() + ")"
	case 6:
		return "(not " + d.clause(depth+1) + ")"
	case 7:
		return "(not-join [" + d.pick(vars) + "] " + d.clause(depth+1) + ")"
	case 8:
		return "(or " + d.branch(depth) + " " + d.branch(depth) + ")"
	case 9:
		return "(or-join " + d.header() + " " + d.branch(depth) + " " + d.branch(depth) + ")"
	case 10:
		return "(or-default " + d.branch(depth) + " " + d.branch(depth) + ")"
	case 11:
		return "(or-default-join " + d.header() + " " + d.branch(depth) + " " + d.branch(depth) + ")"
	case 12:
		return d.subquery(depth)
	default:
		return "(s " + d.pick(vars) + " " + d.pick(vars) + ")"
	}
}

// QueryText draws one query: rules half the time, up to two :in entries, one
// to four clauses, a find of one or two variables or a count, scalar one
// time in eight, an :order-by and a :limit one time in five each.
func (d *Draw) QueryText() string {
	in := []string{"$"}
	if d.r.Intn(2) == 0 {
		in = append(in, "%")
	}
	for i := 0; i < d.r.Intn(3); i++ {
		spec, _ := d.inSpec()
		in = append(in, spec)
	}
	var where []string
	for i := 0; i < 1+d.r.Intn(4); i++ {
		where = append(where, d.clause(0))
	}
	find := []string{d.pick(vars)}
	if d.r.Intn(2) == 0 {
		find = append(find, d.pick(vars))
	}
	if d.r.Intn(5) == 0 {
		find = []string{"(count " + d.pick(vars) + ")"}
	}
	scalar := ""
	if len(find) == 1 && d.r.Intn(8) == 0 {
		scalar = " ."
	}
	tail := ""
	if d.r.Intn(5) == 0 {
		tail += " :order-by [" + find[0] + " [" + d.pick(vars) + " :desc]]"
	}
	if d.r.Intn(5) == 0 {
		tail += " :limit " + strconv.Itoa(d.r.Intn(3))
	}
	return "[:find " + strings.Join(find, " ") + scalar + " :in " + strings.Join(in, " ") + " :where " + strings.Join(where, " ") + tail + "]"
}
