package storage

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// vectorOpKind names one of the writes a transaction makes to a vector.
type vectorOpKind int

const (
	vectorAdd vectorOpKind = iota
	vectorRemove
	vectorSet
	// vectorSave writes the vector through SaveStruct, whose struct writer
	// sets a vector field through Transaction.Set.
	vectorSave
)

// vectorOp is one write a transaction makes to a vector attribute.
type vectorOp struct {
	kind   vectorOpKind
	value  string   // the value an Add appends or a Remove withdraws
	values []string // the vector a Set or a SaveStruct writes
}

func vecAdd(v string) vectorOp      { return vectorOp{kind: vectorAdd, value: v} }
func vecRemove(v string) vectorOp   { return vectorOp{kind: vectorRemove, value: v} }
func vecSet(vs ...string) vectorOp  { return vectorOp{kind: vectorSet, values: vs} }
func vecSave(vs ...string) vectorOp { return vectorOp{kind: vectorSave, values: vs} }

func (op vectorOp) String() string {
	switch op.kind {
	case vectorAdd:
		return "add " + op.value
	case vectorRemove:
		return "remove " + op.value
	case vectorSet:
		return fmt.Sprintf("set %q", op.values)
	case vectorSave:
		return fmt.Sprintf("save %q", op.values)
	}
	panic(fmt.Sprintf("vector op kind %d", op.kind))
}

// vectorAttr is a vector attribute the tests write: a plain vector, or an
// ordered set, which declares UniqueElements, so an Add of a value it holds
// writes nothing.
type vectorAttr struct {
	kw     datalog.Keyword
	unique bool
}

// pullKey is the key a pull answers the attribute under.
func (a vectorAttr) pullKey() string {
	return strings.TrimPrefix(a.kw.String(), ":")
}

var (
	vectorWritesName  = datalog.NewKeyword(":vec/name")
	vectorWritesCells = vectorAttr{kw: datalog.NewKeyword(":vec/cells")}
	vectorWritesSet   = vectorAttr{kw: datalog.NewKeyword(":vec/set"), unique: true}
)

// vectorEntityName is the name a run gives an entity, which the join reader
// binds the entity through.
func vectorEntityName(e datalog.Identity) string {
	return "vector-writes " + e.L85()
}

// vectorWritesCellsStruct and vectorWritesSetStruct hold one vector each, for
// SaveStruct and PullInto.
type vectorWritesCellsStruct struct {
	ID    datalog.Identity `datalog:"-,id"`
	Cells []string         `datalog:"vec/cells"`
}

type vectorWritesSetStruct struct {
	ID  datalog.Identity `datalog:"-,id"`
	Set []string         `datalog:"vec/set"`
}

// vectorWritesSchema declares the entity's name, the plain vector and the
// ordered set.
func vectorWritesSchema(t testing.TB) schema.SchemaProvider {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(vectorWritesName.String()).Type(schema.TypeString).Add().
		Attribute(vectorWritesCells.kw.String()).Type(schema.TypeString).Vector().Add().
		Attribute(vectorWritesSet.kw.String()).Type(schema.TypeString).OrderedSet().Add().
		Build()
	require.NoError(t, err)
	return s
}

// writeVectorOp makes op's write to e's vector attr in tx.
func writeVectorOp(tx *Transaction, e datalog.Identity, attr vectorAttr, op vectorOp) error {
	switch op.kind {
	case vectorAdd:
		return tx.Add(e, attr.kw, op.value)
	case vectorRemove:
		return tx.Remove(e, attr.kw, op.value)
	case vectorSet:
		values := make([]interface{}, len(op.values))
		for i, v := range op.values {
			values[i] = v
		}
		return tx.Set(e, attr.kw, values)
	case vectorSave:
		// SaveStruct leaves a nil slice field as it is, so the vector it
		// writes is always a slice, the empty one included.
		cells := append([]string{}, op.values...)
		var record interface{} = &vectorWritesCellsStruct{ID: e, Cells: cells}
		if attr.unique {
			record = &vectorWritesSetStruct{ID: e, Set: cells}
		}
		saved, err := tx.SaveStruct(record)
		if err != nil {
			return err
		}
		if saved != e {
			return fmt.Errorf("SaveStruct wrote entity %s, not %s", saved, e)
		}
		return nil
	}
	panic(fmt.Sprintf("vector op kind %d", op.kind))
}

// vectorModel is a vector under a transaction's writes, computed without the
// store. Its elements are in order, each with the order it was added in,
// which is the order of the ElementIDs the writes draw.
type vectorModel struct {
	unique  bool
	elems   []vectorModelElem
	added   int
	written bool // some write has reached the vector
}

type vectorModelElem struct {
	value string
	added int
}

// apply makes op's write. An Add appends its value, unless the vector is an
// ordered set holding it. A Remove withdraws the element of its value added
// last. A Set or a SaveStruct keeps the prefix the vector shares with the new
// one and appends the rest of the new one.
func (m *vectorModel) apply(op vectorOp) {
	switch op.kind {
	case vectorAdd:
		if m.unique && slices.Contains(m.values(), op.value) {
			return
		}
		m.append(op.value)
	case vectorRemove:
		last := -1
		for i, el := range m.elems {
			if el.value == op.value && (last < 0 || el.added > m.elems[last].added) {
				last = i
			}
		}
		if last < 0 {
			return
		}
		m.elems = slices.Delete(m.elems, last, last+1)
		m.written = true
	case vectorSet, vectorSave:
		current := m.values()
		prefix := 0
		for prefix < len(current) && prefix < len(op.values) && current[prefix] == op.values[prefix] {
			prefix++
		}
		if prefix < len(current) {
			m.written = true
		}
		m.elems = m.elems[:prefix]
		for _, v := range op.values[prefix:] {
			m.append(v)
		}
	default:
		panic(fmt.Sprintf("vector op kind %d", op.kind))
	}
}

func (m *vectorModel) append(v string) {
	m.added++
	m.elems = append(m.elems, vectorModelElem{value: v, added: m.added})
	m.written = true
}

func (m *vectorModel) values() []string {
	values := make([]string, len(m.elems))
	for i, el := range m.elems {
		values[i] = el.value
	}
	return values
}

// answer is the vector the database is to answer: nil while no write has
// reached it, and its values in order after one, the empty vector included.
func (m *vectorModel) answer() []string {
	if !m.written {
		return nil
	}
	return m.values()
}

// vectorTarget is an entity's vector attribute a run of writes reaches.
type vectorTarget struct {
	e    datalog.Identity
	attr vectorAttr
}

// vectorWrite is one write of a transaction to one of a run's targets.
type vectorWrite struct {
	target int
	op     vectorOp
}

func (w vectorWrite) String() string {
	return fmt.Sprintf("target %d %s", w.target, w.op)
}

// modelAnswers is each target's vector after each of txs, every target
// written from none.
func modelAnswers(targets []vectorTarget, txs [][]vectorWrite) [][][]string {
	models := make([]vectorModel, len(targets))
	for i, target := range targets {
		models[i].unique = target.attr.unique
	}
	answers := make([][][]string, len(txs))
	for k, writes := range txs {
		for _, w := range writes {
			models[w.target].apply(w.op)
		}
		answers[k] = make([][]string, len(targets))
		for i := range models {
			answers[k][i] = models[i].answer()
		}
	}
	return answers
}

// vectorReader is one way the database answers an entity's vector: nil where
// the entity holds none, and its values in order otherwise. tellsAbsence is
// false for a reader whose answer for an absent vector is the empty vector's.
type vectorReader struct {
	name         string
	tellsAbsence bool
	read         func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error)
}

// vectorReaders reach each path that resolves a vector: the pattern matcher's
// arms for a given, a joined and an unbound entity, the streaming resolution
// of a pattern whose attribute is a variable, the lookup behind get-else, the
// pull walk, and the cache's entries behind LookupAttribute and the vector
// accessors.
var vectorReaders = []vectorReader{
	{"query, the entity an input", true,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			return vectorOfOneTuple(d.Query(fmt.Sprintf(`[:find ?v :in $ ?e :where [?e %s ?v]]`, attr.kw), e))
		}},
	{"query, the entity a constant", true,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			return vectorOfOneTuple(d.Query(fmt.Sprintf(`[:find ?v :where [#identity %q %s ?v]]`, e.L85(), attr.kw)))
		}},
	{"query, the entity bound by a join", true,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			return vectorOfOneTuple(d.Query(fmt.Sprintf(`[:find ?v :where [?e %s %q] [?e %s ?v]]`,
				vectorWritesName, vectorEntityName(e), attr.kw)))
		}},
	{"query, the entity unbound", true,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			tuples, err := executor.CollectTuples(d.Query(fmt.Sprintf(`[:find ?e ?v :where [?e %s ?v]]`, attr.kw)))
			return vectorOfTupleWith(e, tuples, err)
		}},
	{"query, the attribute a variable", true,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			tuples, err := executor.CollectTuples(d.Query(`[:find ?a ?v :in $ ?e :where [?e ?a ?v]]`, e))
			return vectorOfTupleWith(attr.kw, tuples, err)
		}},
	{"query, get-else", true,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			got, err := vectorOfOneTuple(d.Query(fmt.Sprintf(
				`[:find ?v :in $ ?e :where [(get-else $ ?e %s ["absent"]) ?v]]`, attr.kw), e))
			if err != nil || !slices.Equal(got, []string{"absent"}) {
				return got, err
			}
			return nil, nil
		}},
	{"query, pull", true,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			tuples, err := executor.CollectTuples(d.Query(fmt.Sprintf(
				`[:find (pull ?e [%s]) :in $ ?e :where [?e %s _]]`, attr.kw, vectorWritesName), e))
			if err != nil {
				return nil, err
			}
			if len(tuples) != 1 {
				return nil, fmt.Errorf("%d tuples, where the entity's pull is one", len(tuples))
			}
			pulled, ok := tuples[0][0].(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("the pull answers %T", tuples[0][0])
			}
			return vectorOfPull(pulled, attr)
		}},
	{"Pull of the attribute", true,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			pulled, err := d.Pull(e, fmt.Sprintf("[%s]", attr.kw))
			if err != nil {
				return nil, err
			}
			return vectorOfPull(pulled, attr)
		}},
	{"Pull of every attribute", true,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			pulled, err := d.Pull(e, "[*]")
			if err != nil {
				return nil, err
			}
			return vectorOfPull(pulled, attr)
		}},
	{"PullMany of the attribute", true,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			pulled, err := d.PullMany([]datalog.Identity{e}, fmt.Sprintf("[%s]", attr.kw))
			if err != nil {
				return nil, err
			}
			if len(pulled) != 1 {
				return nil, fmt.Errorf("%d pulls of one entity", len(pulled))
			}
			return vectorOfPull(pulled[0], attr)
		}},
	{"PullInto", false,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			if attr.unique {
				var pulled vectorWritesSetStruct
				err := d.PullInto(e, &pulled)
				return pulled.Set, err
			}
			var pulled vectorWritesCellsStruct
			err := d.PullInto(e, &pulled)
			return pulled.Cells, err
		}},
	{"ResolveAllAttributes", true,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			resolved, err := d.ResolveAllAttributes(e)
			return vectorOfResolved(attr, resolved, err)
		}},
	{"ResolveEntityAttributes", true,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			resolved, err := d.ResolveEntityAttributes(e, []datalog.Keyword{attr.kw})
			return vectorOfResolved(attr, resolved, err)
		}},
	{"LookupAttribute", true,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			v, found, err := d.LookupAttribute(e, attr.kw)
			if err != nil || !found {
				return nil, err
			}
			return stringsOfVector(v)
		}},
	{"GetVectorLength and GetVectorNth", false,
		func(d *Database, e datalog.Identity, attr vectorAttr) ([]string, error) {
			n, err := d.GetVectorLength(e, attr.kw)
			if err != nil {
				return nil, err
			}
			values := make([]string, 0, n)
			for i := int64(0); i < n; i++ {
				v, err := d.GetVectorNth(e, attr.kw, i)
				if err != nil {
					return nil, err
				}
				s, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("element %d is %T", i, v)
				}
				values = append(values, s)
			}
			return values, nil
		}},
}

// stringsOfVector reads a resolved vector as its strings. A reader answers
// []string where it types the vector by the schema, and []interface{} where
// it does not.
func stringsOfVector(v interface{}) ([]string, error) {
	switch vec := v.(type) {
	case []string:
		return append([]string{}, vec...), nil
	case []interface{}:
		values := make([]string, len(vec))
		for i, el := range vec {
			s, ok := el.(string)
			if !ok {
				return nil, fmt.Errorf("vector element %d is %T", i, el)
			}
			values[i] = s
		}
		return values, nil
	}
	return nil, fmt.Errorf("the vector is %T", v)
}

// vectorOfOneTuple reads a query's answer of one symbol, the vector: no tuple
// where the vector is absent, and one holding it otherwise.
func vectorOfOneTuple(rel executor.Relation, err error) ([]string, error) {
	tuples, err := executor.CollectTuples(rel, err)
	if err != nil {
		return nil, err
	}
	switch len(tuples) {
	case 0:
		return nil, nil
	case 1:
		return stringsOfVector(tuples[0][0])
	}
	return nil, fmt.Errorf("%d tuples, where a vector is one", len(tuples))
}

// vectorOfTupleWith reads a query's answer of two symbols, a key and the
// vector: no tuple holding the key where the vector is absent, and one
// otherwise.
func vectorOfTupleWith(key interface{}, tuples [][]interface{}, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	var matched [][]interface{}
	for _, tuple := range tuples {
		if tuple[0] == key {
			matched = append(matched, tuple)
		}
	}
	switch len(matched) {
	case 0:
		return nil, nil
	case 1:
		return stringsOfVector(matched[0][1])
	}
	return nil, fmt.Errorf("%d tuples hold %v, where a vector is one", len(matched), key)
}

// vectorOfPull reads the attribute from a pull's answer: no vector where the
// answer holds no key for it.
func vectorOfPull(pulled map[string]interface{}, attr vectorAttr) ([]string, error) {
	v, ok := pulled[attr.pullKey()]
	if !ok {
		return nil, nil
	}
	return stringsOfVector(v)
}

// vectorOfResolved reads the attribute from a resolution of an entity's
// attributes: no vector where the resolution holds no entry for it.
func vectorOfResolved(attr vectorAttr, resolved map[datalog.Keyword]interface{}, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	v, ok := resolved[attr.kw]
	if !ok {
		return nil, nil
	}
	return stringsOfVector(v)
}

// vectorAnswersEqual compares a reader's answer with the vector the database
// holds, an absent vector with an empty one only where the reader cannot tell
// the two apart.
func vectorAnswersEqual(want, got []string, tellsAbsence bool) bool {
	if tellsAbsence && (want == nil) != (got == nil) {
		return false
	}
	return slices.Equal(want, got)
}

func describeVector(v []string) string {
	if v == nil {
		return "no vector"
	}
	return fmt.Sprintf("%q", v)
}

// checkVectorReaders reads e's vector through every reader of view and
// reports each answer other than want. With the cache on it reads again,
// readers in reverse order, after clearing the cache, so each reader also
// reads an entry another reader built. Then it clears the cache, fills it
// through WarmCache, and reads once more.
func checkVectorReaders(t *testing.T, at string, view *Database, e datalog.Identity, attr vectorAttr, want []string) {
	t.Helper()
	read := func(pass string, readers []vectorReader) {
		for _, r := range readers {
			got, err := r.read(view, e, attr)
			if err != nil {
				t.Errorf("%s, %s, %s: %v", at, pass, r.name, err)
				continue
			}
			if !vectorAnswersEqual(want, got, r.tellsAbsence) {
				t.Errorf("%s, %s, %s: answers %s, where the vector is %s",
					at, pass, r.name, describeVector(got), describeVector(want))
			}
		}
	}
	read("first reads", vectorReaders)
	if cache := view.Cache(); cache != nil {
		cache.Clear()
		reversed := slices.Clone(vectorReaders)
		slices.Reverse(reversed)
		read("reads after clearing the cache", reversed)
		cache.Clear()
		require.NoError(t, view.WarmCache([]datalog.Keyword{attr.kw}), "%s, warming the cache", at)
		read("reads after warming the cache", vectorReaders)
	}
}

// checkVectorRun writes txs to the targets, reads every target's vector back
// through every reader after each commit, and reads each commit's vectors
// again as of it once every transaction has committed. The first transaction
// also names each target's entity.
func checkVectorRun(t *testing.T, d *Database, targets []vectorTarget, txs [][]vectorWrite, want [][][]string) {
	t.Helper()
	commits := make([]datalog.ElementID, len(txs))
	for k, writes := range txs {
		tx := d.NewTransaction()
		if k == 0 {
			named := map[datalog.Identity]bool{}
			for _, target := range targets {
				if !named[target.e] {
					named[target.e] = true
					require.NoError(t, tx.Add(target.e, vectorWritesName, vectorEntityName(target.e)))
				}
			}
		}
		for _, w := range writes {
			target := targets[w.target]
			require.NoError(t, writeVectorOp(tx, target.e, target.attr, w.op), "transaction %d, %s", k, w)
		}
		id, err := tx.Commit()
		require.NoError(t, err, "transaction %d", k)
		commits[k] = id
		for i, target := range targets {
			checkVectorReaders(t, fmt.Sprintf("after transaction %d, target %d", k, i), d, target.e, target.attr, want[k][i])
		}
	}
	for k, id := range commits {
		view := d.AsOf(id)
		for i, target := range targets {
			checkVectorReaders(t, fmt.Sprintf("as of transaction %d, target %d", k, i), view, target.e, target.attr, want[k][i])
		}
	}
}

// vectorWritesCaches are the cache settings each run of writes is read back
// under.
var vectorWritesCaches = []struct {
	name    string
	disable bool
}{{"cache_on", false}, {"cache_off", true}}

// vectorScenario is a run of transactions writing one entity's vector, with
// the vector the database is to answer after each: nil where no write has
// reached it, and its values in order otherwise, the empty vector included.
type vectorScenario struct {
	name string
	attr vectorAttr
	txs  [][]vectorOp
	want [][]string
}

// run is the scenario's writes as a run of one target.
func (sc vectorScenario) run() ([]vectorTarget, [][]vectorWrite) {
	targets := []vectorTarget{{e: datalog.NewIdentity("vector-writes:entity"), attr: sc.attr}}
	txs := make([][]vectorWrite, len(sc.txs))
	for k, ops := range sc.txs {
		for _, op := range ops {
			txs[k] = append(txs[k], vectorWrite{op: op})
		}
	}
	return targets, txs
}

var vectorWritesScenarios = []vectorScenario{
	{"remove the last added, in one transaction", vectorWritesCells,
		[][]vectorOp{{vecAdd("a"), vecAdd("b"), vecAdd("a"), vecRemove("a")}},
		[][]string{{"a", "b"}}},
	{"remove an element the transaction added, beside a committed equal value", vectorWritesCells,
		[][]vectorOp{{vecAdd("a")}, {vecAdd("b"), vecAdd("a"), vecRemove("a")}},
		[][]string{{"a"}, {"a", "b"}}},
	{"remove twice in one transaction", vectorWritesCells,
		[][]vectorOp{{vecAdd("a"), vecAdd("b"), vecAdd("a"), vecRemove("a"), vecRemove("a")}},
		[][]string{{"b"}}},
	{"add after removing the last added, in one transaction", vectorWritesCells,
		[][]vectorOp{{vecAdd("a"), vecAdd("b"), vecRemove("b"), vecAdd("c")}},
		[][]string{{"a", "c"}}},
	{"remove after a set, in one transaction", vectorWritesCells,
		[][]vectorOp{{vecAdd("a")}, {vecSet("a", "b"), vecRemove("b")}},
		[][]string{{"a"}, {"a"}}},
	{"remove after a save, in one transaction", vectorWritesCells,
		[][]vectorOp{{vecAdd("a")}, {vecSave("a", "b"), vecRemove("b")}},
		[][]string{{"a"}, {"a"}}},
	{"remove a committed element whose successors stay", vectorWritesCells,
		[][]vectorOp{{vecAdd("a"), vecAdd("b"), vecAdd("c")}, {vecRemove("b")}},
		[][]string{{"a", "b", "c"}, {"a", "c"}}},
	{"remove the last added of committed equal values", vectorWritesCells,
		[][]vectorOp{{vecAdd("a"), vecAdd("b"), vecAdd("a")}, {vecRemove("a")}},
		[][]string{{"a", "b", "a"}, {"a", "b"}}},
	{"remove again after a commit removed the last added", vectorWritesCells,
		[][]vectorOp{{vecAdd("a"), vecAdd("b"), vecAdd("a")}, {vecRemove("a")}, {vecRemove("a")}},
		[][]string{{"a", "b", "a"}, {"a", "b"}, {"b"}}},
	{"remove twice in one transaction, of committed equal values", vectorWritesCells,
		[][]vectorOp{{vecAdd("a"), vecAdd("b"), vecAdd("a")}, {vecRemove("a"), vecRemove("a")}},
		[][]string{{"a", "b", "a"}, {"b"}}},
	{"remove twice in one transaction, its own element and then a committed one", vectorWritesCells,
		[][]vectorOp{{vecAdd("a"), vecAdd("b")}, {vecAdd("a"), vecRemove("a"), vecRemove("a")}},
		[][]string{{"a", "b"}, {"b"}}},
	{"remove a value the vector does not hold", vectorWritesCells,
		[][]vectorOp{{vecAdd("a")}, {vecRemove("z")}},
		[][]string{{"a"}, {"a"}}},
	{"remove the only element", vectorWritesCells,
		[][]vectorOp{{vecAdd("a")}, {vecRemove("a")}},
		[][]string{{"a"}, {}}},
	{"remove from a vector never written", vectorWritesCells,
		[][]vectorOp{{vecRemove("a")}},
		[][]string{nil}},
	{"set after an add, in one transaction", vectorWritesCells,
		[][]vectorOp{{vecAdd("a")}, {vecAdd("b"), vecSet("a", "b", "c")}},
		[][]string{{"a"}, {"a", "b", "c"}}},
	{"save after an add, in one transaction", vectorWritesCells,
		[][]vectorOp{{vecAdd("a")}, {vecAdd("b"), vecSave("a", "b", "c")}},
		[][]string{{"a"}, {"a", "b", "c"}}},
	{"set after a set, in one transaction", vectorWritesCells,
		[][]vectorOp{{vecAdd("a")}, {vecSet("a", "b"), vecSet("a", "c")}},
		[][]string{{"a"}, {"a", "c"}}},
	{"set after a remove, in one transaction", vectorWritesCells,
		[][]vectorOp{{vecAdd("a"), vecAdd("b")}, {vecRemove("b"), vecSet("a", "b")}},
		[][]string{{"a", "b"}, {"a", "b"}}},
	{"add after a set, in one transaction", vectorWritesCells,
		[][]vectorOp{{vecAdd("a")}, {vecSet("x"), vecAdd("y")}},
		[][]string{{"a"}, {"x", "y"}}},
	{"add after the first write sets the vector", vectorWritesCells,
		[][]vectorOp{{vecSet("a", "b"), vecAdd("c")}},
		[][]string{{"a", "b", "c"}}},
	{"set to empty", vectorWritesCells,
		[][]vectorOp{{vecAdd("a"), vecAdd("b")}, {vecSet()}},
		[][]string{{"a", "b"}, {}}},
	{"set to empty a vector never written", vectorWritesCells,
		[][]vectorOp{{vecSet()}},
		[][]string{nil}},
	{"ordered set, add a value the transaction added", vectorWritesSet,
		[][]vectorOp{{vecAdd("a"), vecAdd("a")}},
		[][]string{{"a"}}},
	{"ordered set, add a value a commit added", vectorWritesSet,
		[][]vectorOp{{vecAdd("a")}, {vecAdd("a")}},
		[][]string{{"a"}, {"a"}}},
	{"ordered set, add a value after removing its committed element", vectorWritesSet,
		[][]vectorOp{{vecAdd("a")}, {vecRemove("a"), vecAdd("a")}},
		[][]string{{"a"}, {"a"}}},
	{"ordered set, add a value after removing the element the transaction added", vectorWritesSet,
		[][]vectorOp{{vecAdd("a"), vecRemove("a"), vecAdd("a")}},
		[][]string{{"a"}}},
	{"ordered set, add a value after a set left it out", vectorWritesSet,
		[][]vectorOp{{vecAdd("a"), vecAdd("b")}, {vecSet("b"), vecAdd("a")}},
		[][]string{{"a", "b"}, {"b", "a"}}},
	{"ordered set, add a value a set in the transaction wrote", vectorWritesSet,
		[][]vectorOp{{vecAdd("a")}, {vecSet("a", "b"), vecAdd("b")}},
		[][]string{{"a"}, {"a", "b"}}},
	{"ordered set, add a value a commit removed", vectorWritesSet,
		[][]vectorOp{{vecAdd("a")}, {vecRemove("a")}, {vecAdd("a")}},
		[][]string{{"a"}, {}, {"a"}}},
}

// TestVectorWritesReadBackAlikeThroughEveryReader writes each scenario's
// transactions to one entity and reads its vector back through every reader:
// after each commit on the latest view, and as of each commit once all have
// committed, with the cache on and off. Every reader answers the vector the
// scenario states, and the reference model computes the same vectors.
func TestVectorWritesReadBackAlikeThroughEveryReader(t *testing.T) {
	s := vectorWritesSchema(t)
	for _, sc := range vectorWritesScenarios {
		t.Run(sc.name, func(t *testing.T) {
			targets, txs := sc.run()
			want := modelAnswers(targets, txs)
			for k := range sc.want {
				require.Equal(t, sc.want[k], want[k][0],
					"the reference model computes the scenario's vector after transaction %d", k)
			}
			for _, mode := range optimizerModes {
				for _, cache := range vectorWritesCaches {
					t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
						d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: cache.disable})
						checkVectorRun(t, d, targets, txs, want)
					})
				}
			}
		})
	}
}

// concurrentStepKind is what one step of a run of transactions open at once
// does.
type concurrentStepKind int

const (
	// concurrentBeginStep opens the step's transaction. A transaction also
	// opens at its first write, so a begin step opens one earlier.
	concurrentBeginStep concurrentStepKind = iota
	// concurrentWriteStep makes the step's write in its transaction.
	concurrentWriteStep
	// concurrentCommitStep commits the step's transaction, after which the
	// latest view's vector is the step's want.
	concurrentCommitStep
)

// concurrentStep is one step of a run of transactions open at once on one
// database, taken by transaction tx.
type concurrentStep struct {
	tx   int
	kind concurrentStepKind
	op   vectorOp
	want []string
}

func concurrentBegin(tx int) concurrentStep {
	return concurrentStep{tx: tx, kind: concurrentBeginStep}
}

func concurrentWrite(tx int, op vectorOp) concurrentStep {
	return concurrentStep{tx: tx, kind: concurrentWriteStep, op: op}
}

func concurrentCommit(tx int, want ...string) concurrentStep {
	return concurrentStep{tx: tx, kind: concurrentCommitStep, want: want}
}

// TestVectorConcurrentTransactionsReadBackAlikeThroughEveryReader writes one
// entity's vector from transactions open at once on one database. It reads the
// vector back through every reader after each commit on the latest view, and
// as of each commit once all have committed, with the cache on and off. Each
// vector a case states is the one its datoms resolve to: an element follows
// the element its insert names, the elements following one element are in
// ElementID order, and a withdrawn element keeps its place. A transaction
// writes against the vector its read session opened on, with its own writes
// applied, so an element another transaction commits after that is one it
// never saw.
func TestVectorConcurrentTransactionsReadBackAlikeThroughEveryReader(t *testing.T) {
	s := vectorWritesSchema(t)
	cases := []struct {
		name  string
		attr  vectorAttr
		steps []concurrentStep
		asOf  [][]string // the vector as of each commit, in the order of the commits
	}{
		{
			// x is drawn between a and b and follows HEAD, so it comes after
			// a and the elements that follow a: [a b c x]. b's withdrawal
			// keeps its place, so c stays before x. As of the first commit
			// the view holds x, whose ElementID is below that commit's.
			name: "a withdrawn element keeps its place before a concurrent transaction's element",
			attr: vectorWritesCells,
			steps: []concurrentStep{
				concurrentWrite(0, vecAdd("a")),
				concurrentWrite(1, vecAdd("x")),
				concurrentWrite(0, vecAdd("b")),
				concurrentWrite(0, vecAdd("c")),
				concurrentCommit(0, "a", "b", "c"),
				concurrentCommit(1, "a", "b", "c", "x"),
				concurrentWrite(2, vecRemove("b")),
				concurrentCommit(2, "a", "c", "x"),
			},
			asOf: [][]string{{"a", "b", "c", "x"}, {"a", "b", "c", "x"}, {"a", "c", "x"}},
		},
		{
			// Transaction 0 began before transaction 1 committed x, so it
			// holds [y], its own element alone. Its Set of [x y] withdraws y
			// and writes x and y after HEAD, drawn after transaction 1's x,
			// which comes first: [x x y]. As of the first commit the view
			// holds y and x, whose ElementIDs are below that commit's, y
			// first by ElementID: [y x].
			name: "a set leaves a concurrent transaction's element it never saw",
			attr: vectorWritesCells,
			steps: []concurrentStep{
				concurrentWrite(0, vecAdd("y")),
				concurrentWrite(1, vecAdd("x")),
				concurrentCommit(1, "x"),
				concurrentWrite(0, vecSet("x", "y")),
				concurrentCommit(0, "x", "x", "y"),
			},
			asOf: [][]string{{"y", "x"}, {"x", "x", "y"}},
		},
		{
			// The case above, written through SaveStruct.
			name: "a save leaves a concurrent transaction's element it never saw",
			attr: vectorWritesCells,
			steps: []concurrentStep{
				concurrentWrite(0, vecAdd("y")),
				concurrentWrite(1, vecAdd("x")),
				concurrentCommit(1, "x"),
				concurrentWrite(0, vecSave("x", "y")),
				concurrentCommit(0, "x", "x", "y"),
			},
			asOf: [][]string{{"y", "x"}, {"x", "x", "y"}},
		},
		{
			// Both x's follow HEAD. Transaction 0 began before transaction 1
			// committed its x, so the newest x transaction 0 holds is its own,
			// and the Remove takes it. b keeps its place after that x, and
			// transaction 1's x, drawn after both, follows them: [b x]. As of
			// the first commit the view holds transaction 0's x and b, whose
			// ElementIDs are below that commit's: [x b x].
			name: "a remove takes its own element of the value, not one a concurrent transaction committed",
			attr: vectorWritesCells,
			steps: []concurrentStep{
				concurrentWrite(0, vecAdd("x")),
				concurrentWrite(0, vecAdd("b")),
				concurrentWrite(1, vecAdd("x")),
				concurrentCommit(1, "x"),
				concurrentWrite(0, vecRemove("x")),
				concurrentCommit(0, "b", "x"),
			},
			asOf: [][]string{{"x", "b", "x"}, {"b", "x"}},
		},
		{
			// Transaction 0 began before transaction 1 committed x, so it
			// holds no element of x, and its Remove writes nothing: [x].
			name: "a remove of a value only a concurrent transaction committed writes nothing",
			attr: vectorWritesCells,
			steps: []concurrentStep{
				concurrentBegin(0),
				concurrentWrite(1, vecAdd("x")),
				concurrentCommit(1, "x"),
				concurrentWrite(0, vecRemove("x")),
				concurrentCommit(0, "x"),
			},
			asOf: [][]string{{"x"}, {"x"}},
		},
		{
			// Transaction 0 began before transaction 1 committed x and has
			// written nothing to the vector, so it holds the empty vector, and
			// its Set of [y] writes y after HEAD, drawn after x: [x y].
			name: "a set by a transaction holding no element keeps a concurrent transaction's element",
			attr: vectorWritesCells,
			steps: []concurrentStep{
				concurrentBegin(0),
				concurrentWrite(1, vecAdd("x")),
				concurrentCommit(1, "x"),
				concurrentWrite(0, vecSet("y")),
				concurrentCommit(0, "x", "y"),
			},
			asOf: [][]string{{"x"}, {"x", "y"}},
		},
		{
			// Transaction 0 began before transaction 1 committed x, so its
			// ordered set holds no x, and its Add writes one after HEAD, drawn
			// after transaction 1's: [x x], the merge of two writers adding
			// one value at once.
			name: "an ordered set's add writes a value a concurrent transaction committed",
			attr: vectorWritesSet,
			steps: []concurrentStep{
				concurrentBegin(0),
				concurrentWrite(1, vecAdd("x")),
				concurrentCommit(1, "x"),
				concurrentWrite(0, vecAdd("x")),
				concurrentCommit(0, "x", "x"),
			},
			asOf: [][]string{{"x"}, {"x", "x"}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, mode := range optimizerModes {
				for _, cache := range vectorWritesCaches {
					t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
						d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, DisableCache: cache.disable})
						e := datalog.NewIdentity("vector-concurrent:entity")
						naming := d.NewTransaction()
						require.NoError(t, naming.Add(e, vectorWritesName, vectorEntityName(e)))
						_, err := naming.Commit()
						require.NoError(t, err)
						txs := map[int]*Transaction{}
						var commits []datalog.ElementID
						for i, step := range c.steps {
							tx, open := txs[step.tx]
							if !open {
								tx = d.NewTransaction()
								txs[step.tx] = tx
							}
							switch step.kind {
							case concurrentBeginStep:
								// Opening the transaction above is the whole step.
							case concurrentWriteStep:
								require.NoError(t, writeVectorOp(tx, e, c.attr, step.op), "step %d, transaction %d, %s", i, step.tx, step.op)
							case concurrentCommitStep:
								id, err := tx.Commit()
								require.NoError(t, err, "step %d, the commit of transaction %d", i, step.tx)
								commits = append(commits, id)
								checkVectorReaders(t, fmt.Sprintf("after step %d, the commit of transaction %d", i, step.tx), d, e, c.attr, step.want)
							}
						}
						require.Len(t, commits, len(c.asOf), "the case states the vector as of each commit")
						for k, id := range commits {
							checkVectorReaders(t, fmt.Sprintf("as of commit %d", k), d.AsOf(id), e, c.attr, c.asOf[k])
						}
					})
				}
			}
		})
	}
}
