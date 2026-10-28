//go:build !(js && wasm)

package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// smokeMergeKeys is the collection input the merge join serves. The matcher
// estimates a constant attribute's extent as 10,000 datoms
// (estimatePatternCardinality) and takes the merge join for a binding past
// executor.SmallBindingBoundary that is at least half of that estimate, so
// the input binds 5,000 entities.
const smokeMergeKeys = 5 * executor.SmallBindingBoundary

// smokeExtent is how many entities carry :smoke/n: every entity the merge
// join binds, and some it does not.
const smokeExtent = smokeMergeKeys + 10

// smokeTags are :smoke/tag's values, shared among the entities in turn.
var smokeTags = [...]string{"folio", "octavo", "quarto"}

// smokeFixture is what the shapes run against: smokeExtent entities carrying
// :smoke/n and :smoke/tag, the first five a unique :smoke/code, the first
// four a :smoke/name that is a prefix of another's or equal to it; and a
// subject whose :smoke/name was set twice, whose :smoke/tag lost a member,
// and which carries the vector and the blob-tier value.
type smokeFixture struct {
	entities []datalog.Identity
	subject  datalog.Identity
	// first is the commit before the subject's name changed and its tag was
	// removed.
	first datalog.ElementID
}

// smokeSchema declares the fixture's attributes: :smoke/blob stays undeclared,
// so the bytes value's type is read from the datom.
func smokeSchema(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":smoke/n").Type(schema.TypeString).One().Add().
		Attribute(":smoke/tag").Type(schema.TypeString).Many().Add().
		Attribute(":smoke/name").Type(schema.TypeString).One().Add().
		Attribute(":smoke/code").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Attribute(":smoke/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	return s
}

// writeSmokeFixture writes the fixture to d in two commits and returns it.
func writeSmokeFixture(t *testing.T, d *Database) smokeFixture {
	t.Helper()
	n := datalog.NewKeyword(":smoke/n")
	tag := datalog.NewKeyword(":smoke/tag")
	name := datalog.NewKeyword(":smoke/name")
	code := datalog.NewKeyword(":smoke/code")
	skills := datalog.NewKeyword(":smoke/skills")
	blob := datalog.NewKeyword(":smoke/blob")
	f := smokeFixture{subject: datalog.NewIdentity("smoke-subject")}

	tx := d.NewTransaction()
	for i := 0; i < smokeExtent; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("smoke-%d", i))
		require.NoError(t, tx.Add(e, n, fmt.Sprintf("n-%d", i)))
		require.NoError(t, tx.Add(e, tag, smokeTags[i%len(smokeTags)]))
		f.entities = append(f.entities, e)
	}
	for i := 0; i < 5; i++ {
		require.NoError(t, tx.Add(f.entities[i], code, fmt.Sprintf("code-%d", i)))
	}
	for i, prefixName := range []string{"ab", "abc", "abcd", "ab"} {
		require.NoError(t, tx.Add(f.entities[i], name, prefixName))
	}
	require.NoError(t, tx.Add(f.subject, name, "first"))
	require.NoError(t, tx.Add(f.subject, tag, "folio"))
	require.NoError(t, tx.Add(f.subject, tag, "octavo"))
	for _, skill := range []string{"stealth", "archery", "lockpicking"} {
		require.NoError(t, tx.Add(f.subject, skills, skill))
	}
	require.NoError(t, tx.Add(f.subject, blob, makeTier3Data(100000)))
	first, err := tx.Commit()
	require.NoError(t, err)
	f.first = first

	tx = d.NewTransaction()
	require.NoError(t, tx.Set(f.subject, name, "second"))
	require.NoError(t, tx.Remove(f.subject, tag, "folio"))
	_, err = tx.Commit()
	require.NoError(t, err)
	return f
}

// smokeShape is one query the engine routes through the store.
type smokeShape struct {
	name string
	// view is the handle the query runs on; nil runs it on the database.
	view   func(d *Database, f smokeFixture) *Database
	text   string
	inputs func(f smokeFixture) []interface{}
	// strategy is the binding-driven path the shape is served by, required
	// through its completion event; "" for a shape no binding drives.
	strategy annotations.ScanStrategy
}

func subjectInput(f smokeFixture) []interface{} {
	return []interface{}{f.subject}
}

// entityCollection binds the first keys entities as one collection input.
func entityCollection(keys int) func(f smokeFixture) []interface{} {
	return func(f smokeFixture) []interface{} {
		members := make([]interface{}, keys)
		for i, e := range f.entities[:keys] {
			members[i] = e
		}
		return []interface{}{members}
	}
}

func asOfFirst(d *Database, f smokeFixture) *Database { return d.AsOf(f.first) }

func history(d *Database, f smokeFixture) *Database { return d.History() }

// The shapes answering thousands of tuples order them by entity in the query,
// so the two sides of a comparison arrive in one order.
var smokeShapes = []smokeShape{
	{name: "unbound scan",
		text: `[:find ?e ?v :where [?e :smoke/n ?v] :order-by [[?e :asc]]]`},
	{name: "entity bound",
		text: `[:find ?a ?v :in $ ?e :where [?e ?a ?v]]`, inputs: subjectInput},
	{name: "value bound among prefixes",
		text: `[:find ?e :where [?e :smoke/name "ab"]]`},
	{name: "collection input at the boundary",
		text:   `[:find ?e ?v :in $ [?e ...] :where [?e :smoke/n ?v] :order-by [[?e :asc]]]`,
		inputs: entityCollection(executor.SmallBindingBoundary), strategy: annotations.ScanPerKeySeek},
	{name: "collection input past the boundary",
		text:   `[:find ?e ?v :in $ [?e ...] :where [?e :smoke/n ?v] :order-by [[?e :asc]]]`,
		inputs: entityCollection(executor.SmallBindingBoundary + 1), strategy: annotations.ScanHashJoin},
	{name: "collection input of half the estimated extent",
		text:   `[:find ?e ?v :in $ [?e ...] :where [?e :smoke/n ?v] :order-by [[?e :asc]]]`,
		inputs: entityCollection(smokeMergeKeys), strategy: annotations.ScanMergeJoin},
	{name: "one resolution",
		text: `[:find ?v :in $ ?e :where [?e :smoke/name ?v]]`, inputs: subjectInput},
	{name: "many resolution",
		text: `[:find ?v :in $ ?e :where [?e :smoke/tag ?v]]`, inputs: subjectInput},
	{name: "vector resolution",
		text: `[:find ?v :in $ ?e :where [?e :smoke/skills ?v]]`, inputs: subjectInput},
	{name: "unique lookup",
		text: `[:find ?e :where [?e :smoke/code "code-3"]]`},
	{name: "blob-tier value",
		text: `[:find ?v :in $ ?e :where [?e :smoke/blob ?v]]`, inputs: subjectInput},
	{name: "one resolution as of the first commit", view: asOfFirst,
		text: `[:find ?v :in $ ?e :where [?e :smoke/name ?v]]`, inputs: subjectInput},
	{name: "many resolution as of the first commit", view: asOfFirst,
		text: `[:find ?v :in $ ?e :where [?e :smoke/tag ?v]]`, inputs: subjectInput},
	{name: "history of a value set twice", view: history,
		text: `[:find ?v ?tx :in $ ?e :where [?e :smoke/name ?v ?tx]]`, inputs: subjectInput},
	{name: "history of a set that lost a member", view: history,
		text: `[:find ?v ?tx :in $ ?e :where [?e :smoke/tag ?v ?tx]]`, inputs: subjectInput},
}

// smokeAnswers is every shape's answer by name, with the subject's wildcard
// pull beside them.
type smokeAnswers struct {
	shapes map[string][][]interface{}
	pull   map[string]interface{}
}

// answerSmokeShapes runs every shape on d, requiring each binding-driven
// shape's completion event among the events d's handler appends to.
func answerSmokeShapes(t *testing.T, d *Database, f smokeFixture, events *[]annotations.Event) smokeAnswers {
	t.Helper()
	answers := smokeAnswers{shapes: make(map[string][][]interface{}, len(smokeShapes))}
	for _, shape := range smokeShapes {
		*events = nil
		view := d
		if shape.view != nil {
			view = shape.view(d, f)
		}
		var inputs []interface{}
		if shape.inputs != nil {
			inputs = shape.inputs(f)
		}
		got, err := executor.CollectTuples(view.Query(shape.text, inputs...))
		require.NoError(t, err, shape.name)
		require.NotEmpty(t, got, "%s answers nothing, so it reads nothing of the store", shape.name)
		if shape.strategy != "" {
			require.NotNil(t, lastScanComplete(*events, shape.strategy),
				"%s is served by the %s strategy, and no completion among %d events reports it",
				shape.name, shape.strategy, len(*events))
		}
		answers.shapes[shape.name] = got
	}
	pulled, err := d.Pull(f.subject, `[*]`)
	require.NoError(t, err)
	require.NotEmpty(t, pulled)
	answers.pull = pulled
	return answers
}

func requireSmokeAnswers(t *testing.T, want, got smokeAnswers, when string) {
	t.Helper()
	for _, shape := range smokeShapes {
		require.ElementsMatch(t, want.shapes[shape.name], got.shapes[shape.name],
			"%s: %s", when, shape.name)
	}
	require.Equal(t, want.pull, got.pull, "%s: the subject's wildcard pull", when)
}

// TestBadgerAnswersWhatTheTreeStoreAnswersAndAgainAfterReopen is the one
// Badger run of the query shapes the engine routes through the store. The
// engine axis runs on the tree store; this test pins that Badger, reading the
// same fixture, answers every shape as the tree store does, and that the
// store it persisted answers the same after a close and a reopen of its path.
// The cache is off so each shape reads the store, and attribute-fetch fusion
// is off so a collection input reaches the binding-driven paths, whose
// completion events are required.
func TestBadgerAnswersWhatTheTreeStoreAnswersAndAgainAfterReopen(t *testing.T) {
	for _, mode := range backendModes {
		if mode.backend.Name != "badger" {
			continue
		}
		t.Run(mode.name, func(t *testing.T) {
			plannerOpts := mode.plannerOptions()
			plannerOpts.EnableAttributeFetchFusion = false
			options := func(handler annotations.Handler) DatabaseOptions {
				return DatabaseOptions{
					Schema:               smokeSchema(t),
					DisableCache:         true,
					CompressionThreshold: 256,
					ReplicaID:            1,
					PlannerOptions:       &plannerOpts,
					AnnotationHandler:    handler,
				}
			}

			var treeEvents []annotations.Event
			tree := openBackendDB(t, engineBackends()[0], options(func(e annotations.Event) {
				treeEvents = append(treeEvents, e)
			}))
			want := answerSmokeShapes(t, tree, writeSmokeFixture(t, tree), &treeEvents)

			var badgerEvents []annotations.Event
			badgerOpts := backendOptions(t, mode.backend, options(func(e annotations.Event) {
				badgerEvents = append(badgerEvents, e)
			}))
			badger, err := NewDatabaseWithOptions(badgerOpts)
			require.NoError(t, err)
			f := writeSmokeFixture(t, badger)
			requireSmokeAnswers(t, want, answerSmokeShapes(t, badger, f, &badgerEvents), "on badger")
			require.NoError(t, badger.Close())

			reopened, err := NewDatabaseWithOptions(badgerOpts)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			requireSmokeAnswers(t, want, answerSmokeShapes(t, reopened, f, &badgerEvents), "on badger reopened")
		})
	}
}
