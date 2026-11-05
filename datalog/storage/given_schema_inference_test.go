package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A database runs, at an attribute no definition covers, with the cardinality
// the ops of its datoms state, whether or not a schema is given, and at an
// attribute a definition covers, with that definition. Here a dump holds set
// additions at two attributes, the schema declaring one of them
// cardinality-one, and the schema is given at open or afterwards.
func TestAnAttributeNoDefinitionCoversIsInferredUnderAGivenSchema(t *testing.T) {
	one := datalog.NewIdentity("given:one")
	at := func(lamport uint64) datalog.ElementID { return datalog.ElementID{Lamport: lamport, ReplicaID: 1} }
	log := []datalog.Datom{
		{E: one, A: kw(":given/tags"), V: "red", Tx: at(1), Op: datalog.OpCRDTAdd},
		{E: one, A: kw(":given/tags"), V: "blue", Tx: at(2), Op: datalog.OpCRDTAdd},
		{E: one, A: kw(":given/named"), V: "first", Tx: at(3), Op: datalog.OpCRDTAdd},
		{E: one, A: kw(":given/named"), V: "second", Tx: at(4), Op: datalog.OpCRDTAdd},
	}
	var dump strings.Builder
	for i := range log {
		dump.WriteString(FormatDatomEDN(&log[i]))
		dump.WriteByte('\n')
	}
	given, err := schema.NewBuilder().Attribute(":given/named").Type(schema.TypeString).One().Add().Build()
	require.NoError(t, err)
	opens := []struct {
		name string
		open func(t *testing.T) *Database
	}{
		{"the schema given at open", func(t *testing.T) *Database {
			d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory-trees", Schema: given, Dump: strings.NewReader(dump.String())})
			require.NoError(t, err)
			return d
		}},
		{"the schema given after open", func(t *testing.T) *Database {
			d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "memory-trees", Dump: strings.NewReader(dump.String())})
			require.NoError(t, err)
			require.NoError(t, d.SetSchema(given))
			return d
		}},
	}
	for _, o := range opens {
		t.Run(o.name, func(t *testing.T) {
			d := o.open(t)
			defer func() { require.NoError(t, d.Close()) }()
			tags, err := executor.CollectTuples(d.Query(`[:find ?v :where [_ :given/tags ?v]]`))
			require.NoError(t, err)
			require.ElementsMatch(t, [][]interface{}{{"red"}, {"blue"}}, tags, "an attribute no definition covers resolves as its ops state, a set")
			named, err := executor.CollectTuples(d.Query(`[:find ?v :where [_ :given/named ?v]]`))
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"second"}}, named, "the given definition, cardinality one, decides its attribute")
		})
	}
}

// OpenFacts given a schema declares that schema and nothing else. An
// attribute the schema leaves undefined holds every fact through the
// inference.
func TestOpenFactsUnderAGivenSchemaDeclaresThatSchemaAlone(t *testing.T) {
	one := datalog.NewIdentity("facts:one")
	tag, rank := kw(":facts/tag"), kw(":facts/rank")
	given, err := schema.NewBuilder().Attribute(":facts/rank").Type(schema.TypeLong).One().Add().Build()
	require.NoError(t, err)
	d, err := OpenFacts([]datalog.Datom{
		{E: one, A: tag, V: "red"},
		{E: one, A: tag, V: "blue"},
		{E: one, A: rank, V: int64(1)},
	}, DatabaseOptions{Schema: given})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	idents, err := executor.CollectTuples(d.Query(`[:find ?ident :where [_ :db.attribute/declaration ?d] [?d :db/ident ?ident]]`))
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{rank}}, idents, "the store declares the given schema's attributes alone")
	definition := d.Schema().GetAttribute(tag)
	require.NotNil(t, definition, "the database runs with the inferred definition of the undefined attribute")
	require.Equal(t, schema.CardinalityMany, definition.Cardinality, "the inference holds the undefined attribute's facts as a set")
}

// A database OpenFacts opens with a schema and RefuseUnknownAttributes holds
// the facts at an attribute the schema leaves undefined, and refuses a query
// naming that attribute, since the inference declares nothing.
func TestOpenFactsRefusingUnderAGivenSchemaRefusesAnAttributeTheSchemaLeavesUndefined(t *testing.T) {
	one := datalog.NewIdentity("facts:one")
	tag, rank := kw(":facts/tag"), kw(":facts/rank")
	given, err := schema.NewBuilder().Attribute(":facts/rank").Type(schema.TypeLong).One().Add().Build()
	require.NoError(t, err)
	d, err := OpenFacts([]datalog.Datom{
		{E: one, A: tag, V: "red"},
		{E: one, A: tag, V: "blue"},
		{E: one, A: rank, V: int64(1)},
	}, DatabaseOptions{Schema: given, RefuseUnknownAttributes: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	_, err = executor.CollectTuples(d.Query(`[:find ?v :where [_ :facts/tag ?v]]`))
	var unknown *schema.UnknownAttributeError
	require.ErrorAs(t, err, &unknown, "a query naming an attribute the schema leaves undefined is refused")
	ranks, err := executor.CollectTuples(d.Query(`[:find ?v :where [_ :facts/rank ?v]]`))
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, ranks, "a query naming a declared attribute answers")
}

// pullLoose pulls through a tag naming :t/loose, an attribute the schema
// fixture leaves undefined.
type pullLoose struct {
	ID    datalog.Identity `datalog:"-,id"`
	Loose string           `datalog:"t/loose"`
}

// A database that refuses unknown attributes refuses every read naming an
// attribute its schema leaves undefined, though the install infers that
// attribute's definition from datoms present when it runs, since an inferred
// definition is not a declaration. The schema fixture leaves :t/loose
// undefined, and an entity holds a :t/loose fact when an install runs:
// SetSchema after the fact's commit, or an open over a dump holding it. The
// database runs with the inferred definition, and a declared attribute and an
// engine word are not refused.
func TestARefusingDatabaseRefusesAnAttributeWhoseDefinitionIsInferred(t *testing.T) {
	loose := kw(":t/loose")
	installs := []struct {
		name string
		open func(t *testing.T, mode optimizerMode) (*Database, datalog.Identity)
	}{
		{"SetSchema after the fact's commit", func(t *testing.T, mode optimizerMode) (*Database, datalog.Identity) {
			d, entity := refusingDB(t, mode)
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(entity, loose, "b"))
			_, err := tx.Commit()
			require.NoError(t, err)
			require.NoError(t, d.SetSchema(schemaFactsFixture(t)))
			return d, entity
		}},
		{"an open over a dump holding the fact", func(t *testing.T, mode optimizerMode) (*Database, datalog.Identity) {
			holder := datalog.NewIdentity("holder")
			log := []datalog.Datom{
				{E: holder, A: kw(":t/string"), V: "held", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
				{E: holder, A: loose, V: "b", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
			}
			var dump strings.Builder
			for i := range log {
				dump.WriteString(FormatDatomEDN(&log[i]))
				dump.WriteByte('\n')
			}
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: schemaFactsFixture(t), RefuseUnknownAttributes: true, Dump: strings.NewReader(dump.String())})
			return d, holder
		}},
	}
	type read struct {
		name string
		read func(t *testing.T, mode optimizerMode, d *Database, entity datalog.Identity) error
	}
	reads := []read{
		{"a query", func(t *testing.T, _ optimizerMode, d *Database, _ datalog.Identity) error {
			_, err := executor.CollectTuples(d.Query(`[:find ?v :where [_ :t/loose ?v]]`))
			return err
		}},
		{"a get-else", func(t *testing.T, _ optimizerMode, d *Database, _ datalog.Identity) error {
			_, err := executor.CollectTuples(d.Query(`[:find ?v :where [?e :t/string _] [(get-else $ ?e :t/loose "none") ?v]]`))
			return err
		}},
		{"a pull", func(t *testing.T, _ optimizerMode, d *Database, entity datalog.Identity) error {
			_, err := d.Pull(entity, `[:t/loose]`)
			return err
		}},
		{"PullInto", func(t *testing.T, _ optimizerMode, d *Database, entity datalog.Identity) error {
			var pulled pullLoose
			return d.PullInto(entity, &pulled)
		}},
		{"the database as a named source", func(t *testing.T, mode optimizerMode, d *Database, _ datalog.Identity) error {
			plain := createOptimizerModeDB(t, mode, DatabaseOptions{})
			_, err := executor.CollectTuples(plain.Query(`[:find ?v :in $ $other :where [$other _ :t/loose ?v]]`,
				WithSources(map[datalog.Symbol]executor.PatternMatcher{datalog.NewSymbol("$other"): d})))
			return err
		}},
		{"a history handle's query", func(t *testing.T, _ optimizerMode, d *Database, _ datalog.Identity) error {
			_, err := executor.CollectTuples(d.History().Query(`[:find ?v :where [_ :t/loose ?v]]`))
			return err
		}},
		{"the matcher's history copy", func(t *testing.T, _ optimizerMode, d *Database, _ datalog.Identity) error {
			return d.Matcher().(*PatternMatcher).History().UnknownAttributes(datalog.SymDollar, []datalog.Keyword{loose})
		}},
	}
	for _, accessor := range typedAccessors() {
		reads = append(reads, read{"the typed accessor " + accessor.name, func(t *testing.T, _ optimizerMode, d *Database, entity datalog.Identity) error {
			_, err := accessor.read(d, entity, loose)
			return err
		}})
	}
	for _, mode := range optimizerModes {
		for _, install := range installs {
			t.Run(mode.name+"/"+install.name+"/the attributes the database runs with", func(t *testing.T) {
				d, _ := install.open(t, mode)
				require.NotNil(t, d.Schema().GetAttribute(loose), "the install infers :t/loose's definition from its datom")
				held, err := executor.CollectTuples(d.Query(`[:find ?v :where [_ :t/string ?v]]`))
				require.NoError(t, err, "a declared attribute is not refused")
				require.Equal(t, [][]interface{}{{"held"}}, held)
				marks, err := executor.CollectTuples(d.Query(`[:find ?t :where [_ :db/txInstant ?t]]`))
				require.NoError(t, err, "an engine word is not refused")
				require.NotEmpty(t, marks, "the declaration's commit wrote its mark")
			})
			for _, r := range reads {
				t.Run(mode.name+"/"+install.name+"/"+r.name, func(t *testing.T) {
					d, entity := install.open(t, mode)
					requireUnknown(t, r.read(t, mode, d, entity), loose)
				})
			}
		}
	}
}
