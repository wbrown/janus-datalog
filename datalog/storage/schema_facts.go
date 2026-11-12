package storage

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/internal/errjoin"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Attribute definitions are facts. A schema given to a database is asserted
// into its store. A declaration of an attribute is an entity named by its
// seven words, the words of schema.DBIdent and its siblings, each a fact of
// cardinality one on it, so the same declaration made anywhere converges on
// one entity, and a declaration never changes: one that differs in a word is
// another entity. The definition of an attribute is the entity
// db.attribute/<ident>, named by the ident, and it carries one ref,
// :db.attribute/declaration, to its current declaration. A later declaration
// moves the ref, so the ref's history is the definition's history, and two
// replicas' declarations of one attribute resolve to one of them whole. A
// store opened with no schema reads the definitions back, a dump carries
// them, and a query reads them with ordinary patterns.
//
// A schema given at open supersedes the stored definitions for the idents it
// names and leaves the others as they are. Reopening with an unchanged schema
// writes nothing: the writer reads what the store holds and declares only
// what differs.

// attributeEntityPrefix seeds a definition entity from its ident, as
// snapshotEntityPrefix seeds a snapshot marker from its name.
const attributeEntityPrefix = "db.attribute/"

// attributeEntity is the entity the definition of ident lives on.
func attributeEntity(ident datalog.Keyword) datalog.Identity {
	return datalog.NewIdentity(attributeEntityPrefix + ident.String())
}

// declarationEntityPrefix seeds a declaration entity from its words.
const declarationEntityPrefix = "db.declaration/"

// declarationEntity is the entity the declaration def states lives on, named
// by its seven words in order, the doc last since it alone may hold a space.
// A value type or uniqueness constraint the declaration does not carry is
// named by an empty word.
func declarationEntity(def *schema.AttributeDefinition) datalog.Identity {
	word := func(k datalog.Keyword) string {
		if k == nil {
			return ""
		}
		return k.String()
	}
	return datalog.NewIdentity(fmt.Sprintf("%s%s %s %s %s %t %t %s", declarationEntityPrefix,
		def.Ident, word(def.ValueType), def.Cardinality, word(def.Unique),
		def.UniqueElements, def.NeverZeroValue, def.Doc))
}

// attributeDeclarationAttr is the ref a definition entity carries to its
// current declaration. Well-known for the reason the snapshot marker's
// attributes are.
var attributeDeclarationAttr = datalog.WellKnownKeyword(":db.attribute/declaration")

// definitionsQuery binds every definition entity with its current
// declaration. An entity carrying the declaration words that no definition
// entity refers to is not a definition.
const definitionsQuery = `[:find ?a ?d
	:where [?a :db.attribute/declaration ?d]]`

// parsedDefinitionsQuery is definitionsQuery parsed, once a process. Every
// database asks it at its open, where its own parse cache holds nothing, so
// the text parsed through that cache is parsed again by each database opened.
var parsedDefinitionsQuery = sync.OnceValues(func() (*query.Query, error) {
	return parser.ParseQuery(definitionsQuery)
})

// definitionWords is the pull pattern of a definition's seven words.
const definitionWords = `[:db/ident :db/valueType :db/cardinality :db/unique :db/uniqueElements :db/neverZeroValue :db/doc]`

// installedSchema is what a database runs with, published as one immutable
// value: the effective schema, the engine's words over the definitions the
// store carries as facts and the ones its ops infer, the stored definitions
// themselves, the schema a written value is checked against, the set a
// wildcard pull resolves under them, and the set a refusing database admits.
// composeSchema publishes a new value, and every reader loads the current one
// in a single atomic operation, so a reader never pairs one schema's
// declarations with another's definitions.
type installedSchema struct {
	effective schema.SchemaProvider
	// declared is the definitions the database runs with as declarations: the
	// ones its store states, over the ones it took from another database at
	// its open where it took any (takenDeclarations).
	declared *schema.Schema
	// stored is the definitions the database's own store states: declared
	// itself on a database that took none.
	stored *schema.Schema
	// lent is declared as a database opened under this one's schema takes it
	// (lentDeclarations), read from the store by the first such open under
	// this installed value and handed to every later one: the value is
	// immutable, and so is what it lends.
	lent    *takenDeclarations
	lentErr error
	lending sync.Once
	// admission is the schema a written value is checked against: the
	// engine's words over the stored definitions. A definition inferred from
	// the ops is a reading of the datoms, not a declaration, so it admits and
	// refuses no value.
	admission schema.SchemaProvider
	wildcard  map[datalog.Keyword]struct{}
	// vocabulary is the set a refusing database admits: the attributes the
	// store declares with the engine's words, which is wildcard where that is
	// not nil and engineVocabulary where the store declares nothing beyond
	// them. A definition inferred from the ops is not a declaration and is not
	// in it.
	vocabulary map[datalog.Keyword]struct{}
}

// wildcardAttributes is the set a wildcard pull resolves under the stored
// declarations: nil when they declare no attribute beyond the engine's words,
// and a wildcard pull then resolves every attribute the entity carries;
// otherwise the declared attributes with the engine's words, which every
// running schema declares. A definition inferred from the ops is not a
// declaration and is not in the set.
func wildcardAttributes(declared *schema.Schema) map[datalog.Keyword]struct{} {
	var set map[datalog.Keyword]struct{}
	for _, definition := range declared.Attributes() {
		if engineSchema.GetAttribute(definition.Ident) != nil {
			continue
		}
		if set == nil {
			set = make(map[datalog.Keyword]struct{})
		}
		set[definition.Ident] = struct{}{}
	}
	if set == nil {
		return nil
	}
	for word := range engineVocabulary {
		set[word] = struct{}{}
	}
	return set
}

// readSchemaFacts reads the definitions the store holds: the query names each
// definition entity with its current declaration, and one pull over the
// declarations reads each one's words. Both run with no annotation handler,
// so an open reports no events to the caller's.
func readSchemaFacts(d *Database) (*schema.Schema, error) {
	planOpts := d.effectivePlannerOptions()
	planOpts.Handler = nil
	definitionsParsed, err := parsedDefinitionsQuery()
	if err != nil {
		return nil, fmt.Errorf("reading schema facts: %w", err)
	}
	_, definitionsRel, err := d.queryUnderPlannerOptions(planOpts, definitionsParsed)
	tuples, err := executor.CollectTuples(definitionsRel, err)
	if err != nil {
		return nil, fmt.Errorf("reading schema facts: %w", err)
	}
	stored := schema.NewSchema()
	if len(tuples) == 0 {
		return stored, nil
	}
	definitions := make([]datalog.Identity, 0, len(tuples))
	declarations := make([]datalog.Identity, 0, len(tuples))
	for _, tuple := range tuples {
		definition, ok := tuple[0].(datalog.Identity)
		if !ok {
			return nil, fmt.Errorf("reading schema facts: definition entity is %T, not an identity", tuple[0])
		}
		declaration, ok := tuple[1].(datalog.Identity)
		if !ok {
			return nil, fmt.Errorf("reading schema facts: the declaration of %s is %T, not an identity", definition, tuple[1])
		}
		definitions = append(definitions, definition)
		declarations = append(declarations, declaration)
	}
	pattern, err := parser.ParsePullPattern(definitionWords)
	if err != nil {
		return nil, fmt.Errorf("reading schema facts: %w", err)
	}
	puller := executor.NewPullExecutor(d.matcherWithExecOptions(planOpts), d)
	pulled, err := puller.PullMany(declarations, pattern)
	if err != nil {
		return nil, fmt.Errorf("reading schema facts: %w", err)
	}
	for i, words := range pulled {
		facts := make(map[datalog.Keyword]interface{}, len(words))
		for name, value := range words {
			facts[datalog.NewKeyword(name)] = value
		}
		def, err := definitionFromFacts(definitions[i], facts)
		if err != nil {
			return nil, fmt.Errorf("reading schema facts: %w", err)
		}
		stored.Add(def)
	}
	return stored, nil
}

// definitionFromFacts builds the definition the facts on entity state, keyed
// by word. The facts are input, so every word is checked: a keyword outside
// its vocabulary, a word of the wrong type, or a definition on an entity
// other than its ident's own is an error naming the ident and the word.
func definitionFromFacts(entity datalog.Identity, facts map[datalog.Keyword]interface{}) (*schema.AttributeDefinition, error) {
	ident, ok := facts[schema.DBIdent].(datalog.Keyword)
	if !ok {
		return nil, fmt.Errorf("definition on %s: :db/ident is %T, not a keyword", entity, facts[schema.DBIdent])
	}
	if attributeEntity(ident) != entity {
		return nil, fmt.Errorf("definition of %s lives on %s, not on its own entity", ident, entity)
	}
	b := schema.NewBuilder().Attribute(ident.String())

	cardinality, ok := facts[schema.DBCardinality].(datalog.Keyword)
	if !ok {
		return nil, fmt.Errorf("definition of %s: :db/cardinality is %T, not a keyword", ident, facts[schema.DBCardinality])
	}
	switch cardinality {
	case schema.CardinalityOne:
		b.One()
	case schema.CardinalityMany:
		b.Many()
	case schema.CardinalityVector:
		b.Vector()
	default:
		return nil, fmt.Errorf("definition of %s: %s is not a cardinality", ident, cardinality)
	}

	if v, carried := facts[schema.DBValueType]; carried {
		valueType, ok := v.(datalog.Keyword)
		if !ok {
			return nil, fmt.Errorf("definition of %s: :db/valueType is %T, not a keyword", ident, v)
		}
		b.Type(valueType)
	}
	if v, carried := facts[schema.DBUnique]; carried {
		unique, ok := v.(datalog.Keyword)
		if !ok {
			return nil, fmt.Errorf("definition of %s: :db/unique is %T, not a keyword", ident, v)
		}
		b.Unique(unique)
	}
	if v, carried := facts[schema.DBUniqueElements]; carried {
		flag, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("definition of %s: :db/uniqueElements is %T, not a boolean", ident, v)
		}
		b.UniqueElements(flag)
	}
	if v, carried := facts[schema.DBNeverZeroValue]; carried {
		flag, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("definition of %s: :db/neverZeroValue is %T, not a boolean", ident, v)
		}
		if flag {
			b.NeverZeroValue()
		}
	}
	if v, carried := facts[schema.DBDoc]; carried {
		doc, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("definition of %s: :db/doc is %T, not a string", ident, v)
		}
		b.Doc(doc)
	}

	built, err := b.Add().Build()
	if err != nil {
		return nil, fmt.Errorf("definition of %s: %w", ident, err)
	}
	return built.GetAttribute(ident), nil
}

// declarationWord is one of the words a declaration entity carries.
type declarationWord struct {
	word  datalog.Keyword
	value interface{}
}

// assertSchemaFacts declares given's definitions into the store, through one
// transaction, each one that stored does not hold word for word: its seven
// words on the declaration entity its words name, and the definition entity's
// ref moved to that declaration. A word a declaration does not carry is absent
// from its entity, so nothing is withdrawn. Nothing differing opens no
// transaction: Commit writes a commit mark even for an empty transaction, and
// an open that changes nothing writes nothing. Every given definition is
// checked through schema.ValidateDefinition before any is written, since a
// provider of the caller's own hands out definitions as given, and a stored
// definition the schema does not admit refuses every later open. It reports
// whether it wrote: where it did, stored no longer says what the store
// states, and the caller reads the facts again.
func assertSchemaFacts(d *Database, stored, given schema.SchemaProvider) (wrote bool, err error) {
	defs := given.Attributes()
	slices.SortFunc(defs, func(a, b *schema.AttributeDefinition) int { return a.Ident.Compare(b.Ident) })
	for _, def := range defs {
		if err := schema.ValidateDefinition(def); err != nil {
			return false, fmt.Errorf("definition of %s: %w", def.Ident, err)
		}
	}

	var declared []*schema.AttributeDefinition
	for _, def := range defs {
		if old := stored.GetAttribute(def.Ident); old == nil || *old != *def {
			declared = append(declared, def)
		}
	}
	if len(declared) == 0 {
		return false, nil
	}

	tx := d.NewTransaction()
	tx.engine = true
	for _, def := range declared {
		declaration := declarationEntity(def)
		words := []declarationWord{
			{schema.DBIdent, def.Ident},
			{schema.DBCardinality, def.Cardinality},
			{schema.DBUniqueElements, def.UniqueElements},
			{schema.DBNeverZeroValue, def.NeverZeroValue},
		}
		if def.Doc != "" {
			words = append(words, declarationWord{schema.DBDoc, def.Doc})
		}
		if def.ValueType != nil {
			words = append(words, declarationWord{schema.DBValueType, def.ValueType})
		}
		if def.Unique != nil {
			words = append(words, declarationWord{schema.DBUnique, def.Unique})
		}
		for _, w := range words {
			if err := tx.Add(declaration, w.word, w.value); err != nil {
				return false, errjoin.Join(fmt.Errorf("declaring %s: %s: %w", def.Ident, w.word, err), tx.Rollback())
			}
		}
		if err := tx.Add(attributeEntity(def.Ident), attributeDeclarationAttr, declaration); err != nil {
			return false, errjoin.Join(fmt.Errorf("declaring %s: %s: %w", def.Ident, attributeDeclarationAttr, err), tx.Rollback())
		}
	}
	if _, err := tx.Commit(); err != nil {
		return false, fmt.Errorf("declaring schema facts: commit: %w", err)
	}
	return true, nil
}

// errRefuseUnknownNeedsSchema is the refusal of a database that refuses
// unknown attributes and has no schema to check them against: at open with
// the option and no schema, and at SetSchema(nil) on such a database.
var errRefuseUnknownNeedsSchema = errors.New("RefuseUnknownAttributes requires a schema to check attributes against: none was given")

// installSchema declares the given schema's definitions when assertGiven is
// set, reads the definitions the store holds, composes the schema the
// database runs with from them, and installs it with the declared set, all
// under schemaInstallMu.
//
// The schema the database runs with is what the store states. The given
// schema is the writer's input: it is declared into the store, and the store
// is read back after the write. Installs are serialized, so the last reads
// after every write, and SetSchema records the given schema under the same
// hold, so the order the given schemas are recorded in is the order their
// declarations are added in.
//
// The effective schema is the stored definitions over the ones
// inferSchemaFromStore reads off the ops of attributes that have datoms and
// no definition, whether or not a schema is given. A given schema is declared
// into the store, so it decides the idents it names. Inferred definitions
// stay in memory: they are a reading of the ops, never a declaration.
//
// A rewind passes assertGiven false: nothing is written, and the database
// runs with the declarations that survive, until SetSchema or an import
// declares again.
//
// The engine's words are declared over the whole, so each resolves by its
// declaration whatever the store holds, and a database declaring nothing
// runs with the engine's schema alone.
//
// A database that refuses unknown attributes needs a schema to check them
// against, so installing none on it is errRefuseUnknownNeedsSchema.
func (d *Database) installSchema(assertGiven bool) error {
	d.schemaInstallMu.Lock()
	defer d.schemaInstallMu.Unlock()
	d.mu.RLock()
	given := d.givenSchema
	d.mu.RUnlock()
	return d.installLocked(given, assertGiven)
}

// installOpening is the install that ends a database's open: the given
// schema is declared into the store, then adopted where a fork gives its
// parent's declared definitions, and the schema the database runs with is
// composed from what the store states after, in one pass.
//
// A branch's store is its parent's as of the snapshot, so a declaration the
// parent made afterwards is not in it; asserting the parent's declared set,
// never its inferred one, makes the branch's dump self-describing, and a
// branch that already carries the facts writes nothing.
func (d *Database) installOpening(adopted *schema.Schema) error {
	d.schemaInstallMu.Lock()
	defer d.schemaInstallMu.Unlock()
	d.mu.RLock()
	given := d.givenSchema
	d.mu.RUnlock()
	return d.composeSchema(given, true, adopted, true)
}

// installLocked is installSchema with schemaInstallMu held by the caller and
// the schema to install given: the recorded one for an import and a rewind,
// and the argument for SetSchema, which records it only once this install has
// succeeded.
func (d *Database) installLocked(given schema.SchemaProvider, assertGiven bool) error {
	return d.composeSchema(given, assertGiven, nil, false)
}

// composeSchema declares given's definitions when assertGiven is set, then
// adopted's where a fork gives them, and installs the schema composed from
// what the store states after, with schemaInstallMu held by the caller.
//
// The schema's facts are read once, and again after each set that wrote: a
// set is declared against what the store states when it is declared, and a
// set the store already states writes nothing and costs no second read. The
// ops are read once, after the last write.
//
// opening is set for the install that ends a database's open. The cache of a
// database being opened holds no view resolved under a user's definition:
// nothing has read it but this install's own reads of the engine's words,
// whose definitions the install leaves as they were. So that install drops
// no cached view and raises no floor, and an open draws no ElementID from
// the database's clock. Every later install drops the cached views of the
// attributes whose resolution it changed.
//
// On a database that took another's declarations at its open, the definitions
// it runs with as declarations are the ones its store states over the ones it
// took, an ident at a time. A set is declared against those, so a definition
// the database took and a set states word for word is not written; and the
// ones it took are a schema to check against, so such a database refuses
// unknown attributes with no schema given.
func (d *Database) composeSchema(given schema.SchemaProvider, assertGiven bool, adopted *schema.Schema, opening bool) error {
	if d.refuseUnknown && given == nil && d.taken == nil {
		return errRefuseUnknownNeedsSchema
	}
	stored, err := readSchemaFacts(d)
	if err != nil {
		return err
	}
	declared := d.taken.under(stored)
	declare := func(set schema.SchemaProvider) error {
		wrote, err := assertSchemaFacts(d, declared, set)
		if err != nil || !wrote {
			return err
		}
		if stored, err = readSchemaFacts(d); err != nil {
			return err
		}
		declared = d.taken.under(stored)
		return nil
	}
	if given != nil && assertGiven {
		if err := declare(given); err != nil {
			return err
		}
	}
	if adopted != nil {
		if err := declare(adopted); err != nil {
			return err
		}
	}

	inferred, err := inferSchemaFromStore(d.store)
	if err != nil {
		return fmt.Errorf("inferring schema from store: %w", err)
	}
	var effective, admission schema.SchemaProvider
	var wildcard map[datalog.Keyword]struct{}
	if d.taken != nil && stored.Count() == 0 && inferred.Count() == 0 {
		// The store states and infers nothing: the definitions the database
		// took are the whole of what it runs with, composed where they were
		// lent.
		effective, admission, wildcard = d.taken.running, d.taken.running, d.taken.wildcard
	} else {
		effective = schema.Overlay(schema.Overlay(inferred, declared), engineSchema)
		admission = schema.Overlay(declared, engineSchema)
		wildcard = wildcardAttributes(declared)
	}
	vocabulary := wildcard
	if vocabulary == nil {
		vocabulary = engineVocabulary
	}

	previous := d.installed.Load().effective
	d.installed.Store(&installedSchema{effective: effective, declared: declared, stored: stored, admission: admission, wildcard: wildcard, vocabulary: vocabulary})
	if !opening {
		d.invalidateChangedDefinitions(previous, effective)
	}
	return nil
}

// declaredSchema returns the definitions the database runs with as
// declarations: the ones its store carries as facts, over the ones it took
// from another database at its open.
func (d *Database) declaredSchema() *schema.Schema {
	return d.installed.Load().declared
}

// storedSchema returns the definitions the database's own store carries as
// facts.
func (d *Database) storedSchema() *schema.Schema {
	return d.installed.Load().stored
}

// takenDeclarations is the declarations a database opened under another's
// schema took from it (DatabaseOptions.SchemaOf): the definitions, and for
// each ident the datoms that state it in the other's store, the words of its
// declaration entity and the ref its definition entity holds to it. They are
// held beside the database's store, which states none of them. In every other
// respect they are definitions the store states: the schema is composed from
// them under the store's own, a dump writes the ones the store's own do not
// supersede, and a fork's branch holds them as its parent does. The value is
// immutable once made.
type takenDeclarations struct {
	definitions *schema.Schema
	datoms      map[datalog.Keyword][]datalog.Datom
	// running is the engine's words over definitions, and wildcard the set a
	// wildcard pull resolves under them: the schema a database that took these
	// runs with while its own store states no definition and holds no datom to
	// infer one from, composed once and installed by every such database, so
	// its open costs nothing an attribute.
	running  *schema.Schema
	wildcard map[datalog.Keyword]struct{}
}

// under returns the definitions a database runs with as declarations: stored,
// the ones its own store states, over the ones it took. A database that took
// none runs with stored.
func (t *takenDeclarations) under(stored *schema.Schema) *schema.Schema {
	if t == nil {
		return stored
	}
	if stored.Count() == 0 {
		return t.definitions
	}
	return schema.Overlay(t.definitions, stored)
}

// unsuperseded returns the datoms of the declarations the database took that
// stored, the definitions its own store states, does not supersede, in the
// order of their entities: the ones a dump of the database writes among its
// store's own.
func (t *takenDeclarations) unsuperseded(stored *schema.Schema) []datalog.Datom {
	if t == nil {
		return nil
	}
	var datoms []datalog.Datom
	for _, def := range t.definitions.Attributes() {
		if stored.GetAttribute(def.Ident) == nil {
			datoms = append(datoms, t.datoms[def.Ident]...)
		}
	}
	slices.SortStableFunc(datoms, func(a, b datalog.Datom) int { return a.E.Compare(b.E) })
	return datoms
}

// lentDeclarations returns the declarations d runs with, for a database
// opened under d's schema to take: each definition d declares, with the
// datoms that state it. A definition d's own store states is read from the
// store, its declaration entity's current words and the definition entity's
// current ref to it; one d took itself and its store does not supersede is
// lent as d took it.
//
// The declarations are read once for an installed schema and handed to every
// open under it: the first open reads the store with the install lock held,
// so the facts it reads are the ones that installed value was composed from.
func (d *Database) lentDeclarations() (*takenDeclarations, error) {
	if d.mode != latestMode {
		return nil, fmt.Errorf("a read-only temporal handle (AsOf/History) lends no schema")
	}
	d.schemaInstallMu.Lock()
	defer d.schemaInstallMu.Unlock()
	installed := d.installed.Load()
	installed.lending.Do(func() {
		installed.lent, installed.lentErr = d.readLentDeclarations(installed)
	})
	return installed.lent, installed.lentErr
}

// readLentDeclarations reads what installed lends from d's store, with the
// install lock held by the caller.
func (d *Database) readLentDeclarations(installed *installedSchema) (*takenDeclarations, error) {
	lent := &takenDeclarations{definitions: schema.NewSchema(), datoms: make(map[datalog.Keyword][]datalog.Datom)}
	if installed.declared == nil {
		return lent, nil
	}
	for _, def := range installed.declared.Attributes() {
		lent.definitions.Add(def)
		if installed.stored.GetAttribute(def.Ident) == nil {
			lent.datoms[def.Ident] = d.taken.datoms[def.Ident]
			continue
		}
		declaration := declarationEntity(def)
		words, err := currentDatoms(d.store, declaration)
		if err != nil {
			return nil, fmt.Errorf("the declaration of %s: %w", def.Ident, err)
		}
		ref, err := currentDatoms(d.store, attributeEntity(def.Ident), attributeDeclarationAttr, declaration)
		if err != nil {
			return nil, fmt.Errorf("the definition of %s: %w", def.Ident, err)
		}
		lent.datoms[def.Ident] = append(words, ref...)
	}
	lent.running = schema.Overlay(lent.definitions, engineSchema)
	lent.wildcard = wildcardAttributes(lent.definitions)
	return lent, nil
}

// currentDatoms returns the newest datom of each attribute the store holds
// under the EAVT prefix given, an entity alone or with an attribute and a
// value: what the entity states now of each, where an earlier declaration of
// the same words left older datoms beside it.
func currentDatoms(store Store, prefix ...datalog.Value) (current []datalog.Datom, scanErr error) {
	iter, err := OpenScan(store, DiscardIntake, ScanBound{Index: EAVT, Prefix: prefix})
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := iter.Close(); closeErr != nil {
			current, scanErr = nil, errjoin.Join(scanErr, closeErr)
		}
	}()
	newest := make(map[datalog.Keyword]int)
	var datoms []datalog.Datom
	for iter.Next() {
		d, err := iter.Datom()
		if err != nil {
			return nil, err
		}
		at, seen := newest[d.A]
		switch {
		case !seen:
			newest[d.A] = len(datoms)
			datoms = append(datoms, *d)
		case datoms[at].Tx.Less(d.Tx):
			datoms[at] = *d
		}
	}
	return datoms, iter.Error()
}

// invalidateChangedDefinitions drops the cached views of every attribute whose
// cardinality or uniqueness differs between the schema that was in force and
// the one now installed: a cached view was resolved under the cardinality in
// force when it was populated. Either schema is nil on a database declaring
// nothing.
func (d *Database) invalidateChangedDefinitions(before, after schema.SchemaProvider) {
	if d.cache == nil {
		return
	}
	changed := make(map[datalog.Keyword]struct{})
	for _, s := range []schema.SchemaProvider{before, after} {
		if s == nil {
			continue
		}
		for _, def := range s.Attributes() {
			changed[def.Ident] = struct{}{}
		}
	}
	for ident := range changed {
		var was, now *schema.AttributeDefinition
		if before != nil {
			was = before.GetAttribute(ident)
		}
		if after != nil {
			now = after.GetAttribute(ident)
		}
		if definitionResolvesAlike(was, now) {
			continue
		}
		d.cache.InvalidateAttribute(ident)
	}
}

// definitionResolvesAlike reports whether two definitions, either nil, resolve
// an attribute's datoms the same way: the same cardinality and the same
// uniqueness constraint.
func definitionResolvesAlike(a, b *schema.AttributeDefinition) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Cardinality == b.Cardinality && a.Unique == b.Unique
}

// engineSchema declares the words the engine writes or reads on its own, each
// of cardinality one with its type: the commit mark, the entity id a pull
// pattern names, the snapshot marker's words, the branch record's words, the
// definition entity's ref to its declaration, and the seven words of a
// declaration. It is installed over every running schema and never declared
// as facts, so each word resolves by its declaration whatever the store
// holds, and a query naming one runs under the refusal on a database that
// has never minted a marker. A user's write of one is refused
// (validateAttributeStorable), and a given schema declaring one is refused
// (engineWordRedeclared). The commit mark and the entity id are well-known so
// their pointer identity survives ClearInterns.
var engineSchema = func() *schema.Schema {
	one := func(ident, valueType datalog.Keyword) *schema.AttributeDefinition {
		return &schema.AttributeDefinition{Ident: ident, ValueType: valueType, Cardinality: schema.CardinalityOne}
	}
	return schema.NewSchema().
		Add(one(datalog.WellKnownKeyword(":db/txInstant"), schema.TypeInstant)).
		Add(one(query.DBID, schema.TypeRef)).
		Add(one(snapshotNameAttr, schema.TypeString)).
		Add(one(snapshotAtLamportAttr, schema.TypeLong)).
		Add(one(snapshotAtReplicaAttr, schema.TypeLong)).
		Add(one(snapshotCreatedAttr, schema.TypeInstant)).
		Add(one(branchSnapshotAttr, schema.TypeString)).
		Add(one(branchAtLamportAttr, schema.TypeLong)).
		Add(one(branchAtReplicaAttr, schema.TypeLong)).
		Add(one(branchForkLamportAttr, schema.TypeLong)).
		Add(one(branchForkReplicaAttr, schema.TypeLong)).
		Add(one(branchReplicaAttr, schema.TypeLong)).
		Add(one(attributeDeclarationAttr, schema.TypeRef)).
		Add(one(schema.DBIdent, schema.TypeKeyword)).
		Add(one(schema.DBValueType, schema.TypeKeyword)).
		Add(one(schema.DBCardinality, schema.TypeKeyword)).
		Add(one(schema.DBUnique, schema.TypeKeyword)).
		Add(one(schema.DBUniqueElements, schema.TypeBoolean)).
		Add(one(schema.DBNeverZeroValue, schema.TypeBoolean)).
		Add(one(schema.DBDoc, schema.TypeString))
}()

// engineVocabulary is the engine's words as a set: every attribute
// engineSchema declares. A refusing database on a store declaring nothing
// beyond them admits these alone.
var engineVocabulary = func() map[datalog.Keyword]struct{} {
	words := make(map[datalog.Keyword]struct{})
	for _, definition := range engineSchema.Attributes() {
		words[definition.Ident] = struct{}{}
	}
	return words
}()

// engineWordRedeclared reports the first engine word a given schema declares,
// which is refused where a schema is given, at open and at SetSchema: the
// engine's words resolve by the engine's declaration alone.
func engineWordRedeclared(given schema.SchemaProvider) error {
	for _, def := range given.Attributes() {
		if engineSchema.GetAttribute(def.Ident) != nil {
			return fmt.Errorf("attribute %s is the engine's: a schema cannot declare it", def.Ident)
		}
	}
	return nil
}

// RefusesUnknownAttributes implements executor.AttributeVocabulary: the
// matcher is one source and answers for itself under any name, refusing on a
// database opened with RefuseUnknownAttributes.
func (m *PatternMatcher) RefusesUnknownAttributes(_ query.Symbol) bool {
	return m.refuseUnknown
}

// UnknownAttributes implements executor.AttributeVocabulary. On a database
// opened with RefuseUnknownAttributes it refuses every attribute of the
// roster that the store does not declare and the engine does not reserve,
// joined in order. On any other database it refuses nothing, and the roster
// is not read. The matcher is one source and answers for itself under any
// name.
func (m *PatternMatcher) UnknownAttributes(_ query.Symbol, attrs []datalog.Keyword) error {
	if !m.refuseUnknown {
		return nil
	}
	return unknownAttributes(m.vocabulary, attrs)
}

// unknownPullAttributes is UnknownAttributes over a pull pattern's roster,
// the check the Pull family runs before executing.
func (m *PatternMatcher) unknownPullAttributes(p *query.PullPattern) error {
	if !m.refuseUnknown {
		return nil
	}
	return m.UnknownAttributes(datalog.SymDollar, query.PullPatternAttributes(p))
}

// unknownAttributes refuses every attribute of the roster that vocabulary,
// the attributes the store declares with the engine's words, does not hold,
// joined in order; nil when every attribute is known. An attribute whose
// definition the ops infer is not a declaration, so it is refused.
func unknownAttributes(vocabulary map[datalog.Keyword]struct{}, attrs []datalog.Keyword) error {
	var refusals []error
	for _, attr := range attrs {
		if _, declared := vocabulary[attr]; !declared {
			refusals = append(refusals, &schema.UnknownAttributeError{Attribute: attr})
		}
	}
	return errors.Join(refusals...)
}
