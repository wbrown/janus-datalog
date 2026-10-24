package storage

import (
	"errors"
	"fmt"
	"slices"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
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

// definitionWords is the pull pattern of a definition's seven words.
const definitionWords = `[:db/ident :db/valueType :db/cardinality :db/unique :db/uniqueElements :db/neverZeroValue :db/doc]`

// installedSchema is what a database runs with, published as one immutable
// value: the effective schema, the engine's words over the definitions the
// store carries as facts and the ones its ops infer, the stored definitions
// themselves, and the set a wildcard pull resolves under them. installSchema
// and adoptDeclarations publish a new value, and every reader loads the
// current one in a single atomic operation, so a reader never pairs one
// schema's declarations with another's definitions.
type installedSchema struct {
	effective schema.SchemaProvider
	declared  *schema.Schema
	wildcard  map[datalog.Keyword]struct{}
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
	for _, definition := range engineSchema.Attributes() {
		set[definition.Ident] = struct{}{}
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
	tuples, err := executor.CollectTuples(d.queryUnderPlannerOptions(planOpts, definitionsQuery))
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
	for i, facts := range pulled {
		def, err := definitionFromFacts(definitions[i], facts)
		if err != nil {
			return nil, fmt.Errorf("reading schema facts: %w", err)
		}
		stored.Add(def)
	}
	return stored, nil
}

// definitionFromFacts builds the definition the facts on entity state. The
// facts are input, so every word is checked: a keyword outside its vocabulary,
// a word of the wrong type, or a definition on an entity other than its
// ident's own is an error naming the ident and the word.
func definitionFromFacts(entity datalog.Identity, facts map[string]interface{}) (*schema.AttributeDefinition, error) {
	ident, ok := facts["db/ident"].(datalog.Keyword)
	if !ok {
		return nil, fmt.Errorf("definition on %s: :db/ident is %T, not a keyword", entity, facts["db/ident"])
	}
	if attributeEntity(ident) != entity {
		return nil, fmt.Errorf("definition of %s lives on %s, not on its own entity", ident, entity)
	}
	b := schema.NewBuilder().Attribute(ident.String())

	cardinality, ok := facts["db/cardinality"].(datalog.Keyword)
	if !ok {
		return nil, fmt.Errorf("definition of %s: :db/cardinality is %T, not a keyword", ident, facts["db/cardinality"])
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

	if v, carried := facts["db/valueType"]; carried {
		valueType, ok := v.(datalog.Keyword)
		if !ok {
			return nil, fmt.Errorf("definition of %s: :db/valueType is %T, not a keyword", ident, v)
		}
		b.Type(valueType)
	}
	if v, carried := facts["db/unique"]; carried {
		unique, ok := v.(datalog.Keyword)
		if !ok {
			return nil, fmt.Errorf("definition of %s: :db/unique is %T, not a keyword", ident, v)
		}
		b.Unique(unique)
	}
	if v, carried := facts["db/uniqueElements"]; carried {
		flag, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("definition of %s: :db/uniqueElements is %T, not a boolean", ident, v)
		}
		b.UniqueElements(flag)
	}
	if v, carried := facts["db/neverZeroValue"]; carried {
		flag, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("definition of %s: :db/neverZeroValue is %T, not a boolean", ident, v)
		}
		if flag {
			b.NeverZeroValue()
		}
	}
	if v, carried := facts["db/doc"]; carried {
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
// definition the schema does not admit refuses every later open.
func assertSchemaFacts(d *Database, stored, given schema.SchemaProvider) error {
	defs := given.Attributes()
	slices.SortFunc(defs, func(a, b *schema.AttributeDefinition) int { return a.Ident.Compare(b.Ident) })
	for _, def := range defs {
		if err := schema.ValidateDefinition(def); err != nil {
			return fmt.Errorf("definition of %s: %w", def.Ident, err)
		}
	}

	var declared []*schema.AttributeDefinition
	for _, def := range defs {
		if old := stored.GetAttribute(def.Ident); old == nil || *old != *def {
			declared = append(declared, def)
		}
	}
	if len(declared) == 0 {
		return nil
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
				return errors.Join(fmt.Errorf("declaring %s: %s: %w", def.Ident, w.word, err), tx.Rollback())
			}
		}
		if err := tx.Add(attributeEntity(def.Ident), attributeDeclarationAttr, declaration); err != nil {
			return errors.Join(fmt.Errorf("declaring %s: %s: %w", def.Ident, attributeDeclarationAttr, err), tx.Rollback())
		}
	}
	if _, err := tx.Commit(); err != nil {
		return fmt.Errorf("declaring schema facts: commit: %w", err)
	}
	return nil
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
// With a schema given, the effective schema is the stored definitions. A
// supplied schema is authoritative, so nothing is inferred. With none, the
// effective schema is the stored definitions over the ones
// inferSchemaFromStore reads off the ops of attributes that have datoms and
// no definition. Inferred definitions stay in memory: they are a reading of
// the ops, never a declaration.
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

// installLocked is installSchema with schemaInstallMu held by the caller and
// the schema to install given: the recorded one for an import, a rewind and
// an adoption, and the argument for SetSchema, which records it only once
// this install has succeeded.
func (d *Database) installLocked(given schema.SchemaProvider, assertGiven bool) error {
	if d.refuseUnknown && given == nil {
		return errRefuseUnknownNeedsSchema
	}
	if given != nil && assertGiven {
		stored, err := readSchemaFacts(d)
		if err != nil {
			return err
		}
		if err := assertSchemaFacts(d, stored, given); err != nil {
			return err
		}
	}

	stored, err := readSchemaFacts(d)
	if err != nil {
		return err
	}

	effective := schema.Overlay(stored, engineSchema)
	if given == nil {
		inferred, err := inferSchemaFromStore(d.store)
		if err != nil {
			return fmt.Errorf("inferring schema from store: %w", err)
		}
		effective = schema.Overlay(schema.Overlay(inferred, stored), engineSchema)
	}

	previous := d.installed.Load().effective
	d.installed.Store(&installedSchema{effective: effective, declared: stored, wildcard: wildcardAttributes(stored)})
	d.invalidateChangedDefinitions(previous, effective)
	return nil
}

// adoptDeclarations asserts a parent's declared definitions into this branch
// and composes the schema the branch runs with from what its store states
// after, as an open composes its own. A branch's store is its parent's as of
// the snapshot, so a declaration the parent made afterwards is not in it;
// asserting the parent's declared set, never its inferred one, makes the
// branch's dump self-describing, and a branch that already carries the facts
// writes nothing.
func (d *Database) adoptDeclarations(declared *schema.Schema) error {
	d.schemaInstallMu.Lock()
	defer d.schemaInstallMu.Unlock()
	stored, err := readSchemaFacts(d)
	if err != nil {
		return err
	}
	if err := assertSchemaFacts(d, stored, declared); err != nil {
		return err
	}
	d.mu.RLock()
	given := d.givenSchema
	d.mu.RUnlock()
	return d.installLocked(given, false)
}

// declaredSchema returns the definitions the store carries as facts.
func (d *Database) declaredSchema() *schema.Schema {
	return d.installed.Load().declared
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
		d.cache.InvalidateAttribute(ToStorageDatom(datalog.Datom{A: ident}).A)
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
// roster that the schema does not declare and the engine does not reserve,
// joined in order. On any other database it refuses nothing, and the roster
// is not read. The matcher is one source and answers for itself under any
// name.
func (m *PatternMatcher) UnknownAttributes(_ query.Symbol, attrs []datalog.Keyword) error {
	if !m.refuseUnknown {
		return nil
	}
	return unknownAttributes(m.schema, attrs)
}

// unknownPullAttributes is UnknownAttributes over a pull pattern's roster,
// the check the Pull family runs before executing.
func (m *PatternMatcher) unknownPullAttributes(p *query.PullPattern) error {
	if !m.refuseUnknown {
		return nil
	}
	return m.UnknownAttributes(datalog.SymDollar, query.PullPatternAttributes(p))
}

// unknownAttributes refuses every attribute of the roster that s, a running
// schema with the engine's words declared in it, does not declare, joined in
// order; nil when every attribute is known.
func unknownAttributes(s schema.SchemaProvider, attrs []datalog.Keyword) error {
	var refusals []error
	for _, attr := range attrs {
		if s.GetAttribute(attr) == nil {
			refusals = append(refusals, &schema.UnknownAttributeError{Attribute: attr})
		}
	}
	return errors.Join(refusals...)
}
