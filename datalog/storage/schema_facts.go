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
// into its store, one entity per attribute, content-addressed from the ident so
// a definition asserted anywhere converges on the entity it already has. Each
// definition entity carries the words of schema.DBIdent and its siblings, one
// fact of cardinality one per word. A store opened with no schema reads the
// definitions back, a dump carries them, and a query reads them with ordinary
// patterns.
//
// A schema given at open supersedes the stored definitions for the idents it
// names and leaves the others as they are. Reopening with an unchanged schema
// writes nothing: the writer reads what the store holds and asserts only what
// differs.

// attributeEntityPrefix seeds a definition entity from its ident, as
// snapshotEntityPrefix seeds a snapshot marker from its name.
const attributeEntityPrefix = "db.attribute/"

// attributeEntity is the entity the definition of ident lives on.
func attributeEntity(ident datalog.Keyword) datalog.Identity {
	return datalog.NewIdentity(attributeEntityPrefix + ident.String())
}

// definitionsQuery binds every definition entity. An entity carrying an ident
// and no cardinality is not a definition.
const definitionsQuery = `[:find ?a
	:where [?a :db/ident _]
	       [?a :db/cardinality _]]`

// definitionWords is the pull pattern of a definition's seven words.
const definitionWords = `[:db/ident :db/valueType :db/cardinality :db/unique :db/uniqueElements :db/neverZeroValue :db/doc]`

// installedSchema is what a database runs with, published as one immutable
// value: the effective schema, nil when the database declares nothing, and
// the definitions the store carries as facts. installSchema and
// adoptDeclarations publish a new value, and every reader loads the current
// one in a single atomic operation, so a reader never pairs one schema's
// declarations with another's definitions.
type installedSchema struct {
	effective schema.SchemaProvider
	declared  *schema.Schema
}

// readSchemaFacts reads the definitions the store holds: the query names the
// definition entities, and one pull over them reads each one's words. Both run
// with no annotation handler, so an open reports no events to the caller's.
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
	entities := make([]datalog.Identity, 0, len(tuples))
	for _, tuple := range tuples {
		entity, ok := tuple[0].(datalog.Identity)
		if !ok {
			return nil, fmt.Errorf("reading schema facts: definition entity is %T, not an identity", tuple[0])
		}
		entities = append(entities, entity)
	}
	pattern, err := parser.ParsePullPattern(definitionWords)
	if err != nil {
		return nil, fmt.Errorf("reading schema facts: %w", err)
	}
	puller := executor.NewPullExecutor(d.matcherWithExecOptions(planOpts), d)
	pulled, err := puller.PullMany(entities, pattern)
	if err != nil {
		return nil, fmt.Errorf("reading schema facts: %w", err)
	}
	for i, facts := range pulled {
		def, err := definitionFromFacts(entities[i], facts)
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

// schemaFactWrite is one word of one definition to assert or withdraw.
type schemaFactWrite struct {
	entity   datalog.Identity
	word     datalog.Keyword
	value    interface{}
	withdraw bool
}

// assertSchemaFacts asserts given's definitions into the store, each word that
// differs from what stored holds, through one transaction. A word the given
// definition carries and the stored one does not, or carries differently, is
// asserted. A value type or uniqueness constraint the stored definition carries
// and the given one does not is withdrawn, so it no longer resolves. A
// definition stored does not hold is asserted whole. Nothing differing opens no
// transaction: Commit writes a commit mark even for an empty transaction, and
// an open that changes nothing writes nothing.
func assertSchemaFacts(d *Database, stored, given schema.SchemaProvider) error {
	defs := given.Attributes()
	slices.SortFunc(defs, func(a, b *schema.AttributeDefinition) int { return a.Ident.Compare(b.Ident) })

	var writes []schemaFactWrite
	for _, def := range defs {
		entity := attributeEntity(def.Ident)
		assert := func(word datalog.Keyword, value interface{}) {
			writes = append(writes, schemaFactWrite{entity: entity, word: word, value: value})
		}
		withdraw := func(word datalog.Keyword, value interface{}) {
			writes = append(writes, schemaFactWrite{entity: entity, word: word, value: value, withdraw: true})
		}

		old := stored.GetAttribute(def.Ident)
		if old == nil {
			assert(schema.DBIdent, def.Ident)
			assert(schema.DBCardinality, def.Cardinality)
			assert(schema.DBUniqueElements, def.UniqueElements)
			assert(schema.DBNeverZeroValue, def.NeverZeroValue)
			assert(schema.DBDoc, def.Doc)
			if def.ValueType != nil {
				assert(schema.DBValueType, def.ValueType)
			}
			if def.Unique != nil {
				assert(schema.DBUnique, def.Unique)
			}
			continue
		}
		if old.Cardinality != def.Cardinality {
			assert(schema.DBCardinality, def.Cardinality)
		}
		if old.UniqueElements != def.UniqueElements {
			assert(schema.DBUniqueElements, def.UniqueElements)
		}
		if old.NeverZeroValue != def.NeverZeroValue {
			assert(schema.DBNeverZeroValue, def.NeverZeroValue)
		}
		if old.Doc != def.Doc {
			assert(schema.DBDoc, def.Doc)
		}
		switch {
		case def.ValueType == nil && old.ValueType != nil:
			withdraw(schema.DBValueType, old.ValueType)
		case def.ValueType != nil && old.ValueType != def.ValueType:
			assert(schema.DBValueType, def.ValueType)
		}
		switch {
		case def.Unique == nil && old.Unique != nil:
			withdraw(schema.DBUnique, old.Unique)
		case def.Unique != nil && old.Unique != def.Unique:
			assert(schema.DBUnique, def.Unique)
		}
	}
	if len(writes) == 0 {
		return nil
	}

	tx := d.NewTransaction()
	for _, w := range writes {
		var err error
		if w.withdraw {
			err = tx.Remove(w.entity, w.word, w.value)
		} else {
			err = tx.Add(w.entity, w.word, w.value)
		}
		if err != nil {
			return errors.Join(fmt.Errorf("asserting schema facts: %s of %s: %w", w.word, w.entity, err), tx.Rollback())
		}
	}
	if _, err := tx.Commit(); err != nil {
		return fmt.Errorf("asserting schema facts: commit: %w", err)
	}
	return nil
}

// installSchema reads the definitions the store holds, asserts the given
// schema's when assertGiven is set, composes the schema the database runs
// with, and installs it with the declared set.
//
// With a schema given, the effective schema is the declared set: the stored
// definitions under the given ones. A supplied schema is authoritative, so
// nothing is inferred. With none, the effective schema is the stored
// definitions over the ones inferSchemaFromStore reads off the ops of
// attributes that have datoms and no definition. Inferred definitions stay in
// memory: they are a reading of the ops, never a declaration.
//
// A rewind passes assertGiven false: the declarations are then what the
// surviving facts state, and a schema given earlier is not re-asserted past
// the snapshot it was withdrawn to.
//
// A database declaring nothing reports no schema, so a store with no
// definitions and no datoms installs nil.
//
// A database that refuses unknown attributes checks them against the given
// schema, so installing none on it is an error.
func (d *Database) installSchema(assertGiven bool) error {
	d.mu.RLock()
	given := d.givenSchema
	d.mu.RUnlock()
	if d.refuseUnknown && given == nil {
		return errors.New("RefuseUnknownAttributes requires a schema to check attributes against: none was given")
	}
	stored, err := readSchemaFacts(d)
	if err != nil {
		return err
	}

	var declared, effective *schema.Schema
	if given != nil {
		declared = stored
		if assertGiven {
			if err := assertSchemaFacts(d, stored, given); err != nil {
				return err
			}
			declared = schema.Overlay(stored, given)
		}
		effective = declared
	} else {
		inferred, err := inferSchemaFromStore(d.store)
		if err != nil {
			return fmt.Errorf("inferring schema from store: %w", err)
		}
		declared = stored
		effective = schema.Overlay(inferred, stored)
	}

	var installed schema.SchemaProvider
	if effective.HasSchema() {
		installed = effective
	}
	previous := d.installed.Load().effective
	d.installed.Store(&installedSchema{effective: installed, declared: declared})
	d.invalidateChangedDefinitions(previous, installed)
	return nil
}

// adoptDeclarations asserts a parent's declared definitions into this branch
// and records the declared set. A branch's store is its parent's as of the
// snapshot, so a declaration the parent made afterwards is not in it; asserting
// the parent's declared set, never its inferred one, makes the branch's dump
// self-describing, and a branch that already carries the facts writes nothing.
func (d *Database) adoptDeclarations(declared *schema.Schema) error {
	stored, err := readSchemaFacts(d)
	if err != nil {
		return err
	}
	if err := assertSchemaFacts(d, stored, declared); err != nil {
		return err
	}
	current := d.installed.Load()
	d.installed.Store(&installedSchema{effective: current.effective, declared: schema.Overlay(stored, declared)})
	return nil
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

// engineAttributes is the roster of the words the engine writes or reads on
// its own, with no definition stored or in memory: the commit mark, the entity
// id a pull pattern names, the snapshot marker's words, the branch record's
// words, and the seven words of a definition. A query naming one of them runs
// under the refusal whether or not any datom carries it, so the engine's own
// queries run on a database that refuses unknown attributes and has never
// minted a marker. The commit mark and the entity id are well-known so their
// pointer identity survives ClearInterns.
var engineAttributes = []datalog.Keyword{
	datalog.WellKnownKeyword(":db/txInstant"),
	datalog.WellKnownKeyword(":db/id"),
	snapshotNameAttr, snapshotAtLamportAttr, snapshotAtReplicaAttr, snapshotCreatedAttr,
	branchSnapshotAttr, branchAtLamportAttr, branchAtReplicaAttr,
	branchForkLamportAttr, branchForkReplicaAttr, branchReplicaAttr,
	schema.DBIdent, schema.DBValueType, schema.DBCardinality, schema.DBUnique,
	schema.DBUniqueElements, schema.DBNeverZeroValue, schema.DBDoc,
}

// UnknownAttributes implements executor.AttributeVocabulary. On a database
// opened with RefuseUnknownAttributes it refuses the attributes the query
// names on the default source that the schema does not declare and the engine
// does not reserve. On any other database it refuses nothing, and the query
// is not walked.
func (m *PatternMatcher) UnknownAttributes(q *query.Query) error {
	if !m.refuseUnknown {
		return nil
	}
	return m.unknownAmong(q.DefaultSourceAttributes())
}

// unknownPullAttributes is UnknownAttributes for a pull pattern, the check the
// Pull family runs before executing.
func (m *PatternMatcher) unknownPullAttributes(p *query.PullPattern) error {
	if !m.refuseUnknown {
		return nil
	}
	return m.unknownAmong(query.PullPatternAttributes(p))
}

// unknownAmong returns the refusals of every attribute of attrs that is
// unknown, joined in order, or nil when every one is known.
func (m *PatternMatcher) unknownAmong(attrs []datalog.Keyword) error {
	var refusals []error
	for _, attr := range attrs {
		if !m.attributeKnown(attr) {
			refusals = append(refusals, &schema.UnknownAttributeError{Attribute: attr})
		}
	}
	return errors.Join(refusals...)
}

// attributeKnown reports whether the schema declares attr or the engine
// reserves it. A matcher whose schema declares no attribute has none.
func (m *PatternMatcher) attributeKnown(attr datalog.Keyword) bool {
	if m.schema != nil && m.schema.GetAttribute(attr) != nil {
		return true
	}
	return slices.Contains(engineAttributes, attr)
}
