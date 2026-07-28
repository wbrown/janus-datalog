package schema

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// The schema vocabulary is keywords. :db.type/string, :db.cardinality/one and
// :db.unique/identity are the Datomic schema keywords, and datalog.Keyword is
// this codebase's interned keyword: one instance per name, compared by pointer.
// So an AttributeDefinition's value type, cardinality and uniqueness fields are
// datalog.Keyword, and the constants below are the whole of the vocabulary.
//
// The three vocabularies are distinguished by value, not by Go type: three
// keywords drawn from three closed sets. The sets are declared below, and the
// entry points — ParseSchema and the builder — reject a keyword from the wrong
// one rather than storing it. Nothing else can: they are one Go type.
//
// The instances are registered well-known, so ClearInterns restores them with
// their identity intact. Without that a clear would orphan every constant here
// and the next Keyword.Equal against a fresh intern would panic.

// valueTypes is the closed set of :db/valueType keywords, populated by
// defineValueType so the set and the declarations cannot drift apart.
var valueTypes = map[datalog.Keyword]struct{}{}

func defineValueType(name string) datalog.Keyword {
	vt := datalog.WellKnownKeyword(name)
	valueTypes[vt] = struct{}{}
	return vt
}

// Value-type keywords. Datomic-compatible naming.
var (
	TypeString  = defineValueType(":db.type/string")
	TypeLong    = defineValueType(":db.type/long")    // int64
	TypeDouble  = defineValueType(":db.type/double")  // float64
	TypeBoolean = defineValueType(":db.type/boolean") // bool
	TypeInstant = defineValueType(":db.type/instant") // time.Time
	TypeBytes   = defineValueType(":db.type/bytes")   // []byte
	TypeRef     = defineValueType(":db.type/ref")     // datalog.Identity
	TypeKeyword = defineValueType(":db.type/keyword") // datalog.Keyword
	TypeSymbol  = defineValueType(":db.type/symbol")  // datalog.Symbol
	TypeTx      = defineValueType(":db.type/tx")      // datalog.ElementID
)

// cardinalities is the closed set of :db/cardinality keywords a schema may
// declare. CardinalityUnknown is deliberately absent — see its declaration.
var cardinalities = map[datalog.Keyword]struct{}{}

func defineCardinality(name string) datalog.Keyword {
	c := datalog.WellKnownKeyword(name)
	cardinalities[c] = struct{}{}
	return c
}

var (
	CardinalityOne    = defineCardinality(":db.cardinality/one")
	CardinalityMany   = defineCardinality(":db.cardinality/many")
	CardinalityVector = defineCardinality(":db.cardinality/vector") // Ordered collection (RGA)
)

// CardinalityUnknown marks an attribute with no schema definition, whose values
// are all returned rather than CRDT-resolved to one. It is not in the parseable
// set because it describes the absence of a declaration: a schema that declared
// it would be declaring that it had declared nothing.
//
// Distinct from the zero value. A nil cardinality is a definition that has not
// said which it is, and the builder fills it with CardinalityOne.
var CardinalityUnknown = datalog.WellKnownKeyword(":db.cardinality/unknown")

// uniques is the closed set of :db/unique keywords a schema may declare.
//
// There is no keyword for "not unique" and no constant naming its absence: a
// nil Unique is how absence is written, and leaving :db/unique out of the
// definition is how a schema says it. A UniqueNone constant would be a second
// name for nil, standing for a value ParseSchema cannot produce.
var uniques = map[datalog.Keyword]struct{}{}

func defineUnique(name string) datalog.Keyword {
	u := datalog.WellKnownKeyword(name)
	uniques[u] = struct{}{}
	return u
}

var (
	// UniqueValue: the value is globally unique across entities.
	UniqueValue = defineUnique(":db.unique/value")
	// UniqueIdentity: the value is a natural key identifying the entity.
	// Today it differs from UniqueValue only in declared intent — both permit
	// LookupByUnique and neither performs write-time upsert. See
	// docs/reference/CRDT_UNIQUE_SEMANTICS.md, decision D1 (Position 2).
	UniqueIdentity = defineUnique(":db.unique/identity")
)

// Ordering determines how elements are arranged in a collection.
// Currently derived from Cardinality; will become primary in future toolkit.
type Ordering int

const (
	OrderingNone Ordering = iota // Unordered (registers, sets)
	OrderingRGA                  // Chained positions via AfterRef (Vector)
	// Future: OrderingLSeq (independent positions), OrderingTimestamped
)

// Conflict determines how concurrent writes are resolved.
// Currently derived from Cardinality; will become primary in future toolkit.
type Conflict int

const (
	ConflictLWW     Conflict = iota // Last Writer Wins (highest ElementID)
	ConflictAddWins                 // Add beats concurrent remove at same Lamport
	// Future: ConflictRemoveWins, ConflictMV
)

// AttributeDefinition defines schema for a single attribute.
//
// The three keyword fields each draw from a closed set declared above:
// ValueType from valueTypes, Cardinality from cardinalities, Unique from
// uniques. Nothing in the Go type system keeps them apart — they are all
// keywords — so the sets are checked where a definition is built, by
// ParseSchema and by the builder.
type AttributeDefinition struct {
	Ident          datalog.Keyword // The attribute keyword (e.g., :person/name), interned
	ValueType      datalog.Keyword // Required for type validation
	Cardinality    datalog.Keyword // Required for Pull API (default: CardinalityOne)
	Unique         datalog.Keyword // Optional uniqueness constraint; nil is no constraint
	UniqueElements bool            // If true, collection has set semantics (no duplicate values)
	Doc            string          // Optional documentation
}

// HasUniqueConstraint reports whether the attribute declares :db/unique, of
// either kind. Every consumer of the Unique field asks exactly this: the
// UniqueValue/UniqueIdentity distinction is declared intent that no read path
// branches on.
//
// Named apart from IsUniqueElements, which is a different field: that one is
// about duplicate elements within a collection value, this one about the value
// being unique across entities.
func (a *AttributeDefinition) HasUniqueConstraint() bool {
	if a == nil {
		return false
	}
	return a.Unique != nil
}

// GetOrdering returns the ordering strategy for this attribute.
// Currently derived from Cardinality.
func (a *AttributeDefinition) GetOrdering() Ordering {
	if a == nil {
		return OrderingNone
	}
	switch a.Cardinality {
	case CardinalityVector:
		return OrderingRGA
	default:
		return OrderingNone
	}
}

// GetConflict returns the conflict resolution strategy for this attribute.
// Currently derived from Cardinality.
func (a *AttributeDefinition) GetConflict() Conflict {
	if a == nil {
		return ConflictLWW
	}
	switch a.Cardinality {
	case CardinalityOne:
		return ConflictLWW
	default:
		return ConflictAddWins
	}
}

// IsUniqueElements returns true if this attribute enforces element uniqueness.
// True for CardinalityMany (sets are inherently unique), and for Vector with UniqueElements flag.
func (a *AttributeDefinition) IsUniqueElements() bool {
	if a == nil {
		return false
	}
	return a.Cardinality == CardinalityMany || a.UniqueElements
}

// IsOrdered returns true if this attribute maintains element ordering.
func (a *AttributeDefinition) IsOrdered() bool {
	if a == nil {
		return false
	}
	return a.Cardinality == CardinalityVector
}

// IsSet returns true if this attribute has set semantics (unordered, unique).
func (a *AttributeDefinition) IsSet() bool {
	if a == nil {
		return false
	}
	return a.Cardinality == CardinalityMany
}

// IsRegister returns true if this attribute has register semantics (single value, LWW).
func (a *AttributeDefinition) IsRegister() bool {
	if a == nil {
		return false
	}
	return a.Cardinality == CardinalityOne
}

// IsOrderedSet returns true if this attribute has ordered set semantics (ordered + unique).
// This is Vector with UniqueElements enabled.
func (a *AttributeDefinition) IsOrderedSet() bool {
	if a == nil {
		return false
	}
	return a.Cardinality == CardinalityVector && a.UniqueElements
}

// Schema holds all attribute definitions
// Thread-safe for concurrent reads after construction
type Schema struct {
	attributes map[datalog.Keyword]*AttributeDefinition
}

// NewSchema creates an empty schema
func NewSchema() *Schema {
	return &Schema{
		attributes: make(map[datalog.Keyword]*AttributeDefinition),
	}
}

// Add adds an attribute definition to the schema, and is the only write into
// s.attributes — ParseSchema and the builder both reach the map through here.
// Returns the schema for chaining.
//
// It panics on a keyword from the wrong vocabulary. The three vocabularies are
// one Go type, so `Cardinality: TypeString` compiles, and what consumes the
// field is a switch with an arm per cardinality: an unrecognized keyword
// matches none of them and every datom in the group is silently skipped.
//
// A panic rather than an error, on the registration convention. sql.Register
// and http.Handle panic with no Must prefix and no error sibling, because they
// declare something during initialization from source literals, where a failure
// is the caller's own mistake and there is nothing to recover from. The parsing
// convention — regexp.Compile paired with regexp.MustCompile — governs calls
// that convert an input, which this is not. Callers reading genuinely untrusted
// schema text use ParseSchema, which returns errors.
func (s *Schema) Add(def *AttributeDefinition) *Schema {
	if def == nil {
		return s
	}
	// Set default cardinality if not specified
	if def.Cardinality == nil {
		def.Cardinality = CardinalityOne
	}
	// After the default, so a definition that named no cardinality is checked
	// against the value it will actually carry.
	mustBelong(def.Ident, def.ValueType, valueTypes, "a value type")
	mustBelong(def.Ident, def.Cardinality, cardinalities, "a cardinality")
	mustBelong(def.Ident, def.Unique, uniques, "a uniqueness constraint")
	s.attributes[def.Ident] = def
	return s
}

// mustBelong panics unless kw is absent or drawn from set. A nil keyword is
// absence — a definition that has not said which value type or uniqueness
// constraint it carries — and is left alone; Add has already replaced an absent
// cardinality with its default before this runs.
//
// noun names the vocabulary in the message, worded as the builder words it, so
// a definition rejected here and one rejected there read alike.
func mustBelong(ident, kw datalog.Keyword, set map[datalog.Keyword]struct{}, noun string) {
	if kw == nil {
		return
	}
	if _, ok := set[kw]; !ok {
		panic(fmt.Errorf("attribute %s: %s is not %s", ident, kw, noun))
	}
}

// GetAttribute returns the definition for an attribute, or nil if unknown
func (s *Schema) GetAttribute(attr datalog.Keyword) *AttributeDefinition {
	if s == nil {
		return nil
	}
	return s.attributes[attr]
}

// HasSchema returns true if a schema is available with at least one attribute
func (s *Schema) HasSchema() bool {
	return s != nil && len(s.attributes) > 0
}

// IsRef returns true if the attribute is a reference type
func (s *Schema) IsRef(attr datalog.Keyword) bool {
	def := s.GetAttribute(attr)
	return def != nil && def.ValueType == TypeRef
}

// IsMany returns true if the attribute has cardinality many
func (s *Schema) IsMany(attr datalog.Keyword) bool {
	def := s.GetAttribute(attr)
	return def != nil && def.Cardinality == CardinalityMany
}

// IsVector returns true if the attribute has cardinality vector (ordered collection)
func (s *Schema) IsVector(attr datalog.Keyword) bool {
	def := s.GetAttribute(attr)
	return def != nil && def.Cardinality == CardinalityVector
}

// Cardinality returns the cardinality of an attribute, defaulting to CardinalityOne
func (s *Schema) Cardinality(attr datalog.Keyword) datalog.Keyword {
	def := s.GetAttribute(attr)
	if def == nil {
		return CardinalityOne
	}
	return def.Cardinality
}

// Attributes returns all attribute definitions in the schema
func (s *Schema) Attributes() []*AttributeDefinition {
	if s == nil {
		return nil
	}
	result := make([]*AttributeDefinition, 0, len(s.attributes))
	for _, def := range s.attributes {
		result = append(result, def)
	}
	return result
}

// Count returns the number of attributes in the schema
func (s *Schema) Count() int {
	if s == nil {
		return 0
	}
	return len(s.attributes)
}
