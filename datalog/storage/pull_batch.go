package storage

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

type wildcardUniqueLookup struct {
	entity datalog.Identity
	attr   datalog.Keyword
}

// indexKeyIterator is implemented by store iterators that can expose the current
// raw index key without decoding values or resolving blobs.
type indexKeyIterator interface {
	Key() []byte
}

func iteratorCurrentKey(iterator Iterator) ([]byte, bool) {
	keyed, ok := iterator.(indexKeyIterator)
	if !ok {
		return nil, false
	}
	key := keyed.Key()
	if key == nil {
		return nil, false
	}
	return key, true
}

// ResolveAllAttributesMany resolves wildcard pulls for a complete entity set.
// The dominant non-unique EATV traversal shares one store scan session and seeks
// forward per entity; unique-ownership walks and blob dereferences retain their
// specialized reads. Results preserve input order. Duplicate entities are scanned
// once.
func (d *Database) ResolveAllAttributesMany(
	entities []datalog.Identity,
) ([]map[datalog.Keyword]interface{}, error) {
	results := make([]map[datalog.Keyword]interface{}, len(entities))
	if len(entities) == 0 {
		return results, nil
	}

	matcher := d.Matcher().(*PatternMatcher)
	if matcher.isHistoryMode() {
		for i, entity := range entities {
			result, err := d.ResolveAllAttributes(entity)
			if err != nil {
				return nil, err
			}
			results[i] = result
		}
		return results, nil
	}

	uniqueEntities := make(map[[20]byte]datalog.Identity, len(entities))
	for _, entity := range entities {
		if entity != nil {
			uniqueEntities[entity.Hash()] = entity
		}
	}
	sortedEntities := make([]datalog.Identity, 0, len(uniqueEntities))
	for _, entity := range uniqueEntities {
		sortedEntities = append(sortedEntities, entity)
	}
	sort.Slice(sortedEntities, func(i, j int) bool {
		return bytes.Compare(sortedEntities[i].Bytes(), sortedEntities[j].Bytes()) < 0
	})

	resolved := make(map[[20]byte]map[datalog.Keyword]interface{}, len(sortedEntities))
	declaredAttrs := d.declaredWildcardAttributes()
	var uniqueLookups []wildcardUniqueLookup
	iterator, err := d.store.ScanKeysOnly(ScanBound{Index: EATV})
	if err != nil {
		return nil, fmt.Errorf("batch wildcard scan failed: %w", err)
	}
	for _, entity := range sortedEntities {
		entityResult, pending, err := d.resolveWildcardEntity(
			matcher,
			iterator,
			entity,
			declaredAttrs,
		)
		if err != nil {
			_ = iterator.Close()
			return nil, err
		}
		resolved[entity.Hash()] = entityResult
		uniqueLookups = append(uniqueLookups, pending...)
	}
	iterErr := iterator.Error()
	closeErr := iterator.Close()
	if iterErr != nil {
		return nil, fmt.Errorf("batch wildcard scan failed: %w", iterErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("batch wildcard scan close failed: %w", closeErr)
	}

	for _, lookup := range uniqueLookups {
		value, err := d.ResolveEntityAttributes(
			lookup.entity,
			[]datalog.Keyword{lookup.attr},
		)
		if err != nil {
			return nil, err
		}
		if resolvedValue, ok := value[lookup.attr]; ok {
			resolved[lookup.entity.Hash()][lookup.attr] = resolvedValue
		}
	}

	for i, entity := range entities {
		if entity == nil {
			continue
		}
		results[i] = cloneResolvedAttributes(resolved[entity.Hash()])
	}
	return results, nil
}

func (d *Database) resolveWildcardEntity(
	matcher *PatternMatcher,
	iterator Iterator,
	entity datalog.Identity,
	declaredAttrs map[datalog.Keyword]struct{},
) (map[datalog.Keyword]interface{}, []wildcardUniqueLookup, error) {
	result := make(map[datalog.Keyword]interface{})
	var pending []wildcardUniqueLookup
	var currentAttr datalog.Keyword
	var currentDatoms []datalog.Datom

	flush := func() {
		if currentAttr == nil || len(currentDatoms) == 0 {
			return
		}
		if declaredAttrs != nil {
			if _, declared := declaredAttrs[currentAttr]; !declared {
				currentDatoms = currentDatoms[:0]
				return
			}
		}
		if d.isUniqueAttribute(currentAttr) {
			pending = append(pending, wildcardUniqueLookup{
				entity: entity,
				attr:   currentAttr,
			})
		} else if value, present := d.resolveWildcardDatoms(
			matcher,
			entity,
			currentAttr,
			currentDatoms,
		); present {
			result[currentAttr] = value
		}
		currentDatoms = currentDatoms[:0]
	}

	// One scan, one seek per entity: the bound names this entity's run, and the
	// iterator stops at its end. Nothing here re-checks the entity, and nothing
	// here reads a key — the run's end is the seam's to enforce, and a caller
	// that enforced it itself could only do so by slicing the encoded key.
	iterator.Seek(ScanBound{Index: EATV, Prefix: []datalog.Value{entity}})
	for iterator.Next() {
		datom, err := iterator.Datom()
		if err != nil {
			return nil, nil, err
		}
		if matcher.shouldFilterTx(datom.Tx) {
			continue
		}
		if currentAttr != nil && datom.A != currentAttr {
			flush()
		}
		currentAttr = datom.A
		currentDatoms = append(currentDatoms, *datom)
	}
	flush()
	return result, pending, nil
}

func (d *Database) declaredWildcardAttributes() map[datalog.Keyword]struct{} {
	s, ok := d.schema.(*schema.Schema)
	if !ok || !s.HasSchema() {
		return nil
	}
	attrs := s.Attributes()
	declared := make(map[datalog.Keyword]struct{}, len(attrs))
	for _, definition := range attrs {
		declared[definition.Ident] = struct{}{}
	}
	return declared
}

func (d *Database) resolveWildcardDatoms(
	matcher *PatternMatcher,
	entity datalog.Identity,
	attr datalog.Keyword,
	datoms []datalog.Datom,
) (interface{}, bool) {
	storageDatom := ToStorageDatom(datalog.Datom{E: entity, A: attr})
	cardinality := matcher.GetCardinality(storageDatom.A)
	if d.cache != nil {
		if key, ok := matcher.cacheKey(storageDatom.E, storageDatom.A); ok {
			d.cache.PopulateFromDatoms(key, cardinality, datoms)
		}
	}

	switch cardinality {
	case schema.CardinalityMany:
		members, _ := ResolveAddWinsFromDatoms(datoms)
		if len(members) == 0 {
			return nil, false
		}
		values := make([]interface{}, 0, len(members))
		for _, value := range members {
			values = append(values, value)
		}
		return values, true
	case schema.CardinalityVector:
		values, _, maxID := ResolveRGAFromDatoms(datoms)
		if maxID.IsZero() {
			return nil, false
		}
		return typedVector(values, d.attributeValueType(attr)), true
	default:
		value, _ := ResolveLWWFromDatoms(datoms)
		if value == nil {
			return nil, false
		}
		return value, true
	}
}

func (d *Database) isUniqueAttribute(attr datalog.Keyword) bool {
	if d.schema == nil {
		return false
	}
	return d.schema.GetAttribute(attr).HasUniqueConstraint()
}

func (d *Database) attributeValueType(attr datalog.Keyword) datalog.Keyword {
	if d.schema == nil {
		return nil
	}
	definition := d.schema.GetAttribute(attr)
	if definition == nil {
		return nil
	}
	return definition.ValueType
}

func cloneResolvedAttributes(
	attrs map[datalog.Keyword]interface{},
) map[datalog.Keyword]interface{} {
	if len(attrs) == 0 {
		return make(map[datalog.Keyword]interface{})
	}
	cloned := make(map[datalog.Keyword]interface{}, len(attrs))
	for attr, value := range attrs {
		cloned[attr] = cloneResolvedAttributeValue(value)
	}
	return cloned
}

func cloneResolvedAttributeValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case []byte:
		cloned := make([]byte, len(typed))
		copy(cloned, typed)
		return cloned
	case []interface{}:
		cloned := make([]interface{}, len(typed))
		for i, element := range typed {
			cloned[i] = cloneResolvedAttributeValue(element)
		}
		return cloned
	case []string:
		cloned := make([]string, len(typed))
		copy(cloned, typed)
		return cloned
	case []int64:
		cloned := make([]int64, len(typed))
		copy(cloned, typed)
		return cloned
	case []float64:
		cloned := make([]float64, len(typed))
		copy(cloned, typed)
		return cloned
	case []bool:
		cloned := make([]bool, len(typed))
		copy(cloned, typed)
		return cloned
	default:
		return value
	}
}
