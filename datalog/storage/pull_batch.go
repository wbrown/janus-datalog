package storage

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/dgraph-io/badger/v4"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

type wildcardUniqueLookup struct {
	entity datalog.Identity
	attr   datalog.Keyword
}

// ResolveAllAttributesMany resolves wildcard pulls for a complete entity set
// through one Badger read transaction and one iterator. Results preserve input
// order. Duplicate entities are scanned once.
func (d *Database) ResolveAllAttributesMany(
	entities []datalog.Identity,
) ([]map[datalog.Keyword]interface{}, error) {
	results := make([]map[datalog.Keyword]interface{}, len(entities))
	if len(entities) == 0 {
		return results, nil
	}

	matcher := d.Matcher().(*BadgerMatcher)
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
	var uniqueLookups []wildcardUniqueLookup
	err := d.store.db.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchValues = false
		iterator := txn.NewIterator(options)
		defer iterator.Close()

		for _, entity := range sortedEntities {
			entityResult, pending, err := d.resolveWildcardEntity(
				matcher,
				iterator,
				entity,
			)
			if err != nil {
				return err
			}
			resolved[entity.Hash()] = entityResult
			uniqueLookups = append(uniqueLookups, pending...)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("batch wildcard scan failed: %w", err)
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
	matcher *BadgerMatcher,
	iterator *badger.Iterator,
	entity datalog.Identity,
) (map[datalog.Keyword]interface{}, []wildcardUniqueLookup, error) {
	entityBytes := entity.Bytes()
	start, end := d.store.encoder.EncodePrefixRange(EATV, entityBytes[:])
	result := make(map[datalog.Keyword]interface{})
	var pending []wildcardUniqueLookup
	var currentAttr datalog.Keyword
	var currentDatoms []datalog.Datom

	flush := func() {
		if currentAttr == nil || len(currentDatoms) == 0 {
			return
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

	for iterator.Seek(start); iterator.Valid(); iterator.Next() {
		key := iterator.Item().Key()
		if bytes.Compare(key, end) >= 0 {
			break
		}
		keyCopy := iterator.Item().KeyCopy(nil)
		datom, err := DatomFromKey(EATV, keyCopy, d.store.encoder, d.store.db)
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
		currentDatoms = append(currentDatoms, datom)
	}
	flush()
	return result, pending, nil
}

func (d *Database) resolveWildcardDatoms(
	matcher *BadgerMatcher,
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
	definition := d.schema.GetAttribute(attr)
	return definition != nil && definition.Unique != ""
}

func (d *Database) attributeValueType(attr datalog.Keyword) schema.ValueType {
	if d.schema == nil {
		return ""
	}
	definition := d.schema.GetAttribute(attr)
	if definition == nil {
		return ""
	}
	return definition.ValueType
}

func cloneResolvedAttributes(
	attrs map[datalog.Keyword]interface{},
) map[datalog.Keyword]interface{} {
	if len(attrs) == 0 {
		return nil
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
		return append([]byte(nil), typed...)
	case []interface{}:
		cloned := make([]interface{}, len(typed))
		for i, element := range typed {
			cloned[i] = cloneResolvedAttributeValue(element)
		}
		return cloned
	case []string:
		return append([]string(nil), typed...)
	case []int64:
		return append([]int64(nil), typed...)
	case []float64:
		return append([]float64(nil), typed...)
	case []bool:
		return append([]bool(nil), typed...)
	default:
		return value
	}
}
