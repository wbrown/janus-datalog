package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// deferredErrorIterator models a storage scan that ends with a sticky error:
// Next() reports exhaustion and Error() carries the failure, per the Iterator
// contract in store.go.
type deferredErrorIterator struct {
	err error
}

func (it *deferredErrorIterator) Next() bool                     { return false }
func (it *deferredErrorIterator) Datom() (*datalog.Datom, error) { return nil, nil }
func (it *deferredErrorIterator) Close() error                   { return nil }
func (it *deferredErrorIterator) Seek(key []byte)                {}
func (it *deferredErrorIterator) ElementID() datalog.ElementID   { return datalog.ElementID{} }
func (it *deferredErrorIterator) Error() error                   { return it.err }

// indexScanOverrideStore delegates to a real store but substitutes the
// iterator returned by ScanKeysOnly for one index — models a failing scan on
// that index while every other index behaves normally.
type indexScanOverrideStore struct {
	Store
	index IndexType
	iter  Iterator
}

func (s *indexScanOverrideStore) ScanKeysOnly(index IndexType, start, end []byte) (Iterator, error) {
	if index == s.index {
		return s.iter, nil
	}
	return s.Store.ScanKeysOnly(index, start, end)
}

func (s *indexScanOverrideStore) Scan(index IndexType, start, end []byte) (Iterator, error) {
	if index == s.index {
		return s.iter, nil
	}
	return s.Store.Scan(index, start, end)
}

// datomErrorIterator models a scan whose entry decode fails: Next() yields
// one position and Datom() returns the error.
type datomErrorIterator struct {
	err     error
	stepped bool
}

func (it *datomErrorIterator) Next() bool {
	if it.stepped {
		return false
	}
	it.stepped = true
	return true
}
func (it *datomErrorIterator) Datom() (*datalog.Datom, error) { return nil, it.err }
func (it *datomErrorIterator) Close() error                   { return nil }
func (it *datomErrorIterator) Seek(key []byte)                {}
func (it *datomErrorIterator) ElementID() datalog.ElementID   { return datalog.ElementID{} }
func (it *datomErrorIterator) Error() error                   { return nil }

// scanFailureShapes returns the two failure shapes every scan loop must
// surface: a sticky deferred error (failed scan presenting as exhausted) and
// a per-entry decode error.
func scanFailureShapes(err error) map[string]Iterator {
	return map[string]Iterator{
		"deferred scan error": &deferredErrorIterator{err: err},
		"datom decode error":  &datomErrorIterator{err: err},
	}
}

// TestResolveMaxOtherTxForValueSurfacesScanErrors pins the supersession
// check's error contract: a failed or partially-decodable AVET scan must
// surface as an error, never read as "no competing assertion" — that would
// emit a value that is actually superseded, silently corrupting unique
// resolution.
func TestResolveMaxOtherTxForValueSurfacesScanErrors(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { _ = store.Close() })

	injected := fmt.Errorf("simulated AVET scan failure")
	for name, iter := range scanFailureShapes(injected) {
		t.Run(name, func(t *testing.T) {
			matcher := NewBadgerMatcher(&indexScanOverrideStore{
				Store: store,
				index: AVET,
				iter:  iter,
			})

			var aStorage Attribute
			copy(aStorage[:], datalog.NewKeyword(":user/email").String())
			except := Entity(datalog.NewIdentity("user:alice").Hash())

			_, err := matcher.resolveMaxOtherTxForValue(aStorage, "a@example.com", except)
			if err == nil {
				t.Fatal("expected the scan failure to surface, got nil error")
			}
		})
	}
}

// TestResolveAVLWWSurfacesScanErrors pins the (A, V)-LWW owner resolution's
// error contract: a failed or partially-decodable AVET scan must surface as
// an error, never read as "no entity owns this value".
func TestResolveAVLWWSurfacesScanErrors(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { _ = store.Close() })

	injected := fmt.Errorf("simulated AVET scan failure")
	for name, iter := range scanFailureShapes(injected) {
		t.Run(name, func(t *testing.T) {
			matcher := NewBadgerMatcher(&indexScanOverrideStore{
				Store: store,
				index: AVET,
				iter:  iter,
			})

			var aStorage Attribute
			copy(aStorage[:], datalog.NewKeyword(":user/email").String())
			v := "a@example.com"
			vBytes := encodeValueForSearch(v, matcher.encoder)

			_, _, err := matcher.resolveAVLWW(aStorage, vBytes, v)
			if err == nil {
				t.Fatal("expected the scan failure to surface, got nil error")
			}
		})
	}
}

// TestPrefetchEntitiesSurfacesScanErrors pins the prefetch error contract: a
// store failure during cache warming returns to the caller, who fails the
// query — prefetch being an optimization does not make store failures
// ignorable.
func TestPrefetchEntitiesSurfacesScanErrors(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { _ = store.Close() })

	injected := fmt.Errorf("simulated EATV scan failure")
	for name, iter := range scanFailureShapes(injected) {
		t.Run(name, func(t *testing.T) {
			matcher := NewBadgerMatcher(&indexScanOverrideStore{
				Store: store,
				index: EATV,
				iter:  iter,
			})
			// Prefetch is a no-op without a cache to warm.
			matcher.SetCache(NewCache())

			err := matcher.PrefetchEntities([]datalog.Identity{datalog.NewIdentity("user:alice")})
			if err == nil {
				t.Fatal("expected the scan failure to surface, got nil error")
			}
		})
	}
}

// TestValidateCandidateSurfacesScanErrors pins the V-bound validation error
// contract: a failed or partially-decodable EATV point scan must surface as
// an error, never collapse to "candidate doesn't match" — that silently
// drops valid results.
func TestValidateCandidateSurfacesScanErrors(t *testing.T) {
	store := NewMemoryStore(&BinaryKeyEncoder{})
	t.Cleanup(func() { _ = store.Close() })

	injected := fmt.Errorf("simulated EATV scan failure")
	for name, iter := range scanFailureShapes(injected) {
		t.Run(name, func(t *testing.T) {
			matcher := NewBadgerMatcher(&indexScanOverrideStore{
				Store: store,
				index: EATV,
				iter:  iter,
			})
			// NewBadgerMatcher attaches no cache, so validation takes the
			// EATV point-scan branch.
			it := &validatingVBoundIterator{
				matcher:         matcher,
				validationIndex: EATV,
				currentBoundV:   "bound-value",
			}

			ok, err := it.validateCandidate(datalog.NewIdentity("user:alice"), datalog.NewKeyword(":user/email"))
			if err == nil {
				t.Fatal("expected the scan failure to surface, got nil error")
			}
			if ok {
				t.Fatal("a failed validation scan must not report a match")
			}
		})
	}
}
