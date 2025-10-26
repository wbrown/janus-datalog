# Key Mask Iterator Implementation Plan

## Problem Statement

Current predicate pushdown with key mask filtering is **slower** than traditional decoding despite doing 100x less work. The benchmarks show:

- **Traditional**: 1.3ms - Decodes ALL 10,000 datoms
- **KeyMask (current)**: 2.4ms - Only decodes 100 matching datoms
- **Raw operations**: Byte comparison + 100 decodings SHOULD be 28x faster

The overhead comes from:
1. Multiple abstraction layers (KeyMaskFilterWrapper wrapping KeyOnlyIterator wrapping BadgerIterator)
2. Type checks and interface conversions
3. Function call overhead per iteration
4. Not being integrated into the core iteration loop

## Root Cause Analysis

### What Traditional Iterator Does
```go
// KeyOnlyIterator.Next() - ALWAYS decodes
func (i *KeyOnlyIterator) Next() bool {
    hasNext := i.BadgerIterator.Next()  // Checks bounds
    key := i.it.Item().Key()
    i.currentDatom = DatomFromKey(key)  // ALWAYS decodes
    return true
}

// iter.Datom() - Just returns cached value
func (i *KeyOnlyIterator) Datom() (*Datom, error) {
    return i.currentDatom, nil  // No work!
}
```

### What Our KeyMask Does (Wrong)
```go
// Multiple layers of wrapping
KeyMaskFilterWrapper {
    baseIter: KeyOnlyIterator {
        BadgerIterator { ... }
    }
}

// Extra type checks and function calls
for wrapper.Next() {           // Call 1
    for baseIter.Next() {      // Call 2  
        for badgerIter.Next() { // Call 3
            // Finally do work
        }
    }
}
```

## Proposed Solution: Direct Key Mask Iterator

### Design Principles
1. **NO WRAPPERS** - Single iterator type that does everything
2. **INLINE OPERATIONS** - Byte comparison directly in the hot loop
3. **LAZY DECODING** - Only decode when mask matches
4. **CACHE SMARTLY** - Cache decoded datom like Traditional does

### Implementation

```go
// storage/key_mask_iterator_v2.go

type KeyMaskIterator struct {
    // BadgerDB essentials
    txn   *badger.Txn
    it    *badger.Iterator
    
    // Bounds checking (CRITICAL - was missing!)
    start []byte
    end   []byte
    index IndexType
    
    // Key mask filtering
    maskOffset int      // Where in key to check (e.g., 53 for value in AEVT)
    maskLength int      // How many bytes to compare (e.g., 9 for int64)
    maskTarget []byte   // What we're looking for
    
    // Decoding
    encoder      KeyEncoder
    currentDatom *datalog.Datom  // Cached like Traditional
    currentError error
    
    // Stats (optional)
    keysScanned int
    keysMatched int
}

func (i *KeyMaskIterator) Next() bool {
    for {
        // Check if we have more keys
        if !i.it.Valid() {
            return false
        }
        
        key := i.it.Item().Key()
        
        // CRITICAL: Check bounds (was missing in our benchmarks!)
        if i.end != nil && bytes.Compare(key, i.end) >= 0 {
            return false
        }
        
        i.keysScanned++
        
        // INLINE byte comparison - no function call
        if len(key) >= i.maskOffset+i.maskLength &&
           bytes.Equal(key[i.maskOffset:i.maskOffset+i.maskLength], i.maskTarget) {
            
            i.keysMatched++
            
            // Decode and cache ONLY if mask matches
            i.currentDatom, i.currentError = DatomFromKey(i.index, key, i.encoder)
            
            i.it.Next() // Advance for next call
            
            if i.currentError == nil {
                return true
            }
        }
        
        i.it.Next() // Skip non-matching key
    }
}

func (i *KeyMaskIterator) Datom() (*datalog.Datom, error) {
    // Just return cached value like Traditional
    return i.currentDatom, i.currentError
}
```

### Integration Points

1. **Modify BadgerStore.ScanKeysOnly()**
   ```go
   func (s *BadgerStore) ScanKeysOnly(index IndexType, start, end []byte, 
                                      mask *KeyMaskConstraint) (Iterator, error) {
       if mask != nil && mask.CanOptimize(index) {
           return NewKeyMaskIterator(s, index, start, end, mask)
       }
       return NewKeyOnlyIterator(s, index, start, end)
   }
   ```

2. **Update BadgerMatcher**
   ```go
   func (m *BadgerMatcher) matchUnboundAsRelation(...) {
       // Convert equality constraints to key masks
       var mask *KeyMaskConstraint
       for _, constraint := range constraints {
           if eq, ok := constraint.(*equalityConstraint); ok {
               mask = CreateKeyMaskFromConstraint(eq, index)
               break // Use first suitable constraint
           }
       }
       
       // Pass mask to storage layer
       iter, err := m.store.ScanKeysOnly(index, start, end, mask)
   }
   ```

## Performance Expectations

Based on our raw operation benchmarks:
- 10,000 byte comparisons: 19µs
- 100 decodings: 7µs  
- **Total: ~26µs theoretical**

With BadgerDB overhead (based on Traditional at 1.3ms for 10,000 items):
- BadgerDB iteration overhead: ~1.2ms
- Byte comparisons: ~20µs
- 100 decodings: ~10µs
- **Expected: ~1.23ms (6% faster than Traditional)**

The gain is modest for int64 values but would be significant for:
- String values (avoid string allocation)
- Complex types (avoid deserialization)
- Large datasets (bigger reduction ratio)
- Network/disk I/O bound scenarios

## Implementation Steps

1. **Create `key_mask_iterator_v2.go`** with the clean implementation
2. **Add `mask` parameter to `ScanKeysOnly()`** 
3. **Update `BadgerMatcher`** to detect and use masks
4. **Benchmark against Traditional** with various data types
5. **Document when to use** (selective predicates, complex types)

## When NOT to Use Key Masks

- Simple int64 values with low selectivity (overhead > benefit)
- When you need the full datom anyway (no filtering benefit)
- Complex predicates that can't be expressed as byte equality

## Success Criteria

1. **Faster than Traditional** for selective predicates (< 10% matches)
2. **No wrapper overhead** - single iterator type
3. **Zero allocations** for non-matching keys
4. **Transparent to query engine** - storage layer optimization

## Conclusion

The key mask approach is sound - we can avoid 99% of decodings. But the implementation must be **tight**:
- No abstractions or wrappers
- Inline byte comparison in the hot loop  
- Proper bounds checking
- Integrated into the storage layer

With a proper implementation, we should see 5-10% improvement for int64 filtering and much larger gains for complex types or highly selective predicates.