# Key-Only Scanning Optimization

## Critical Discovery

The BadgerDB storage implementation stores REDUNDANT data:
- **KEY**: Contains all datom components in index order (E,A,V,Tx for EAVT)
- **VALUE**: Contains the SAME datom serialized again

This means we're storing and fetching duplicate information!

## Current Storage

```go
// In assertDatom
value := sd.Bytes()  // Full datom serialized
for _, idx := range indices {
    key := s.encoder.EncodeKey(idx, d)  // Full datom in different order
    txn.Set(key, value)  // REDUNDANT!
}
```

For EAVT index:
- Key: `[0x00][20-byte E][32-byte A][variable V][20-byte Tx]`
- Value: `[20-byte E][32-byte A][variable V][20-byte Tx]` (serialized)

## Optimization Opportunity

### 1. For Count Queries
```go
// Current: Fetches keys AND values
opts.PrefetchValues = true  // Fetches redundant values
count := 0
for it.Next() {
    it.Datom()  // Deserializes value
    count++
}

// Optimized: Key-only iteration
opts.PrefetchValues = false  // Don't fetch values!
count := 0
for it.Next() {
    count++  // Just count keys
}
```

This should be **10-100x faster** for counting!

### 2. For Full Scans
We could decode the datom FROM THE KEY:

```go
func DatomFromKey(index IndexType, key []byte, encoder KeyEncoder) (*datalog.Datom, error) {
    e, a, v, tx, err := encoder.DecodeKey(index, key)
    if err != nil {
        return nil, err
    }
    
    // Reconstruct datom from key components
    return &datalog.Datom{
        E:  datalog.NewIdentityFromBytes(e),
        A:  datalog.NewKeywordFromBytes(a),
        V:  datalog.ValueFromBytes(v),
        Tx: TxFromBytes(tx),
    }, nil
}
```

### 3. Storage Redesign Option

Instead of storing redundant data, we could:
- **Key**: Index-specific ordering of datom components
- **Value**: Empty or just a presence marker

This would:
- Reduce storage by ~50%
- Eliminate value fetching entirely
- Make scans much faster

## Implementation Plan

### Quick Win (Immediate)
Add key-only counting:
```go
func (s *BadgerStore) Count(index IndexType, start, end []byte) (int64, error) {
    txn := s.db.NewTransaction(false)
    defer txn.Discard()
    
    opts := badger.DefaultIteratorOptions
    opts.PrefetchValues = false  // KEY ONLY!
    opts.PrefetchSize = 10000     // Prefetch more keys
    
    it := txn.NewIterator(opts)
    defer it.Close()
    
    var count int64
    it.Seek(start)
    for it.Valid() {
        if end != nil && bytes.Compare(it.Item().Key(), end) >= 0 {
            break
        }
        count++
        it.Next()
    }
    
    return count, nil
}
```

### Medium Term
Implement DatomFromKey to avoid value fetching:
- Decode datom from key components
- Use for all read operations
- Keep values for compatibility

### Long Term
Redesign storage to eliminate redundancy:
- Store only keys
- Use key components as the source of truth
- Migrate existing data

## Expected Impact

### Count Queries
- Current: 125ms for 124k datoms (fetching keys + values)
- Expected: <10ms (key-only iteration)
- **Speedup: 12x+**

### Full Scans
- Current: Fetch key + value, deserialize value
- Optimized: Fetch key only, decode from key
- **Speedup: 2-5x**

### Storage Size
- Current: Each datom stored twice (key + value)
- Optimized: Each datom stored once (key only)
- **Reduction: 50%**

## Risk Assessment

### Compatibility
- Existing code expects values
- Need careful migration

### Complexity
- Decoding from keys adds complexity
- Different indices have different orderings

### Testing
- Need thorough testing of key decoding
- Ensure no data loss

## Conclusion

We're currently using BadgerDB inefficiently by storing redundant data. Key-only scanning could provide immediate 10x+ speedups for count queries and 2-5x for full scans. This is a MAJOR optimization opportunity!