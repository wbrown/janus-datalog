      COMPREHENSIVE TRACE: READ PATH FOR BOUND ENTITY QUERY

       Based on my thorough investigation of the janus-datalog codebase, here's the complete trace of the read path for a query like [:find ?v :in $ ?e
       :where [?e :crawl/adventurer ?v]] where ?e is bound from input.

       ---
       1. INDEX SELECTION AND SCAN BOUNDS

       Query Pattern: [?e :crawl/adventurer ?v] with ?e bound from input

       chooseIndex() Decision (matcher.go lines 489-677):
       - Entity (?e) is bound: Non-nil identity from input
       - Attribute (:crawl/adventurer) is constant: Present in pattern
       - Value (?v) is unbound: Nil (variable)

       Selected Index: EATV (lines 566-567)
       // E and A bound, V unbound - use cardinality-aware index selection
       // For CRDT semantics:
       // - CardinalityMany: EAVT groups by V first, enabling add-wins resolution
       // - CardinalityOne/Vector: EATV orders by Tx first, first entry is current
       card := schema.CardinalityOne
       if m.schema != nil {
           if attrDef := m.schema.GetAttribute(aPtr); attrDef != nil {
               card = attrDef.Cardinality
           }
       }
       if card == schema.CardinalityMany {
           // EAVT: E → A → V → Tx - values grouped together for add-wins
           start, end := encoder.EncodePrefixRange(EAVT, eBytes[:], aStorage[:])
           return EAVT, start, end
       }
       // EATV: E → A → Tx → V - first entry is current (highest Tx)
       start, end := encoder.EncodePrefixRange(EATV, eBytes[:], aStorage[:])
       return EATV, start, end

       Scan Range:
       - Start: [EATV prefix][E (20 bytes)][A (32 bytes)][Tx MIN]
       - End: [EATV prefix][E (20 bytes)][A (32 bytes)][Tx MAX]
       - Scans all transactions for this (E, A) pair in descending Tx order (highest Tx first due to bitwise NOT encoding)

       ---
       2. HOW CRDT RESOLUTION WORKS

       The Key Insight: Tx is encoded with bitwise NOT for descending order. First datom encountered = highest Tx = CRDT winner.

       CRDTResolvingIterator (crdt_resolving_iterator.go lines 8-65):

       // For CardinalityOne (cardinality-one attributes):
       case schema.CardinalityOne:
           if isNewGroup {  // First entry for this (E, A)
               if datom.Op == datalog.OpCRDTRemove {
                   // Value was retracted — attribute doesn't exist. Skip group.
                   continue
               }
               // First entry for this (E, A) — emit (LWW winner)
               it.currentDatom = datom
               return true
           }
           // Same (E, A) — skip (already emitted or skipped the winner)
           continue

       What This Means:
       - Iterator wraps raw storage scan
       - For CardinalityOne: Emits ONLY the first entry (highest Tx)
       - Skips all subsequent entries for same (E, A)
       - No buffering, no copying - just filtering
       - Zero space overhead for resolution

       For CardinalityMany (lines 147-150):
       case schema.CardinalityMany:
           if result := it.processAddWins(datom); result != nil {
               it.currentDatom = result
               return true
           }
       - Tracks highest ADD and REMOVE Lamport per value
       - Emit value only if ADD Lamport >= REMOVE Lamport (add-wins on tie)
       - Again: No buffering, emit immediately

       ---
       3. THE MATCHER EXECUTION PATH: QUERY VS PULLINТО

       Query Path (matcher_relations.go lines 44-200):

       func (m *PatternMatcher) Match(pattern *query.DataPattern, bindings executor.Relations) (executor.Relation, error) {
           // Extract values from pattern
           e := m.extractValue(pattern.GetE())  // Gets bound entity from input via bindings
           a := m.extractValue(pattern.GetA())  // Constant attribute
           v := m.extractValue(pattern.GetV())  // Nil (unbound)

           // chooseIndex: E bound, A constant, V unbound → EATV
           index, start, end := m.chooseIndex(e, a, v, tx)

           // Cache check FIRST (lines 97-108)
           if m.cache != nil && m.txID == 0 {
               if a := m.extractValue(pattern.GetA()); a != nil {
                   if aKw, ok := a.(datalog.Keyword); ok {
                       cacheResult, handled := m.matchWithBindingsFromCache(pattern, bindingRel, symbols, aKw)
                       if handled {
                           return cacheResult, nil  // CACHE HIT: Return immediately
                       }
                   }
               }
           }

           // If cache miss or not applicable, scan storage
           rawStorageIter, err := m.reader.ScanKeysOnly(bound)
           // Wrap with CRDTResolvingIterator
           regularIter.storageIter = NewCRDTResolvingIterator(rawStorageIter, m.schema, m.txID)
       }

       PullInto Path (pull.go lines 184-201):

       func (pe *PullExecutor) lookupAttribute(entity datalog.Identity, attr datalog.Keyword) (interface{}, bool, error) {
           // Use EntityLookupMatcher interface if available
           if lookupMatcher, ok := pe.matcher.(EntityLookupMatcher); ok {
               val, found, err = lookupMatcher.LookupAttribute(entity, attr)
               return val, found, err
           }

           // Fallback: uses pattern matching
           pattern := &query.DataPattern{
               Elements: []query.PatternElement{
                   query.Constant{Value: entity},
                   query.Constant{Value: attr},
                   query.Variable{Name: datalog.NewSymbol("?v")},
               },
           }
           rel, err := pe.matcher.Match(query.PatternQuery(pattern), nil)
       }

       Key Difference: PullInto can use the LookupAttribute() interface (matcher.go lines 789-936) for direct cache access.

       ---
       4. THE EA CACHE MECHANISM

       Cache Structure (cache.go lines 58-82):

       type Cache struct {
           entries sync.Map         // map[CacheKey]*CacheEntry
           maxVersions sync.Map     // map[CacheKey]datalog.ElementID (CRITICAL for freshness)
           entityAttrs sync.Map     // map[Entity]*sync.Map (for entity-level tracking)
       }

       type CacheKey struct {
           E Entity      // 20-byte entity hash
           A Attribute   // 32-byte attribute
       }

       type CacheEntry struct {
           version     datalog.ElementID  // Max ElementID when resolved
           cardinality schema.Cardinality
           oneValue    any                // For CardinalityOne
           manySet     map[any]bool       // For CardinalityMany
           vectorList  []any              // For CardinalityVector
       }

       GetOrResolve() Flow (cache.go lines 89-121):

       func (c *Cache) GetOrResolve(key CacheKey, resolver CacheResolver) *CacheEntry {
           // FAST PATH: Check if entry exists
           if val, ok := c.entries.Load(key); ok {
               entry := val.(*CacheEntry)

               // Check freshness via maxVersions (O(1) map lookup, NO storage seek!)
               if maxVal, ok := c.maxVersions.Load(key); ok {
                   currentMax := maxVal.(datalog.ElementID)
                   if entry.version == currentMax {
                       return entry  // Fresh - HIT
                   }
               }
               // Stale - fall through to rebuild
           }

           // SLOW PATH: Rebuild from storage
           entry := c.rebuild(key, resolver)  // Calls ResolveLWW/ResolveAddWins/ResolveRGA
           if entry != nil {
               c.entries.Store(key, entry)
               c.UpdateMaxVersion(key, entry.version)  // Update freshness tracker
           }
           return entry
       }

       Critical Detail - UpdateMaxVersion (cache.go lines 126-148):

       func (c *Cache) UpdateMaxVersion(key CacheKey, elemID datalog.ElementID) {
           for {
               val, loaded := c.maxVersions.Load(key)
               if !loaded {
                   // No existing value - try to store
                   if _, swapped := c.maxVersions.LoadOrStore(key, elemID); swapped {
                       return
                   }
                   continue
               }
               current := val.(datalog.ElementID)
               if current.Compare(elemID) >= 0 {
                   return // Current is already >= new value
               }
               // Try to update to new max using CompareAndSwap
               if c.maxVersions.CompareAndSwap(key, current, elemID) {
                   return
               }
               // CAS failed, retry
           }
       }

       This is atomic and lock-free! Uses CAS loops to update max versions without locks.

       ---
       5. CACHE INVALIDATION AND VERSION UPDATES ON COMMIT

       Transaction.Commit() (database.go lines 1954-2061):

       func (t *Transaction) Commit() (uint64, error) {
           // ... Assert/Retract datoms ...

           // Update cache: track max versions and invalidate stale entries (lines 2013-2051)
           if t.db.cache != nil {
               touched := make([]CacheKey, 0, len(t.datoms)+len(t.retracts))
               seenKeys := make(map[CacheKey]bool)

               // Process asserted datoms
               for _, d := range t.datoms {
                   eBytes := Entity(d.E.Hash())
                   var aBytes Attribute
                   copy(aBytes[:], d.A.String())

                   key := CacheKey{E: eBytes, A: aBytes}
                   if !seenKeys[key] {
                       seenKeys[key] = true
                       touched = append(touched, key)
                       t.db.cache.UpdateMaxVersion(key, d.Tx)  // ATOMIC update
                   }
               }

               // Process retracted datoms (same logic)
               for _, d := range t.retracts {
                   eBytes := Entity(d.E.Hash())
                   var aBytes Attribute
                   copy(aBytes[:], d.A.String())

                   key := CacheKey{E: eBytes, A: aBytes}
                   if !seenKeys[key] {
                       seenKeys[key] = true
                       touched = append(touched, key)
                       t.db.cache.UpdateMaxVersion(key, d.Tx)  // ATOMIC update
                   }
               }

               // Invalidate cache entries for all touched (E, A) pairs
               t.db.cache.Invalidate(touched)
           }
       }

       Invalidate() (cache.go lines 150-160):

       func (c *Cache) Invalidate(touched []CacheKey) {
           for _, key := range touched {
               c.entries.Delete(key)  // Remove resolved entries
           }
           // NOTE: maxVersions is NOT cleared here!
           // It's updated by UpdateMaxVersion() during commit,
           // preserving the max for freshness checks
       }

       ---
       6. RACE CONDITION ANALYSIS

       Question: After Commit() returns, could a subsequent read miss a datom that was just written?

       Answer: NO - Impossible in single-threaded code.

       Why:

       1. BadgerDB MVCC: Commit writes to BadgerDB atomically. All subsequent reads see consistent state.
       2. Cache Invalidation Pattern:
         - UpdateMaxVersion() updates atomically (CAS-based)
         - Invalidate() deletes entries
         - Next read calls GetOrResolve() which checks maxVersions
         - If maxVersions > entry.version, entry is stale → rebuild from storage
         - Storage scan will see committed datoms
       3. No Persisted State Across Queries:
         - Each query gets fresh PatternMatcher from Database.Matcher() (lines 368-393)
         - OR: If same matcher instance, its txID=0 means "see latest" (lines 247, 809)
         - Matcher holds NO mutable state (only read configs: schema, cache, txID)
       4. Cache State:
         - Only holds resolved values (not assertions)
         - Cache entries are invalidated on commit
         - maxVersions is updated to reflect writes
         - If entry is stale, GetOrResolve() rebuilds from storage

       Timeline:
       T0: Commit() writes datom with Tx=1000, calls UpdateMaxVersion(key, Tx=1000)
       T1: maxVersions[key] = 1000
       T2: Cache entries[key] has version=500 (old)
       T3: Invalidate() deletes entries[key]
       T4: Next read calls GetOrResolve(key):
           - entries[key] is gone
           - Call rebuild() → scans EATV → sees datom with Tx=1000
           - Returns entry with version=1000
           - Update entries[key] = entry, UpdateMaxVersion confirms 1000

       All state is read-only in queries:
       - matcher.store: Read-only during query execution
       - matcher.schema: Read-only config
       - matcher.cache: Provides values, never modifies during query
       - matcher.txID: Immutable per matcher instance

       ---
       7. PATTERNMATCHER STATE AND PERSISTENCE

       PatternMatcher Structure (matcher.go lines 16-27):

       type PatternMatcher struct {
           store             *BadgerStore    // Shared with database (same instance)
           txID              uint64          // 0 = latest, or specific transaction
           timeRanges        []executor.TimeRange
           builderCache      *sync.Map       // Cached tuple builders
           handler           annotations.Handler
           options           executor.ExecutorOptions  // Executor config
           forceJoinStrategy *JoinStrategy
           schema            schema.SchemaProvider     // Reference to database schema
           cache             *Cache                    // Reference to database cache
       }

       Creation (database.go lines 368-393):

       func (d *Database) Matcher() executor.PatternMatcher {
           matcher := NewPatternMatcherWithOptions(d.store, execOpts)
           // Set schema for CRDT cardinality-aware resolution
           if d.schema != nil {
               matcher.SetSchema(d.schema)  // Reference, not copy
           }
           // Set cache for CRDT resolution O(1) access
           if d.cache != nil {
               matcher.SetCache(d.cache)    // Reference, not copy
           }
           return matcher
       }

       Key Point:
       - Fresh PatternMatcher created per query
       - OR: Same matcher instance across queries (both OK - stateless reads)
       - store reference is shared (same database)
       - schema and cache are references to database state
       - No state persists across iterations (builderCache is just tuple builders, not query state)

       Two-Query Test:
       // Query 1
       matcher1 := db.Matcher()
       pattern1 := [?e :attr ?v]
       rel1 := matcher1.Match(pattern1, nil)  // Scan 1, see value X

       // COMMIT writes new value
       tx := db.NewTransaction()
       tx.Add(e, attr, value_Y)
       tx.Commit()

       // Query 2
       matcher2 := db.Matcher()  // Fresh matcher
       pattern2 := [?e :attr ?v]
       rel2 := matcher2.Match(pattern2, nil)  // Scan 2
       // GUARANTEED to see both X and Y (or just Y if cardinality-one)

       Why guaranteed:
       1. matcher2 is new, no state from matcher1
       2. matcher2.store points to same BadgerDB
       3. BadgerDB MVCC guarantees committed writes are visible
       4. Cache invalidated on commit, so no stale values

       ---
       SUMMARY

       Read Path Flow:
       Query [?e :attr ?v] with ?e bound
         ↓
       chooseIndex() → EATV (E + A bound, V unbound)
         ↓
       ScanKeysOnly(EATV, [E][A][Tx_min], [E][A][Tx_max])
         → Returns in descending Tx order (bitwise NOT encoding)
         ↓
       NewCRDTResolvingIterator(rawIter, schema, txID)
         → For CardinalityOne: Emit first, skip rest (zero-copy)
         → For CardinalityMany: Emit qualifying ADDs (streaming)
         ↓
       Return as StreamingRelation (lazy iteration)

       Cache Mechanism:
       - cache.GetOrResolve(key) checks maxVersions (atomic, O(1))
       - If fresh: return cached entry
       - If stale: rebuild from storage, update maxVersions
       - On commit: UpdateMaxVersion() (CAS-based), then Invalidate()

       No Race Conditions:
       - BadgerDB MVCC handles atomicity
       - Cache invalidation on commit ensures freshness
       - Matchers are stateless for reads
       - All subsequent reads see committed state

       Critical Line Numbers:
       - Commit: lines 1954-2061 (database.go)
       - Cache GetOrResolve: lines 89-121 (cache.go)
       - UpdateMaxVersion: lines 126-148 (cache.go)
       - CRDTResolvingIterator: lines 67-150 (crdt_resolving_iterator.go)
       - chooseIndex: lines 489-677 (matcher.go)
       - Match with bindings: lines 44-200 (matcher_relations.go)
