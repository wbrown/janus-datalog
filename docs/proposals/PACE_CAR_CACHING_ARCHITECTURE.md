# Proposal: Pace Car Caching Architecture for Pipeline Parallelism

**Status**: Proposed
**Author**: Wes Brown
**Date**: 2025-10-16
**Related**: `fix-buffered-iterator-architecture` branch, LAZY_MATERIALIZATION_PLAN.md

---

## Executive Summary

Current `StreamingRelation` caching blocks all subsequent consumers until the first consumer **completes** iteration. This prevents pipeline parallelism in multi-phase queries.

**Proposed**: "Pace car" architecture where the first consumer acts as the leader, and subsequent consumers follow along incrementally, reading from cache as it's built. This enables true streaming with bounded memory usage and concurrent phase execution.

**Key Benefits**:
- Pipeline parallelism: Phases overlap instead of running sequentially
- Lower latency: First results available immediately (not after full phase completion)
- Bounded memory: Buffer size = speed differential between consumers, not full dataset
- Better CPU utilization: Multiple phases running concurrently

**Inspiration**: Clojure's chunked lazy sequences

---

## Problem: Current Architecture Blocks Pipeline Parallelism

### Current Behavior

```go
type StreamingRelation struct {
    cache         []Tuple
    cacheReady    bool              // False until FIRST consumer COMPLETES
    cacheComplete chan struct{}     // Closed when first consumer DONE
}

// First consumer (builds cache)
it1 := rel.Iterator()
for it1.Next() {
    // Reads from storage, appends to cache
    process(it1.Tuple())
}
// Closes cacheComplete channel

// Second consumer (reads cache)
it2 := rel.Iterator()
// BLOCKS here until first consumer FINISHES
<-r.cacheComplete
for it2.Next() {
    // Reads from COMPLETE cache
    process(it2.Tuple())
}
```

**Problem**: Second consumer waits for **entire cache** to be built before starting.

### Impact on Multi-Phase Queries

```
Query: Pattern → Expression → Aggregation

Current (Sequential):
  Phase 1 (Pattern):    [==========] 100ms, 10K tuples
  Phase 2 (Expression): [==========] 100ms (WAITS for Phase 1 to finish)
  Phase 3 (Aggregation):[==========] 100ms (WAITS for Phase 2 to finish)
  Total: 300ms

Phase 2 could START as soon as Phase 1 produces its FIRST tuple, but doesn't!
```

**Wasted opportunity**: All cores idle while waiting for previous phase to complete.

---

## Proposed: Pace Car Architecture

### Core Concept

**First consumer is the "pace car"** - sets the pace of realization.
**Subsequent consumers "follow"** - read from cache as it grows, staying "close behind" the pace car.

```go
// Pace car (first consumer)
for paceIt := rel.Iterator(); paceIt.Next(); {
    tuple := readFromStorage()
    appendToCache(tuple)     // Cache grows incrementally
    signalFollowers()        // Wake up followers
    process(tuple)           // Pace car does its work
}

// Follower (subsequent consumer)
for followIt := rel.Iterator(); followIt.Next(); {
    if position < len(cache) {
        tuple := cache[position]  // Read from cache
        position++
    } else if paceCarActive {
        waitForCacheGrowth()      // Wait for pace car to add more
    } else {
        break  // Pace car done, cache complete
    }
    process(tuple)
}
```

**Key**: Followers don't wait for **completion**, they wait for **next chunk**.

### Multi-Phase Pipeline

```
Query: Pattern → Expression → Aggregation

Proposed (Pipeline):
  Phase 1 (Pattern):    [==========] 100ms
  Phase 2 (Expression):  ↑[==========] starts after first tuple
  Phase 3 (Aggregation):   ↑[==========] starts after first result

Timeline:
  0ms:   Phase 1 starts
  1ms:   Phase 1 produces first tuple → Phase 2 starts
  2ms:   Phase 2 produces first result → Phase 3 starts
  100ms: All phases complete

Total: ~102ms (vs 300ms sequential!)
Speedup: 3x for 3-phase query
```

**All phases run concurrently with bounded buffering.**

---

## Implementation Design

### Option 1: Per-Tuple Signaling (Simple but Inefficient)

```go
type StreamingRelation struct {
    cache         []Tuple
    cacheGrew     chan struct{}  // Signal on each append
    paceCarActive bool
    paceCarPos    int
    mu            sync.Mutex
}

type PaceCarIterator struct {
    inner  Iterator
    rel    *StreamingRelation
}

func (it *PaceCarIterator) Next() bool {
    if !it.inner.Next() {
        it.rel.paceCarActive = false
        close(it.rel.cacheGrew)
        return false
    }

    tuple := it.inner.Tuple()

    it.rel.mu.Lock()
    it.rel.cache = append(it.rel.cache, tuple)
    it.rel.paceCarPos++
    it.rel.mu.Unlock()

    // Signal followers (non-blocking)
    select {
    case it.rel.cacheGrew <- struct{}{}:
    default:
    }

    return true
}

type FollowerIterator struct {
    rel     *StreamingRelation
    pos     int
    current Tuple
}

func (it *FollowerIterator) Next() bool {
    for {
        it.rel.mu.Lock()

        // Can we read from cache?
        if it.pos < len(it.rel.cache) {
            it.current = it.rel.cache[it.pos]
            it.pos++
            it.rel.mu.Unlock()
            return true
        }

        // Is pace car done?
        if !it.rel.paceCarActive {
            it.rel.mu.Unlock()
            return false
        }

        it.rel.mu.Unlock()

        // Wait for more data
        _, ok := <-it.rel.cacheGrew
        if !ok {
            return false  // Channel closed, pace car done
        }
    }
}
```

**Pros**: Simple, easy to understand
**Cons**: High synchronization overhead (signal per tuple)

---

### Option 2: Chunked Signaling (Efficient, Clojure-style)

```go
const CACHE_CHUNK_SIZE = 32  // Clojure uses 32

type CacheChunk struct {
    tuples [CACHE_CHUNK_SIZE]Tuple
    count  int  // Actual tuples in chunk (last chunk may be partial)
}

type StreamingRelation struct {
    chunks        []CacheChunk
    chunkReady    chan int         // Signals chunk N is ready
    paceCarActive bool
    currentChunk  CacheChunk       // Pace car fills this
    chunkPos      int              // Position within current chunk
    mu            sync.Mutex
}

type PaceCarIterator struct {
    inner Iterator
    rel   *StreamingRelation
}

func (it *PaceCarIterator) Next() bool {
    if !it.inner.Next() {
        it.finalize()
        return false
    }

    tuple := it.inner.Tuple()

    it.rel.mu.Lock()

    // Add to current chunk
    it.rel.currentChunk.tuples[it.rel.chunkPos] = tuple
    it.rel.chunkPos++
    it.rel.currentChunk.count = it.rel.chunkPos

    // Chunk full? Commit it
    if it.rel.chunkPos >= CACHE_CHUNK_SIZE {
        it.commitChunk()
    }

    it.rel.mu.Unlock()
    return true
}

func (it *PaceCarIterator) commitChunk() {
    // Append chunk to chunks list
    chunkNum := len(it.rel.chunks)
    it.rel.chunks = append(it.rel.chunks, it.rel.currentChunk)

    // Reset current chunk
    it.rel.currentChunk = CacheChunk{}
    it.rel.chunkPos = 0

    // Signal followers (non-blocking)
    select {
    case it.rel.chunkReady <- chunkNum:
    default:
    }
}

type FollowerIterator struct {
    rel       *StreamingRelation
    chunkIdx  int
    tupleIdx  int
    current   Tuple
}

func (it *FollowerIterator) Next() bool {
    for {
        it.rel.mu.Lock()

        // Can we read from current chunk?
        if it.chunkIdx < len(it.rel.chunks) {
            chunk := it.rel.chunks[it.chunkIdx]
            if it.tupleIdx < chunk.count {
                it.current = chunk.tuples[it.tupleIdx]
                it.tupleIdx++
                it.rel.mu.Unlock()
                return true
            }
            // Chunk exhausted, move to next
            it.chunkIdx++
            it.tupleIdx = 0
            it.rel.mu.Unlock()
            continue
        }

        // Is pace car done?
        if !it.rel.paceCarActive {
            it.rel.mu.Unlock()
            return false
        }

        it.rel.mu.Unlock()

        // Wait for next chunk
        _, ok := <-it.rel.chunkReady
        if !ok {
            return false  // Channel closed
        }
    }
}
```

**Pros**:
- Low synchronization overhead (signal per 32 tuples)
- Better cache locality (32 tuples in array)
- Matches Clojure's proven design

**Cons**:
- More complex
- Slight latency (wait for chunk boundary)

**Recommended**: Option 2 (chunked)

---

## Performance Analysis

### Memory Usage

**Current (Block Until Complete)**:
```
Single iteration:  0 bytes (no cache if !shouldCache)
Multiple iteration: N tuples × sizeof(Tuple)

Example: 1M tuples × 1KB = 1GB cached forever
```

**Proposed (Pace Car)**:
```
Cache size ≈ (producer_rate - consumer_rate) × pipeline_depth

Example:
  Pace car: 10K tuples/sec
  Follower: 9K tuples/sec
  Difference: 1K tuples/sec
  Pipeline depth: 10 seconds

  Steady state cache: 10K tuples (not 1M!)

If follower catches up: Cache stops growing
If follower falls behind: Cache grows to accommodate
```

**Key advantage**: Memory scales with speed differential, not dataset size.

### Latency

**Current**:
```
Time to first result = Phase 1 complete + Phase 2 start
                     = 100ms + 0ms = 100ms
```

**Proposed**:
```
Time to first result = Phase 1 first tuple + Phase 2 first tuple
                     = 1ms + 1ms = 2ms

50x latency improvement!
```

### Throughput

**Current (Sequential)**:
```
Total time = sum(phase_times)
           = T1 + T2 + T3
           = 100ms + 100ms + 100ms = 300ms

Throughput = N tuples / 300ms
```

**Proposed (Pipeline)**:
```
Total time ≈ max(phase_times) + startup_overhead
           ≈ max(100ms, 100ms, 100ms) + 2ms = 102ms

Throughput = N tuples / 102ms

Speedup: 300ms / 102ms ≈ 3x for 3-phase query
```

**Scalability**: Speedup approaches number of phases (for balanced phases).

---

## Clojure Comparison

### Clojure's Chunked Sequences

```clojure
; Clojure realizes in chunks
(defn chunk-seq [coll]
  (lazy-seq
    (when-let [s (seq coll)]
      (let [chunk (chunk-first s)]  ; Realizes 32 elements
        (chunk-cons chunk
                    (chunk-seq (chunk-rest s)))))))

; Multiple consumers automatically synchronize
(let [s (range 1000000)]
  ; Consumer 1 (pace car)
  (future (doseq [x s] (process1 x)))

  ; Consumer 2 (follower)
  (future (doseq [x s] (process2 x)))

  ; Both run concurrently!
  ; Consumer 2 "chases" Consumer 1
  ; Realized chunks are shared
  ; Old chunks can be GC'd (head retention)
)
```

**What Clojure does automatically**:
1. ✓ Chunked realization (32 elements)
2. ✓ Concurrent consumers
3. ✓ Automatic synchronization
4. ✓ Head GC when no references

**What we'd need to implement manually**:
1. ✓ Chunked realization (we choose chunk size)
2. ✓ Concurrent consumers (pace car + followers)
3. ✓ Manual synchronization (channels)
4. ✓ Head GC (track all consumer positions, slice off head periodically)

### Key Difference: Head GC

**Clojure**: Automatic GC of realized chunks once all consumers pass them
```clojure
; If all consumers are past chunk 0:
;   → Chunk 0 becomes unreachable
;   → GC collects it automatically
;   → Memory freed

; Memory usage: N_chunks × chunk_size × (slowest - fastest consumer)
```

**Go (Our Implementation)**: Manual head GC via tracking + sealing
```go
type StreamingRelation struct {
    chunks        []CacheChunk
    chunkReady    chan int
    paceCarActive bool
    consumerPositions map[IteratorID]int  // Track each consumer's position
    sealed        bool                     // No more consumers will be created
    mu            sync.Mutex
}

// Must be called when all consumers have been created
func (r *StreamingRelation) Seal() {
    r.mu.Lock()
    r.sealed = true
    r.mu.Unlock()
}

// Creating iterator after Seal() is a programming error - panic immediately
func (r *StreamingRelation) Iterator() Iterator {
    r.mu.Lock()
    defer r.mu.Unlock()

    if r.sealed {
        panic("StreamingRelation.Iterator() called after Seal() - cannot create new consumers after sealing")
    }

    // ... normal iterator creation
}

// Periodically (e.g., every 100 chunks or on memory pressure):
func (r *StreamingRelation) gcHead() {
    r.mu.Lock()
    defer r.mu.Unlock()

    // Can't GC until sealed (don't know if more consumers coming)
    if !r.sealed {
        return
    }

    // Find the laggard (minimum position across all consumers)
    minPos := math.MaxInt
    for _, pos := range r.consumerPositions {
        if pos < minPos {
            minPos = pos
        }
    }

    // All consumers past this point, can GC
    if minPos > 0 {
        // Copy to new slice so backing array can be GC'd
        newChunks := make([]CacheChunk, len(r.chunks)-minPos)
        copy(newChunks, r.chunks[minPos:])
        r.chunks = newChunks

        // Adjust all consumer positions
        for id := range r.consumerPositions {
            r.consumerPositions[id] -= minPos
        }
    }
}

// Memory usage: Same as Clojure - bounded by speed differential
```

**Critical constraint**: Must call `Seal()` after all consumers are created, otherwise new consumers will try to read GC'd chunks.

**Our Architectural Advantage**: Unlike a general-purpose lazy sequence library, **Relations are internal implementation details**. The planner controls the entire execution graph and knows:
- Exactly how many consumers each relation will have
- When all consumers have been created
- The complete consumption pattern upfront

This means `Seal()` can be called **automatically** by the executor, not manually by user code. The query plan IS the factory for all consumers, so we have perfect knowledge.

**In practice**:
```go
// Executor creates relation and ALL its iterators atomically
inputRel := phases[n].Execute()

// Phase knows how many iterators it needs from the plan
for _, pattern := range phase.Patterns {
    iterators = append(iterators, inputRel.Iterator())
}

// Executor seals automatically - no more consumers possible
inputRel.Seal()

// Now GC can proceed safely
```

This is **much simpler** than exposing `Seal()` in a public API where you can't know consumption patterns. Our closed execution model makes head GC trivial.

### Fail-Fast Safety: Panic on Iterator After Seal

**Design decision**: Creating an iterator after `Seal()` is a **programming error**, not a runtime condition. We panic immediately:

```go
if r.sealed {
    panic("StreamingRelation.Iterator() called after Seal()")
}
```

**Why panic, not error?**

This is exactly like Go's `close()` behavior for channels:
```go
ch := make(chan int)
close(ch)
close(ch)  // PANIC: "close of closed channel"
```

Go doesn't return an error - it panics. Why? **Because it's a bug in your code, not a runtime condition.**

**Same reasoning here**:
- ✅ **Fail fast** - Catch bugs immediately at the point of error
- ✅ **Unambiguous** - No "maybe it works" - it either works or panics
- ✅ **Forces correctness** - Can't accidentally create race conditions or GC bugs
- ✅ **Matches Go idioms** - Consistent with channel closing behavior

If you try to create an iterator after sealing, you have a **bug in the executor**, not a query that's hard to execute. The panic will show up in tests immediately, not silently corrupt data in production.

---

## Use Cases and Benefits

### Use Case 1: OHLC Aggregation Queries

```
Query: Scan 1M price bars → Group by symbol → Aggregate OHLC

Current:
  Phase 1: Scan 1M bars (1 second)
  Phase 2: Group by symbol (WAITS 1 second, then 0.5 seconds)
  Phase 3: Aggregate OHLC (WAITS 1.5 seconds, then 0.2 seconds)
  Total: 1.7 seconds

Pace Car:
  Phase 1: Scan 1M bars (1 second)
  Phase 2: Group as bars arrive (starts at 1ms, runs for 1 second)
  Phase 3: Aggregate as groups arrive (starts at 2ms, runs for 1 second)
  Total: ~1 second

Speedup: 1.7x
```

### Use Case 2: Multi-Join Queries

```
Query: Table A → Join B → Join C → Filter

Current:
  Join A-B: 200ms (build hash table A, scan B)
  Join B-C: 300ms (WAITS 200ms, build hash table, scan C)
  Filter:   100ms (WAITS 500ms, then filter)
  Total: 600ms

Pace Car:
  Join A-B: Streams results as A×B matches found
  Join C:   Starts joining as soon as first A×B result available
  Filter:   Filters results as they arrive
  Total: ~300ms (overlapped execution)

Speedup: 2x
```

### Use Case 3: Streaming Dashboards

```
Scenario: Real-time dashboard querying 1M events

Current:
  User clicks "Refresh"
  → Wait 2 seconds for query to complete
  → Display results

Pace Car:
  User clicks "Refresh"
  → First results appear in 10ms
  → Results stream in over 2 seconds
  → User sees data immediately (perceived latency: 10ms vs 2000ms)

Perceived speedup: 200x!
```

---

## Implementation Roadmap

### Phase 1: Proof of Concept (1-2 weeks)

**Goal**: Validate chunked caching works with single producer, single consumer

**Tasks**:
1. Implement `CacheChunk` structure
2. Implement `PaceCarIterator` with chunk commits
3. Implement `FollowerIterator` with chunk reading
4. Add chunk signaling channel
5. Write tests:
   - Single pace car, single follower
   - Verify both iterate full dataset
   - Verify follower waits on chunk boundaries
   - Verify no data loss

**Success Criteria**: Tests pass, both iterators complete successfully

---

### Phase 2: Multi-Consumer Support (1 week)

**Goal**: Support multiple followers reading concurrently

**Tasks**:
1. Update signaling to broadcast (all followers notified)
2. Handle race conditions (multiple followers reading same chunk)
3. Add tests:
   - Single pace car, multiple followers
   - Verify all followers get full dataset
   - Verify concurrent reads are safe

**Success Criteria**: N followers can read concurrently without corruption

---

### Phase 3: Cancellation and Error Handling (1 week)

**Goal**: Handle early termination and errors gracefully

**Tasks**:
1. Implement pace car cancellation (if all followers stop)
2. Implement follower cancellation (LIMIT clause)
3. Propagate errors from pace car to followers
4. Add tests:
   - Follower stops early (LIMIT)
   - Pace car errors mid-stream
   - All followers stop (pace car should stop)

**Success Criteria**: No goroutine leaks, errors propagate correctly

---

### Phase 4: Integration with Executor (2 weeks)

**Goal**: Enable pipeline parallelism in multi-phase queries

**Tasks**:
1. Update `Materialize()` to enable pace car mode
2. Update executor to NOT materialize between phases
3. Add executor option: `EnablePipelineParallelism`
4. Benchmark multi-phase queries
5. Compare against current sequential execution

**Success Criteria**:
- Multi-phase queries show speedup
- Memory usage bounded (not growing indefinitely)
- No correctness regressions

---

### Phase 5: Optimization (1-2 weeks)

**Goal**: Tune for production performance

**Tasks**:
1. Experiment with chunk sizes (16, 32, 64, 128)
2. Profile synchronization overhead
3. Optimize hot paths (tuple copying, locking)
4. Add adaptive chunk sizing based on tuple size
5. Consider lock-free ring buffer for chunks

**Success Criteria**:
- Minimal overhead vs sequential execution
- Speedup demonstrated on real queries

---

### Phase 6: Head GC

**Goal**: Free cache chunks once all consumers pass them

**Tasks**:
1. Add `consumerPositions map[IteratorID]int` and `sealed bool` to StreamingRelation
2. Add `Seal()` method to mark "no more consumers will be created"
3. Track each iterator's position on Next() calls
4. Implement `gcHead()` to find laggard and slice off consumed chunks
5. Call gcHead() periodically (e.g., every 100 chunks, or on memory pressure)
6. Update executor to call `Seal()` after creating all iterators for a phase
7. Add iterator registration/deregistration for tracking
8. Add tests:
   - Verify chunks are GC'd once all consumers pass
   - Verify memory doesn't grow indefinitely with slow consumer
   - Verify position tracking correctness
   - **Verify Seal() prevents GC until called**
   - **Verify Iterator() after Seal() panics (not error) - fail-fast safety**
   - Verify panic message is clear and actionable

**Success Criteria**: Long-running queries with slow consumers maintain bounded memory

**Critical**: The `Seal()` call is essential - GC cannot proceed until we know no more consumers will be created. In query execution, this happens naturally when a phase creates all its iterators from the input relation.

---

## Risks and Mitigations

### Risk 1: Increased Complexity

**Risk**: Chunked caching more complex than current block-until-complete

**Mitigation**:
- Encapsulate complexity in iterator types
- Hide behind existing `Materialize()` API
- Make opt-in via `EnablePipelineParallelism` flag
- Extensive testing and documentation

### Risk 2: Synchronization Overhead

**Risk**: Per-chunk signaling might be expensive

**Mitigation**:
- Benchmark different chunk sizes
- Non-blocking signal sends (don't wait if follower not ready)
- Consider lock-free alternatives for hot paths

### Risk 3: Memory Growth

**Risk**: If follower is very slow, cache grows large

**Mitigation**:
- Monitor cache size, log warnings
- Add max cache size limit (error if exceeded)
- Document that slow consumers can cause memory pressure
- Future: Implement head GC

### Risk 4: Correctness Bugs

**Risk**: Concurrent access to cache chunks could corrupt data

**Mitigation**:
- Chunks are immutable once committed
- Pace car exclusively writes to `currentChunk`
- Followers read from `chunks` slice (append-only)
- Extensive concurrency testing
- Leverage existing CachingIterator correctness

### Risk 5: Iterator After Seal

**Risk**: Executor might try to create iterator after sealing (programming bug)

**Mitigation**:
- **Deliberate panic** - Fail fast like `close()` on closed channel
- Clear panic message: "Iterator() called after Seal()"
- Caught immediately in tests, not silently in production
- Consistent with Go idioms (panics for programming errors)
- **This is not a bug risk** - it's a **bug detection feature**

---

## Alternatives Considered

### Alternative 1: Fully Materialize Upfront

**Approach**: Always materialize before any consumer starts

**Pros**: Simple, no synchronization complexity
**Cons**: No pipeline parallelism, high latency, high memory

**Decision**: Rejected - defeats purpose of streaming

---

### Alternative 2: Lock-Free Ring Buffer

**Approach**: Use lock-free ring buffer instead of slice + mutex

**Pros**: Lower overhead, better throughput
**Cons**: Fixed buffer size, complex implementation, risk of deadlock if buffer full

**Decision**: Defer - start with mutex-based, optimize if needed

---

### Alternative 3: Copy Cache Per Consumer

**Approach**: Each consumer gets independent copy of cache

**Pros**: No synchronization needed
**Cons**: N× memory usage, defeats purpose of caching

**Decision**: Rejected - memory usage unacceptable

---

## Success Metrics

**Correctness**:
- [ ] All existing tests pass
- [ ] New concurrency tests pass (no data loss, no corruption)
- [ ] Differential validation tests pass (same results as current)

**Performance**:
- [ ] Multi-phase query speedup: ≥1.5x on 3-phase queries
- [ ] Latency improvement: First result ≤10ms (vs 100ms+ sequential)
- [ ] Memory bounded: Cache ≤2× steady-state differential
- [ ] Throughput: No regression on single-phase queries

**Usability**:
- [ ] Opt-in via flag (existing queries unchanged)
- [ ] Clear documentation
- [ ] Performance characteristics documented
- [ ] Error messages helpful

---

## Architectural Advantage: Closed Execution Model

**Key Insight**: Relations and iterators are **internal implementation details**, not public API. This gives us a massive advantage over general-purpose lazy sequence libraries.

### What We Know (That Libraries Don't)

**The planner has complete knowledge**:
```go
// Planner produces the execution graph
plan := planner.Plan(query)

// For each relation, planner knows:
// - How many patterns will consume it
// - How many iterators will be created
// - When all iterators have been created
// - The complete consumption pattern upfront
```

**General-purpose library** (like Clojure's lazy-seq):
```clojure
; User can create consumers at any time
(def s (range 1000000))
(def c1 (future (doseq [x s] ...)))  ; First consumer
; ... hours later ...
(def c2 (future (doseq [x s] ...)))  ; New consumer! Can't GC head!
```

**Our closed model**:
```go
// Executor creates ALL consumers atomically
func (e *Executor) ExecutePhase(phase *Phase, inputRel Relation) Relation {
    // Create exactly N iterators (known from phase plan)
    iterators := make([]Iterator, len(phase.Patterns))
    for i := range phase.Patterns {
        iterators[i] = inputRel.Iterator()
    }

    // Seal automatically - no more consumers POSSIBLE
    inputRel.Seal()

    // Execute with knowledge that consumption graph is complete
    return e.execute(iterators)
}
```

### Why This Matters

| Challenge | General Library | Our Closed Model |
|-----------|----------------|------------------|
| **Sealing** | User must call `Seal()` manually | Executor seals automatically |
| **Safety** | User can forget to seal → leak | Impossible to forget - built into executor |
| **Head GC** | Conservative (can't know if more consumers) | Aggressive (perfect knowledge) |
| **Complexity** | Exposed in API | Hidden implementation detail |

**Result**: We can implement head GC more simply and safely than a general-purpose library because we control the entire consumption graph.

---

## Conclusion

The pace car caching architecture brings us **much closer to Clojure's lazy-seq semantics** by enabling true pipeline parallelism with bounded memory usage.

**What we gain**:
1. ✅ Pipeline parallelism (3x speedup potential on 3-phase queries)
2. ✅ Low latency (first results in milliseconds)
3. ✅ Bounded memory (grows with speed differential, not dataset size)
4. ✅ Better CPU utilization (all cores active)
5. ✅ Head GC (automatic sealing via closed execution model)
6. ✅ **Perfect consumption knowledge** (planner controls entire graph)

**What we still miss vs Clojure**:
1. ❌ Language-level chunking (manual implementation)
2. ❌ Zero-overhead abstraction (some synchronization cost)

**But**: For production Go code, this is a **massive improvement** over current sequential execution. Our closed execution model actually gives us **advantages over general-purpose libraries** - we can implement head GC more safely because we control the consumption graph.

**Recommendation**: Implement in phases, starting with proof of concept. This is the right direction for enabling true streaming with pipeline parallelism.

---

## References

- **Current Implementation**: `datalog/executor/relation.go` (CachingIterator)
- **Clojure Chunked Sequences**: https://clojure.org/reference/sequences#_chunked_sequences
- **Related Proposal**: `docs/LAZY_MATERIALIZATION_PLAN.md`
- **Branch Context**: `fix-buffered-iterator-architecture` (29 commits to streaming correctness)
