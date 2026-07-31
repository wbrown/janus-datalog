# Distributed Janus Datalog

**Status:** Proposal
**Author:** wbrown
**Date:** 2026-01-29
**Prerequisite:** [CRDT_VECTOR_STORAGE.md](CRDT_VECTOR_STORAGE.md) (CRDT-Based Storage Model)

---

## Abstract

This proposal outlines a phased approach to evolving Janus Datalog from a single-process embedded database to a distributed, horizontally-scalable system. The CRDT-based storage model provides the foundation—Lamport clocks enable conflict-free merging, and content-addressed datom sharding provides natural load distribution.

---

## Motivation

Janus Datalog currently uses BadgerDB as a single-process, single-host backend. This works well for development and moderate-scale production, but has inherent limitations:

1. **No resilience** - Single point of failure; node loss means downtime
2. **Vertical scaling only** - Limited by single-node memory/disk/CPU
3. **No read scaling** - All queries hit the same storage
4. **Terabyte barrier** - Single BadgerDB instance practical limit ~500GB-1TB

### Why Not Use an Existing Distributed Database?

| Option | Problem |
|--------|---------|
| **FoundationDB** | 10KB key size limit; our V (value) component can exceed this for lore text |
| **TiKV** | Viable, but adds operational complexity and external dependency |
| **CockroachDB** | Overkill; we don't need SQL |
| **Datomic** | Closed source, expensive, JVM-only, single-transactor bottleneck, no vectors |

Building on our CRDT storage model with content-addressed sharding gives us:
- No key size limits
- Conflict-free concurrent writes (no coordination needed)
- Natural load distribution
- Native vector support (Datomic doesn't have this)

---

## Foundation: CRDT Storage Model

The CRDT-based storage model (see [CRDT_VECTOR_STORAGE.md](CRDT_VECTOR_STORAGE.md)) provides the distributed foundation:

**ElementID (Lamport, NodeID):**
- Every write gets a unique `ElementID(Lamport, NodeID)`
- Lamport clocks provide causal ordering without synchronized clocks
- NodeID breaks ties deterministically
- Conflict resolution is automatic—no coordination needed

**Append-only writes:**
- No retractions, only appends with new ElementID
- Cardinality-one: highest ElementID wins
- Cardinality-many: add-wins semantics with tombstones
- Cardinality-vector: RGA merge

**Unified cache:**
- Caches resolved CRDT views per (E, A)
- Freshness checked via MaxElementID
- Invalidated on local writes, lazily refreshed

---

## Architecture

### Sharding by Datom Identity

Sharding is based on the cryptographic identity of the datom itself, not the entity:

```
DatomIdentity = Hash(E, A, I, V)
Shard = DatomIdentity % NumShards
```

**Why datom identity, not entity:**

| Aspect | Entity Sharding | Datom Identity Sharding |
|--------|-----------------|-------------------------|
| Load distribution | Hot entities = hot shards | Natural distribution |
| Entity size limits | Large entities strain one shard | No limits |
| Routing | Need entity→shard mapping | Just hash the datom |
| Write coordination | Entity-local | None (CRDT merge) |

**Why this works:**
- CRDTs don't need locality for consistency
- ElementID provides deterministic ordering regardless of storage location
- Scatter-gather collects datoms, CRDT resolution produces the answer
- Resolved views are cached—subsequent reads are O(1)

### Cluster Topology

```
┌─────────────────────────────────────────────────────────────────┐
│                        Query Router                              │
│   (Hash datom identity, scatter-gather, CRDT merge, cache)       │
└───────────┬─────────────────┬─────────────────┬─────────────────┘
            │                 │                 │
     ┌──────▼──────┐   ┌──────▼──────┐   ┌──────▼──────┐
     │  Shard 0    │   │  Shard 1    │   │  Shard 2    │
     │ Hash mod 3=0│   │ Hash mod 3=1│   │ Hash mod 3=2│
     ├─────────────┤   ├─────────────┤   ├─────────────┤
     │ Raft Group  │   │ Raft Group  │   │ Raft Group  │
     │ (3 replicas)│   │ (3 replicas)│   │ (3 replicas)│
     └─────────────┘   └─────────────┘   └─────────────┘
```

Each shard is a Raft group for durability. Raft provides replication, not ordering—Lamport clocks handle ordering.

---

## Write Path

### Local Writes

```go
func (n *Node) Write(datoms []Datom) error {
    for i := range datoms {
        // Assign ElementID with local Lamport clock
        datoms[i].ElementID = n.nextElementID()
    }

    // Group by shard
    bySherd := make(map[int][]Datom)
    for _, d := range datoms {
        shard := hash(d.E, d.A, d.ElementID, d.V) % n.numShards
        byShard[shard] = append(byShard[shard], d)
    }

    // Send to shards in parallel (Raft replication)
    var wg sync.WaitGroup
    for shardID, shardDatoms := range byShard {
        wg.Add(1)
        go func(id int, ds []Datom) {
            defer wg.Done()
            n.shards[id].Append(ds)  // Raft replicated append
        }(shardID, shardDatoms)
    }
    wg.Wait()

    // Invalidate local cache
    n.cache.Invalidate(touchedKeys(datoms))
    return nil
}

func (n *Node) nextElementID() ElementID {
    n.lamport++
    return ElementID{Lamport: n.lamport, NodeID: n.nodeID}
}
```

### Lamport Clock Synchronization

When receiving datoms from other nodes:

```go
func (n *Node) onReceive(datoms []Datom) {
    // Update Lamport clock: L = max(L, L_remote) + 1
    for _, d := range datoms {
        if d.ElementID.Lamport > n.lamport {
            n.lamport = d.ElementID.Lamport
        }
    }
    n.lamport++

    // Apply to local storage
    n.store.Append(datoms)
    n.cache.Invalidate(touchedKeys(datoms))
}
```

This ensures causal ordering: if A caused B, then `Lamport(A) < Lamport(B)`.

---

## Read Path

### Scatter-Gather with CRDT Merge

```go
func (n *Node) Pull(entity Identity) (*Entity, error) {
    // Check cache first
    if cached, ok := n.cache.Get(entity); ok {
        return cached, nil
    }

    // Scatter: request all datoms for entity from all shards
    var allDatoms []Datom
    var mu sync.Mutex
    var wg sync.WaitGroup

    for _, shard := range n.shards {
        wg.Add(1)
        go func(s *Shard) {
            defer wg.Done()
            datoms := s.ScanEntity(entity)  // Returns all datoms where E = entity
            mu.Lock()
            allDatoms = append(allDatoms, datoms...)
            mu.Unlock()
        }(shard)
    }
    wg.Wait()

    // CRDT merge: resolve current values per attribute
    resolved := n.crdtResolve(allDatoms)

    // Cache the resolved view
    n.cache.Put(entity, resolved)

    return resolved, nil
}

func (n *Node) crdtResolve(datoms []Datom) *Entity {
    // Group by attribute
    byAttr := groupByAttribute(datoms)

    result := &Entity{}
    for attr, attrDatoms := range byAttr {
        switch n.schema.Cardinality(attr) {
        case CardinalityOne:
            // Highest ElementID wins
            result.Set(attr, highestElementID(attrDatoms).V)

        case CardinalityMany:
            // Add-wins set resolution
            result.Set(attr, resolveAddWins(attrDatoms))

        case CardinalityVector:
            // RGA reconstruction
            result.Set(attr, reconstructRGA(attrDatoms))
        }
    }
    return result
}
```

### Query Optimization

| Query Pattern | Strategy |
|---------------|----------|
| `[entity :attr ?v]` | Scatter-gather for entity, resolve, return attr |
| `[?e :attr value]` | Scatter-gather AVET index, dedupe entities |
| `[?e :attr ?v]` | Full scatter, stream results |

The cache amortizes scatter-gather cost. After first access, reads are O(1) until invalidation.

---

## Raft for Durability

Each shard uses Raft for replication, not ordering:

```go
type ShardFSM struct {
    store *BadgerStore
}

func (f *ShardFSM) Apply(log *raft.Log) interface{} {
    var datoms []Datom
    msgpack.Unmarshal(log.Data, &datoms)

    // Just append—no conflict resolution here
    // CRDT resolution happens at read time
    return f.store.Append(datoms)
}
```

**Read consistency options:**

| Mode | Latency | Consistency | Use Case |
|------|---------|-------------|----------|
| Local | ~100µs | Eventually consistent | Most reads |
| Quorum | ~2ms | Majority agreement | Critical reads |

With CRDTs, "eventually consistent" is well-defined—all nodes converge to the same resolved values.

---

## Comparison with Datomic

| Aspect | Datomic | Distributed Janus |
|--------|---------|-------------------|
| **Write scaling** | Single transactor (bottleneck) | Any node accepts writes (CRDT) |
| **Conflict resolution** | Last-write-wins (lossy) | CRDT merge (lossless) |
| **Vectors** | Not supported | Native with RGA |
| **Sharding** | None (single transactor) | Content-addressed datoms |
| **Coordination** | Required for writes | None (Lamport clocks) |
| **License** | Proprietary ($5K+/year) | Open source |
| **Runtime** | JVM | Native Go |

---

## Implementation Roadmap

### Phase 1: Multi-Node Replication
- [ ] Implement Lamport clock in Transaction
- [ ] Add NodeID configuration
- [ ] Implement datom identity hashing
- [ ] Add Hashicorp Raft for shard replication
- [ ] Implement scatter-gather query router
- [ ] Integration tests with node failures

### Phase 2: Production Hardening
- [ ] Shard rebalancing (add/remove nodes)
- [ ] Snapshot and recovery
- [ ] Metrics and observability
- [ ] Deployment documentation

### Phase 3: Performance Optimization
- [ ] RDMA transport for low-latency scatter-gather
- [ ] Bloom filters for entity→shard hints
- [ ] Predictive cache warming

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Scatter-gather latency | Cache resolved views; most reads hit cache |
| Lamport clock drift | Sync on every message; bounded by max seen |
| Large entity scatter | Bloom filters hint which shards have datoms |
| Network partitions | Raft quorum; CRDT merge on reconnect |

---

## References

- [Hashicorp Raft](https://github.com/hashicorp/raft) - Go Raft implementation
- [Lamport Clocks](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) - Original paper
- [RGA: Replicated Growable Array](https://hal.inria.fr/inria-00555588/document) - CRDT for vectors
- [CRDT Primer](https://crdt.tech/) - Background on conflict-free data types
