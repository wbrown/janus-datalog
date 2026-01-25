# Janus Indexing: A Game Engine Programmer's Guide

**TLDR for people who know ECS/spatial structures but not databases.**

## The Core Idea

Imagine you have a huge pile of facts (datoms), each with 4 components:
```
[Entity, Attribute, Value, Transaction]
[player_42, :health, 100, tx_999]
[sword_7, :damage, 50, tx_888]
```

You want to query these efficiently:
- "What are all attributes of entity X?" (loading a game object)
- "What entities have health > 50?" (finding targets)
- "What entities reference item Y?" (finding who's holding a weapon)

**Problem**: Different queries need different orderings. There's no single sort that's optimal for everything.

**Solution**: Store the same data sorted 5 different ways. Pick the best one per query.

---

## The 5 Indices (Think: 5 Different Acceleration Structures)

| Index | Sort Order | Game Engine Analogy |
|-------|------------|---------------------|
| **EAVT** | Entity → Attr → Val → Tx | Octree: "Give me everything in this spatial cell" |
| **AEVT** | Attr → Entity → Val → Tx | Component array: "Give me all Health components" |
| **AVET** | Attr → Val → Entity → Tx | Inverted index: "Find all entities with Health=100" |
| **VAET** | Val → Attr → Entity → Tx | Reverse refs: "Who's referencing this entity?" |
| **TAEV** | Tx → Attr → Entity → Val | Timeline: "What changed this frame/tick?" |

**Key insight**: This is like having both an octree AND a BVH AND a grid AND a kd-tree. Each structure is optimal for certain queries. You don't traverse all of them—you pick the right one.

---

## Fixed-Size Keys (Think: Vertex Buffer Stride)

Every index key is **72 bytes fixed**, laid out like a vertex buffer:

```
┌─────────────────────────────────────────────────────────┐
│ E: 20 bytes │ A: 32 bytes │ V: variable │ Tx: 20 bytes │
└─────────────────────────────────────────────────────────┘
     ↑              ↑              ↑            ↑
   Entity ID    Attribute      Value      Transaction
  (SHA1 hash)   (keyword)     (any type)    (timestamp)
```

**Why this matters**:
- Like GPU vertex attributes at fixed offsets
- Can jump directly to any component without parsing
- Range scans are predictable memory accesses
- No pointer chasing

---

## L85 Encoding (Think: Normalized Coordinates That Sort Correctly)

Entities and transactions are SHA1 hashes (20 raw bytes). We encode them as text using **L85**—a custom Base85 that **preserves sort order**.

```
Raw bytes:    0x00 < 0x01 < 0xFF
L85 encoded:  "!"  < "$"  < "}"
```

**Why not just use raw bytes?**
- Debuggable: You can print keys, log them, paste them
- Safe: No binary escaping issues in JSON/URLs/configs
- Sorted: Lexicographic string comparison = byte comparison

**Game analogy**: Like using normalized device coordinates (NDC) where -1 to +1 maps cleanly to screen space, but here the mapping preserves comparison operators.

---

## Prefix Scans (Think: Frustum Culling for Data)

Queries work by **prefix matching**. The key ordering makes this a range scan:

```
Query: "All attributes of entity X"
Index: EAVT (sorted by Entity first)

Keys in storage (conceptually):
  [A, attr1, val, tx]  ← skip
  [B, attr1, val, tx]  ← skip
  [X, attr1, val, tx]  ← MATCH
  [X, attr2, val, tx]  ← MATCH
  [X, attr3, val, tx]  ← MATCH
  [Y, attr1, val, tx]  ← stop

Scan range: start="X" end="X+1" (exclusive)
```

**Game analogy**: Like a frustum cull—you define min/max bounds and only iterate what's inside. No need to check every datom.

---

## Index Selection (Think: Choosing Your Accel Structure)

When executing a query pattern like `[?e :health ?v]`:
- **:health is bound** → Use AEVT (sorted by Attribute)
- Creates prefix scan over all entities with that attribute

Pattern `[player_42 ?a ?v]`:
- **Entity is bound** → Use EAVT
- Scans all attributes of that entity

Pattern `[?e :assigned-to weapon_7]`:
- **Attribute and Value bound** → Use AVET
- Finds all entities with that attribute-value pair

**The runtime picks the best index automatically based on which variables are bound.**

---

## Why It's Fast (Summary)

| Optimization | Game Equivalent |
|--------------|-----------------|
| 5 pre-sorted indices | Multiple acceleration structures |
| Fixed 72-byte keys | Fixed stride vertex buffers |
| L85 sort-preserving encoding | Normalized coords that compare correctly |
| Prefix range scans | Frustum culling on sorted data |
| Key-only iteration | No fetching—data IS the index |
| Index auto-selection | Picking octree vs BVH per query |

---

## Quick Reference: Which Index for Which Query

```
"Get all of entity X"           → EAVT (Entity first)
"Get all :health values"        → AEVT (Attribute first)
"Find entities where :hp = 100" → AVET (Attr+Value first)
"Who references entity Y?"      → VAET (Value first)
"What changed in transaction T?"→ TAEV (Transaction first)
```

---

## The ECS Parallel

If you're used to ECS:
- **Datom** = Component instance + metadata
- **Entity** = Entity ID
- **Attribute** = Component type
- **AEVT index** = Archetype storage (all entities with same components together)
- **EAVT index** = Entity lookup table

The key difference: Janus stores *every* attribute change as an immutable fact with a transaction timestamp. It's append-only, like a git history for your game state.
