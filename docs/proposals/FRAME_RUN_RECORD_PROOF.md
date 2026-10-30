# The frame's record of keys run holds its keys in the run's tuples, as a proof

## Sources

- The code: `datalog/executor/window_frame.go` (`windowFrame`, `windowFrames`, `windowRun`), `datalog/executor/windowed_form.go` (`newWindowedIterator`, `runWindow`), `datalog/executor/kept_branch.go` (`buildBranchTuples`, `collectBranch`), `datalog/executor/tuple_key.go` (`TupleKeyMap`, `PutIfAbsentPositions`, `hashTuplePositions`, `tupleValuesEqualPositions`), `datalog/executor/join_index.go` (`JoinIndex`, `tupleKeysMatch`), `datalog/executor/relation.go` (`newMaterializedRelationFromSet`).
- The scope design: `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §4.2, a frame's run over a window is the window's distinct keys less those an earlier window ran; §4.3, the frames module; §4.6, `JoinIndex`'s contracts. `docs/proposals/OR_FALLBACK_RELATION_PROOF.md` L2, the frames.
- The record: `docs/proposals/TUPLEKEYMAP_OPTIMIZATION.md`, a set that stores no value. `docs/proposals/BINDING_MATCH_PROOF.md` P1, a relation is never written after construction.
- The failure: the allocation profile of the reproduction of `docs/bugs/JANUS_REPRO_FILLED.md`, its `unfilled` query through the CLI, attributes its largest share of bytes to the record's writes: an owned values slice and a map entry per key run, beside the key tuple the run holds, and the run slice's growth. `PERFORMANCE_STATUS.md` carries the figures.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- **F** is a frame: its required symbols, its subject positions **S**, the frame positions **P** of each entry of S, and its environment values. S and P are parallel: the subject's value at S[i] stands at the frame's position P[i].
- The **key** of a subject tuple t is the list of t's values at S, in S's order.
- The **key tuple** of t is the tuple over F's required symbols holding the environment's values and, at each P[i], t[S[i]].
- The **record** R is the keys F's windows have run.
- A key is **run** when a window's run carried its key tuple.
- A **member** of a set is a tuple the set holds as given.
- The **set** is `TupleSet`: members held in place, each member's key read at its stored positions, probed by a tuple at the probing positions, through an open-addressed table of member indices, a power of two in length and at most half full, probed linearly from the key's hash's slot. The table doubles when full, and the members are given the capacity the table admits before its next doubling. A member's hash is computed from the member whenever the table is built.

## 2. Preconditions

- **P1 (the record today).** `windowRun` puts each window tuple into a `TupleKeyMap` at S. On a key absent from the map, `PutIfAbsentPositions` allocates an owned slice of the key's values and a map entry holding it and a stored value. `windowRun` then builds the key tuple and appends it to the run.
- **P2 (the key tuple carries the key).** For every i, the key tuple's value at P[i] is t[S[i]]. So the key tuple read at P is t's key.
- **P3 (the hash and the equality).** `hashTuplePositions` folds a tuple's values at the given positions in order, so the key tuple hashed at P equals t hashed at S. `tupleKeysMatch` compares a held tuple at its positions with a probing tuple at its positions, value by value, so a key tuple compared at P with t at S is the equality of their keys.
- **P4 (one reader).** `windowRun` alone reads R, by membership. The stored value, `true`, is read by nothing.
- **P5 (a key tuple is written once).** The key tuple is written in `windowRun` and never after. The run holds it, the branch's plan reads the run, and no reader of a relation writes a tuple (`BINDING_MATCH_PROOF.md` P1).
- **P6 (lifetime).** `windowFrames` creates F and R at `newWindowedIterator`, once per form execution, and they die with it.
- **P7 (`JoinIndex`'s layout).** `JoinIndex` holds its tuples in place, reads each key from the tuple at the stored positions, maps each hash to its first group, chains the groups under one hash, and allocates nothing on a probe. Its `Add` allocates one group holding one tuple per new key.
- **P8 (the run slice).** `windowRun` appends the run's tuples to a nil slice. A window's run holds at most one tuple per window tuple.

## 3. Theorems

**T1 (membership).** A record that holds the key tuples of the keys run, probed by a subject tuple at S and read at P, answers that a key is run exactly when the map answered it.

**T2 (the run).** Each window's run is the same tuples in the same order as before.

**T3 (allocation).** Per key new to the record, the record allocates nothing beside the key tuple the run holds, other than the set's own growth. Per key run before, it allocates nothing. The run slice is allocated once per window.

**T4 (nothing else changes).** The kept branch, the form's policy and the continuations read each window's run as before. The record's lifetime is unchanged.

**T5 (the set's contracts).** The set holds one member per distinct key. Two members under one hash are told apart by their keys' values. A probe allocates nothing. A member is held as given, never copied, so it is stable for the set's life.

## 4. Lemmas

### L1. The key tuple is the record's member (T1)

By P2 the key tuple read at P is t's key, and by P3 its hash at P is t's hash at S and its equality at P with a probing tuple at S is the equality of keys. So a set holding the key tuples, with P as the stored positions and S as the probing positions, holds each key run under the hash and the equality the map held it under. A probe of a later window tuple finds a member exactly when the map found an entry. ∎

### L2. The run is unchanged (T2)

`windowRun` walks the window in order, skips a tuple whose key the record holds, and otherwise builds the key tuple, records it and appends it. By L1 the skip decision is the map's, so the same tuples are built in the same order. The run relation holds the same key tuples. ∎

### L3. The allocations (T3)

The key tuple is built for every new key as before. The set holds it in place (P7's layout), so no values slice and no entry holding a value are allocated. The set's growth is its member and chain slices and its hash map. A key the record holds is answered by a probe, which allocates nothing. The run slice sized to the window holds every tuple the window can contribute (P8) in one allocation. ∎

### L4. Nothing else changes (T4)

By P4 nothing but `windowRun` reads R. By L2 the run each consumer receives is unchanged. By P5 holding the key tuples in R beside the run changes nothing a reader sees. By P6 R lives as long as before. ∎

### L5. The set (T5)

The set's `Add` holds the member as given and writes its index into the first empty slot at or after its key's hash's, after the caller's probe found no member with its key, so each distinct key has one member. A probe walks the slots from the probing tuple's hash's until an empty one, comparing keys at the two position lists, so two keys under one hash sit in two slots and are told apart. A member is placed again at every doubling, by the hash computed from it, so the table always holds every member. A probe reads the table and the members and allocates nothing. A member is never copied. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named.

- `TestTupleSet_HoldsOneMemberPerKeyProbedByThePositionsGiven`, `TestTupleSet_TwoKeysUnderOneHashAreToldApart`, `TestTupleSet_ProbeAllocatesNothing`, `TestTupleSet_AMemberIsHeldAsGiven` and `TestTupleSet_AddAllocatesOnlyItsGrowth` (executor, new) (T3, T5).
- `TestWindowFrame_WindowRunIsTheWindowsDistinctKeysLessThoseRunBefore`, `TestWindowFrame_WindowRunDistinguishesKeysByTypedValue` and `TestWindowFrames_ResolvesAFrameSymbolByTheSymbolItStandsFor` (executor), run unchanged (T1, T2).
- The scope contract tests of `datalog/executor` and `datalog/storage`, run unchanged (T4).

## 6. The change the proof licenses

- `datalog/executor/tuple_set.go` (new): `TupleSet`, as §1 defines it, with `NewTupleSet`, `Probe`, `Add` and `Len`.
- `datalog/executor/window_frame.go`: the frame's record is a `TupleSet` probed at S and stored at P; `windowRun` probes each window tuple, builds the key tuple of a key not run, adds it to the record and appends it to a run slice sized to the window.
- `docs/proposals/TUPLEKEYMAP_OPTIMIZATION.md`: names the set the tree now carries and its first reader.
- The tests of §5.

## 7. What the proof does not cover

- The window's own tuples, copied by the reader when the subject reuses its workspace.
- `DedupIterator`'s key map, the first target the record names, which still stores a value per key.
- The fallback's record of the keys its branches answered, which is the policy's and not the frame's.
- The per-window key map the reader builds for the window event's distinct key count under a handler.
- The members' retention. The set holds every key tuple for the form execution, as the map held every key's values.

## 8. Alternatives excluded

- `JoinIndex` as the record. It holds each key in a group of one tuple, and the group's slice is one allocation per key.
- A Go map from each key's hash to its member's index, with a chain for the keys under one hash. Its tables double from empty in every form execution, each slot carrying a value and control bytes beside its key, and that growth was the record's largest cost on the reproduction's allocation profile, more than the keys themselves. The table of member indices, four bytes a slot, is what remains of it.
- The members' hashes kept beside them, so that a doubling places every member without hashing it again. They were a part of the growth the profile attributed to the record. Without them a doubling hashes each member once more.
- `TupleKeyMap` with the key tuple as its entry's values. Its equality reads the stored values in the probing positions' order, so the key tuple would have to be laid out in S's order, which the environment's values and P break.
- The record elided under a key of the subject within the required run. The subject of the failure carries no such key, and the record is needed wherever keys repeat.
