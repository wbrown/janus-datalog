# Live Query Subscriptions (`QueryAndSubscribe`)

**Status:** Proposal — semantic contract and staged design; build pending
**Author:** wbrown (design), drafted with Claude
**Date:** 2026-07-12
**Builds on:**
- [../INPUT_PARAMETER_SEMANTICS.md](../INPUT_PARAMETER_SEMANTICS.md) — query inputs as execution environment
- [../reference/CRDT.md](../reference/CRDT.md) — resolved latest-state semantics
- [../reference/MULTI_SOURCE.md](../reference/MULTI_SOURCE.md) — source routing and matcher composition
- [BRANCHING_AND_SNAPSHOTS.md](BRANCHING_AND_SNAPSHOTS.md) — pinned bases and gap-free snapshot reasoning
- [../ideas/OPTIMIZATION_OPPORTUNITIES.md](../ideas/OPTIMIZATION_OPPORTUNITIES.md) — property-aware execution and compositional planning

---

## Abstract

Applications using Janus can query current state, but they have no declarative
way to learn when that result changes. They either poll, maintain parallel
application-side indexes/caches, subscribe to domain-specific events, or
invalidate broad regions of UI/application state after every transaction. All
four approaches duplicate knowledge already present in the query.

This proposal adds a live-query API:

```text
initial result at a pinned basis W
then ordered result deltas for commits after W
```

A subscriber provides an ordinary Datalog query and inputs. Janus executes it
once, returns the current materialized relation, derives a dependency signature
from the query, and watches committed transactions that could affect the
result. When a relevant transaction commits, Janus reevaluates at the new
committed basis, diffs the old and new relations with typed Datalog equality,
and emits added/removed rows only when the resolved result actually changed.

The first implementation deliberately uses dependency invalidation plus full
query reevaluation. It is semantically clean, useful for many real workloads,
and establishes measurements before Janus attempts incremental maintenance of
joins, aggregates, negation, fallback, or Top-N.

The core correctness requirement is gap-free registration:

```text
register + capture basis
    → execute initial query at that basis
    → replay queued commits strictly after that basis
```

No commit may be missed or observed twice between the initial result and the
first change.

---

## 1. Motivation

The motivating application question is:

> How does the application know that the state represented by this query has
> changed?

Today, the application must know which writes can affect which reads. That
creates a second, usually less precise dependency system outside Janus:

- A UI queries a task list, then manually listens for task, status, project,
  assignee, and permission events.
- A resolver caches a derived aggregate and must remember every attribute that
  contributes to it.
- A game system queries visible world state and broadly invalidates on any
  world write.
- A service polls because it cannot prove which transaction topics correspond
  to a nested Datalog query.

The Datalog query already states the logical dependency. Janus should be able
to retain that declaration and notify the application when its result relation
changes.

This follows the existing "lean on Janus" philosophy:

```text
application asks for declarative state
Janus owns dependency tracking and consistency
application reacts to result changes
```

---

## 2. Goals and non-goals

### Goals

- Return a consistent initial result and every subsequent resolved result
  change without a registration race.
- Reuse the existing query parser, planner, executor, CRDT resolution, relation
  equality, and annotation infrastructure.
- Emit relation deltas (`Added`, `Removed`), not merely "a possibly relevant
  datom was written."
- Never invoke application callbacks while holding transaction, cache, registry,
  or storage locks.
- Make ordering, basis, cancellation, backpressure, and errors explicit.
- Support ordinary latest-state queries first.
- Permit a correct full-reevaluation implementation before incremental
  execution.
- Keep the subscription API in-process and idiomatic for embedded Go usage.
- Measure invalidation precision and reevaluation cost before optimizing.

### Non-goals (first implementation)

- Durable subscriptions that survive process restart.
- Cross-process or cross-host delivery.
- Arbitrary multi-source subscriptions.
- Incremental maintenance of every relational operator.
- Trigger/action execution inside the committing transaction.
- Exactly-once external side effects.
- Fixed `AsOf` subscriptions (a fixed snapshot is immutable).
- Querying an eventually changing remote source without a source change feed.
- Replacing transaction reports for consumers that need raw operations.

---

## 3. API shape

The primary API should be channel-backed. A callback adapter can be provided
without making callbacks the lifecycle/backpressure primitive.

```go
type QueryDelta struct {
    Basis   SnapshotBasis
    Added   executor.Relation
    Removed executor.Relation
}

type QuerySubscription struct {
    Initial executor.Relation
}

func (s *QuerySubscription) Changes() <-chan QueryDelta
func (s *QuerySubscription) Err() error
func (s *QuerySubscription) Close() error

func (d *Database) QueryAndSubscribe(
    ctx context.Context,
    query string,
    inputs ...interface{},
) (*QuerySubscription, error)
```

The callback form is an adapter:

```go
func (d *Database) QueryAndSubscribeFunc(
    ctx context.Context,
    query string,
    callback func(QueryDelta),
    inputs ...interface{},
) (*QuerySubscription, error)
```

The channel form makes these contracts visible:

- Cancellation through `context.Context` or `Close`.
- Ordered delivery.
- Bounded buffering.
- Backpressure/coalescing policy.
- Terminal errors.
- Ownership of materialized delta relations.

### 3.1 Initial result

`Initial` is fully materialized and re-iterable. Subscription setup cannot
return a lazy storage relation whose execution overlaps later commits.

### 3.2 Delta relations

`Added` and `Removed` contain the query's declared result shape, after
aggregation, projection, Pull rendering policy, ordering/limit semantics, and
set deduplication according to the supported contract.

Both relations are materialized and owned by the event. Consumers may retain
them after receiving the next event.

### 3.3 Empty changes

No event is emitted when a relevant transaction does not change the resolved
result. This is common under CRDT semantics: a losing LWW operation is a real
committed datom but may leave current query state unchanged.

---

## 4. Snapshot and registration semantics

The hardest bug class is a commit occurring between initial query execution and
subscription registration.

These orders are wrong:

```text
query initial → commit → register       // misses commit
register → commit → query latest        // commit may be in initial and event
```

The correct protocol:

1. Under the database's subscription/commit sequence lock:
   - Allocate/register the subscription in `initializing` state.
   - Capture the latest fully committed basis `W`.
   - Configure its queue to accept events strictly after `W`.
2. Release the lock.
3. Execute the initial query against `AsOf(W)`.
4. Materialize and store the initial result.
5. Transition the subscription to `active`.
6. Process queued commit events in sequence order.

The commit path:

1. Atomically commits authoritative datoms and transaction metadata.
2. Completes cache version updates/invalidation.
3. Publishes one immutable commit event with its basis and dependency summary.
4. Returns to the writer without running subscriber queries inline.

### 4.1 Empty database basis

Janus currently uses `&ElementID{}` as the History mode sentinel. An empty
latest-state basis therefore cannot be represented by calling
`AsOf(ElementID{})`.

The subscription design needs an explicit basis type:

```go
type SnapshotBasis struct {
    Tx    datalog.ElementID
    Empty bool
}
```

or another representation that distinguishes:

- Empty latest snapshot.
- Raw History mode.
- A real committed transaction basis.

Do not overload the zero `ElementID` sentinel.

### 4.2 Ordering

Commit events are delivered in Janus commit-publication order. Each event
contains the transaction metadata `ElementID`, which is the high-water mark for
that logical commit.

For future distributed/multi-replica ingestion, a single total delivery order
may not represent causality. That extension should use the branch/frontier
snapshot model rather than silently treating arrival order as a global commit
order.

---

## 5. Commit event

The subscription registry does not need every datom value to decide whether a
query may be affected. The commit path can publish:

```go
type CommitEvent struct {
    Sequence uint64
    Basis    SnapshotBasis
    Touched  []EntityAttribute
}

type EntityAttribute struct {
    Entity    datalog.Identity
    Attribute datalog.Keyword
}
```

Optional future fields:

- Source symbol.
- CRDT operation class.
- Attribute-level high-water marks.
- Transaction metadata entity.
- Raw operation references for History subscriptions.

The first version should keep the event immutable, compact, and independent of
transaction-owned slices.

### 5.1 Publication point

Publish only after:

- Badger transaction commit succeeds.
- Cache in-flight sentinels are resolved.
- Max-version updates and invalidation complete.
- The transaction has a stable metadata `ElementID`.

A subscriber reevaluating immediately after publication must observe the full
post-commit state.

### 5.2 Commit latency

Commit publication may enqueue lightweight events while holding the sequencing
lock. It must not:

- Execute queries.
- Diff relations.
- Invoke callbacks.
- Block on a slow subscriber.
- Wait for an unbounded channel.

---

## 6. Query dependency signature

Each subscription stores a conservative signature:

```go
type QueryDependencies struct {
    Sources           map[query.Symbol]bool
    Attributes        map[datalog.Keyword]bool
    DynamicAttributes bool
    Entities          map[datalog.Identity]bool // optional input-derived narrowing
}
```

The signature is derived recursively from:

- Data patterns.
- Nested subqueries.
- `or`, `or-join`, `or-default`, and their join variants.
- `not` and `not-join`.
- Pull patterns.
- `get-else`, `missing?`, and `get-some`.
- Rules, when implemented.
- Query inputs that bind entities, attributes, or sources.

### 6.1 Constant attributes

```clojure
[?e :task/status ?status]
```

depends on `:task/status`.

### 6.2 Dynamic attributes

```clojure
[?e ?attribute ?value]
```

sets `DynamicAttributes=true` and conservatively matches every attribute
commit.

### 6.3 Bound entities

```clojure
[:in $ ?task
 :where [?task :task/status ?status]]
```

can optionally narrow dependency to one entity. This is an optimization; the
attribute dependency alone remains correct.

### 6.4 Expressions and predicates

Ordinary arithmetic/string/time predicates depend only on symbols provided by
other clauses and add no storage dependency.

Database functions add attributes:

```clojure
[(get-else $ ?e :task/token-count 0) ?tokens]
```

depends on `:task/token-count`.

Custom registered functions with hidden database reads cannot be analyzed. They
must either:

- Declare dependencies as registration metadata.
- Force `DynamicAttributes=true`.
- Be rejected from subscriptions.

### 6.5 Pull

Pull dependencies include every explicit attribute in the pattern. Wildcard
Pull is dynamic and depends on all attributes of any matching entity.

Nested Pull references extend dependencies recursively.

### 6.6 Conservative by design

A false positive causes an unnecessary reevaluation.

A false negative loses a result change.

The extractor must therefore choose false positives whenever uncertain.

---

## 7. Subscription registry

Avoid a global scan of all subscriptions on every commit. Maintain inverted
dependency indexes:

```text
attribute → subscription IDs
entity    → subscription IDs  (optional narrowing)
wildcard subscriptions
```

The registry owns:

- Subscription ID allocation.
- Lifecycle state (`initializing`, `active`, `closed`, `failed`).
- Pinned basis and last delivered basis.
- Materialized prior result.
- Pending/coalesced commit events.
- Dependency signature.
- Cancellation and output channel.

It must not own query execution logic; reevaluation uses the ordinary database
query path.

### 7.1 Registration and removal

Registration/removal is synchronized with commit event sequencing. Removal is
idempotent. Closing a subscription:

- Stops new event enqueue.
- Cancels in-flight reevaluation.
- Closes the output channel after workers exit.
- Releases the retained prior relation.
- Never closes the parent database.

### 7.2 Duplicate invalidation

One transaction may touch several attributes in the same query. The registry
enqueues at most one reevaluation request per subscription per commit.

---

## 8. Phase 1 execution: full reevaluation

For each relevant commit:

1. Coalesce/queue according to delivery policy.
2. Execute the original query and inputs at the commit basis.
3. Materialize the new result.
4. Diff the old and new result as sets with typed equality.
5. Emit `Removed = old - new` and `Added = new - old`.
6. Replace the retained prior result.
7. Advance the last delivered basis.

### 8.1 Typed relation diff

Use the same equality/hash law as query execution:

```text
ValuesEqual(a,b) ⇒ TupleHash(a)=TupleHash(b)
```

The diff implementation must:

- Use full result tuples as keys.
- Copy tuples from workspace-reusing iterators.
- Propagate iterator and close errors.
- Reject non-Datalog values before terminal presentation.
- Handle signed zero, bytes, time, Identity, Keyword, Symbol, and vectors.

An independent O(n²) differential oracle should test the optimized diff.

### 8.2 Query plans

The subscription stores parsed query/inputs and may reuse the normal plan cache.
It must not retain mutable executor relations or one-shot iterators between
reevaluations.

### 8.3 Reevaluation concurrency

One subscription is reevaluated serially. Different subscriptions may run in a
bounded worker pool.

Do not spawn one goroutine per subscription per commit.

---

## 9. Delivery and backpressure

The API must decide whether it represents every intermediate transaction or
eventual latest state.

### Option A — every commit

Every relevant commit is reevaluated and every non-empty transition is emitted.

Advantages:

- Complete transition history.
- Simple basis progression.

Costs:

- Slow consumers create unbounded pressure unless commits block or the
  subscription fails.
- Repeated expensive query evaluation under write bursts.

### Option B — latest-state coalescing

While a subscriber is busy, commits coalesce to the newest basis. The next diff
is computed from the last delivered result directly to that newest state.

Advantages:

- Bounded work and queue size.
- Appropriate for UI/cache synchronization.
- No state change is lost in the final result, though intermediate transitions
  may be skipped.

Costs:

- Not an audit/event stream.
- A value may change A→B→A with no delivered event.

### Recommendation

Default to latest-state coalescing. Offer every-commit delivery as an explicit
option with a bounded buffer and documented overflow behavior.

Possible options:

```go
type SubscriptionOptions struct {
    Delivery     DeliveryMode
    Buffer       int
    OnOverflow   OverflowPolicy
}
```

No default policy may silently block `Transaction.Commit`.

---

## 10. CRDT semantics

Subscriptions observe query results over resolved state, not operation arrival.

### CardinalityOne

- Winning LWW write changes a projected value: remove old row, add new row.
- Losing LWW write: relevant dependency, reevaluation, no delta.
- Winning tombstone: remove matching rows.

### CardinalityMany

- Winning add: add result rows.
- Winning remove: remove result rows.
- Losing remove/add under add-wins: no delta.

### CardinalityVector

If the vector is projected as one value, a change removes the old vector tuple
and adds the new one. Future incremental maintenance may emit element deltas,
but that is not the Phase 1 query-result contract.

### Unique attributes

Ownership changes under `(A,V)`-LWW can remove one entity's row and add
another's even when the touched entity set appears narrow. Dependency
extraction must subscribe by attribute, not rely solely on touched entity
narrowing for unique values.

### Operation with no current-state effect

No delta is emitted. Transaction reports and query subscriptions serve
different purposes:

- Transaction report: what operations committed.
- Query subscription: how resolved declarative state changed.

---

## 11. Query feature semantics

### Joins

Full reevaluation naturally handles join fanout and disappearance. One changed
attribute can add/remove many result rows.

### Aggregates

The aggregate output relation is diffed like any other. A count change is:

```text
Removed: [old-count]
Added:   [new-count]
```

### OR and fallback

Changing a primary branch can replace a fallback row or vice versa. The delta
contains the observable relation transition, not branch events.

### NOT

A newly matching inner clause may remove an outer result; a removed inner match
may add it. Full reevaluation handles this correctly.

### Ordering without limit

Relations have set semantics. If membership is unchanged but order changes,
`Added`/`Removed` is empty.

If consumers need positional reorder events, that is a separate ordered-view
contract.

### Ordering with limit

Top-N membership is observable. A ranking change can produce:

```text
Removed: row leaving Top-N
Added:   row entering Top-N
```

Rows that reorder within the retained set do not produce a set delta.

An optional event may include the newly materialized ordered snapshot for UI
consumers; this is an API decision (§18).

### Pull

Pull maps are result presentation, not relational values. Two options:

1. Diff entity/value tuples before Pull rendering, then render changed rows.
2. Render full results and perform a presentation-layer deep diff.

The first is consistent with Janus's closed value-domain rule and is
recommended. Pull dependencies still include nested attributes so relevant
changes trigger reevaluation.

### Query inputs

Inputs are immutable for the subscription lifetime. Changing an input creates
a new subscription.

Relation/collection inputs must be copied/materialized at registration so
caller mutation cannot change future reevaluations.

---

## 12. Temporal modes

### Latest

Primary supported mode. Each commit advances the basis and may produce a delta.

### Fixed AsOf

Immutable by definition. `QueryAndSubscribe` on a fixed `AsOf` handle should
return an error or a subscription that closes after `Initial`; returning a live
stream would be misleading.

Recommendation: return a clear unsupported error.

### History

History is append-only at the operation level. A History subscription could
emit newly committed raw datoms without full query reevaluation for simple
patterns, but arbitrary History queries may still contain joins/filters.

Defer History subscriptions. They are closer to filtered transaction reports
than current-state live queries and deserve a separate contract.

### Future snapshots/branches

A branch subscription follows that branch's moving frontier. A named immutable
snapshot behaves like fixed AsOf. Merge events may cause large result deltas.

---

## 13. Multi-source queries

A local database knows only about its own commits. A query joining `$`, `$users`,
and `$permissions` cannot remain live unless every mutable source provides a
change feed with basis semantics.

Phase 1 should support:

- Default local `$` source.
- Additional immutable sources.

Reject subscriptions with mutable named sources unless they implement a future
source capability:

```go
type ChangeSource interface {
    SubscribeChanges(ctx context.Context) (<-chan SourceChange, error)
}
```

This is an optional capability below the uniform `PatternMatcher` query seam,
not a second matcher API. The query executor remains backend-agnostic.

Cross-source basis composition and event ordering are deferred.

---

## 14. Errors and lifecycle

### Initial query error

Registration is removed and `QueryAndSubscribe` returns the error. No
subscription escapes.

### Reevaluation error

Recommended Phase 1 behavior:

- Store the terminal error.
- Close the subscription.
- Close the changes channel.
- Do not emit a partial delta.

Automatic retry can be added later for classified transient errors.

### Consumer cancellation

`Close` and context cancellation are idempotent. In-flight query iterators and
worker jobs are canceled/closed.

### Database close

`Database.Close`:

1. Stops accepting subscriptions.
2. Closes all active subscriptions.
3. Waits for subscription workers to exit.
4. Synchronizes/closes storage through the existing idempotent close path.

### Panic isolation

The channel API contains no user callback in Janus workers. The callback adapter
must recover or terminate only its own adapter goroutine according to a
documented policy; it must never crash commit processing.

---

## 15. Observability

Structured annotations should include:

```text
subscription/register
subscription/initial.begin
subscription/initial.complete
subscription/invalidate
subscription/coalesce
subscription/reevaluate.begin
subscription/reevaluate.complete
subscription/delta
subscription/error
subscription/close
```

Useful fields:

- Subscription ID.
- Basis/sequence.
- Dependency attribute count.
- Triggering touched attributes/entities.
- Coalesced commit count.
- Query latency.
- Old/new result sizes.
- Added/removed counts.
- Queue depth.
- Close/error reason.

Handlers receive no result values by default; values may contain sensitive
application data.

---

## 16. Performance model

Let:

- `S` = active subscriptions.
- `T` = attributes touched by a commit.
- `M(A)` = subscriptions depending on attribute `A`.
- `Q` = cost of one query reevaluation.

With an inverted dependency registry, invalidation is:

```text
O(sum over touched A of M(A))
```

not `O(S)`.

Phase 1 reevaluation cost is:

```text
O(number of invalidated subscriptions × Q)
```

The metrics that determine whether Phase 2 is needed:

- Invalidation false-positive rate.
- Reevaluations per commit.
- Percentage of reevaluations producing empty deltas.
- Query latency distribution.
- Coalesced commits per delivered delta.
- Subscription count and dependency fanout.
- Commit-to-delivery latency.
- Added/removed rows per reevaluation.

Do not implement incremental joins before measurements show full reevaluation
is the limiting cost.

---

## 17. Staged implementation plan

### Stage 0 — contract prototype

1. Define basis, delta, delivery, and lifecycle types.
2. Build a deterministic in-memory commit event sequence in tests.
3. Prove gap-free initialization with controlled concurrent commits.
4. Decide default coalescing/backpressure policy.
5. Benchmark full reevaluation for representative subscriptions.

### Stage 1 — local latest-state subscriptions

1. Add subscription registry and inverted attribute dependencies.
2. Extract dependencies from parsed/optimized query structure.
3. Integrate one commit event after storage/cache commit completion.
4. Execute initial query at a pinned basis.
5. Queue commits during initialization.
6. Reevaluate serially per subscription.
7. Diff materialized relations and emit deltas.
8. Implement cancellation/database close.
9. Add annotations and metrics.

### Stage 2 — invalidation precision

1. Add safe bound-entity narrowing.
2. Track correlation keys for common query shapes.
3. Skip reevaluation when touched entity/attribute cannot join the query.
4. Share equivalent reevaluations where query+inputs+basis are identical.
5. Measure false-positive reduction.

### Stage 3 — selective reevaluation

1. Bind touched entities/correlation keys as query inputs.
2. Recompute only affected result partitions where compositional proofs exist.
3. Merge partition deltas into retained result state.
4. Fall back to full reevaluation for unsupported shapes.

### Stage 4 — incremental relational maintenance

Only if profiling justifies it:

- Incremental select/project.
- Hash-join delta propagation.
- Aggregate state updates.
- Semi/anti and NOT maintenance.
- OR/fallback branch transitions.
- Top-N incremental heap maintenance.

This stage resembles Rete/differential dataflow and should be a separate
proposal once Phase 1 workload evidence exists.

### Stage 5 — mutable multi-source feeds

Define basis composition and `ChangeSource` only after the local contract is
stable.

---

## 18. Open architectural decisions

These require explicit owner decisions:

1. **Primary API:** channel only, callback only, or channel plus adapter.
2. **Initial delivery:** field on subscription, separate return value, or first
   channel event.
3. **Default delivery:** latest-state coalescing vs every commit.
4. **Overflow:** coalesce, block subscription worker, terminate, or drop with
   reset event. Commit must never block.
5. **Delta shape:** `Added`/`Removed` only vs optional full current snapshot.
6. **Ordering:** set membership only vs positional/reorder events.
7. **Pull:** pre-render entity tuple diff vs presentation deep diff.
8. **Error policy:** terminal vs retry for classified transient errors.
9. **Empty database basis:** explicit `SnapshotBasis` representation.
10. **Dependency source:** parsed query, algebra tree, RealizedPlan, or a
    normalized combination.
11. **Registered functions:** declared dependencies, wildcard, or rejection.
12. **Relation inputs:** maximum retained size and copying policy.
13. **Per-database limits:** subscription count, queue memory, worker count.
14. **History:** unsupported initially vs separate filtered transaction stream.
15. **Multi-source:** immutable-only initial restriction.
16. **Durability:** ephemeral only vs future resumable cursor.

---

## 19. Correctness test matrix

### Gap-free initialization

- Commit before registration lock.
- Commit while initial query runs.
- Several commits queued during initialization.
- Commit immediately after activation.
- Empty database initial basis.
- Initial query error removes registration.

### CRDT behavior

- Winning/losing CardinalityOne writes.
- Tombstone and re-add.
- CardinalityMany add/remove/add-wins conflicts.
- Vector insert/tombstone/reordering.
- Unique ownership transfer between entities.
- Operations that trigger dependency but produce no delta.

### Query shapes

- Single pattern.
- Multi-pattern join and fanout.
- Predicate and expression.
- Aggregate.
- NOT/semi/anti.
- OR and `or-default` branch transitions.
- Nested subqueries and correlated aggregates.
- Order-only change.
- `:order-by` + `:limit` membership change.
- Pull explicit/nested/wildcard dependencies.
- Scalar, tuple, collection, and relation inputs.

### Delivery

- Slow consumer under coalescing.
- Every-commit bounded buffer.
- Overflow policy.
- Cancellation before/while/after reevaluation.
- Callback panic isolation.
- Database close with active subscribers.
- Subscriber closes itself from callback adapter.

### Errors/resources

- Iterator and close errors during initial query.
- Iterator and close errors during reevaluation.
- Storage/decode failure.
- No partial delta on error.
- Goroutine and iterator leak checks.
- Race detector with concurrent commits/subscriptions/close.
- Shuffled repeated execution.

### Differential model

For randomized commit/query sequences:

1. Execute the ordinary query at every committed basis.
2. Compute expected deltas with a slow pairwise `ValuesEqual` oracle.
3. Compare subscription events exactly.
4. Repeat across deterministic seed matrices.

The oracle must not use the production subscription diff implementation.

---

## 20. Alternatives considered

### Polling

Simple but wastes work, adds latency, and moves consistency policy to every
application.

### Transaction reports only

Useful for raw operation consumers, but applications must reproduce query
dependencies and CRDT resolution to know whether a result changed.

### Entity/attribute watchers

Better than polling but still imperative and insufficient for joins,
aggregates, NOT, fallback, Pull, and Top-N.

### Emit full result on every change

Simple API but expensive for large results and forces every consumer to diff.
Could be an option, not the primary contract.

### Callback-only API

Hides backpressure, buffering, cancellation, and terminal error semantics.
Provide only as an adapter over channels.

### Incremental dataflow first

Premature. It multiplies operator-specific state and correctness obligations
before real subscription workloads establish the need.

### Application-maintained materialized views

Recreates the exact duplicated-state problem this feature is intended to solve.

---

## 21. Relationship to compound derived indexes

Live subscriptions and derived compound indexes solve complementary problems:

- Compound indexes accelerate repeated lookup of resolved state.
- Subscriptions notify consumers when resolved query state changes.

Both rely on:

- Schema/query dependency extraction.
- Commit touched-attribute/entity summaries.
- Pinned bases.
- Derived state that is never authoritative.
- Fallback to ordinary query execution for correctness.

A future compound index could reduce subscription reevaluation cost, but neither
feature should depend on the other. Phase 1 subscriptions use ordinary query
execution; compound indexing proceeds only from separate performance evidence.

---

## 22. Recommendation

Proceed with Stage 0 and a narrow Stage 1:

- Local default source only.
- Latest-state queries only.
- Ephemeral in-process subscriptions.
- Materialized initial relation.
- Latest-state coalescing by default.
- Full query reevaluation.
- Typed `Added`/`Removed` relation deltas.
- Conservative attribute dependency extraction.
- Terminal reevaluation errors.
- Explicit cancellation and database-close integration.

Do not start incremental join/aggregate maintenance until production
measurements show full reevaluation is inadequate.

This first version already solves the application problem: Janus, not the
application, knows when declarative state changed and delivers the resulting
state transition without polling or duplicated dependency logic.
