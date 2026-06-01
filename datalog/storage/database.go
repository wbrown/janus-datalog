package storage

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// resolveQuery converts a query input (string or *query.Query) to *query.Query.
// String inputs are cached in d.parseCache to avoid re-parsing identical queries.
func (d *Database) resolveQuery(q interface{}) (*query.Query, error) {
	switch v := q.(type) {
	case string:
		if cached, ok := d.parseCache.Get(v); ok {
			return cached, nil
		}
		parsed, err := parser.ParseQuery(v)
		if err != nil {
			return nil, err
		}
		d.parseCache.Set(v, parsed)
		return parsed, nil
	case *query.Query:
		return v, nil
	default:
		return nil, fmt.Errorf("query must be string or *query.Query, got %T", q)
	}
}

// Database provides the main API for reading and writing datoms
type Database struct {
	store             *BadgerStore
	txCounter         atomic.Uint64
	mu                sync.RWMutex
	activeTx          map[*Transaction]bool
	planCache         *planner.PlanCache      // Shared query plan cache
	parseCache        *ParseCache             // Shared query parse cache
	schema            schema.SchemaProvider   // Optional schema for validation
	annotationHandler annotations.Handler     // Optional handler for query tracing
	plannerOptions    *planner.PlannerOptions // Optional planner options override
	clock             *LamportClock           // CRDT: Lamport clock for ordering (nil if not in CRDT mode)
	replicaID         uint64                  // CRDT: This database's replica identifier
	cache             *Cache                  // CRDT: Unified cache for resolved CRDT views
	temporalTxID      *datalog.ElementID      // nil = current; set = temporal mode (AsOf/History)

	// onCommitWindow, if set, is invoked inside Commit after the storage commit
	// returns but before the cache is updated — the window where a stale cached
	// read was once possible. Test-only (nil in production); lets a test run a
	// reader deterministically in that window. See the cache stale-read tests.
	onCommitWindow func()
}

// NewDatabase creates a new database with BadgerDB storage
func NewDatabase(path string) (*Database, error) {
	return NewDatabaseWithOptions(DatabaseOptions{Path: path})
}

// NewDatabaseWithSchema creates a database with schema validation
func NewDatabaseWithSchema(path string, s schema.SchemaProvider) (*Database, error) {
	db, err := NewDatabase(path)
	if err != nil {
		return nil, err
	}
	db.schema = s
	return db, nil
}

// DatabaseOptions configures database creation
type DatabaseOptions struct {
	Path                 string                  // Path to the database directory
	Schema               schema.SchemaProvider   // Optional schema for validation
	AnnotationHandler    annotations.Handler     // Optional handler for query tracing
	ReplicaID            uint64                  // For CRDT mode: 0 = auto-generate random; non-zero = use specified. Ignored for existing DBs.
	DisableCache         bool                    // Disable EA cache; queries resolve directly from storage
	PlannerOptions       *planner.PlannerOptions // Optional override for default planner options
	CompressionThreshold int                     // Compress string/[]byte values >= this size (0 = default 512; -1 to disable)
}

// NewDatabaseWithOptions creates a database with the specified options.
// This is the most flexible constructor, supporting all configuration options.
//
// Options:
//   - Path: Required. Directory for BadgerDB storage.
//   - Schema: Optional schema for validation.
//   - ReplicaID: For CRDT mode. 0 = auto-generate random; non-zero = use specified.
//   - DisableCache: Disable EA cache; queries resolve directly from storage.
//
// Example:
//
//	db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
//	    Path: "/path/to/db",
//	})
func NewDatabaseWithOptions(opts DatabaseOptions) (*Database, error) {
	if opts.Path == "" {
		return nil, fmt.Errorf("database path is required")
	}

	encoder := NewKeyEncoder(BinaryStrategy)
	// Default compression threshold is 512 bytes. Use -1 to disable.
	// Values below ~500 bytes rarely compress due to ~300 bytes of
	// FSE table + block header overhead in the compressed format.
	threshold := opts.CompressionThreshold
	if threshold == 0 {
		threshold = 512
	}
	if threshold > 0 {
		if be, ok := encoder.(*BinaryKeyEncoder); ok {
			be.CompressionThreshold = threshold
		}
	}
	store, err := NewBadgerStore(opts.Path, encoder)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	// Determine ReplicaID: check metadata first (existing DB), then opts, then generate
	var replicaID uint64
	storedReplicaID, found, err := store.GetMetadataUint64("replica_id")
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("failed to read replica_id metadata: %w", err)
	}

	if found {
		// Existing database: use stored ReplicaID (opts.ReplicaID ignored)
		replicaID = storedReplicaID
	} else {
		// New database: use specified or generate random
		if opts.ReplicaID != 0 {
			replicaID = opts.ReplicaID
		} else {
			replicaID = rand.Uint64()
		}
		// Persist the ReplicaID for future opens
		if err := store.SetMetadataUint64("replica_id", replicaID); err != nil {
			store.Close()
			return nil, fmt.Errorf("failed to persist replica_id metadata: %w", err)
		}
	}

	// Create Lamport clock for CRDT ordering
	clock := NewLamportClock(replicaID)

	// Restore clock from max ElementID in TAEV index
	// This ensures new writes have higher Lamport values than existing data
	maxElementID, err := store.MaxElementID()
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("failed to get max ElementID: %w", err)
	}
	if !maxElementID.IsZero() {
		clock.Restore(maxElementID)
	}

	var cache *Cache
	if !opts.DisableCache {
		cache = NewCache()
	}

	// When the caller supplies no schema, reconstruct one from the CRDT ops
	// already stored on disk so vector/many attributes resolve correctly instead
	// of collapsing to a single LWW value (e.g. the `datalog` CLI opening an
	// existing database). A supplied schema is authoritative and wins entirely —
	// no inference. On an empty store this yields an empty schema, equivalent to
	// the previous nil behavior.
	effectiveSchema := opts.Schema
	if effectiveSchema == nil {
		inferred, ierr := inferSchemaFromStore(store)
		if ierr != nil {
			store.Close()
			return nil, fmt.Errorf("inferring schema from store: %w", ierr)
		}
		if inferred.HasSchema() {
			effectiveSchema = inferred
		}
	}

	return &Database{
		store:             store,
		activeTx:          make(map[*Transaction]bool),
		planCache:         planner.NewPlanCache(1000, 0),
		parseCache:        NewParseCache(1000),
		schema:            effectiveSchema,
		annotationHandler: annotations.Synchronized(opts.AnnotationHandler),
		plannerOptions:    opts.PlannerOptions,
		clock:             clock,
		replicaID:         replicaID,
		cache:             cache,
	}, nil
}

// Schema returns the current schema (may be nil)
func (d *Database) Schema() schema.SchemaProvider {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.schema
}

// SetSchema sets or replaces the schema for validation
// Schema changes take effect for new transactions
func (d *Database) SetSchema(s schema.SchemaProvider) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.schema = s
}

// ReplicaID returns this database's replica identifier for CRDT operations.
// Each database instance should have a unique ReplicaID for proper multi-replica
// merge semantics. The ID is auto-generated if not specified in DatabaseOptions.
func (d *Database) ReplicaID() uint64 {
	return d.replicaID
}

// Clock returns the Lamport clock for CRDT operations.
// Returns nil if the database was created without CRDT support.
func (d *Database) Clock() *LamportClock {
	return d.clock
}

// Cache returns the CRDT resolution cache.
// Used by the matcher for O(1) access to resolved CRDT views.
func (d *Database) Cache() *Cache {
	return d.cache
}

// WarmCache pre-populates cache entries for the specified attributes.
// Use this after process restart for attributes with high query frequency
// to avoid cold-start latency on first access.
//
// For each attribute, this scans all entities and populates:
// - Per-(E,A) cache entries with resolved CRDT values
// - Attribute-level version tracking for freshness checks
//
// This is O(n) where n = total datoms for the specified attributes.
// Call during application startup, not on the hot path.
func (d *Database) WarmCache(attributes []datalog.Keyword) error {
	matcher := NewBadgerMatcher(d.store)
	matcher.SetSchema(d.schema)

	for _, attr := range attributes {
		var aBytes Attribute
		copy(aBytes[:], attr.String())

		// Build prefix for this attribute on AEVT index
		prefix := make([]byte, 1+32) // prefix byte + A
		prefix[0] = byte(AEVT)
		copy(prefix[1:33], aBytes[:])

		// Scan AEVT for all entities with this attribute
		iter, err := d.store.Scan(AEVT, prefix, prefixEnd(prefix))
		if err != nil {
			return fmt.Errorf("warming cache for %s: %w", attr.String(), err)
		}

		var maxAttrVersion datalog.ElementID
		seenEntities := make(map[Entity]bool)

		for iter.Next() {
			datom, err := iter.Datom()
			if err != nil {
				iter.Close()
				return fmt.Errorf("warming cache for %s: %w", attr.String(), err)
			}

			// Track max version for attribute-level freshness
			if datom.Tx.Compare(maxAttrVersion) > 0 {
				maxAttrVersion = datom.Tx
			}

			// Get entity bytes
			eBytes := Entity(datom.E.Hash())

			// Resolve each (E, A) once
			if !seenEntities[eBytes] {
				seenEntities[eBytes] = true
				key := CacheKey{E: eBytes, A: aBytes}
				d.cache.GetOrResolve(key, matcher)
			}
		}
		iter.Close()

		// Update attribute-level version
		d.cache.UpdateAttributeVersion(aBytes, maxAttrVersion)
	}

	return nil
}

// GetVectorNth returns the nth element of a vector attribute.
// Uses the cache for O(1) access when the cache is fresh.
// Returns nil if index is out of bounds or attribute is not a vector.
func (d *Database) GetVectorNth(e datalog.Identity, a datalog.Keyword, n int64) (any, error) {
	eBytes := Entity(e.Hash())

	var aBytes Attribute
	copy(aBytes[:], a.String())

	key := CacheKey{E: eBytes, A: aBytes}

	matcher := NewBadgerMatcher(d.store)
	matcher.SetSchema(d.schema)

	// Cache is an optimization, not a correctness requirement: when DisableCache
	// is set (d.cache == nil), resolve the entry directly from storage via
	// ResolveEntry, which produces the same *CacheEntry shape and determines
	// cardinality the same way (GetCardinality over d.schema).
	var entry *CacheEntry
	if d.cache != nil {
		entry = d.cache.GetOrResolve(key, matcher)
	} else {
		entry = ResolveEntry(key, matcher)
	}
	if entry == nil {
		return nil, nil
	}

	if entry.Cardinality() != schema.CardinalityVector {
		return nil, fmt.Errorf("attribute %s is not a vector", a.String())
	}

	vec := entry.VectorList()
	if n < 0 || n >= int64(len(vec)) {
		return nil, nil // Out of bounds
	}

	return vec[n], nil
}

// GetVectorLength returns the length of a vector attribute.
// Uses the cache for O(1) access when the cache is fresh.
// Returns 0 if the attribute is not a vector or doesn't exist.
func (d *Database) GetVectorLength(e datalog.Identity, a datalog.Keyword) (int64, error) {
	eBytes := Entity(e.Hash())

	var aBytes Attribute
	copy(aBytes[:], a.String())

	key := CacheKey{E: eBytes, A: aBytes}

	matcher := NewBadgerMatcher(d.store)
	matcher.SetSchema(d.schema)

	// Cache is an optimization, not a correctness requirement: when DisableCache
	// is set (d.cache == nil), resolve the entry directly from storage via
	// ResolveEntry, which produces the same *CacheEntry shape and determines
	// cardinality the same way (GetCardinality over d.schema).
	var entry *CacheEntry
	if d.cache != nil {
		entry = d.cache.GetOrResolve(key, matcher)
	} else {
		entry = ResolveEntry(key, matcher)
	}
	if entry == nil {
		return 0, nil
	}

	if entry.Cardinality() != schema.CardinalityVector {
		return 0, fmt.Errorf("attribute %s is not a vector", a.String())
	}

	return int64(len(entry.VectorList())), nil
}

// AnnotationHandler returns the current annotation handler (may be nil)
func (d *Database) AnnotationHandler() annotations.Handler {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.annotationHandler
}

// SetAnnotationHandler sets a handler for query tracing and performance observability.
// When set, all queries executed via Query will emit annotation events.
// Pass nil to disable annotations.
//
// Example:
//
//	db.SetAnnotationHandler(func(event annotations.Event) {
//	    fmt.Printf("%s: %v (latency: %v)\n", event.Name, event.Data, event.Latency)
//	})
func (d *Database) SetAnnotationHandler(handler annotations.Handler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Serialize: the engine emits annotations from parallel workers, so the
	// handler must be safe to call concurrently.
	d.annotationHandler = annotations.Synchronized(handler)
}

// NewTransaction starts a new write transaction.
//
// Panics if called on a read-only temporal handle (AsOf/History). Temporal
// views share the parent's store and have no write-side state; use the
// parent handle for writes.
func (d *Database) NewTransaction() *Transaction {
	if d.temporalTxID != nil {
		panic("NewTransaction called on a read-only temporal database handle (AsOf/History); use the parent handle for writes")
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	tx := &Transaction{
		db:                d,
		datoms:            make([]datalog.Datom, 0),
		retracts:          make([]datalog.Datom, 0),
		lastVectorElement: make(map[entityAttrKey]datalog.ElementID),
	}

	d.activeTx[tx] = true
	return tx
}

// NewTransactionAt starts a new write transaction with a specific time
// This is a convenience method for financial/temporal data
func (d *Database) NewTransactionAt(t time.Time) *Transaction {
	tx := d.NewTransaction()
	tx.SetTime(t)
	return tx
}

// Matcher returns a PatternMatcher for the current database state
// effectivePlannerOptions returns the database's planner options: the
// caller-supplied override (DatabaseOptions.PlannerOptions) when set, otherwise
// DefaultPlannerOptions(). Every query-construction path (Query, NewExecutor,
// Matcher) funnels through this accessor so the executor and the default-source
// matcher always agree on the effective options. See
// BUG_PLANNER_OPTIONS_NOT_PROPAGATED_TO_MATCHER.
func (d *Database) effectivePlannerOptions() planner.PlannerOptions {
	if d.plannerOptions != nil {
		return *d.plannerOptions
	}
	return DefaultPlannerOptions()
}

// Matcher returns the default-source pattern matcher, configured from the
// database's effective planner options (override or defaults) so the relations
// it produces carry the same executor options the query runs with — not a
// hardcoded default set.
func (d *Database) Matcher() executor.PatternMatcher {
	return d.matcherWithExecOptions(d.effectivePlannerOptions())
}

// Match implements executor.PatternMatcher — the Database itself can answer pattern queries.
// This delegates to the Database's Matcher(), which uses the full BadgerDB index infrastructure.
func (d *Database) Match(pattern *query.DataPattern, bindings executor.Relations) (executor.Relation, error) {
	return d.Matcher().Match(pattern, bindings)
}

// Compile-time verification that Database implements PatternMatcher
var _ executor.PatternMatcher = (*Database)(nil)

// AsOf returns a read-only Database handle that queries state as of the given
// transaction. The returned handle uses CRDT resolution filtered to that point
// in causal time.
//
// The temporal handle shares the parent's underlying store and schema, but owns
// a private EA cache scoped to the handle's lifetime. The snapshot is immutable,
// so this cache fills lazily, never needs invalidation, and is freed when the
// handle is garbage-collected — AsOf reads never accumulate in the parent's
// global latest-state cache.
// It supports reads (Query, NewExecutor, NewExecutorWithOptions, Matcher, Pull)
// but not writes: NewTransaction will panic. Close is a no-op — the parent
// owns the store lifetime.
func (d *Database) AsOf(txID datalog.ElementID) *Database {
	return &Database{
		store:             d.store,
		schema:            d.schema,
		annotationHandler: d.annotationHandler,
		planCache:         d.planCache,
		cache:             NewCache(),
		clock:             d.clock,
		replicaID:         d.replicaID,
		temporalTxID:      &txID,
	}
}

// History returns a read-only Database handle that returns all raw datoms
// without CRDT resolution. Every write is visible, including superseded values.
//
// The temporal handle shares the parent's underlying store, cache, and schema.
// It supports reads (Query, NewExecutor, NewExecutorWithOptions, Matcher, Pull)
// but not writes: NewTransaction will panic. Close is a no-op — the parent
// owns the store lifetime.
func (d *Database) History() *Database {
	empty := datalog.ElementID{}
	return &Database{
		store:             d.store,
		schema:            d.schema,
		annotationHandler: d.annotationHandler,
		planCache:         d.planCache,
		cache:             d.cache,
		clock:             d.clock,
		replicaID:         d.replicaID,
		temporalTxID:      &empty,
	}
}

// DefaultPlannerOptions returns the default planner and executor options for the database
func DefaultPlannerOptions() planner.PlannerOptions {
	return planner.PlannerOptions{
		// Planner / algebra optimization
		EnableAlgebraOptimizer: true,  // Relational algebra IR clause rewriting (decorrelation via compile → optimize → decompile)
		EnableScanSharing:      false, // Share unbound scan results across subqueries via LazySeq (benchmarked: performance-neutral)
		EnableEntityPrefetch:   false, // Warm EA cache after first DataPattern via PrefetchEntities (benchmarked: performance-neutral)

		// Executor streaming options (NEW: enabled by default for performance)
		EnableIteratorComposition: true,  // Lazy evaluation throughout pipeline
		EnableTrueStreaming:       true,  // No auto-materialization
		EnableSymmetricHashJoin:   false, // Conservative for now

		// Executor parallel options
		EnableParallelSubqueries: true, // Parallel subquery execution
		MaxSubqueryWorkers:       0,    // 0 = runtime.NumCPU()

		// Other executor options
		EnableStreamingJoins:       false, // Keep false for stability
		EnableStreamingAggregation: true,  // Streaming aggregation
		EnableDebugLogging:         false,

		// Fuse same-entity [?e :const-attr ?fresh] fetches into a per-tuple
		// LookupAttribute column attach instead of match + hash join.
		EnableAttributeFetchFusion: true,

		// Storage join strategy
		IndexNestedLoopThreshold: 0, // Default to HashJoinScan for all binding sizes

	}
}

// NewExecutor creates a new query executor that uses the database's plan cache
func (d *Database) NewExecutor() *executor.Executor {
	opts := d.effectivePlannerOptions()
	opts.Cache = d.planCache // Use database's cache
	return executor.NewExecutorWithOptions(d.matcherWithExecOptions(opts), d, opts)
}

// NewExecutorWithOptions creates a new query executor with custom planner
// options and the database's plan cache.
//
// Builds the pattern matcher from the same custom options via
// matcherWithExecOptions, so the executor and the matcher's relations agree on
// the effective options, and schema, cache, annotation handler, and temporal
// mode (AsOf/History) are all applied.
func (d *Database) NewExecutorWithOptions(opts planner.PlannerOptions) *executor.Executor {
	opts.Cache = d.planCache
	matcher := d.matcherWithExecOptions(opts)
	return executor.NewExecutorWithOptions(matcher, d, opts)
}

// matcherWithExecOptions builds a fully-configured BadgerMatcher using the
// given planner options (converted through executor.ExecutorOptionsFromPlanner,
// the single source of truth shared with the executor) while applying all
// database-level state: schema, cache, annotation handler, temporal mode.
// Matcher() funnels through here with the database's effective options.
func (d *Database) matcherWithExecOptions(opts planner.PlannerOptions) executor.PatternMatcher {
	matcher := NewBadgerMatcherWithOptions(d.store, executor.ExecutorOptionsFromPlanner(opts))
	matcher.SetHandler(d.annotationHandler)
	if d.schema != nil {
		matcher.SetSchema(d.schema)
	}
	if d.cache != nil {
		matcher.SetCache(d.cache)
	}
	if d.temporalTxID != nil {
		return matcher.AsOf(*d.temporalTxID)
	}
	return matcher
}

// Store returns the underlying store for direct access (debugging/testing)
func (d *Database) Store() *BadgerStore {
	return d.store
}

// Close closes the database.
//
// On a temporal handle (AsOf/History), Close is a no-op — the parent owns
// the store lifetime. Closing a temporal handle must not close the shared
// underlying store.
func (d *Database) Close() error {
	if d.temporalTxID != nil {
		return nil // Read-only view; parent owns the store.
	}

	// Copy active transactions while holding lock, then release before calling Rollback
	// to avoid deadlock (Rollback also acquires d.mu)
	d.mu.Lock()
	txs := make([]*Transaction, 0, len(d.activeTx))
	for tx := range d.activeTx {
		txs = append(txs, tx)
	}
	d.mu.Unlock()

	// Rollback any active transactions (without holding d.mu)
	for _, tx := range txs {
		tx.Rollback()
	}

	return d.store.Close()
}

// PlanCache returns the database's query plan cache
func (d *Database) PlanCache() *planner.PlanCache {
	return d.planCache
}

// SetPlanCache sets a custom plan cache or disables caching (if nil)
func (d *Database) SetPlanCache(cache *planner.PlanCache) {
	d.planCache = cache
}

// ClearPlanCache clears the query plan cache
func (d *Database) ClearPlanCache() {
	if d.planCache != nil {
		d.planCache.Clear()
	}
}

// ParseCache returns the database's query parse cache
func (d *Database) ParseCache() *ParseCache {
	return d.parseCache
}

// SetParseCache sets a custom parse cache or disables caching (if nil)
func (d *Database) SetParseCache(cache *ParseCache) {
	d.parseCache = cache
}

// QueryOption configures query execution.
type QueryOption func(*queryOptions)

type queryOptions struct {
	sources map[query.Symbol]executor.PatternMatcher
}

// WithSources adds additional data sources for multi-source queries.
// The default source ($) is always the database itself.
func WithSources(sources map[query.Symbol]executor.PatternMatcher) QueryOption {
	return func(o *queryOptions) {
		for k, v := range sources {
			o.sources[k] = v
		}
	}
}

// buildSourceMap creates the full source map for query execution.
// The provided map contains user-supplied sources. The default source ($)
// is set to the database's matcher if not explicitly overridden.
func buildSourceMap(
	provided map[query.Symbol]executor.PatternMatcher,
	dbMatcher executor.PatternMatcher,
) map[query.Symbol]executor.PatternMatcher {
	sources := make(map[query.Symbol]executor.PatternMatcher)
	for sym, src := range provided {
		sources[sym] = src
	}
	if _, ok := sources[datalog.SymDollar]; !ok {
		sources[datalog.SymDollar] = dbMatcher
	}
	return sources
}

// validateQuerySources checks that all sources declared in the query's :in clause
// are available in the source map.
func validateQuerySources(q *query.Query, available map[query.Symbol]executor.PatternMatcher) error {
	for _, inp := range q.In {
		if dbInput, ok := inp.(query.DatabaseInput); ok {
			if _, ok := available[dbInput.Name]; !ok {
				return fmt.Errorf("source %s declared in :in clause but not provided", dbInput.Name)
			}
		}
	}
	return nil
}

// extractQueryOptions separates QueryOption values from regular inputs.
func extractQueryOptions(inputs []interface{}) (queryOptions, []interface{}) {
	var opts queryOptions
	opts.sources = make(map[query.Symbol]executor.PatternMatcher)
	var regularInputs []interface{}

	for _, arg := range inputs {
		if opt, ok := arg.(QueryOption); ok {
			opt(&opts)
		} else {
			regularInputs = append(regularInputs, arg)
		}
	}

	return opts, regularInputs
}

// Explain returns the query plan for a query without executing it.
// The query can be either an EDN string or a *query.Query from the query builder.
// This is useful for understanding index selection, phase structure, and
// how input parameters affect symbol binding.
//
// Example:
//
//	plan, err := db.Explain(`[:find ?e :in $ ?scenario :where [?e :task/scenario ?scenario]]`, scenarioID)
//	if err != nil { ... }
//	fmt.Println(plan.String())  // Human-readable plan output
//
// The plan shows:
//   - Index selection (EAVT, AEVT, AVET, VAET, TAEV)
//   - Selectivity scores for patterns
//   - Symbol availability and binding through phases
//   - Predicate and expression assignment
func (d *Database) Explain(queryInput interface{}, inputs ...interface{}) (*planner.RealizedPlan, error) {
	// Resolve the query (string or *query.Query)
	q, err := d.resolveQuery(queryInput)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve query: %w", err)
	}

	// Validate inputs match :in clause (same validation as Query)
	_, err = d.convertInputsToRelations(q, inputs)
	if err != nil {
		return nil, err
	}

	// Create executor to get its planner (ensures same options as execution)
	exec := d.NewExecutor()

	// Get the query plan from the executor's planner (planning is not annotated
	// on this plan-only path).
	queryPlanner := exec.GetPlanner()
	return queryPlanner.PlanQuery(q, nil)
}

// AnalyzeResult contains the query plan and execution statistics from Analyze().
type AnalyzeResult struct {
	Plan      *planner.RealizedPlan // The query plan
	Result    executor.Relation     // Query result (not materialized)
	Events    []annotations.Event   // All annotation events from execution
	TotalTime time.Duration         // Total execution time
}

// String returns a formatted analysis report showing the query plan
// and execution statistics.
func (ar *AnalyzeResult) String() string {
	var sb strings.Builder

	// Plan section
	sb.WriteString(ar.Plan.String())
	sb.WriteString("\n")

	// Execution summary
	sb.WriteString("Execution:\n")
	sb.WriteString(fmt.Sprintf("  Total time: %v\n", ar.TotalTime))
	size := ar.Result.Size()
	if size >= 0 {
		sb.WriteString(fmt.Sprintf("  Result tuples: %d\n", size))
	} else {
		sb.WriteString("  Result tuples: (streaming - call Result.Size() to materialize)\n")
	}

	// Group events by type for summary
	eventCounts := make(map[string]int)
	eventTimes := make(map[string]time.Duration)
	for _, e := range ar.Events {
		eventCounts[e.Name]++
		eventTimes[e.Name] += e.Latency
	}

	if len(eventCounts) > 0 {
		sb.WriteString("\nEvent Summary:\n")

		// Pattern matching events
		if t, ok := eventTimes[annotations.PatternStorageScan]; ok {
			sb.WriteString(fmt.Sprintf("  Storage scans: %d (%.2fms total)\n",
				eventCounts[annotations.PatternStorageScan], float64(t.Microseconds())/1000))
		}

		// Join events
		hashJoins := eventCounts[annotations.JoinHash]
		nestedJoins := eventCounts[annotations.JoinNested]
		mergeJoins := eventCounts[annotations.JoinMerge]
		if hashJoins+nestedJoins+mergeJoins > 0 {
			sb.WriteString(fmt.Sprintf("  Joins: hash=%d, nested=%d, merge=%d\n",
				hashJoins, nestedJoins, mergeJoins))
			if t := eventTimes[annotations.JoinHash]; t > 0 {
				sb.WriteString(fmt.Sprintf("    Hash join time: %.2fms\n", float64(t.Microseconds())/1000))
			}
		}

		// Phase events
		if t, ok := eventTimes[annotations.PhaseComplete]; ok {
			sb.WriteString(fmt.Sprintf("  Phases completed: %d (%.2fms total)\n",
				eventCounts[annotations.PhaseComplete], float64(t.Microseconds())/1000))
		}
	}

	// Detailed event trace
	sb.WriteString("\nEvent Trace:\n")
	for _, e := range ar.Events {
		sb.WriteString(fmt.Sprintf("  [%6.2fms] %s", float64(e.Latency.Microseconds())/1000, e.Name))
		// Add key metrics from Data
		if e.Data != nil {
			if tuples, ok := e.Data["tuples"]; ok {
				sb.WriteString(fmt.Sprintf(" (tuples=%v)", tuples))
			}
			if inputSize, ok := e.Data["input_size"]; ok {
				sb.WriteString(fmt.Sprintf(" (input=%v)", inputSize))
			}
			if outputSize, ok := e.Data["output_size"]; ok {
				sb.WriteString(fmt.Sprintf(" (output=%v)", outputSize))
			}
			if index, ok := e.Data["index"]; ok {
				sb.WriteString(fmt.Sprintf(" (index=%v)", index))
			}
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

// Analyze executes a query and returns detailed execution statistics.
// The query can be either an EDN string or a *query.Query from the query builder.
// This is like EXPLAIN ANALYZE in PostgreSQL - it actually runs the query
// and captures timing information at each step.
//
// Example:
//
//	result, err := db.Analyze(`[:find ?e :in $ ?scenario :where [?e :task/scenario ?scenario]]`, scenarioID)
//	if err != nil { ... }
//	fmt.Println(result.String())  // Shows plan + execution trace
//
// The result includes:
//   - Query plan with index selection
//   - Query result as a Relation (call .Size() or iterate to access)
//   - Event trace with timing for each operation
//   - Summary statistics
func (d *Database) Analyze(queryInput interface{}, inputs ...interface{}) (*AnalyzeResult, error) {
	startTime := time.Now()

	// Resolve the query (string or *query.Query)
	q, err := d.resolveQuery(queryInput)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve query: %w", err)
	}

	// Validate and convert inputs
	inputRelations, err := d.convertInputsToRelations(q, inputs)
	if err != nil {
		return nil, err
	}

	// Create executor (this also creates the planner with proper options)
	exec := d.NewExecutor()

	// Get the query plan from the executor's planner. The Analyze collector below
	// captures execution annotations; planning here is not annotated (matching
	// prior behavior).
	queryPlanner := exec.GetPlanner()
	plan, err := queryPlanner.PlanQuery(q, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to plan query: %w", err)
	}

	// Create a collector to capture all events
	var events []annotations.Event
	collector := annotations.NewCollector(func(e annotations.Event) {
		events = append(events, e)
	})

	// Execute with annotation collection
	result, err := exec.ExecuteWithRelations(executor.NewContext(collector.Handler()), q, inputRelations)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	// Analyze is EXPLAIN ANALYZE-style: it must reflect ACTUAL execution. The
	// default relation is lazy — storage scans, joins, filters, and deferred
	// iterator errors happen only when the result is iterated. Drive that work
	// here via ForEach (which enforces the deferred-error contract) so the
	// captured events, TotalTime, and any error cover the whole query, not just
	// plan/pipeline construction. The drained tuples become a materialized,
	// re-iterable relation for the caller.
	symbols := result.Symbols()
	tuples := make([]executor.Tuple, 0)
	if err := executor.ForEach(result, func(t executor.Tuple) error {
		cp := make(executor.Tuple, len(t))
		copy(cp, t)
		tuples = append(tuples, cp)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	materialized := executor.NewMaterializedRelation(symbols, tuples)

	totalTime := time.Since(startTime)

	return &AnalyzeResult{
		Plan:      plan,
		Result:    materialized,
		Events:    events,
		TotalTime: totalTime,
	}, nil
}

// QueryInto executes a Datalog query and populates a slice of structs with the results.
// The query can be either an EDN string or a *query.Query from the query builder.
// Fields are mapped by `datalog` tags (e.g., `datalog:"?name"`) or by position if untagged.
//
// For aggregates, use the full expression as the tag (e.g., `datalog:"(sum ?salary)"`).
//
// Example:
//
//	type PersonResult struct {
//	    Name string `datalog:"?name"`
//	    Age  int64  `datalog:"?age"`
//	}
//
//	var results []PersonResult
//	err := db.QueryInto(&results, `[:find ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]`)
//
// Example with query builder:
//
//	e, name, age := qb.NewVar("e"), qb.NewVar("name"), qb.NewVar("age")
//	q := qb.Query().Find(name, age).Where(qb.Pat(e, PersonName, name), qb.Pat(e, PersonAge, age)).MustBuild()
//	err := db.QueryInto(&results, q)
//
// Example with aggregates:
//
//	type DeptStats struct {
//	    Dept     string  `datalog:"?dept"`
//	    TotalPay float64 `datalog:"(sum ?salary)"`
//	}
//
//	var stats []DeptStats
//	err := db.QueryInto(&stats, `[:find ?dept (sum ?salary) :where [?e :employee/dept ?dept] [?e :employee/salary ?salary]]`)
func (d *Database) QueryInto(dest interface{}, queryInput interface{}, inputs ...interface{}) error {
	// Validate dest is *[]T
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return dlreflect.ErrNotPointerToSlice
	}
	sliceVal := destVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return dlreflect.ErrNotPointerToSlice
	}
	elemType := sliceVal.Type().Elem()
	// Check for Identity/Keyword types BEFORE dereferencing - they are pointer type aliases
	// (*identity, *keyword) and should not be dereferenced
	elemIsPtr := elemType.Kind() == reflect.Ptr && elemType != identityType && elemType != keywordType
	if elemIsPtr {
		elemType = elemType.Elem()
	}

	// Resolve the query (string or *query.Query)
	q, err := d.resolveQuery(queryInput)
	if err != nil {
		return fmt.Errorf("failed to resolve query: %w", err)
	}

	// Execute query as streaming relation
	rel, err := d.Query(q, inputs...)
	if err != nil {
		return err
	}
	// Check if element type is a struct or scalar
	// time.Time and Keyword are structs but treated as scalars
	// Identity is a pointer type alias and goes through scalar path automatically
	if elemType.Kind() == reflect.Struct && !isScalarStructType(elemType) {
		// Struct path - use mapper
		findSymbols := extractFindSymbolStrings(q.Find)
		mapper, err := dlreflect.NewQueryResultMapper(elemType, findSymbols)
		if err != nil {
			return err
		}

		newSlice := reflect.MakeSlice(sliceVal.Type(), 0, 0)
		// ForEach surfaces iterator errors (decode/blob/CRDT failures deferred
		// to Error()) instead of letting them look like a complete result.
		if ferr := executor.ForEach(rel, func(t executor.Tuple) error {
			elem := reflect.New(elemType).Elem()
			if err := mapper.MapTuple(t, elem); err != nil {
				return err
			}
			newSlice = reflect.Append(newSlice, elem)
			return nil
		}); ferr != nil {
			return ferr
		}
		sliceVal.Set(newSlice)
		return nil
	}

	// Scalar path - single symbol queries only
	if len(q.Find) != 1 {
		return fmt.Errorf("scalar QueryInto requires exactly 1 find element, got %d", len(q.Find))
	}

	newSlice := reflect.MakeSlice(sliceVal.Type(), 0, 0)
	if ferr := executor.ForEach(rel, func(tuple executor.Tuple) error {
		if len(tuple) == 0 {
			return nil
		}
		elemVal := reflect.New(elemType).Elem()
		if err := setScalarValue(elemVal, tuple[0]); err != nil {
			return err
		}
		if elemIsPtr {
			ptr := reflect.New(elemType)
			ptr.Elem().Set(elemVal)
			newSlice = reflect.Append(newSlice, ptr)
		} else {
			newSlice = reflect.Append(newSlice, elemVal)
		}
		return nil
	}); ferr != nil {
		return ferr
	}
	sliceVal.Set(newSlice)
	return nil
}

// QueryOneInto executes a Datalog query expecting at most one result and populates a value.
// The query can be either an EDN string or a *query.Query from the query builder.
// Supports both struct destinations (multi-symbol) and scalar destinations (single-symbol).
// Returns (true, nil) if a result was found and mapped successfully.
// Returns (false, nil) if the query returns no results (empty result is valid, not an error).
// Returns (false, ErrMultipleResults) if more than one result exists.
// Returns (false, err) for other errors (parse errors, type errors, etc.).
//
// Example (struct):
//
//	var result PersonResult
//	found, err := db.QueryOneInto(&result, `[:find ?name ?age :where ...]`)
//
// Example (scalar):
//
//	var name string
//	found, err := db.QueryOneInto(&name, `[:find ?name :where [?e :person/name ?name] [(= ?e eid)]]`)
func (d *Database) QueryOneInto(dest interface{}, queryInput interface{}, inputs ...interface{}) (found bool, err error) {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return false, fmt.Errorf("QueryOneInto requires pointer destination")
	}
	elemVal := destVal.Elem()
	elemType := elemVal.Type()

	// Resolve the query (string or *query.Query)
	q, err := d.resolveQuery(queryInput)
	if err != nil {
		return false, fmt.Errorf("failed to resolve query: %w", err)
	}

	// Execute query as streaming relation
	rel, err := d.Query(q, inputs...)
	if err != nil {
		return false, err
	}
	iter := rel.Iterator()
	defer iter.Close()

	// Read first tuple. A false Next() can mean empty OR a deferred iterator
	// failure — check Error() so a failure isn't reported as "not found".
	if !iter.Next() {
		if e := iter.Error(); e != nil {
			return false, e
		}
		return false, nil
	}
	firstTuple := make([]interface{}, len(iter.Tuple()))
	copy(firstTuple, iter.Tuple())

	// Check for multiple results. A false Next() here can also be a failure;
	// don't let it look like "exactly one result".
	if iter.Next() {
		return false, dlreflect.ErrMultipleResults
	}
	if e := iter.Error(); e != nil {
		return false, e
	}

	// Check if destination is a struct (but not time.Time, Identity, Keyword which are scalars)
	isStruct := elemType.Kind() == reflect.Struct && !isScalarStructType(elemType)

	if isStruct {
		findSymbols := extractFindSymbolStrings(q.Find)
		mapper, err := dlreflect.NewQueryResultMapper(elemType, findSymbols)
		if err != nil {
			return false, err
		}
		if err := mapper.MapTuple(firstTuple, elemVal); err != nil {
			return false, err
		}
		return true, nil
	}

	// Scalar path - single symbol queries only
	if len(q.Find) != 1 {
		return false, fmt.Errorf("scalar QueryOneInto requires exactly 1 find element, got %d", len(q.Find))
	}

	if len(firstTuple) == 0 {
		return false, fmt.Errorf("query returned empty tuple")
	}
	if err := setScalarValue(elemVal, firstTuple[0]); err != nil {
		return false, err
	}
	return true, nil
}

// extractFindSymbolStrings extracts symbol names from :find clause as strings.
// For variables, returns "?name". For aggregates, returns "(sum ?x)".
func extractFindSymbolStrings(find []query.FindElement) []string {
	symbols := make([]string, len(find))
	for i, elem := range find {
		symbols[i] = elem.String()
	}
	return symbols
}

// Scalar struct types - these are structs but treated as scalar values
var (
	timeType     = reflect.TypeOf(time.Time{})
	identityType = reflect.TypeOf((datalog.Identity)(nil))
	keywordType  = reflect.TypeOf((datalog.Keyword)(nil)) // Keyword is *keyword, no .Elem()
)

// isScalarStructType returns true if the type is a struct that should be treated as a scalar
func isScalarStructType(t reflect.Type) bool {
	return t == timeType || t == identityType || t == keywordType
}

// mapScalarResults maps single-symbol query results to a scalar slice.
func mapScalarResults(results [][]interface{}, sliceVal reflect.Value, elemIsPtr bool) error {
	elemType := sliceVal.Type().Elem()
	if elemIsPtr {
		elemType = elemType.Elem()
	}

	newSlice := reflect.MakeSlice(sliceVal.Type(), len(results), len(results))

	for i, tuple := range results {
		if len(tuple) == 0 {
			continue
		}
		val := tuple[0]

		var elemVal reflect.Value
		if elemIsPtr {
			elemVal = reflect.New(elemType).Elem()
		} else {
			elemVal = newSlice.Index(i)
		}

		if err := setScalarValue(elemVal, val); err != nil {
			return fmt.Errorf("tuple %d: %w", i, err)
		}

		if elemIsPtr {
			ptr := reflect.New(elemType)
			ptr.Elem().Set(elemVal)
			newSlice.Index(i).Set(ptr)
		}
	}

	sliceVal.Set(newSlice)
	return nil
}

// setScalarValue sets a reflect.Value to the given interface{} value with type coercion.
func setScalarValue(dest reflect.Value, val interface{}) error {
	if val == nil {
		return nil // Leave zero value for nil
	}

	destType := dest.Type()
	valReflect := reflect.ValueOf(val)

	// Direct type match
	if valReflect.Type().AssignableTo(destType) {
		dest.Set(valReflect)
		return nil
	}

	// Type coercion by destination kind
	switch destType.Kind() {
	case reflect.String:
		if s, ok := val.(string); ok {
			dest.SetString(s)
			return nil
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := val.(type) {
		case int64:
			dest.SetInt(v)
			return nil
		case int:
			dest.SetInt(int64(v))
			return nil
		case float64:
			dest.SetInt(int64(v))
			return nil
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch v := val.(type) {
		case int64:
			dest.SetUint(uint64(v))
			return nil
		case uint64:
			dest.SetUint(v)
			return nil
		case float64:
			dest.SetUint(uint64(v))
			return nil
		}

	case reflect.Float32, reflect.Float64:
		switch v := val.(type) {
		case float64:
			dest.SetFloat(v)
			return nil
		case int64:
			dest.SetFloat(float64(v))
			return nil
		}

	case reflect.Bool:
		if b, ok := val.(bool); ok {
			dest.SetBool(b)
			return nil
		}

	case reflect.Struct:
		// Handle scalar struct types
		if destType == timeType {
			switch t := val.(type) {
			case time.Time:
				dest.Set(reflect.ValueOf(t))
				return nil
			case *time.Time:
				if t != nil {
					dest.Set(reflect.ValueOf(*t))
					return nil
				}
			}
		}
		if destType == identityType {
			if id, ok := val.(datalog.Identity); ok && id != nil {
				dest.Set(reflect.ValueOf(id))
				return nil
			}
		}
		if destType == keywordType {
			if kw, ok := val.(datalog.Keyword); ok && kw != nil {
				dest.Set(reflect.ValueOf(kw))
				return nil
			}
		}

	case reflect.Slice:
		// Handle []byte
		if destType.Elem().Kind() == reflect.Uint8 {
			if b, ok := val.([]byte); ok {
				dest.SetBytes(b)
				return nil
			}
		}
	}

	// Try conversion as last resort
	if valReflect.Type().ConvertibleTo(destType) {
		dest.Set(valReflect.Convert(destType))
		return nil
	}

	return fmt.Errorf("cannot convert %T to %s", val, destType)
}

// GetExecutor returns a new query executor
// This provides direct access to the executor for advanced use cases
func (d *Database) GetExecutor() *executor.Executor {
	return d.NewExecutor()
}

// entityAttrKey is used to track per-(E, A) state within a transaction.
// Used for vector appends to chain elements correctly.
type entityAttrKey struct {
	E [20]byte // Entity hash
	A string   // Attribute string
}

// Transaction represents a write transaction
type Transaction struct {
	db                *Database
	datoms            []datalog.Datom
	retracts          []datalog.Datom
	mu                sync.Mutex
	closed            bool
	txTime            *time.Time                          // Optional custom transaction time
	lastVectorElement map[entityAttrKey]datalog.ElementID // Track last appended element per (E,A) for RGA chaining
}

// SetTime sets a custom transaction time for this transaction
// This is useful for backdated data (e.g., historical prices)
func (t *Transaction) SetTime(txTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.txTime = &txTime
}

// Add adds a value using schema-aware CRDT semantics.
// The schema determines the semantics - callers don't need to know the cardinality.
//
// Behavior by cardinality:
// - Cardinality-One: LWW semantics (highest ElementID wins)
// - Cardinality-Many: Add-wins set semantics (value added with OpCRDTAdd)
// - Cardinality-Vector: RGA append semantics (element chained after previous in tx)
//
// For schemaless attributes (no schema or attribute not defined in schema),
// Add() stores the raw value with LWW semantics (cardinality-one behavior).
func (t *Transaction) Add(e datalog.Identity, a datalog.Keyword, v interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return fmt.Errorf("transaction is closed")
	}

	if err := validateAttributeStorable(a); err != nil {
		return err
	}

	// Nil values are not allowed in relational algebra - absence of fact represents "no value"
	if v == nil {
		return fmt.Errorf("nil value not allowed for attribute %s: use absence of fact to represent no value", a.String())
	}

	// Normalize integer width to canonical int64 at the API boundary, so a Go int
	// stores and matches the same as int64 (and never reaches the encoder as a
	// non-int64 width).
	v = datalog.NormalizeValue(v)

	// Schema validation (if schema present)
	if err := schema.ValidateDatom(t.db.Schema(), a, v); err != nil {
		return fmt.Errorf("schema validation failed for %s: %w", a.String(), err)
	}

	// Check cardinality for CRDT semantics
	var card schema.Cardinality
	var def *schema.AttributeDefinition
	hasSchema := false
	if s := t.db.Schema(); s != nil {
		if def = s.GetAttribute(a); def != nil {
			card = def.Cardinality
			hasSchema = true
		}
	}

	// Schema-aware behavior: Add() works for all cardinalities
	// The schema determines the semantics, not the method call
	elemID := t.db.clock.Next()

	if hasSchema {
		switch card {
		case schema.CardinalityMany:
			// CRDT add-wins semantics: store raw value with Op=Add
			// Op is stored in the key (between V and Tx) for proper AVET lookups
			t.datoms = append(t.datoms, datalog.Datom{
				E:  e,
				A:  a,
				V:  v, // Raw value - enables AVET lookups
				Tx: elemID,
				Op: datalog.OpCRDTAdd,
			})
		case schema.CardinalityOne:
			// CRDT LWW semantics: just append, no retraction needed
			// The storage layer will return only the highest ElementID on read
			t.datoms = append(t.datoms, datalog.Datom{
				E:  e,
				A:  a,
				V:  v,
				Tx: elemID,
			})
		case schema.CardinalityVector:
			// RGA append semantics - chain elements within transaction.
			// Bug #5 fix: Store raw value in V, AfterRef in datom.AfterRef field.
			//
			// IMPORTANT: Add() does NOT read from storage. AfterRef is set to the last
			// element added in THIS transaction, or HEAD (zero) if this is the first.
			//
			// CONCURRENT WRITE BEHAVIOR:
			// When two replicas Add() concurrently to the same vector (before sync),
			// both elements will have AfterRef=HEAD (or the same predecessor).
			// After merge, BOTH elements are preserved. Order is determined by
			// ElementID comparison during RGA reconstruction, NOT by wall-clock time.
			//
			// Example: Replica A adds "stealth", Replica B adds "magic" concurrently.
			// Both have AfterRef=HEAD. After merge, result is ["stealth", "magic"]
			// or ["magic", "stealth"] depending on which replica has lower ReplicaID.
			//
			// This is NOT last-writer-wins - all concurrent writes are preserved.
			// See docs/reference/CRDT.md for detailed semantics.
			key := entityAttrKey{E: e.Hash(), A: a.String()}

			// OrderedSet uniqueness check: if UniqueElements is true, check if value already exists
			if def.UniqueElements {
				if t.vectorContainsValue(e, a, v) {
					// Value already exists in ordered set - no-op
					return nil
				}
			}

			afterRef := t.lastVectorElement[key] // Zero value = HEAD

			t.datoms = append(t.datoms, datalog.Datom{
				E:        e,
				A:        a,
				V:        v, // Raw value, not RGAElement wrapper
				Tx:       elemID,
				Op:       datalog.OpRGAInsert, // New Op type indicates AfterRef present
				AfterRef: afterRef,
			})

			t.lastVectorElement[key] = elemID
		default:
			// Unknown cardinality - treat as cardinality-one
			t.datoms = append(t.datoms, datalog.Datom{
				E:  e,
				A:  a,
				V:  v,
				Tx: elemID,
			})
		}
	} else {
		// Schemaless mode: CardinalityOne (LWW) semantics
		// OpNone is valid and means "CardinalityOne LWW assertion"
		t.datoms = append(t.datoms, datalog.Datom{
			E:  e,
			A:  a,
			V:  v,
			Tx: elemID,
			Op: datalog.OpNone,
		})
	}

	return nil
}

// Remove removes a value using CRDT tombstone semantics.
//
// Behavior by cardinality:
// - Cardinality-One: Writes OpCRDTRemove; attribute doesn't exist if tombstone has highest Tx
// - Cardinality-Many: Writes OpCRDTRemove; add-wins resolves per-value conflicts
// - Cardinality-Vector: RGA LIFO tombstone; removes most recently added matching element
// - Schemaless/undefined: Defaults to CardinalityOne
func (t *Transaction) Remove(e datalog.Identity, a datalog.Keyword, v interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return fmt.Errorf("transaction is closed")
	}

	if err := validateAttributeStorable(a); err != nil {
		return err
	}

	// Nil values are not allowed
	if v == nil {
		return fmt.Errorf("nil value not allowed for Remove on attribute %s", a.String())
	}

	// Normalize integer width to canonical int64 at the API boundary.
	v = datalog.NormalizeValue(v)

	// Determine cardinality for Remove semantics
	s := t.db.Schema()
	var def *schema.AttributeDefinition
	if s != nil {
		def = s.GetAttribute(a)
	}

	// Determine cardinality: schemaless/undefined defaults to CardinalityOne
	var card schema.Cardinality
	if def != nil {
		card = def.Cardinality
	} else {
		// Schemaless or undefined attribute: CardinalityOne (LWW)
		card = schema.CardinalityOne
	}

	switch card {
	case schema.CardinalityMany:
		// CRDT add-wins semantics: store raw value with Op=Remove (tombstone)
		// Op is stored in the key (between V and Tx) for proper AVET lookups
		// Per-operation Lamport: each Remove() gets its own ElementID for causal ordering
		elemID := t.db.clock.Next()
		t.datoms = append(t.datoms, datalog.Datom{
			E:  e,
			A:  a,
			V:  v, // Raw value - enables AVET lookups
			Tx: elemID,
			Op: datalog.OpCRDTRemove,
		})
	case schema.CardinalityOne:
		// Write tombstone — CRDTResolvingIterator checks Op on first entry
		elemID := t.db.clock.Next()
		t.datoms = append(t.datoms, datalog.Datom{
			E:  e,
			A:  a,
			V:  v,
			Tx: elemID,
			Op: datalog.OpCRDTRemove,
		})
	case schema.CardinalityVector:
		// RGA LIFO Remove: tombstone the most recently added element matching value
		// O(k) via EAVT scan where k = entries for this (E, A, V) combination
		//
		// Design decision: Remove() uses LIFO (most recent) semantics because:
		// 1. O(k) performance - direct index lookup, no RGA reconstruction
		// 2. CRDT-friendly - "most recent" = "the one I most recently added"
		// 3. LIFO/Stack pattern - useful for undo operations
		// Users wanting to remove the FIRST occurrence must use read-modify-write via Set()

		// Build set of element IDs that have pending tombstones in this transaction
		// This allows multiple Remove() calls in the same transaction to work correctly
		pendingTombstones := make(map[datalog.ElementID]bool)
		for _, d := range t.datoms {
			if d.A.Matches(a) && e.Equal(d.E) && d.Op == datalog.OpRGATombstone {
				pendingTombstones[d.AfterRef] = true
			}
		}

		// Build EAVT prefix: [prefix][E][A][V] for O(k) scan
		// EAVT key order: [E][A][V][Tx↓][Op][AfterRef?]
		// Tx descending means highest Tx (most recent) comes first
		eBytes := e.Hash()
		var aBytes [32]byte
		copy(aBytes[:], a.String())
		vType := byte(datalog.Type(v))
		vData := datalog.ValueBytes(v)
		vBytes := append([]byte{vType}, vData...)

		prefix := make([]byte, 1+20+32+len(vBytes))
		prefix[0] = byte(EAVT)
		copy(prefix[1:21], eBytes[:])
		copy(prefix[21:53], aBytes[:])
		copy(prefix[53:], vBytes)

		iter, err := t.db.store.Scan(EAVT, prefix, prefixEnd(prefix))
		if err != nil {
			return fmt.Errorf("EAVT scan for vector Remove failed: %w", err)
		}
		defer iter.Close()

		// Scan in Tx descending order:
		// - Tombstones (higher Tx) come before the inserts they delete
		// - Collect tombstone AfterRefs, stop at first non-tombstoned insert
		tombstonedIDs := make(map[datalog.ElementID]bool)
		var targetID datalog.ElementID
		var targetValue interface{}

		for iter.Next() {
			datom, err := iter.Datom()
			if err != nil {
				continue
			}

			if datom.Op == datalog.OpRGATombstone {
				// Record this tombstone's target
				tombstonedIDs[datom.AfterRef] = true
			} else if datom.Op == datalog.OpRGAInsert {
				// Check if this insert is tombstoned (committed or pending)
				if !tombstonedIDs[datom.Tx] && !pendingTombstones[datom.Tx] {
					// Found the most recent non-tombstoned insert
					targetID = datom.Tx
					targetValue = datom.V
					break // Early termination - no need to scan further
				}
			}
		}

		// If no matching element found, Remove is a no-op (not an error)
		if targetID.IsZero() {
			return nil
		}

		// Write tombstone for the target element
		tombstoneID := t.db.clock.Next()
		t.datoms = append(t.datoms, datalog.Datom{
			E:        e,
			A:        a,
			V:        targetValue, // Raw value (for verification/debugging)
			Tx:       tombstoneID, // New tombstone ID
			Op:       datalog.OpRGATombstone,
			AfterRef: targetID, // ID of element being tombstoned
		})
	default:
		return fmt.Errorf("Remove not valid for cardinality %v", def.Cardinality)
	}

	return nil
}

// toAnySlice converts a typed slice (e.g. []string, []int64) to []interface{}
// using reflection. Returns the converted slice and true, or nil and false if
// the value is not a slice.
// validateAttributeStorable rejects attribute keywords whose UTF-8 form is too
// long to store. The storage Attribute is a fixed [32]byte; a longer name would
// be silently truncated and alias other attributes sharing its first 32 bytes
// (see datalog.MaxAttributeBytes). Rejecting at write/schema time turns that
// silent corruption into a clear error.
func validateAttributeStorable(a datalog.Keyword) error {
	if a == nil {
		return fmt.Errorf("nil attribute")
	}
	if n := len(a.String()); n > datalog.MaxAttributeBytes {
		return fmt.Errorf("attribute %q is %d bytes, exceeds the %d-byte storage limit", a.String(), n, datalog.MaxAttributeBytes)
	}
	return nil
}

// memberKey derives a hashable map key for a cardinality-many set member:
// a type tag plus the value's encoded bytes. []byte (and other non-comparable
// values) cannot be used as Go map keys directly, so membership maps key by
// this string and carry the original value separately. It is byte-for-byte the
// same key resolveAddWinsSet uses internally, so the Set diff lines up with
// stored membership. Note: this keys floats by raw bits, which differs from
// datalog.ValuesEqual at float corner cases (±0.0, NaN) but matches the read
// path — the property that matters here.
func memberKey(v interface{}) string {
	return string(append([]byte{byte(datalog.Type(v))}, datalog.ValueBytes(v)...))
}

func toAnySlice(v interface{}) ([]interface{}, bool) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice {
		return nil, false
	}
	result := make([]interface{}, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		result[i] = rv.Index(i).Interface()
	}
	return result, true
}

// Set sets a value for a cardinality-one attribute using CRDT LWW semantics.
// For cardinality-one: just appends the new value. The storage layer handles
// Last-Writer-Wins resolution - no read-before-write or retraction needed.
//
// This method is part of the CRDT implementation:
// - Cardinality-One: Highest ElementID wins (LWW semantics)
// - Cardinality-Many: Replaces entire set (reads current, generates Add/Remove ops)
// - Cardinality-Vector: Replaces entire vector
func (t *Transaction) Set(e datalog.Identity, a datalog.Keyword, v interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return fmt.Errorf("transaction is closed")
	}

	if err := validateAttributeStorable(a); err != nil {
		return err
	}

	// Nil values are not allowed in relational algebra - absence of fact represents "no value"
	if v == nil {
		return fmt.Errorf("nil value not allowed for attribute %s: use absence of fact to represent no value", a.String())
	}

	// Normalize integer width to canonical int64 at the API boundary.
	v = datalog.NormalizeValue(v)

	// Check cardinality first - determines validation strategy
	card := schema.CardinalityOne
	if s := t.db.Schema(); s != nil {
		if def := s.GetAttribute(a); def != nil {
			card = def.Cardinality
		}
	}

	switch card {
	case schema.CardinalityOne:
		// Schema validation for single value
		if err := schema.ValidateDatom(t.db.Schema(), a, v); err != nil {
			return fmt.Errorf("schema validation failed for %s: %w", a.String(), err)
		}
		// CRDT LWW semantics: just append, no retraction needed
		// The storage layer will return only the highest ElementID on read
		// Per-operation Lamport: each Set() gets its own ElementID for causal ordering
		elemID := t.db.clock.Next()
		t.datoms = append(t.datoms, datalog.Datom{
			E:  e,
			A:  a,
			V:  v,
			Tx: elemID,
		})

	case schema.CardinalityMany:
		// Set() for cardinality-many replaces the entire set
		// This requires reading current membership and diffing
		newSlice, ok := v.([]interface{})
		if !ok {
			// Try []any (same thing, different spelling)
			if anySlice, ok2 := v.([]any); ok2 {
				newSlice = anySlice
			} else if converted, ok2 := toAnySlice(v); ok2 {
				newSlice = converted
			} else {
				return fmt.Errorf("Set for cardinality-many requires a slice, got %T", v)
			}
		}

		// Schema validation for each element in the slice
		for _, val := range newSlice {
			if err := schema.ValidateDatom(t.db.Schema(), a, val); err != nil {
				return fmt.Errorf("schema validation failed for %s element: %w", a.String(), err)
			}
		}

		// Build set of new values for O(1) lookup, keyed by memberKey so
		// non-comparable members like []byte can be map keys; the stored value
		// is the original so emitted datoms keep their real type. Keying by
		// content also dedups duplicate slice members.
		newSet := make(map[string]interface{}, len(newSlice))
		for _, val := range newSlice {
			newSet[memberKey(val)] = val
		}

		// Get current set membership from committed state
		matcher := NewBadgerMatcher(t.db.store)
		eBytes := e.Hash()
		var aBytes [32]byte
		copy(aBytes[:], a.String())
		currentResult, err := matcher.resolveAddWinsSet(eBytes[:], aBytes[:])
		if err != nil {
			return fmt.Errorf("failed to read current set membership: %w", err)
		}

		// Apply pending transaction operations to get effective current set.
		// This ensures Set() sees Add/Remove ops from earlier in the same
		// transaction. Keyed by memberKey; values are the original members.
		effectiveSet := make(map[string]interface{})
		for _, member := range currentResult.Members {
			effectiveSet[memberKey(member)] = member
		}

		// Scan pending datoms for this (E, A) pair
		pendingAdds := make(map[string]datalog.ElementID)
		pendingRemoves := make(map[string]datalog.ElementID)
		pendingValues := make(map[string]interface{})
		for _, datom := range t.datoms {
			if datom.E.Hash() != eBytes || datom.A.String() != a.String() {
				continue
			}
			k := memberKey(datom.V)
			pendingValues[k] = datom.V
			// NEW FORMAT: Op is a field on Datom, V is the raw value
			if datom.Op == datalog.OpCRDTAdd {
				pendingAdds[k] = datom.Tx
			} else if datom.Op == datalog.OpCRDTRemove {
				pendingRemoves[k] = datom.Tx
			}
		}

		// Apply pending ops using add-wins semantics. For each value, compare
		// highest pending add Lamport vs highest pending remove Lamport.
		for k, val := range pendingValues {
			addTx, hasAdd := pendingAdds[k]
			removeTx, hasRemove := pendingRemoves[k]

			if hasAdd && !hasRemove {
				// Only add pending
				effectiveSet[k] = val
			} else if !hasAdd && hasRemove {
				// Only remove pending
				delete(effectiveSet, k)
			} else {
				// Both add and remove pending - compare Lamport (add-wins at same Lamport)
				if addTx.Lamport >= removeTx.Lamport {
					effectiveSet[k] = val
				} else {
					delete(effectiveSet, k)
				}
			}
		}

		// Remove members that are in effective set but not in new set
		for k, member := range effectiveSet {
			if _, ok := newSet[k]; !ok {
				elemID := t.db.clock.Next()
				// NEW FORMAT: Op is a field on Datom, V is the raw value
				t.datoms = append(t.datoms, datalog.Datom{
					E:  e,
					A:  a,
					V:  member, // Raw value
					Tx: elemID,
					Op: datalog.OpCRDTRemove,
				})
			}
		}

		// Add members that are in new set but not in effective set.
		// newSet is deduplicated by content, avoiding duplicate writes.
		for k, val := range newSet {
			if _, ok := effectiveSet[k]; !ok {
				elemID := t.db.clock.Next()
				// NEW FORMAT: Op is a field on Datom, V is the raw value
				t.datoms = append(t.datoms, datalog.Datom{
					E:  e,
					A:  a,
					V:  val, // Raw value
					Tx: elemID,
					Op: datalog.OpCRDTAdd,
				})
			}
		}

	case schema.CardinalityVector:
		// Set() for cardinality-vector uses prefix-diff optimization:
		// - Find common prefix between old and new vectors
		// - Only tombstone elements after the common prefix
		// - Only insert elements after the common prefix
		// This makes appends O(k) instead of O(n+m) for the common case.
		newSlice, ok := v.([]interface{})
		if !ok {
			// Try []any (same thing, different spelling)
			if anySlice, ok2 := v.([]any); ok2 {
				newSlice = anySlice
			} else if converted, ok2 := toAnySlice(v); ok2 {
				newSlice = converted
			} else {
				return fmt.Errorf("Set for cardinality-vector requires a slice, got %T", v)
			}
		}

		// Schema validation for each element in the slice
		for _, val := range newSlice {
			if err := schema.ValidateDatom(t.db.Schema(), a, val); err != nil {
				return fmt.Errorf("schema validation failed for %s element: %w", a.String(), err)
			}
		}

		// Get current vector from cache (if enabled) or resolve directly
		eBytes := e.Hash()
		var aBytes Attribute
		copy(aBytes[:], a.String())
		key := CacheKey{E: Entity(eBytes), A: aBytes}

		matcher := NewBadgerMatcher(t.db.store)
		matcher.SetSchema(t.db.schema)

		var entry *CacheEntry
		if t.db.cache != nil {
			entry = t.db.cache.GetOrResolve(key, matcher)
		} else {
			// Cache disabled - resolve directly from storage
			entry = ResolveEntry(key, matcher)
		}

		var oldList []any
		var oldIndex []datalog.ElementID
		if entry != nil && entry.Cardinality() == schema.CardinalityVector {
			oldList = entry.VectorList()
			oldIndex = entry.VectorIndex()
		}

		// Find common prefix - elements that don't need to change
		// RGA constraint: we can only keep a PREFIX because descendants of
		// tombstoned elements become unreachable in the DFS traversal.
		commonPrefix := 0
		maxPrefix := len(oldList)
		if len(newSlice) < maxPrefix {
			maxPrefix = len(newSlice)
		}
		for i := 0; i < maxPrefix; i++ {
			if !datalog.ValuesEqual(oldList[i], newSlice[i]) {
				break
			}
			commonPrefix++
		}

		// Tombstone old[commonPrefix:] - elements after common prefix
		for i := commonPrefix; i < len(oldList); i++ {
			tombstoneID := t.db.clock.Next()
			t.datoms = append(t.datoms, datalog.Datom{
				E:        e,
				A:        a,
				V:        oldList[i],  // Raw value (for verification/debugging)
				Tx:       tombstoneID, // New tombstone ID
				Op:       datalog.OpRGATombstone,
				AfterRef: oldIndex[i], // ID of element being tombstoned
			})
		}

		// Insert new[commonPrefix:] - elements after common prefix
		// Chain from last kept element (or HEAD if prefix is 0)
		afterRef := datalog.ElementID{} // HEAD
		if commonPrefix > 0 {
			afterRef = oldIndex[commonPrefix-1]
		}

		eaKey := entityAttrKey{E: e.Hash(), A: a.String()}
		for _, val := range newSlice[commonPrefix:] {
			elemID := t.db.clock.Next()

			t.datoms = append(t.datoms, datalog.Datom{
				E:        e,
				A:        a,
				V:        val,
				Tx:       elemID,
				Op:       datalog.OpRGAInsert,
				AfterRef: afterRef,
			})

			afterRef = elemID
		}
		// Update lastVectorElement for subsequent Add() calls in same transaction
		t.lastVectorElement[eaKey] = afterRef

	default:
		return fmt.Errorf("Set not valid for unknown cardinality %v", card)
	}

	return nil
}

// vectorContainsValue checks if a value already exists in an ordered set (vector with unique elements).
// It checks both committed data (via cache) and pending datoms in this transaction.
func (t *Transaction) vectorContainsValue(e datalog.Identity, a datalog.Keyword, v interface{}) bool {
	// Check pending datoms in this transaction first
	eHash := e.Hash()
	for _, d := range t.datoms {
		if d.A == a && d.E.Hash() == eHash && d.Op == datalog.OpRGAInsert {
			if datalog.ValuesEqual(d.V, v) {
				return true
			}
		}
	}

	// Check committed data via cache (if enabled) or direct resolution
	eBytes := e.Hash()
	var aBytes Attribute
	copy(aBytes[:], a.String())
	cacheKey := CacheKey{E: Entity(eBytes), A: aBytes}

	matcher := NewBadgerMatcher(t.db.store)
	matcher.SetSchema(t.db.schema)

	var entry *CacheEntry
	if t.db.cache != nil {
		entry = t.db.cache.GetOrResolve(cacheKey, matcher)
	} else {
		entry = ResolveEntry(cacheKey, matcher)
	}

	if entry != nil && entry.Cardinality() == schema.CardinalityVector {
		for _, existing := range entry.VectorList() {
			if datalog.ValuesEqual(existing, v) {
				return true
			}
		}
	}

	return false
}

// Retract removes a datom
func (t *Transaction) Retract(e datalog.Identity, a datalog.Keyword, v interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return fmt.Errorf("transaction is closed")
	}

	if err := validateAttributeStorable(a); err != nil {
		return err
	}

	// Nil values are not allowed - must specify exact value to retract
	if v == nil {
		return fmt.Errorf("nil value not allowed for retraction of %s: must specify exact value to retract", a.String())
	}

	t.retracts = append(t.retracts, datalog.Datom{
		E:  e,
		A:  a,
		V:  v,
		Tx: datalog.ElementID{}, // Will be set on commit
	})

	return nil
}

// AddEntity adds all datoms for an entity map
func (t *Transaction) AddEntity(e datalog.Identity, attrs map[datalog.Keyword]interface{}) error {
	for attr, value := range attrs {
		if err := t.Add(e, attr, value); err != nil {
			return err
		}
	}
	return nil
}

// AddMap is a convenience method that creates an entity ID and adds the attributes
func (t *Transaction) AddMap(attrs map[string]interface{}) (datalog.Identity, error) {
	// Generate entity ID
	e := datalog.NewIdentity(fmt.Sprintf("e%d", time.Now().UnixNano()))

	// Convert string keys to keywords and add
	kwAttrs := make(map[datalog.Keyword]interface{})
	for k, v := range attrs {
		kwAttrs[datalog.NewKeyword(k)] = v
	}

	if err := t.AddEntity(e, kwAttrs); err != nil {
		return nil, err
	}

	return e, nil
}

// SaveStruct persists a struct to the database with upsert semantics.
// If the struct has an ID field (tagged with `datalog:"-,id"`) and it's empty, a new ID is generated.
// The generated or existing ID is returned and also set on the struct's ID field.
//
// Upsert behavior:
//   - Cardinality-one fields: retracts old value if different, adds new value
//   - Cardinality-many fields (nil slice): leaves existing values unchanged
//   - Cardinality-many fields (empty slice): clears all existing values
//   - Cardinality-many fields (non-empty): diff-based update (only changes what's different)
//
// Example:
//
//	person := Person{Name: "Alice", Age: 30}
//	id, err := tx.SaveStruct(&person)
//	// person.ID is now set to the generated identity
//
//	// Later, modify and save again
//	person.Name = "Alice Smith"
//	id, err = tx.SaveStruct(&person)  // Updates name, age unchanged
func (t *Transaction) SaveStruct(v interface{}) (datalog.Identity, error) {
	matcher := t.db.Matcher().(*BadgerMatcher)
	return dlreflect.SaveStruct(t, matcher, v, t.db.Schema())
}

// Commit commits the transaction
func (t *Transaction) Commit() (datalog.ElementID, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return datalog.ElementID{}, fmt.Errorf("transaction is closed")
	}

	// Uniqueness is a read-time CRDT resolution rule, not a write-time gate.
	// All writes succeed; the canonical owner of a unique (A, V) is determined
	// at read time by (A, V)-LWW. See docs/proposals/CRDT_UNIQUE_SEMANTICS.md.

	// Record transaction time for metadata
	var txTime time.Time
	if t.txTime != nil {
		txTime = *t.txTime
	} else {
		txTime = time.Now()
	}

	// Per-operation Lamport: datoms already have their Tx set by Add/Remove/Set.
	// Only legacy Retract() method needs Tx assignment at commit time.
	// Each retract gets its own per-operation Lamport for causal ordering.
	for i := range t.retracts {
		t.retracts[i].Tx = t.db.clock.Next()
	}

	// Build transaction metadata. Same Lamport as the rest — final operation
	// in the logical commit, used as the high-water mark for as-of queries.
	metadataElemID := t.db.clock.Next()
	txEntity := datalog.NewIdentity(fmt.Sprintf("tx:%d", metadataElemID.Lamport))
	txMetadata := []datalog.Datom{
		{
			E:  txEntity,
			A:  datalog.NewKeyword(":db/txInstant"),
			V:  txTime,
			Tx: metadataElemID,
		},
	}

	// Open a single storage transaction for the entire logical commit so
	// retractions, assertions, and metadata writes are atomic. Any failure
	// rolls back all of them; no partial commit is observable.
	stx, err := t.db.store.BeginTx()
	if err != nil {
		return datalog.ElementID{}, fmt.Errorf("failed to begin storage transaction: %w", err)
	}

	commitErr := func() error {
		if len(t.retracts) > 0 {
			if err := stx.Retract(t.retracts); err != nil {
				return fmt.Errorf("failed to retract datoms: %w", err)
			}
		}
		if len(t.datoms) > 0 {
			if err := stx.Assert(t.datoms); err != nil {
				return fmt.Errorf("failed to assert datoms: %w", err)
			}
		}
		// Transaction metadata is part of the same atomic commit; failure
		// rolls back the entire transaction (no more best-effort metadata).
		if err := stx.Assert(txMetadata); err != nil {
			return fmt.Errorf("failed to write transaction metadata: %w", err)
		}
		return nil
	}()

	if commitErr != nil {
		_ = stx.Rollback()
		return datalog.ElementID{}, commitErr
	}

	// Mark every (E, A) this commit touches as in-flight BEFORE the storage
	// commit. While in-flight, readers resolve those keys straight from storage,
	// so the storage commit and the cache update become atomic from a reader's
	// perspective — no reader observes the pre-commit value once stx.Commit()
	// returns. Cleared by the Invalidate below (success) or on commit failure.
	var touched []CacheKey
	if t.db.cache != nil {
		touched = t.touchedCacheKeys()
		t.db.cache.BeginInFlight(touched)
	}

	if err := stx.Commit(); err != nil {
		if t.db.cache != nil {
			t.db.cache.Invalidate(touched) // clear in-flight sentinels
		}
		return datalog.ElementID{}, fmt.Errorf("failed to commit storage transaction: %w", err)
	}

	// Test-only: run a reader in the post-commit / pre-cache-update window.
	if t.db.onCommitWindow != nil {
		t.db.onCommitWindow()
	}

	// Update cache: track max versions and invalidate stale entries
	// Skip if cache is disabled
	if t.db.cache != nil {
		seenKeys := make(map[CacheKey]bool)

		// Attributes written in this commit that are declared Unique.
		// Writes to these attributes can silently supersede other entities'
		// cached values under (A, V)-LWW fallback, so we invalidate all
		// cached (E, A) entries for the attribute after per-key updates.
		uniqueAttrsWritten := make(map[Attribute]bool)
		sch := t.db.Schema()
		checkUnique := func(a datalog.Keyword, aBytes Attribute) {
			if sch == nil || !sch.HasSchema() {
				return
			}
			if def := sch.GetAttribute(a); def != nil && def.Unique != "" {
				uniqueAttrsWritten[aBytes] = true
			}
		}

		// Process asserted datoms
		for _, d := range t.datoms {
			eBytes := Entity(d.E.Hash())

			var aBytes Attribute
			copy(aBytes[:], d.A.String())

			key := CacheKey{E: eBytes, A: aBytes}
			if !seenKeys[key] {
				seenKeys[key] = true
				t.db.cache.UpdateMaxVersion(key, d.Tx)
			}
			checkUnique(d.A, aBytes)
		}

		// Process retracted datoms
		for _, d := range t.retracts {
			eBytes := Entity(d.E.Hash())

			var aBytes Attribute
			copy(aBytes[:], d.A.String())

			key := CacheKey{E: eBytes, A: aBytes}
			if !seenKeys[key] {
				seenKeys[key] = true
				t.db.cache.UpdateMaxVersion(key, d.Tx)
			}
			checkUnique(d.A, aBytes)
		}

		// Invalidate cache entries for all touched (E, A) pairs. This also
		// deletes the in-flight sentinels set by BeginInFlight above, ending the
		// in-flight window: subsequent reads rebuild from storage at the new
		// value, and maxVersions was just bumped to match.
		t.db.cache.Invalidate(touched)

		// Conservative unique-attr invalidation: for each unique attribute
		// written in this commit, invalidate every cached entry for that
		// attribute across all entities. See CRDT_UNIQUE_SEMANTICS.md D3.
		for aBytes := range uniqueAttrsWritten {
			t.db.cache.InvalidateAttribute(aBytes)
		}
	}

	// Clean up
	t.closed = true
	t.db.mu.Lock()
	delete(t.db.activeTx, t)
	t.db.mu.Unlock()

	// Return the metadata ElementID - it has the highest Lamport in this tx,
	// making it the correct high-water mark for as-of queries
	return metadataElemID, nil
}

// touchedCacheKeys returns the deduplicated set of (E, A) cache keys this
// transaction's asserts and retracts touch. Computed before the storage commit
// so the keys can be marked in-flight; reused afterward to invalidate them.
func (t *Transaction) touchedCacheKeys() []CacheKey {
	touched := make([]CacheKey, 0, len(t.datoms)+len(t.retracts))
	seen := make(map[CacheKey]bool)
	add := func(e datalog.Identity, a datalog.Keyword) {
		var aBytes Attribute
		copy(aBytes[:], a.String())
		key := CacheKey{E: Entity(e.Hash()), A: aBytes}
		if !seen[key] {
			seen[key] = true
			touched = append(touched, key)
		}
	}
	for _, d := range t.datoms {
		add(d.E, d.A)
	}
	for _, d := range t.retracts {
		add(d.E, d.A)
	}
	return touched
}

// Rollback aborts the transaction
func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	t.closed = true
	t.datoms = nil
	t.retracts = nil

	t.db.mu.Lock()
	delete(t.db.activeTx, t)
	t.db.mu.Unlock()

	return nil
}

// Stats returns database statistics
func (d *Database) Stats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})
	stats["transactions"] = d.txCounter.Load()

	// Could add more stats from BadgerDB
	return stats, nil
}

// convertInputsToRelations converts Go values to executor.Relation based on the :in clause
func (d *Database) convertInputsToRelations(q *query.Query, inputs []interface{}) ([]executor.Relation, error) {
	inputRelations := make([]executor.Relation, 0, len(inputs))
	inputIdx := 0

	for _, inputSpec := range q.In {
		switch spec := inputSpec.(type) {
		case query.DatabaseInput:
			// Skip source markers ($, $users, etc.) - don't consume a regular input
			continue

		case query.ScalarInput:
			if inputIdx >= len(inputs) {
				return nil, fmt.Errorf("not enough inputs: expected input for %s (have %d inputs, need %d)", spec.Symbol, len(inputs), inputIdx+1)
			}

			// Create single-value relation (normalize integer width to int64 so
			// an int parameter matches stored int64 data).
			rel := executor.NewMaterializedRelation(
				[]query.Symbol{spec.Symbol},
				[]executor.Tuple{{datalog.NormalizeValue(inputs[inputIdx])}},
			)
			inputRelations = append(inputRelations, rel)
			inputIdx++

		case query.CollectionInput:
			if inputIdx >= len(inputs) {
				return nil, fmt.Errorf("not enough inputs: expected collection for %s", spec.Symbol)
			}

			// Convert slice to relation
			slice := reflect.ValueOf(inputs[inputIdx])
			if slice.Kind() != reflect.Slice && slice.Kind() != reflect.Array {
				return nil, fmt.Errorf("expected slice or array for collection input %s, got %T", spec.Symbol, inputs[inputIdx])
			}

			tuples := make([]executor.Tuple, slice.Len())
			for i := 0; i < slice.Len(); i++ {
				tuples[i] = executor.Tuple{datalog.NormalizeValue(slice.Index(i).Interface())}
			}

			rel := executor.NewMaterializedRelation(
				[]query.Symbol{spec.Symbol},
				tuples,
			)
			inputRelations = append(inputRelations, rel)
			inputIdx++

		case query.TupleInput:
			if inputIdx >= len(inputs) {
				return nil, fmt.Errorf("not enough inputs: expected tuple for %v", spec.Symbols)
			}

			// Expect a slice for tuple input
			slice := reflect.ValueOf(inputs[inputIdx])
			if slice.Kind() != reflect.Slice && slice.Kind() != reflect.Array {
				return nil, fmt.Errorf("expected slice or array for tuple input, got %T", inputs[inputIdx])
			}

			if slice.Len() != len(spec.Symbols) {
				return nil, fmt.Errorf("tuple input length mismatch: expected %d values, got %d", len(spec.Symbols), slice.Len())
			}

			// Create single tuple
			tuple := make(executor.Tuple, slice.Len())
			for i := 0; i < slice.Len(); i++ {
				tuple[i] = datalog.NormalizeValue(slice.Index(i).Interface())
			}

			rel := executor.NewMaterializedRelation(spec.Symbols, []executor.Tuple{tuple})
			inputRelations = append(inputRelations, rel)
			inputIdx++

		case query.RelationInput:
			if inputIdx >= len(inputs) {
				return nil, fmt.Errorf("not enough inputs: expected relation for %v", spec.Symbols)
			}

			// Expect a slice of slices for relation input
			outerSlice := reflect.ValueOf(inputs[inputIdx])
			if outerSlice.Kind() != reflect.Slice && outerSlice.Kind() != reflect.Array {
				return nil, fmt.Errorf("expected slice of slices for relation input, got %T", inputs[inputIdx])
			}

			tuples := make([]executor.Tuple, outerSlice.Len())
			for i := 0; i < outerSlice.Len(); i++ {
				innerSlice := outerSlice.Index(i)
				if innerSlice.Kind() != reflect.Slice && innerSlice.Kind() != reflect.Array {
					return nil, fmt.Errorf("expected slice for relation tuple %d, got %T", i, innerSlice.Interface())
				}

				if innerSlice.Len() != len(spec.Symbols) {
					return nil, fmt.Errorf("relation tuple %d length mismatch: expected %d values, got %d", i, len(spec.Symbols), innerSlice.Len())
				}

				tuple := make(executor.Tuple, innerSlice.Len())
				for j := 0; j < innerSlice.Len(); j++ {
					tuple[j] = datalog.NormalizeValue(innerSlice.Index(j).Interface())
				}
				tuples[i] = tuple
			}

			rel := executor.NewMaterializedRelation(spec.Symbols, tuples)
			inputRelations = append(inputRelations, rel)
			inputIdx++
		}
	}

	// Check we used all inputs
	if inputIdx < len(inputs) {
		return nil, fmt.Errorf("too many inputs: query expects %d inputs but got %d", inputIdx, len(inputs))
	}

	return inputRelations, nil
}

// Pull retrieves entity data according to a pull pattern
// This provides Datomic-style entity attribute retrieval with nested reference following.
//
// The pattern syntax:
//   - Simple attributes: [:entity/name :entity/code]
//   - Wildcard: [*] - all attributes
//   - Nested references: [{:entity/region [:region/code :region/name]}]
//   - Default values: [(default :entity/status "unknown")]
//   - Limits (for cardinality-many): [(limit :entity/tags 10)]
//
// Examples:
//
//	// Get name and code for an entity
//	result, err := db.Pull(entityID, `[:entity/name :entity/code]`)
//
//	// Get all attributes
//	result, err := db.Pull(entityID, `[*]`)
//
// LookupByUnique returns the entity currently owning (attr, value) under
// (A, V)-LWW resolution. Returns a nil Identity with a nil error if no
// entity currently claims the value.
//
// The attribute must be declared unique in the schema (UniqueValue or
// UniqueIdentity). Calling LookupByUnique on a non-unique attribute, an
// attribute not present in the schema, or a database without a schema
// returns an error.
//
// LookupByUnique is the primitive for natural-key lookup in the CRDT model:
// application-layer upsert ("find-or-create by email") is built on top of
// it. See docs/proposals/CRDT_UNIQUE_SEMANTICS.md for the design rationale.
//
// Concurrent writers may change the canonical owner between calls; treat
// the result as a snapshot. If an application needs to verify a write
// "won," it should call LookupByUnique after the commit and compare the
// returned Identity to its own.
func (d *Database) LookupByUnique(attr datalog.Keyword, value interface{}) (datalog.Identity, error) {
	if d.schema == nil {
		return nil, fmt.Errorf("LookupByUnique requires a schema declaring %s as unique", attr.String())
	}
	def := d.schema.GetAttribute(attr)
	if def == nil {
		return nil, fmt.Errorf("LookupByUnique: attribute %s not found in schema", attr.String())
	}
	if def.Unique == "" {
		return nil, fmt.Errorf("LookupByUnique: attribute %s is not unique", attr.String())
	}

	matcher, ok := d.Matcher().(*BadgerMatcher)
	if !ok {
		return nil, fmt.Errorf("LookupByUnique: unsupported matcher type %T", d.Matcher())
	}

	var aBytes Attribute
	copy(aBytes[:], attr.String())
	vBytes := encodeValueForSearch(value, d.store.encoder)

	owner, _, err := matcher.resolveAVLWW(aBytes, vBytes, value)
	if err != nil {
		return nil, fmt.Errorf("LookupByUnique: resolution failed: %w", err)
	}
	return owner, nil
}

//	// Get attributes with nested reference
//	result, err := db.Pull(entityID, `[:entity/name {:entity/region [:region/code]}]`)
//
// Returns nil if the entity does not exist.
func (d *Database) Pull(entityID datalog.Identity, patternStr string) (map[string]interface{}, error) {
	// Parse the pull pattern
	pattern, err := parser.ParsePullPattern(patternStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pull pattern: %w", err)
	}

	// Create pull executor with database matcher
	matcher := d.Matcher()
	puller := executor.NewPullExecutor(matcher, d)

	// Execute pull
	return puller.Pull(entityID, pattern)
}

// PullMany retrieves data for multiple entities using the same pull pattern
// This is more efficient than calling Pull multiple times.
//
// Example:
//
//	results, err := db.PullMany(
//	    []datalog.Identity{entity1, entity2, entity3},
//	    `[:entity/name :entity/code]`,
//	)
//
// Returns a slice of maps, one per entity. Nil entries indicate non-existent entities.
func (d *Database) PullMany(entityIDs []datalog.Identity, patternStr string) ([]map[string]interface{}, error) {
	// Parse the pull pattern
	pattern, err := parser.ParsePullPattern(patternStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pull pattern: %w", err)
	}

	// Create pull executor with database matcher
	matcher := d.Matcher()
	puller := executor.NewPullExecutor(matcher, d)

	// Execute pull for all entities
	return puller.PullMany(entityIDs, pattern)
}

// ResolveEntityAttributes resolves CRDT values for the requested attributes of an entity.
// This is the core resolution method that properly applies CRDT semantics:
// - CardinalityOne: LWW (Last-Writer-Wins)
// - CardinalityMany: Add-wins set
// - CardinalityVector: RGA ordered list
//
// The method uses difference-based logic:
// - Checks which attributes are already cached for this entity
// - If few missing: individual GetOrResolve calls
// - If many missing: full EAVT scan then resolve
//
// Returns a map of keyword -> resolved value. Missing attributes are not included.
func (d *Database) ResolveEntityAttributes(entity datalog.Identity, attrs []datalog.Keyword) (map[datalog.Keyword]interface{}, error) {
	if len(attrs) == 0 {
		return make(map[datalog.Keyword]interface{}), nil
	}

	matcher := d.Matcher().(*BadgerMatcher)
	result := make(map[datalog.Keyword]interface{})

	// getValueType returns the schema value type for a keyword
	getValueType := func(kw datalog.Keyword) schema.ValueType {
		if d.schema != nil {
			if s, ok := d.schema.(*schema.Schema); ok {
				if def := s.GetAttribute(kw); def != nil {
					return def.ValueType
				}
			}
		}
		return ""
	}

	// Cache-less path: the cache is an optimization, not a correctness
	// requirement. When DisableCache is set, query the matcher directly for
	// each attribute. The matcher applies CRDT resolution via
	// CRDTResolvingIterator when no cache is set.
	if d.cache == nil || matcher.isHistoryMode() {
		for _, kw := range attrs {
			card := schema.CardinalityOne
			if d.schema != nil {
				if def := d.schema.GetAttribute(kw); def != nil {
					card = def.Cardinality
				}
			}
			val, err := d.resolveAttributeViaMatcher(entity, kw, matcher, card, getValueType(kw))
			if err != nil {
				return nil, fmt.Errorf("failed to resolve %s: %w", kw.String(), err)
			}
			if val != nil {
				result[kw] = val
			}
		}
		return result, nil
	}

	eBytes := Entity(entity.Hash())

	// Get what's already cached for this entity
	cachedAttrs := d.cache.GetCachedAttrs(eBytes)

	// Partition needed attrs into cached vs missing
	var missing []datalog.Keyword
	for _, kw := range attrs {
		var aBytes Attribute
		copy(aBytes[:], kw.String())

		if cachedAttrs != nil && cachedAttrs[aBytes] {
			// Already cached - GetOrResolve will do freshness check
			key, ok := matcher.cacheKey(eBytes, aBytes)
			if !ok {
				continue
			}
			if entry := d.cache.GetOrResolve(key, matcher); entry != nil {
				if val := entryToValue(entry, getValueType(kw)); val != nil {
					result[kw] = val
				}
			}
		} else {
			missing = append(missing, kw)
		}
	}

	if len(missing) == 0 {
		return result, nil // All from cache
	}

	// For now, use individual GetOrResolve for missing attrs
	// TODO: Add threshold-based scan optimization
	for _, kw := range missing {
		var aBytes Attribute
		copy(aBytes[:], kw.String())
		key, ok := matcher.cacheKey(eBytes, aBytes)
		if !ok {
			continue
		}
		if entry := d.cache.GetOrResolve(key, matcher); entry != nil {
			if val := entryToValue(entry, getValueType(kw)); val != nil {
				result[kw] = val
			}
		}
	}

	return result, nil
}

// resolveAttributeViaMatcher resolves a single (entity, attribute) by
// querying the matcher directly. Used when the EA cache is disabled — the
// matcher applies CRDT resolution (LWW for one, add-wins for many, RGA for
// vector) via CRDTResolvingIterator. Returns nil if the entity has no
// current value for the attribute.
func (d *Database) resolveAttributeViaMatcher(entity datalog.Identity, attr datalog.Keyword, matcher *BadgerMatcher, card schema.Cardinality, valueType schema.ValueType) (interface{}, error) {
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	rel, err := matcher.Match(pattern, nil)
	if err != nil {
		return nil, err
	}

	iter := rel.Iterator()
	defer iter.Close()

	switch card {
	case schema.CardinalityMany:
		// Matcher emits one tuple per current member (add-wins resolved).
		var values []interface{}
		for iter.Next() {
			tuple := iter.Tuple()
			if len(tuple) > 0 && tuple[0] != nil {
				values = append(values, tuple[0])
			}
		}
		if len(values) == 0 {
			return nil, nil
		}
		return values, nil

	case schema.CardinalityVector:
		// Matcher emits a single tuple containing the RGA-resolved vector.
		if iter.Next() {
			tuple := iter.Tuple()
			if len(tuple) > 0 && tuple[0] != nil {
				if vec, ok := tuple[0].([]interface{}); ok {
					if len(vec) == 0 {
						return nil, nil
					}
					if valueType != "" {
						return typedVector(vec, valueType), nil
					}
					return vec, nil
				}
				// Unexpected type — return as-is rather than dropping data.
				return tuple[0], nil
			}
		}
		return nil, nil

	default:
		// CardinalityOne and unknown both use LWW semantics; matcher emits
		// the LWW winner first. Take it.
		if iter.Next() {
			tuple := iter.Tuple()
			if len(tuple) > 0 {
				return tuple[0], nil
			}
		}
		return nil, nil
	}
}

// entryToValue converts a CacheEntry to its appropriate value representation.
// For vector entries, valueType is used to return typed slices (e.g. []string).
func entryToValue(entry *CacheEntry, valueType schema.ValueType) interface{} {
	switch entry.Cardinality() {
	case schema.CardinalityOne:
		return entry.OneValue()
	case schema.CardinalityMany:
		// Convert set to slice for API consistency
		set := entry.ManySet()
		if len(set) == 0 {
			return nil
		}
		values := make([]interface{}, 0, len(set))
		for _, v := range set {
			values = append(values, v)
		}
		return values
	case schema.CardinalityVector:
		return typedVector(entry.VectorList(), valueType)
	default:
		return entry.OneValue()
	}
}

// ResolveAllAttributes retrieves all CRDT-resolved attributes for an entity.
// This is used by wildcard pulls to get all attributes with proper CRDT
// resolution.
//
// Two paths:
//   - With schema: enumerate the schema's attributes and resolve each
//   - Without schema: scan EAVT to discover the entity's attributes, then resolve
//
// Both paths delegate the per-attribute resolution to ResolveEntityAttributes,
// which handles cache-aware and cache-less modes uniformly. Per the codebase
// principle that the cache is an optimization, not a correctness requirement,
// this method works correctly under DisableCache: true.
//
// Returns a map of keyword -> resolved value. The value type depends on
// cardinality:
//   - CardinalityOne: single value (LWW)
//   - CardinalityMany: []interface{} (add-wins set)
//   - CardinalityVector: []interface{} (RGA ordered list)
func (d *Database) ResolveAllAttributes(entity datalog.Identity) (map[datalog.Keyword]interface{}, error) {
	// If schema exists, enumerate schema attributes.
	if d.schema != nil {
		if s, ok := d.schema.(*schema.Schema); ok && s.HasSchema() {
			attrs := s.Attributes()
			keywords := make([]datalog.Keyword, len(attrs))
			for i, def := range attrs {
				keywords[i] = def.Ident
			}
			return d.ResolveEntityAttributes(entity, keywords)
		}
	}

	// No schema: scan EAVT to discover all attributes for this entity, then
	// delegate per-attribute resolution to ResolveEntityAttributes.
	eBytes := entity.Bytes()
	encoder := d.store.encoder
	start, end := encoder.EncodePrefixRange(EAVT, eBytes[:])

	iter, err := d.store.Scan(EAVT, start, end)
	if err != nil {
		return nil, fmt.Errorf("EAVT scan failed: %w", err)
	}
	defer iter.Close()

	seenAttrs := make(map[Attribute]datalog.Keyword)
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			continue
		}
		sd := ToStorageDatom(*datom)
		if _, seen := seenAttrs[sd.A]; !seen {
			seenAttrs[sd.A] = datom.A
		}
	}

	keywords := make([]datalog.Keyword, 0, len(seenAttrs))
	for _, kw := range seenAttrs {
		keywords = append(keywords, kw)
	}
	return d.ResolveEntityAttributes(entity, keywords)
}

// PullInto retrieves entity data and populates the provided struct.
// The struct fields are mapped to attributes based on datalog struct tags.
// A pull pattern is automatically generated from the struct definition.
// Schema is used to properly handle cardinality-many attributes.
//
// Example:
//
//	type Person struct {
//	    ID      datalog.Identity `datalog:"-,id"`
//	    Name    string           `datalog:"name"`
//	    Age     int64            `datalog:"age"`
//	    Friends []*Person        `datalog:"friends"`
//	}
//	var person Person
//	err := db.PullInto(entityID, &person)
func (d *Database) PullInto(entityID datalog.Identity, v interface{}) error {
	if entityID == nil {
		return fmt.Errorf("PullInto: entity ID is nil")
	}

	// Generate pull pattern from struct
	patternStr := dlreflect.GeneratePullPattern(v, d.Schema())
	if patternStr == "" {
		return fmt.Errorf("could not generate pull pattern for %T", v)
	}

	// Parse the pattern
	pattern, err := parser.ParsePullPattern(patternStr)
	if err != nil {
		return fmt.Errorf("failed to parse pull pattern: %w", err)
	}

	// Resolve with schema for proper cardinality handling
	resolved := schema.ResolvePullPattern(pattern, d.Schema())

	// Create pull executor and execute
	matcher := d.Matcher()
	puller := executor.NewPullExecutor(matcher, d)
	result, err := puller.PullResolved(entityID, resolved)
	if err != nil {
		return err
	}

	// Populate struct from result, including the ID field
	return dlreflect.ReadStructWithID(result, v, d.Schema(), entityID)
}

// PullIntoMany retrieves data for multiple entities and populates the provided slice.
// The slice must be a pointer to a slice of structs (e.g., *[]Person or *[]*Person).
// Schema is used to properly handle cardinality-many attributes.
//
// Example:
//
//	var people []Person
//	err := db.PullIntoMany(entityIDs, &people)
func (d *Database) PullIntoMany(entityIDs []datalog.Identity, v interface{}) error {
	// Get the slice element type
	sliceVal := reflect.ValueOf(v)
	if sliceVal.Kind() != reflect.Ptr {
		return fmt.Errorf("PullIntoMany requires pointer to slice")
	}
	sliceVal = sliceVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("PullIntoMany requires pointer to slice, got pointer to %s", sliceVal.Kind())
	}

	elemType := sliceVal.Type().Elem()
	isPtr := elemType.Kind() == reflect.Ptr
	if isPtr {
		elemType = elemType.Elem()
	}

	// Create a sample struct to generate pattern
	sampleStruct := reflect.New(elemType).Interface()
	patternStr := dlreflect.GeneratePullPattern(sampleStruct, d.Schema())
	if patternStr == "" {
		return fmt.Errorf("could not generate pull pattern for %s", elemType.Name())
	}

	// Parse the pattern
	pattern, err := parser.ParsePullPattern(patternStr)
	if err != nil {
		return fmt.Errorf("failed to parse pull pattern: %w", err)
	}

	// Resolve with schema for proper cardinality handling
	resolved := schema.ResolvePullPattern(pattern, d.Schema())

	// Create pull executor and execute
	matcher := d.Matcher()
	puller := executor.NewPullExecutor(matcher, d)
	results, err := puller.PullResolvedMany(entityIDs, resolved)
	if err != nil {
		return err
	}

	// Populate slice from results
	newSlice := reflect.MakeSlice(sliceVal.Type(), 0, len(results))
	for i, result := range results {
		if result == nil {
			continue
		}

		var newElem reflect.Value
		if isPtr {
			newElem = reflect.New(elemType)
		} else {
			newElem = reflect.New(elemType)
		}

		// Use ReadStructWithID to also populate the ID field
		if err := dlreflect.ReadStructWithID(result, newElem.Interface(), d.Schema(), entityIDs[i]); err != nil {
			return err
		}

		if isPtr {
			newSlice = reflect.Append(newSlice, newElem)
		} else {
			newSlice = reflect.Append(newSlice, newElem.Elem())
		}
	}

	sliceVal.Set(newSlice)
	return nil
}
