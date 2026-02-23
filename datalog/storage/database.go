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
	planCache         *planner.PlanCache    // Shared query plan cache
	parseCache        *ParseCache           // Shared query parse cache
	schema            schema.SchemaProvider // Optional schema for validation
	annotationHandler annotations.Handler   // Optional handler for query tracing
	clock             *LamportClock         // CRDT: Lamport clock for ordering (nil if not in CRDT mode)
	replicaID         uint64                // CRDT: This database's replica identifier
	cache             *Cache                // CRDT: Unified cache for resolved CRDT views
	temporalTxID      *datalog.ElementID    // nil = current; set = temporal mode (AsOf/History)
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
	Path              string                // Path to the database directory
	Schema            schema.SchemaProvider // Optional schema for validation
	AnnotationHandler annotations.Handler   // Optional handler for query tracing
	ReplicaID         uint64                // For CRDT mode: 0 = auto-generate random; non-zero = use specified. Ignored for existing DBs.
	DisableCache      bool                  // Disable EA cache; queries resolve directly from storage
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

	store, err := NewBadgerStore(opts.Path, NewKeyEncoder(BinaryStrategy))
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

	return &Database{
		store:             store,
		activeTx:          make(map[*Transaction]bool),
		planCache:         planner.NewPlanCache(1000, 0),
		parseCache:        NewParseCache(1000),
		schema:            opts.Schema,
		annotationHandler: opts.AnnotationHandler,
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

	entry := d.cache.GetOrResolve(key, matcher)
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

	entry := d.cache.GetOrResolve(key, matcher)
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
	d.annotationHandler = handler
}

// NewTransaction starts a new write transaction
func (d *Database) NewTransaction() *Transaction {
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
func (d *Database) Matcher() executor.PatternMatcher {
	// Convert default planner options to executor options
	opts := DefaultPlannerOptions()
	execOpts := executor.ExecutorOptions{
		EnableIteratorComposition:       opts.EnableIteratorComposition,
		EnableTrueStreaming:             opts.EnableTrueStreaming,
		EnableSymmetricHashJoin:         opts.EnableSymmetricHashJoin,
		EnableParallelSubqueries:        opts.EnableParallelSubqueries,
		MaxSubqueryWorkers:              opts.MaxSubqueryWorkers,
		EnableStreamingJoins:            opts.EnableStreamingJoins,
		EnableStreamingAggregation:      opts.EnableStreamingAggregation,
		EnableStreamingAggregationDebug: opts.EnableStreamingAggregationDebug,
		EnableDebugLogging:              opts.EnableDebugLogging,
		IndexNestedLoopThreshold:        opts.IndexNestedLoopThreshold,
	}
	matcher := NewBadgerMatcherWithOptions(d.store, execOpts)
	// Set schema for CRDT cardinality-aware resolution
	if d.schema != nil {
		matcher.SetSchema(d.schema)
	}
	// Set cache for CRDT resolution O(1) access
	if d.cache != nil {
		matcher.SetCache(d.cache)
	}
	// Apply temporal mode if set (AsOf/History)
	if d.temporalTxID != nil {
		return matcher.AsOf(*d.temporalTxID)
	}
	return matcher
}

// Match implements executor.PatternMatcher — the Database itself can answer pattern queries.
// This delegates to the Database's Matcher(), which uses the full BadgerDB index infrastructure.
func (d *Database) Match(pattern *query.DataPattern, bindings executor.Relations) (executor.Relation, error) {
	return d.Matcher().Match(pattern, bindings)
}

// Compile-time verification that Database implements PatternMatcher
var _ executor.PatternMatcher = (*Database)(nil)

// AsOf returns a new Database handle that queries state as of the given transaction.
// The returned handle uses CRDT resolution filtered to that point in causal time.
func (d *Database) AsOf(txID datalog.ElementID) *Database {
	return &Database{
		store:             d.store,
		schema:            d.schema,
		annotationHandler: d.annotationHandler,
		planCache:         d.planCache,
		cache:             d.cache,
		clock:             d.clock,
		replicaID:         d.replicaID,
		temporalTxID:      &txID,
	}
}

// History returns a new Database handle that returns all raw datoms without
// CRDT resolution. Every write is visible, including superseded values.
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
		// Planner architecture selection
		UseClauseBasedPlanner: true, // Use new clause-based planner (greedy phasing, pure clause transformations)

		// Planner options (for old planner - kept for compatibility when UseClauseBasedPlanner: false)
		EnableDynamicReordering:     true, // Phase reordering by symbol connectivity
		EnablePredicatePushdown:     true, // Early predicate filtering (not storage-level)
		EnableSubqueryDecorrelation: true, // Selinger's decorrelation optimization
		EnableParallelDecorrelation: true, // Execute decorrelated merged queries in parallel
		MaxPhases:                   10,
		EnableFineGrainedPhases:     true, // Selectivity-based phase creation

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

		// Storage join strategy
		IndexNestedLoopThreshold: 0, // Default to HashJoinScan for all binding sizes

	}
}

// NewExecutor creates a new query executor that uses the database's plan cache
func (d *Database) NewExecutor() *executor.Executor {
	opts := DefaultPlannerOptions()
	opts.Cache = d.planCache // Use database's cache
	return executor.NewExecutorWithOptions(d.Matcher(), d, opts)
}

// NewExecutorWithOptions creates a new query executor with custom options and the database's plan cache
func (d *Database) NewExecutorWithOptions(opts planner.PlannerOptions) *executor.Executor {
	// Override cache with database's cache
	opts.Cache = d.planCache
	// Create matcher with custom options
	execOpts := executor.ExecutorOptions{
		EnableIteratorComposition:       opts.EnableIteratorComposition,
		EnableTrueStreaming:             opts.EnableTrueStreaming,
		EnableSymmetricHashJoin:         opts.EnableSymmetricHashJoin,
		EnableParallelSubqueries:        opts.EnableParallelSubqueries,
		MaxSubqueryWorkers:              opts.MaxSubqueryWorkers,
		EnableStreamingJoins:            opts.EnableStreamingJoins,
		EnableStreamingAggregation:      opts.EnableStreamingAggregation,
		EnableStreamingAggregationDebug: opts.EnableStreamingAggregationDebug,
		EnableDebugLogging:              opts.EnableDebugLogging,
		IndexNestedLoopThreshold:        opts.IndexNestedLoopThreshold,
	}
	matcher := NewBadgerMatcherWithOptions(d.store, execOpts)
	return executor.NewExecutorWithOptions(matcher, d, opts)
}

// Store returns the underlying store for direct access (debugging/testing)
func (d *Database) Store() *BadgerStore {
	return d.store
}

// Close closes the database
func (d *Database) Close() error {
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

	// Get the query plan from the executor's planner
	queryPlanner := exec.GetPlanner()
	return queryPlanner.PlanQuery(q)
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

	// Get the query plan from the executor's planner
	queryPlanner := exec.GetPlanner()
	plan, err := queryPlanner.PlanQuery(q)
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

	totalTime := time.Since(startTime)

	return &AnalyzeResult{
		Plan:      plan,
		Result:    result,
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
	iter := rel.Iterator()
	defer iter.Close()

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
		for iter.Next() {
			elem := reflect.New(elemType).Elem()
			if err := mapper.MapTuple(iter.Tuple(), elem); err != nil {
				return err
			}
			newSlice = reflect.Append(newSlice, elem)
		}
		sliceVal.Set(newSlice)
		return nil
	}

	// Scalar path - single symbol queries only
	if len(q.Find) != 1 {
		return fmt.Errorf("scalar QueryInto requires exactly 1 find element, got %d", len(q.Find))
	}

	newSlice := reflect.MakeSlice(sliceVal.Type(), 0, 0)
	for iter.Next() {
		tuple := iter.Tuple()
		if len(tuple) == 0 {
			continue
		}

		var elemVal reflect.Value
		if elemIsPtr {
			elemVal = reflect.New(elemType).Elem()
		} else {
			elemVal = reflect.New(elemType).Elem()
		}

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

	// Read first tuple
	if !iter.Next() {
		return false, nil
	}
	firstTuple := make([]interface{}, len(iter.Tuple()))
	copy(firstTuple, iter.Tuple())

	// Check for multiple results
	if iter.Next() {
		return false, dlreflect.ErrMultipleResults
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

	// Nil values are not allowed in relational algebra - absence of fact represents "no value"
	if v == nil {
		return fmt.Errorf("nil value not allowed for attribute %s: use absence of fact to represent no value", a.String())
	}

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

	// Nil values are not allowed
	if v == nil {
		return fmt.Errorf("nil value not allowed for Remove on attribute %s", a.String())
	}

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

	// Nil values are not allowed in relational algebra - absence of fact represents "no value"
	if v == nil {
		return fmt.Errorf("nil value not allowed for attribute %s: use absence of fact to represent no value", a.String())
	}

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
			} else {
				return fmt.Errorf("Set for cardinality-many requires []interface{}, got %T", v)
			}
		}

		// Schema validation for each element in the slice
		for _, val := range newSlice {
			if err := schema.ValidateDatom(t.db.Schema(), a, val); err != nil {
				return fmt.Errorf("schema validation failed for %s element: %w", a.String(), err)
			}
		}

		// Build set of new values for O(1) lookup
		newSet := make(map[interface{}]bool, len(newSlice))
		for _, val := range newSlice {
			newSet[val] = true
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

		// Apply pending transaction operations to get effective current set
		// This ensures Set() sees Add/Remove ops from earlier in the same transaction
		effectiveSet := make(map[interface{}]bool)
		for member := range currentResult.Members {
			effectiveSet[member] = true
		}

		// Scan pending datoms for this (E, A) pair
		pendingAdds := make(map[interface{}]datalog.ElementID)
		pendingRemoves := make(map[interface{}]datalog.ElementID)
		for _, datom := range t.datoms {
			if datom.E.Hash() != eBytes || datom.A.String() != a.String() {
				continue
			}
			// NEW FORMAT: Op is a field on Datom, V is the raw value
			if datom.Op == datalog.OpCRDTAdd {
				pendingAdds[datom.V] = datom.Tx
			} else if datom.Op == datalog.OpCRDTRemove {
				pendingRemoves[datom.V] = datom.Tx
			}
		}

		// Apply pending ops using add-wins semantics
		// For each value, compare highest pending add Lamport vs highest pending remove Lamport
		allPendingValues := make(map[interface{}]bool)
		for v := range pendingAdds {
			allPendingValues[v] = true
		}
		for v := range pendingRemoves {
			allPendingValues[v] = true
		}

		for val := range allPendingValues {
			addTx, hasAdd := pendingAdds[val]
			removeTx, hasRemove := pendingRemoves[val]

			if hasAdd && !hasRemove {
				// Only add pending
				effectiveSet[val] = true
			} else if !hasAdd && hasRemove {
				// Only remove pending
				delete(effectiveSet, val)
			} else {
				// Both add and remove pending - compare Lamport (add-wins at same Lamport)
				if addTx.Lamport >= removeTx.Lamport {
					effectiveSet[val] = true
				} else {
					delete(effectiveSet, val)
				}
			}
		}

		// Remove members that are in effective set but not in new set
		for member := range effectiveSet {
			if !newSet[member] {
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

		// Add members that are in new set but not in effective set
		// Iterate newSet (deduplicated) to avoid duplicate writes for duplicate slice values
		for val := range newSet {
			if !effectiveSet[val] {
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
			} else {
				return fmt.Errorf("Set for cardinality-vector requires []interface{}, got %T", v)
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
	matcher := NewBadgerMatcher(t.db.Store())
	return dlreflect.SaveStruct(t, matcher, v, t.db.Schema())
}

// Commit commits the transaction
func (t *Transaction) Commit() (datalog.ElementID, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return datalog.ElementID{}, fmt.Errorf("transaction is closed")
	}

	// Validate uniqueness constraints before committing
	if err := t.validateUniqueness(); err != nil {
		return datalog.ElementID{}, err
	}

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

	// Apply retractions first
	if len(t.retracts) > 0 {
		if err := t.db.store.Retract(t.retracts); err != nil {
			return datalog.ElementID{}, fmt.Errorf("failed to retract datoms: %w", err)
		}
	}

	// Then apply assertions
	if len(t.datoms) > 0 {
		if err := t.db.store.Assert(t.datoms); err != nil {
			return datalog.ElementID{}, fmt.Errorf("failed to assert datoms: %w", err)
		}
	}

	// Add transaction metadata with its own Lamport timestamp
	// This is the final operation in the transaction, so its Lamport is the "transaction time"
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
	if err := t.db.store.Assert(txMetadata); err != nil {
		// Log but don't fail the transaction
		fmt.Printf("Warning: failed to write transaction metadata: %v\n", err)
	}

	// Update cache: track max versions and invalidate stale entries
	// Skip if cache is disabled
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
				t.db.cache.UpdateMaxVersion(key, d.Tx)
			}
		}

		// Process retracted datoms
		for _, d := range t.retracts {
			eBytes := Entity(d.E.Hash())

			var aBytes Attribute
			copy(aBytes[:], d.A.String())

			key := CacheKey{E: eBytes, A: aBytes}
			if !seenKeys[key] {
				seenKeys[key] = true
				touched = append(touched, key)
				t.db.cache.UpdateMaxVersion(key, d.Tx)
			}
		}

		// Invalidate cache entries for all touched (E, A) pairs
		t.db.cache.Invalidate(touched)
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

// validateUniqueness checks uniqueness constraints for all datoms in the transaction
func (t *Transaction) validateUniqueness() error {
	s := t.db.Schema()
	if s == nil || !s.HasSchema() {
		return nil // No schema = no validation
	}

	// Track values seen in this transaction for within-transaction uniqueness
	// Key: attribute + serialized value
	seenInTx := make(map[string]datalog.Identity)

	matcher := NewBadgerMatcher(t.db.store)

	for _, d := range t.datoms {
		def := s.GetAttribute(d.A)
		if def == nil || def.Unique == "" {
			continue // No uniqueness constraint
		}

		// Create a key for tracking this attr+value combination
		txKey := fmt.Sprintf("%s:%v", d.A.String(), d.V)

		// Check within transaction uniqueness
		if existingEntity, ok := seenInTx[txKey]; ok {
			if existingEntity != d.E {
				return fmt.Errorf("uniqueness violation for %s: value %v already used by entity %s in this transaction",
					d.A.String(), d.V, existingEntity.String())
			}
			// Same entity, same value - OK (idempotent update)
			continue
		}
		seenInTx[txKey] = d.E

		// Check database for existing value
		// Create a pattern [?e :attr value _] to find entities with this value
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")}, // Entity variable
				query.Constant{Value: d.A},                    // Bound attribute
				query.Constant{Value: d.V},                    // Bound value
				query.Blank{},                                 // Transaction wildcard
			},
		}

		results, err := matcher.Match(pattern, nil)
		if err != nil {
			return fmt.Errorf("failed to check uniqueness for %s: %w", d.A.String(), err)
		}

		// Find the index of ?e in the result symbols
		symbols := results.Symbols()
		eIndex := -1
		for i, sym := range symbols {
			if sym == datalog.NewSymbol("?e") {
				eIndex = i
				break
			}
		}
		if eIndex < 0 {
			continue // No entity symbol in results (shouldn't happen)
		}

		// Check if any existing datoms have a different entity
		iter := results.Iterator()
		for iter.Next() {
			tuple := iter.Tuple()
			if eIndex >= len(tuple) {
				continue
			}
			// Get Identity (now always a pointer type)
			existingEntity, ok := tuple[eIndex].(datalog.Identity)
			if !ok || existingEntity == nil {
				continue
			}
			if !existingEntity.Equal(d.E) {
				iter.Close()
				return fmt.Errorf("uniqueness violation for %s: value %v already exists on entity %s",
					d.A.String(), d.V, existingEntity.String())
			}
		}
		iter.Close()
	}

	return nil
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

			// Create single-value relation
			rel := executor.NewMaterializedRelation(
				[]query.Symbol{spec.Symbol},
				[]executor.Tuple{{inputs[inputIdx]}},
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
				tuples[i] = executor.Tuple{slice.Index(i).Interface()}
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
				tuple[i] = slice.Index(i).Interface()
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
					tuple[j] = innerSlice.Index(j).Interface()
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

	eBytes := Entity(entity.Hash())
	matcher := d.Matcher().(*BadgerMatcher)
	result := make(map[datalog.Keyword]interface{})

	// Get what's already cached for this entity
	cachedAttrs := d.cache.GetCachedAttrs(eBytes)

	// Partition needed attrs into cached vs missing
	var missing []datalog.Keyword
	for _, kw := range attrs {
		var aBytes Attribute
		copy(aBytes[:], kw.String())

		if cachedAttrs != nil && cachedAttrs[aBytes] {
			// Already cached - GetOrResolve will do freshness check
			key := CacheKey{E: eBytes, A: aBytes}
			if entry := d.cache.GetOrResolve(key, matcher); entry != nil {
				if val := entryToValue(entry); val != nil {
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
		key := CacheKey{E: eBytes, A: aBytes}
		if entry := d.cache.GetOrResolve(key, matcher); entry != nil {
			if val := entryToValue(entry); val != nil {
				result[kw] = val
			}
		}
	}

	return result, nil
}

// entryToValue converts a CacheEntry to its appropriate value representation
func entryToValue(entry *CacheEntry) interface{} {
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
		for v := range set {
			values = append(values, v)
		}
		return values
	case schema.CardinalityVector:
		return entry.VectorList()
	default:
		return entry.OneValue()
	}
}

// ResolveAllAttributes retrieves all CRDT-resolved attributes for an entity.
// This is used by wildcard pulls to get all attributes with proper CRDT resolution.
//
// It scans EAVT for all datoms with the given entity, discovers unique attributes,
// and resolves each using the cache (LWW for one, add-wins for many, RGA for vector).
//
// Returns a map of keyword -> resolved value. The value type depends on cardinality:
// - CardinalityOne: single value (LWW)
// - CardinalityMany: []interface{} (add-wins set)
// - CardinalityVector: []interface{} (RGA ordered list)
func (d *Database) ResolveAllAttributes(entity datalog.Identity) (map[datalog.Keyword]interface{}, error) {
	if d.cache == nil {
		return nil, fmt.Errorf("ResolveAllAttributes requires cache")
	}

	// If schema exists, delegate to ResolveEntityAttributes with all schema attrs.
	// This uses difference-based logic: check cache first, scan only if needed.
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

	// No schema: must scan EAVT to discover all attributes for this entity
	eBytes := entity.Bytes()
	encoder := d.store.encoder
	start, end := encoder.EncodePrefixRange(EAVT, eBytes[:])

	iter, err := d.store.Scan(EAVT, start, end)
	if err != nil {
		return nil, fmt.Errorf("EAVT scan failed: %w", err)
	}
	defer iter.Close()

	// Discover unique attributes
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

	// Resolve each attribute using cache
	matcher := d.Matcher().(*BadgerMatcher)
	eEntity := Entity(eBytes)
	result := make(map[datalog.Keyword]interface{})

	for aBytes, kw := range seenAttrs {
		key := CacheKey{E: eEntity, A: aBytes}
		entry := d.cache.GetOrResolve(key, matcher)
		if entry == nil {
			continue
		}

		// Determine cardinality for proper value conversion
		card := schema.CardinalityOne
		if d.schema != nil {
			if def := d.schema.GetAttribute(kw); def != nil {
				card = def.Cardinality
			}
		}

		switch card {
		case schema.CardinalityOne:
			if v := entry.OneValue(); v != nil {
				result[kw] = v
			}
		case schema.CardinalityMany:
			set := entry.ManySet()
			if len(set) > 0 {
				values := make([]interface{}, 0, len(set))
				for v := range set {
					values = append(values, v)
				}
				result[kw] = values
			}
		case schema.CardinalityVector:
			list := entry.VectorList()
			if len(list) > 0 {
				result[kw] = list
			}
		}
	}

	return result, nil
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
