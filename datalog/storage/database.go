package storage

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Database provides the main API for reading and writing datoms
type Database struct {
	store     *BadgerStore
	txCounter atomic.Uint64
	mu        sync.RWMutex
	activeTx  map[*Transaction]bool
	useTimeTx bool                   // Use time-based transaction IDs
	planCache *planner.PlanCache     // Shared query plan cache
	schema    schema.SchemaProvider  // Optional schema for validation
}

// NewDatabase creates a new database with BadgerDB storage
func NewDatabase(path string) (*Database, error) {
	// Use Binary encoding explicitly (matches BadgerStore default)
	store, err := NewBadgerStore(path, NewKeyEncoder(BinaryStrategy))
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	return &Database{
		store:     store,
		activeTx:  make(map[*Transaction]bool),
		planCache: planner.NewPlanCache(1000, 0), // 1000 plans, default TTL
	}, nil
}

// NewDatabaseWithTimeTx creates a database that uses time-based transaction IDs
func NewDatabaseWithTimeTx(path string) (*Database, error) {
	db, err := NewDatabase(path)
	if err != nil {
		return nil, err
	}
	db.useTimeTx = true
	return db, nil
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
	Path        string               // Path to the database directory
	UseTimeTx   bool                 // Use time-based transaction IDs
	RetractMode RetractMode          // How retractions are handled
	Schema      schema.SchemaProvider // Optional schema for validation
}

// NewDatabaseWithOptions creates a database with the specified options.
// This is the most flexible constructor, supporting all configuration options.
//
// Options:
//   - Path: Required. Directory for BadgerDB storage.
//   - UseTimeTx: If true, use nanosecond timestamps as transaction IDs.
//   - RetractMode: RetractDelete (default) deletes data; RetractHistory preserves full audit trail.
//   - Schema: Optional schema for validation.
//
// Example:
//
//	db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
//	    Path:        "/path/to/db",
//	    RetractMode: storage.RetractHistory,
//	})
//	// Now db.History() returns a PatternMatcher for querying all changes
func NewDatabaseWithOptions(opts DatabaseOptions) (*Database, error) {
	if opts.Path == "" {
		return nil, fmt.Errorf("database path is required")
	}

	store, err := NewBadgerStoreWithRetractMode(opts.Path, NewKeyEncoder(BinaryStrategy), opts.RetractMode)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	return &Database{
		store:     store,
		activeTx:  make(map[*Transaction]bool),
		planCache: planner.NewPlanCache(1000, 0),
		useTimeTx: opts.UseTimeTx,
		schema:    opts.Schema,
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

// NewTransaction starts a new write transaction
func (d *Database) NewTransaction() *Transaction {
	d.mu.Lock()
	defer d.mu.Unlock()

	tx := &Transaction{
		db:       d,
		datoms:   make([]datalog.Datom, 0),
		retracts: make([]datalog.Datom, 0),
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
	return NewBadgerMatcherWithOptions(d.store, execOpts)
}

// AsOf returns a PatternMatcher for a specific transaction
func (d *Database) AsOf(txID uint64) executor.PatternMatcher {
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
	return NewBadgerMatcherWithOptions(d.store, execOpts).AsOf(txID)
}

// History returns a PatternMatcher for querying the full history of the database.
// This includes all assertions and retractions with the Op (operation) field.
// History queries can use 5-element patterns: [?e ?a ?v ?tx ?op]
//
// Returns nil if the database was not created with RetractHistory mode.
// In RetractHistory mode, all assertions and retractions are preserved in history indices.
//
// Example:
//
//	historyMatcher := db.History()
//	if historyMatcher == nil {
//	    // Database doesn't support history queries
//	}
//	// Query all changes to an entity
//	// Pattern: [entity-id ?a ?v ?tx ?op]
func (d *Database) History() executor.PatternMatcher {
	if d.store.RetractMode() != RetractHistory {
		return nil
	}
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
	return NewHistoryMatcherWithOptions(d.store, execOpts)
}

// RetractMode returns the retraction mode of the database.
// RetractDelete means retractions delete data from current-state indices.
// RetractHistory means retractions are preserved in history indices for audit trails.
func (d *Database) RetractMode() RetractMode {
	return d.store.RetractMode()
}

// DefaultPlannerOptions returns the default planner and executor options for the database
func DefaultPlannerOptions() planner.PlannerOptions {
	return planner.PlannerOptions{
		// Planner options
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

		// Executor architecture (Stage B)
		UseQueryExecutor: true, // Use new QueryExecutor by default (production-ready as of October 2025)
	}
}

// NewExecutor creates a new query executor that uses the database's plan cache
func (d *Database) NewExecutor() *executor.Executor {
	opts := DefaultPlannerOptions()
	opts.Cache = d.planCache // Use database's cache
	return executor.NewExecutorWithOptions(d.Matcher(), opts)
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
	return executor.NewExecutorWithOptions(matcher, opts)
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

// ExecuteQuery executes a Datalog query string and returns results as a slice of tuples
// This is a convenience method that handles parsing and execution
//
// Example:
//
//	results, err := db.ExecuteQuery(`[:find ?name :where [?e :person/name ?name]]`)
func (d *Database) ExecuteQuery(queryStr string) ([][]interface{}, error) {
	return d.ExecuteQueryWithInputs(queryStr)
}

// ExecuteQueryWithInputs executes a parameterized Datalog query with input parameters
// This provides type-safe query execution without string formatting
//
// Input parameters are matched with the :in clause in order (after the $ database parameter):
//   - Scalar inputs: ?name
//   - Collection inputs: [?foods ...]
//   - Tuple inputs: [[?name ?age]]
//   - Relation inputs: [[?name ?age] ...]
//
// Examples:
//
//	// Scalar input
//	results, err := db.ExecuteQueryWithInputs(
//	    `[:find ?e :in $ ?name :where [?e :person/name ?name]]`,
//	    "Alice",
//	)
//
//	// Multiple scalar inputs
//	results, err := db.ExecuteQueryWithInputs(
//	    `[:find ?e :in $ ?name ?min-age :where [?e :person/name ?name] [?e :person/age ?age] [(>= ?age ?min-age)]]`,
//	    "Alice", 25,
//	)
//
//	// Collection input
//	results, err := db.ExecuteQueryWithInputs(
//	    `[:find ?e ?food :in $ [?food ...] :where [?e :person/likes ?food]]`,
//	    []string{"pizza", "pasta"},
//	)
func (d *Database) ExecuteQueryWithInputs(queryStr string, inputs ...interface{}) ([][]interface{}, error) {
	// Parse the query
	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	// Convert inputs to Relations based on :in clause
	inputRelations, err := d.convertInputsToRelations(q, inputs)
	if err != nil {
		return nil, err
	}

	// Execute the query
	exec := d.NewExecutor()
	result, err := exec.ExecuteWithRelations(executor.NewContext(nil), q, inputRelations)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	// Convert result to [][]interface{}
	return relationToSlice(result), nil
}

// ExecuteHistoryQuery executes a Datalog query against the history database
// This requires the database to be created with RetractHistory mode
//
// History queries support 5-element patterns: [?e ?a ?v ?tx ?op]
// where ?op is true for assertions and false for retractions
//
// Example:
//
//	results, err := db.ExecuteHistoryQuery(`[:find ?v ?tx ?op :where [?e :person/name ?v ?tx ?op]]`)
func (d *Database) ExecuteHistoryQuery(queryStr string) ([][]interface{}, error) {
	return d.ExecuteHistoryQueryWithInputs(queryStr)
}

// ExecuteHistoryQueryWithInputs executes a parameterized query against the history database
// This is the history equivalent of ExecuteQueryWithInputs
func (d *Database) ExecuteHistoryQueryWithInputs(queryStr string, inputs ...interface{}) ([][]interface{}, error) {
	historyMatcher := d.History()
	if historyMatcher == nil {
		return nil, fmt.Errorf("history queries require RetractHistory mode (database was created with RetractDelete mode)")
	}

	// Parse the query
	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	// Convert inputs to Relations based on :in clause
	inputRelations, err := d.convertInputsToRelations(q, inputs)
	if err != nil {
		return nil, err
	}

	// Execute the query using the history matcher
	opts := DefaultPlannerOptions()
	opts.Cache = d.planCache
	exec := executor.NewExecutorWithOptions(historyMatcher, opts)
	result, err := exec.ExecuteWithRelations(executor.NewContext(nil), q, inputRelations)
	if err != nil {
		return nil, fmt.Errorf("history query execution failed: %w", err)
	}

	// Convert result to [][]interface{}
	return relationToSlice(result), nil
}

// GetExecutor returns a new query executor
// This provides direct access to the executor for advanced use cases
func (d *Database) GetExecutor() *executor.Executor {
	return d.NewExecutor()
}

// Transaction represents a write transaction
type Transaction struct {
	db       *Database
	datoms   []datalog.Datom
	retracts []datalog.Datom
	mu       sync.Mutex
	closed   bool
	txTime   *time.Time // Optional custom transaction time
}

// SetTime sets a custom transaction time for this transaction
// This is useful for backdated data (e.g., historical prices)
func (t *Transaction) SetTime(txTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.txTime = &txTime
}

// Add asserts a new datom
func (t *Transaction) Add(e datalog.Identity, a datalog.Keyword, v interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return fmt.Errorf("transaction is closed")
	}

	// Schema validation (if schema present)
	if err := schema.ValidateDatom(t.db.Schema(), a, v); err != nil {
		return fmt.Errorf("schema validation failed for %s: %w", a.String(), err)
	}

	t.datoms = append(t.datoms, datalog.Datom{
		E:  e,
		A:  a,
		V:  v,
		Tx: 0, // Will be set on commit
	})

	return nil
}

// Retract removes a datom
func (t *Transaction) Retract(e datalog.Identity, a datalog.Keyword, v interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return fmt.Errorf("transaction is closed")
	}

	t.retracts = append(t.retracts, datalog.Datom{
		E:  e,
		A:  a,
		V:  v,
		Tx: 0, // Will be set on commit
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
		return datalog.Identity{}, err
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
func (t *Transaction) Commit() (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return 0, fmt.Errorf("transaction is closed")
	}

	// Validate uniqueness constraints before committing
	if err := t.validateUniqueness(); err != nil {
		return 0, err
	}

	// Get transaction ID (time-based or sequential)
	var txID uint64
	var txTime time.Time

	// Use custom time if provided, otherwise use current time
	if t.txTime != nil {
		txTime = *t.txTime
	} else {
		txTime = time.Now()
	}

	if t.db.useTimeTx {
		// Use nanosecond timestamp as transaction ID
		txID = uint64(txTime.UnixNano())
	} else {
		// Use sequential counter
		txID = t.db.txCounter.Add(1)
	}

	// Set transaction ID on all datoms
	for i := range t.datoms {
		t.datoms[i].Tx = txID
	}
	for i := range t.retracts {
		t.retracts[i].Tx = txID
	}

	// Apply retractions first
	if len(t.retracts) > 0 {
		if err := t.db.store.Retract(t.retracts); err != nil {
			return 0, fmt.Errorf("failed to retract datoms: %w", err)
		}
	}

	// Then apply assertions
	if len(t.datoms) > 0 {
		if err := t.db.store.Assert(t.datoms); err != nil {
			return 0, fmt.Errorf("failed to assert datoms: %w", err)
		}
	}

	// Add transaction metadata
	txEntity := datalog.NewIdentity(fmt.Sprintf("tx:%d", txID))
	txMetadata := []datalog.Datom{
		{
			E:  txEntity,
			A:  datalog.NewKeyword(":db/txInstant"),
			V:  txTime,
			Tx: txID,
		},
	}
	if err := t.db.store.Assert(txMetadata); err != nil {
		// Log but don't fail the transaction
		fmt.Printf("Warning: failed to write transaction metadata: %v\n", err)
	}

	// Clean up
	t.closed = true
	t.db.mu.Lock()
	delete(t.db.activeTx, t)
	t.db.mu.Unlock()

	return txID, nil
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
				query.Variable{Name: query.Symbol("?e")},    // Entity variable
				query.Constant{Value: d.A},                  // Bound attribute
				query.Constant{Value: d.V},                  // Bound value
				query.Blank{},                               // Transaction wildcard
			},
		}

		results, err := matcher.Match(pattern, nil)
		if err != nil {
			return fmt.Errorf("failed to check uniqueness for %s: %w", d.A.String(), err)
		}

		// Find the index of ?e in the result columns
		columns := results.Columns()
		eIndex := -1
		for i, col := range columns {
			if col == query.Symbol("?e") {
				eIndex = i
				break
			}
		}
		if eIndex < 0 {
			continue // No entity column in results (shouldn't happen)
		}

		// Check if any existing datoms have a different entity
		iter := results.Iterator()
		for iter.Next() {
			tuple := iter.Tuple()
			if eIndex >= len(tuple) {
				continue
			}
			// Handle both value and pointer types for Identity
			var existingEntity datalog.Identity
			switch e := tuple[eIndex].(type) {
			case datalog.Identity:
				existingEntity = e
			case *datalog.Identity:
				existingEntity = *e
			default:
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
			// Skip $ - doesn't consume an input
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

// relationToSlice converts an executor.Relation to [][]interface{}
func relationToSlice(rel executor.Relation) [][]interface{} {
	// Don't preallocate if size is unknown (-1)
	size := rel.Size()
	var rows [][]interface{}
	if size >= 0 {
		rows = make([][]interface{}, 0, size)
	} else {
		rows = make([][]interface{}, 0)
	}

	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()
		row := make([]interface{}, len(tuple))
		for i, v := range tuple {
			row[i] = v
		}
		rows = append(rows, row)
	}

	return rows
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
	puller := executor.NewPullExecutor(matcher)

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
	puller := executor.NewPullExecutor(matcher)

	// Execute pull for all entities
	return puller.PullMany(entityIDs, pattern)
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
	puller := executor.NewPullExecutor(matcher)
	result, err := puller.PullResolved(entityID, resolved)
	if err != nil {
		return err
	}

	// Populate struct from result
	return dlreflect.ReadStruct(result, v, d.Schema())
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
	puller := executor.NewPullExecutor(matcher)
	results, err := puller.PullResolvedMany(entityIDs, resolved)
	if err != nil {
		return err
	}

	// Populate slice from results
	newSlice := reflect.MakeSlice(sliceVal.Type(), 0, len(results))
	for _, result := range results {
		if result == nil {
			continue
		}

		var newElem reflect.Value
		if isPtr {
			newElem = reflect.New(elemType)
		} else {
			newElem = reflect.New(elemType)
		}

		if err := dlreflect.ReadStruct(result, newElem.Interface(), d.Schema()); err != nil {
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
