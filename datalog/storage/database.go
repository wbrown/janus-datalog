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
)

// Database provides the main API for reading and writing datoms
type Database struct {
	store     *BadgerStore
	txCounter atomic.Uint64
	mu        sync.RWMutex
	activeTx  map[*Transaction]bool
	useTimeTx bool               // Use time-based transaction IDs
	planCache *planner.PlanCache // Shared query plan cache
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
	}
	return NewBadgerMatcherWithOptions(d.store, execOpts).AsOf(txID)
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
	d.mu.Lock()
	defer d.mu.Unlock()

	// Rollback any active transactions
	for tx := range d.activeTx {
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

// Commit commits the transaction
func (t *Transaction) Commit() (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return 0, fmt.Errorf("transaction is closed")
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
	rows := make([][]interface{}, 0, rel.Size())
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
