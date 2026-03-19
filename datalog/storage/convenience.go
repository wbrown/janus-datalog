package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Pre-parsed query for typed attribute accessors.
var queryGetAttr = func() *query.Query {
	q, err := parser.ParseQuery(`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`)
	if err != nil {
		panic(err)
	}
	return q
}()

// Query executes a Datalog query and returns a streaming Relation.
// The caller should iterate via rel.Iterator() and must call iter.Close()
// when done.
func (d *Database) Query(queryInput interface{}, inputs ...interface{}) (executor.Relation, error) {
	// Separate QueryOptions from regular inputs
	opts, regularInputs := extractQueryOptions(inputs)

	q, err := d.resolveQuery(queryInput)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve query: %w", err)
	}

	// Build source map — database is always $
	sources := buildSourceMap(opts.sources, d.Matcher())

	// Validate declared sources exist
	if err := validateQuerySources(q, sources); err != nil {
		return nil, err
	}

	// Create SourceRouter with full source map
	router := executor.NewSourceRouter(sources)

	inputRelations, err := d.convertInputsToRelations(q, regularInputs)
	if err != nil {
		return nil, err
	}

	// Execute with the SourceRouter as the PatternMatcher
	var planOpts planner.PlannerOptions
	if d.plannerOptions != nil {
		planOpts = *d.plannerOptions
	} else {
		planOpts = DefaultPlannerOptions()
	}
	planOpts.Cache = d.planCache
	exec := executor.NewExecutorWithOptions(router, d, planOpts)
	result, err := exec.ExecuteWithRelations(executor.NewContext(d.annotationHandler), q, inputRelations)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	return result, nil
}

// Assert adds datoms in a single transaction.
func (d *Database) Assert(datoms []datalog.Datom) error {
	tx := d.NewTransaction()
	for _, dat := range datoms {
		if err := tx.Add(dat.E, dat.A, dat.V); err != nil {
			tx.Rollback()
			return err
		}
	}
	_, err := tx.Commit()
	return err
}

// GetString returns a string attribute value.
func (d *Database) GetString(e datalog.Identity, a datalog.Keyword) (string, bool, error) {
	var v string
	found, err := d.QueryOneInto(&v, queryGetAttr, e, a)
	return v, found, err
}

// GetInt returns an int64 attribute value.
func (d *Database) GetInt(e datalog.Identity, a datalog.Keyword) (int64, bool, error) {
	var v int64
	found, err := d.QueryOneInto(&v, queryGetAttr, e, a)
	return v, found, err
}

// GetFloat returns a float64 attribute value.
func (d *Database) GetFloat(e datalog.Identity, a datalog.Keyword) (float64, bool, error) {
	var v float64
	found, err := d.QueryOneInto(&v, queryGetAttr, e, a)
	return v, found, err
}

// GetBool returns a bool attribute value.
func (d *Database) GetBool(e datalog.Identity, a datalog.Keyword) (bool, bool, error) {
	var v bool
	found, err := d.QueryOneInto(&v, queryGetAttr, e, a)
	return v, found, err
}

// GetRef returns an entity reference attribute value.
func (d *Database) GetRef(e datalog.Identity, a datalog.Keyword) (datalog.Identity, bool, error) {
	var v datalog.Identity
	found, err := d.QueryOneInto(&v, queryGetAttr, e, a)
	return v, found, err
}

// GetStrings returns all string values for a cardinality-many attribute.
func (d *Database) GetStrings(e datalog.Identity, a datalog.Keyword) ([]string, error) {
	var vals []string
	if err := d.QueryInto(&vals, queryGetAttr, e, a); err != nil {
		return nil, err
	}
	return vals, nil
}
