package storage

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
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
// when done. A registered handler sees the query invoked once and completed
// once: refused or failed before the result exists, or, for a query that
// runs, when the result's iterator is exhausted or closed.
func (d *Database) Query(queryInput interface{}, inputs ...interface{}) (executor.Relation, error) {
	_, rel, err := d.queryUnderPlannerOptions(d.effectivePlannerOptions(), queryInput, inputs...)
	return rel, err
}

// queryUnderPlannerOptions is Query's body with the planner options supplied
// rather than read from the database. Query passes the database's own; tests
// that vary the optimizer profile per query pass theirs. One query path either
// way — read session, source routing, input conversion, session-bound result.
// It returns the query it resolved beside the result, for a caller that reads
// the query's find.
func (d *Database) queryUnderPlannerOptions(
	planOpts planner.PlannerOptions,
	queryInput interface{},
	inputs ...interface{},
) (*query.Query, executor.Relation, error) {
	// Separate QueryOptions from regular inputs
	opts, regularInputs := extractQueryOptions(inputs)

	// A handler sees every query the database is asked, invoked once and
	// completed once. A query refused here, before the executor's entry, is
	// reported as the executor reports a query it refuses: invoked, with the
	// query's rendering as received, then completed with the refusal. A query
	// that does not resolve is reported with a rendering of the value it was
	// given, text as it was given. A query that reaches ExecuteWithRelations
	// is reported invoked there, and completed there when it is refused or
	// fails in execution. A query that runs completes where it ends: its
	// session-bound result reports the completion, with the tuples drained,
	// when its iterator is exhausted or closed.
	handler := planOpts.Handler
	var queryStart time.Time
	var completion *queryCompletion
	if handler != nil {
		queryStart = time.Now()
		completion = &queryCompletion{handler: handler, start: queryStart}
	}
	refused := func(rendered string, err error) (*query.Query, executor.Relation, error) {
		if handler != nil {
			handler(annotations.Event{
				Name:  annotations.QueryInvoked,
				Start: queryStart,
				Data:  map[string]interface{}{annotations.KeyQuery: rendered},
			})
			handler(annotations.TimedEvent(annotations.QueryComplete, queryStart,
				map[string]interface{}{
					annotations.KeyRelationsCount: 0,
					annotations.KeyTuplesCount:    0,
					annotations.KeySuccess:        false,
					annotations.KeyError:          err.Error(),
				}))
		}
		return nil, nil, err
	}

	q, err := d.resolveQuery(queryInput)
	if err != nil {
		text, isText := queryInput.(string)
		if !isText {
			text = fmt.Sprint(queryInput)
		}
		return refused(text, fmt.Errorf("failed to resolve query: %w", err))
	}

	// One read session per query: every storage read — eager during
	// execution or lazy during result consumption — observes one snapshot.
	// The session closes when the result is exhausted or closed.
	matcher, session, err := d.sessionMatcher(planOpts)
	if err != nil {
		return refused(q.String(), err)
	}

	// Build source map — database is always $
	sources := buildSourceMap(opts.sources, matcher)

	// Validate declared sources exist
	if err := validateQuerySources(q, sources); err != nil {
		_ = session.Close()
		return refused(q.String(), err)
	}

	// Create SourceRouter with full source map
	router := executor.NewSourceRouter(sources)

	inputRelations, rules, err := d.convertInputsToRelations(q, regularInputs)
	if err != nil {
		_ = session.Close()
		return refused(q.String(), err)
	}

	// Execute with the SourceRouter as the PatternMatcher. The executor has
	// its plans carry their rosters when a source the router holds refuses
	// unknown attributes, for the check at its entry.
	planOpts.Cache = d.planCache
	exec := executor.NewExecutorWithOptions(router, d, planOpts)
	result, err := exec.ExecuteWithRelations(executor.NewContext().WithRules(rules).WithCancellation(opts.ctx), q, inputRelations)
	if err != nil {
		_ = session.Close()
		return nil, nil, fmt.Errorf("query execution failed: %w", err)
	}

	return q, newSessionBoundRelation(result, session, completion), nil
}

// sessionMatcher mints the default-source matcher with a fresh read session
// attached, so every storage read of one query observes one snapshot. The
// caller owns the session's lifecycle; on success it is released by the
// session-bound result, on failure the caller closes it directly.
func (d *Database) sessionMatcher(opts planner.PlannerOptions) (executor.PatternMatcher, ReadSession, error) {
	session, err := d.store.NewReadSession()
	if err != nil {
		return nil, nil, err
	}
	matcher := d.matcherWithExecOptions(opts)
	// matcherWithExecOptions constructs *PatternMatcher in both its arms
	// (fresh, or the AsOf copy for temporal handles).
	matcher.(*PatternMatcher).AttachReadSession(session)
	return matcher, session, nil
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

// A typed accessor names the attribute it reads, so a database that refuses
// unknown attributes refuses an accessor naming one its schema does not
// declare before the query runs, as it refuses a query naming it. The query
// the accessors share binds the attribute through an input, which the
// vocabulary check at the executor's entry reads as a value.

// GetString returns a string attribute value.
func (d *Database) GetString(e datalog.Identity, a datalog.Keyword) (string, bool, error) {
	if err := d.UnknownAttributes(datalog.SymDollar, []datalog.Keyword{a}); err != nil {
		return "", false, err
	}
	var v string
	found, err := d.QueryOneInto(&v, queryGetAttr, e, a)
	return v, found, err
}

// GetInt returns an int64 attribute value.
func (d *Database) GetInt(e datalog.Identity, a datalog.Keyword) (int64, bool, error) {
	if err := d.UnknownAttributes(datalog.SymDollar, []datalog.Keyword{a}); err != nil {
		return 0, false, err
	}
	var v int64
	found, err := d.QueryOneInto(&v, queryGetAttr, e, a)
	return v, found, err
}

// GetFloat returns a float64 attribute value.
func (d *Database) GetFloat(e datalog.Identity, a datalog.Keyword) (float64, bool, error) {
	if err := d.UnknownAttributes(datalog.SymDollar, []datalog.Keyword{a}); err != nil {
		return 0, false, err
	}
	var v float64
	found, err := d.QueryOneInto(&v, queryGetAttr, e, a)
	return v, found, err
}

// GetBool returns a bool attribute value.
func (d *Database) GetBool(e datalog.Identity, a datalog.Keyword) (bool, bool, error) {
	if err := d.UnknownAttributes(datalog.SymDollar, []datalog.Keyword{a}); err != nil {
		return false, false, err
	}
	var v bool
	found, err := d.QueryOneInto(&v, queryGetAttr, e, a)
	return v, found, err
}

// GetRef returns an entity reference attribute value.
func (d *Database) GetRef(e datalog.Identity, a datalog.Keyword) (datalog.Identity, bool, error) {
	if err := d.UnknownAttributes(datalog.SymDollar, []datalog.Keyword{a}); err != nil {
		return nil, false, err
	}
	var v datalog.Identity
	found, err := d.QueryOneInto(&v, queryGetAttr, e, a)
	return v, found, err
}

// GetStrings returns all string values for a cardinality-many attribute.
func (d *Database) GetStrings(e datalog.Identity, a datalog.Keyword) ([]string, error) {
	if err := d.UnknownAttributes(datalog.SymDollar, []datalog.Keyword{a}); err != nil {
		return nil, err
	}
	var vals []string
	if err := d.QueryInto(&vals, queryGetAttr, e, a); err != nil {
		return nil, err
	}
	return vals, nil
}
