package qb

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// QueryBuilder constructs a *query.Query using fluent methods.
// The built query can be passed directly to Database.QueryInto,
// Database.ExecuteQuery, etc.
type QueryBuilder struct {
	find    []query.FindElement
	in      []query.InputSpec
	where   []query.Clause
	orderBy []query.OrderByClause
	errors  []error
}

// Query starts building a new query.
//
// Example:
//
//	e, name, age := qb.NewVar("e"), qb.NewVar("name"), qb.NewVar("age")
//	q := qb.Query().
//	    Find(name, age).
//	    Where(
//	        qb.Pat(e, PersonName, name),
//	        qb.Pat(e, PersonAge, age),
//	        qb.Gt(age, 21),
//	    ).MustBuild()
func Query() *QueryBuilder {
	return &QueryBuilder{}
}

// Find specifies what to return from the query.
//
// Accepts:
//   - *Var: returns the variable's value
//   - Agg: returns an aggregation result (Sum, Count, etc.)
//
// Example:
//
//	qb.Query().Find(name, age)              // return two variables
//	qb.Query().Find(dept, qb.Sum(salary))   // return variable and aggregation
func (b *QueryBuilder) Find(elements ...interface{}) *QueryBuilder {
	for _, elem := range elements {
		findElem, err := toFindElement(elem)
		if err != nil {
			b.errors = append(b.errors, err)
			continue
		}
		b.find = append(b.find, findElem)
	}
	return b
}

// In specifies input parameters for the query.
// The first input is typically DB (the database), followed by parameter specs.
//
// Example:
//
//	minAge := qb.NewVar("minAge")
//	qb.Query().
//	    Find(name).
//	    In(qb.DB, qb.Scalar(minAge)).
//	    Where(...)
func (b *QueryBuilder) In(specs ...interface{}) *QueryBuilder {
	for _, spec := range specs {
		inputSpec, err := toInputSpec(spec)
		if err != nil {
			b.errors = append(b.errors, err)
			continue
		}
		b.in = append(b.in, inputSpec)
	}
	return b
}

// Where adds clauses to the query.
//
// Accepts:
//   - *Pattern: data patterns from Pat(e, a, v), Pat(e, a, v, tx), Pat(e, a, v, tx, op)
//   - *Comparison: comparisons from Lt(), Gt(), Eq(), etc.
//   - *ChainedComparison: from Chained(), Range()
//   - *Expression: from Add().As(), Str().As(), etc.
//   - *NotClause: from Not()
//   - *NotJoinClause: from NotJoin()
//   - *OrClause: from Or()
//   - *OrJoinClause: from OrJoin()
//   - *SubqueryBuilder: from Subquery()
//
// Example:
//
//	qb.Query().
//	    Find(name, age).
//	    Where(
//	        qb.Pat(e, PersonName, name),
//	        qb.Pat(e, PersonAge, age),
//	        qb.Gt(age, 21),
//	    )
func (b *QueryBuilder) Where(clauses ...interface{}) *QueryBuilder {
	for i, clause := range clauses {
		c, err := toClause(clause)
		if err != nil {
			b.errors = append(b.errors, err)
			continue
		}
		if c == nil {
			panic(fmt.Sprintf("Where: clause %d converted to nil", i))
		}
		b.where = append(b.where, c)
	}
	return b
}

// OrderBy adds ordering to the query results.
//
// Example:
//
//	qb.Query().
//	    Find(name, age).
//	    Where(...).
//	    OrderBy(qb.Asc(name), qb.Desc(age))
func (b *QueryBuilder) OrderBy(specs ...interface{}) *QueryBuilder {
	for _, spec := range specs {
		orderSpec, err := toOrderByClause(spec)
		if err != nil {
			b.errors = append(b.errors, err)
			continue
		}
		b.orderBy = append(b.orderBy, orderSpec)
	}
	return b
}

// Build finalizes and validates the query, returning the built *query.Query.
// Returns an error if the query is invalid or if any errors occurred during construction.
func (b *QueryBuilder) Build() (*query.Query, error) {
	if len(b.errors) > 0 {
		return nil, fmt.Errorf("query builder errors: %v", b.errors)
	}
	if len(b.find) == 0 {
		return nil, fmt.Errorf("query must have at least one find element")
	}
	if len(b.where) == 0 {
		return nil, fmt.Errorf("query must have at least one where clause")
	}

	return &query.Query{
		Find:    b.find,
		In:      b.in,
		Where:   b.where,
		OrderBy: b.orderBy,
	}, nil
}

// MustBuild is like Build but panics on error.
// Useful for static query definitions where errors indicate programming bugs.
//
// Example:
//
//	var FindAdults = qb.Query().
//	    Find(name, age).
//	    Where(...).
//	    MustBuild()
func (b *QueryBuilder) MustBuild() *query.Query {
	q, err := b.Build()
	if err != nil {
		panic(err)
	}
	return q
}

// OrderSpec represents an ordering specification.
type OrderSpec struct {
	v         *Var
	direction query.OrderDirection
}

// Asc creates an ascending order specification.
func Asc(v *Var) OrderSpec {
	return OrderSpec{v: v, direction: query.OrderAsc}
}

// Desc creates a descending order specification.
func Desc(v *Var) OrderSpec {
	return OrderSpec{v: v, direction: query.OrderDesc}
}

// findElementer is implemented by types that can convert to FindElement
type findElementer interface {
	toFindElement() query.FindElement
}

// toFindElement converts various types to query.FindElement
func toFindElement(elem interface{}) (query.FindElement, error) {
	switch x := elem.(type) {
	case *Var:
		return x.toFindElement(), nil
	case Agg:
		return x.toFindElement(), nil
	case findElementer:
		return x.toFindElement(), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to FindElement", elem)
	}
}

// toInputSpec converts various types to query.InputSpec
func toInputSpec(spec interface{}) (query.InputSpec, error) {
	switch x := spec.(type) {
	case InputSpec:
		return x.toInputSpec(), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to InputSpec", spec)
	}
}

// toClause converts various types to query.Clause
func toClause(c interface{}) (query.Clause, error) {
	switch x := c.(type) {
	case Clause:
		return x.toClause(), nil
	case *Pattern:
		return x.toClause(), nil
	case *Comparison:
		return x.toClause(), nil
	case *ChainedComparison:
		return x.toClause(), nil
	case *Expression:
		return x.toClause(), nil
	case *NotClause:
		return x.toClause(), nil
	case *NotJoinClause:
		return x.toClause(), nil
	case *OrClause:
		return x.toClause(), nil
	case *OrJoinClause:
		return x.toClause(), nil
	case *SubqueryBuilder:
		return x.toClause(), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to Clause", c)
	}
}

// toOrderByClause converts various types to query.OrderByClause
func toOrderByClause(spec interface{}) (query.OrderByClause, error) {
	switch x := spec.(type) {
	case OrderSpec:
		return query.OrderByClause{
			Variable:  x.v.Symbol(),
			Direction: x.direction,
		}, nil
	case *Var:
		// Default to ascending
		return query.OrderByClause{
			Variable:  x.Symbol(),
			Direction: query.OrderAsc,
		}, nil
	default:
		return query.OrderByClause{}, fmt.Errorf("cannot convert %T to OrderByClause", spec)
	}
}
