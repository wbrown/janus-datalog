// Package qb provides an idiomatic Go query builder for Datalog queries.
//
// The key insight: Go variable identity IS the join condition. Using the same
// *Var pointer in multiple patterns creates a join between those patterns.
//
// # Define Attributes as Constants
//
// Define schema attributes as package-level constants to prevent typos and
// enable IDE completion:
//
//	var (
//	    PersonName = qb.Kw(":person/name")
//	    PersonAge  = qb.Kw(":person/age")
//	    PersonCity = qb.Kw(":person/city")
//	)
//
// # Basic Usage
//
//	e := qb.NewVar("e")
//	name := qb.NewVar("name")
//	age := qb.NewVar("age")
//
//	q := qb.Query().
//	    Find(name, age).
//	    Where(
//	        qb.Pat(e, PersonName, name),
//	        qb.Pat(e, PersonAge, age),  // same e = join
//	        qb.Gt(age, 21),
//	    ).
//	    MustBuild()
//
//	results, err := db.ExecuteQuery(q)
//
// # Variables
//
// Variables represent unknowns in queries. Same pointer = same logical variable:
//
//	e := qb.NewVar("e")
//	name := qb.NewVar("name")
//	// Using same variable in multiple patterns creates a join
//
// # Patterns
//
// Pat creates patterns of any arity:
//
//	qb.Pat(e, a, v)           // [e a v] database pattern
//	qb.Pat(e, a, v, tx)       // [e a v tx] with transaction
//	qb.Pat(e, a, v, tx, op)   // [e a v tx op] history
//	qb.Pat(name, age)         // 2-tuple input relation
//
// # Input Parameters
//
//	qb.DB                     // database source
//	qb.Scalar(v)              // single value
//	qb.Collection(v)          // iterate values [?v ...]
//	qb.Tuple(a, b)            // single tuple [[?a ?b]]
//	qb.Relation(a, b)         // multiple tuples [[?a ?b] ...]
//
// # Predicates
//
//	qb.Lt(a, b)               // a < b
//	qb.Lte(a, b)              // a <= b
//	qb.Gt(a, b)               // a > b
//	qb.Gte(a, b)              // a >= b
//	qb.Eq(a, b)               // a = b
//	qb.Ne(a, b)               // a != b
//	qb.Range(min, v, max)          // min < v < max
//	qb.RangeInclusive(min, v, max) // min <= v <= max
//
// # Aggregations
//
//	qb.Sum(v)
//	qb.Count(v)
//	qb.Avg(v)
//	qb.Min(v)
//	qb.Max(v)
//
// # Expressions
//
//	qb.Add(a, b).As(result)
//	qb.Sub(a, b).As(result)
//	qb.Mul(a, b).As(result)
//	qb.Div(a, b).As(result)
//	qb.Str(parts...).As(result)
//	qb.Ground(value).As(result)
//	qb.Year(time).As(result)
//
// # Logical Clauses
//
//	qb.Not(clauses...)
//	qb.NotJoin(joinVars, clauses...)
//	qb.Or().Branch(clauses...).Branch(clauses...)
//	qb.OrJoin(joinVars...).Branch(clauses...).Branch(clauses...)
//
// # Subqueries
//
//	innerQ := qb.Query().
//	    Find(qb.Max(salary)).
//	    In(qb.DB, qb.Scalar(dept)).
//	    Where(qb.Pat(e, EmpDept, dept), qb.Pat(e, EmpSalary, salary))
//
//	qb.Subquery(innerQ, dept).BindTuple(maxSalary)
//
// Subqueries have lexical scoping - reuse variable names like ?t across subqueries:
//
//	t, s := qb.NewVar("t"), qb.NewVar("s")
//	subquery1 := qb.Query().Find(qb.Count(t)).In(qb.DB, qb.Scalar(s)).Where(...)
//
//	t, s = qb.NewVar("t"), qb.NewVar("s")  // reassign Go vars, same Datalog names
//	subquery2 := qb.Query().Find(qb.Count(t)).In(qb.DB, qb.Scalar(s)).Where(...)
//
// See docs/reference/QUERY_BUILDER.md for complete documentation.
package qb
