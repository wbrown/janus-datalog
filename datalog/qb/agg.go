package qb

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Agg represents an aggregation function result.
// Used in Find() to aggregate over grouped results.
type Agg struct {
	fn  query.Symbol
	arg *Var
}

// Sum creates a sum aggregation over a variable.
//
// Example:
//
//	qb.Query().
//	    Find(dept, qb.Sum(salary)).
//	    Where(...)
func Sum(v *Var) Agg {
	return Agg{fn: datalog.SymSum, arg: v}
}

// Count creates a count aggregation over a variable.
//
// Example:
//
//	qb.Query().
//	    Find(dept, qb.Count(employee)).
//	    Where(...)
func Count(v *Var) Agg {
	return Agg{fn: datalog.SymCount, arg: v}
}

// Avg creates an average aggregation over a variable.
//
// Example:
//
//	qb.Query().
//	    Find(dept, qb.Avg(salary)).
//	    Where(...)
func Avg(v *Var) Agg {
	return Agg{fn: datalog.SymAvg, arg: v}
}

// Min creates a minimum aggregation over a variable.
//
// Example:
//
//	qb.Query().
//	    Find(symbol, qb.Min(price)).
//	    Where(...)
func Min(v *Var) Agg {
	return Agg{fn: datalog.SymMin, arg: v}
}

// Max creates a maximum aggregation over a variable.
//
// Example:
//
//	qb.Query().
//	    Find(symbol, qb.Max(price)).
//	    Where(...)
func Max(v *Var) Agg {
	return Agg{fn: datalog.SymMax, arg: v}
}

// toFindElement converts Agg to a query.FindElement
func (a Agg) toFindElement() query.FindElement {
	return query.FindAggregate{
		Function: a.fn,
		Arg:      a.arg.Symbol(),
	}
}
