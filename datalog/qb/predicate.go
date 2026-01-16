package qb

import (
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Comparison represents a binary comparison predicate.
type Comparison struct {
	op    query.CompareOp
	left  interface{}
	right interface{}
}

// Lt creates a less-than comparison: left < right
//
// Example:
//
//	qb.Lt(age, 30)       // ?age < 30
//	qb.Lt(price, limit)  // ?price < ?limit
func Lt(left, right interface{}) *Comparison {
	return &Comparison{op: query.OpLT, left: left, right: right}
}

// Lte creates a less-than-or-equal comparison: left <= right
func Lte(left, right interface{}) *Comparison {
	return &Comparison{op: query.OpLTE, left: left, right: right}
}

// Gt creates a greater-than comparison: left > right
//
// Example:
//
//	qb.Gt(age, 21)  // ?age > 21
func Gt(left, right interface{}) *Comparison {
	return &Comparison{op: query.OpGT, left: left, right: right}
}

// Gte creates a greater-than-or-equal comparison: left >= right
func Gte(left, right interface{}) *Comparison {
	return &Comparison{op: query.OpGTE, left: left, right: right}
}

// Eq creates an equality comparison: left = right
//
// Example:
//
//	qb.Eq(status, qb.V("active"))  // ?status = "active"
func Eq(left, right interface{}) *Comparison {
	return &Comparison{op: query.OpEQ, left: left, right: right}
}

// Ne creates a not-equal comparison: left != right
func Ne(left, right interface{}) *Comparison {
	return &Comparison{op: query.OpNE, left: left, right: right}
}

// toClause converts Comparison to a query.Clause
func (c *Comparison) toClause() query.Clause {
	return &query.Comparison{
		Op:    c.op,
		Left:  toTerm(c.left),
		Right: toTerm(c.right),
	}
}

// ChainedComparison represents a chained comparison like (< 0 ?x 100).
// This is evaluated as (0 < ?x) AND (?x < 100).
type ChainedComparison struct {
	op    query.CompareOp
	terms []interface{}
}

// Chained creates a chained comparison with 3 or more terms.
// All adjacent pairs are compared with the same operator.
//
// Example:
//
//	qb.Chained(query.OpLT, 0, x, 100)  // 0 < ?x < 100
//	qb.Chained(query.OpLTE, a, b, c)   // ?a <= ?b <= ?c
func Chained(op query.CompareOp, terms ...interface{}) *ChainedComparison {
	return &ChainedComparison{op: op, terms: terms}
}

// Range creates a range check: min < v < max
// This is a convenience wrapper for Chained with OpLT.
//
// Example:
//
//	qb.Range(0, price, 100)  // 0 < ?price < 100
func Range(min interface{}, v *Var, max interface{}) *ChainedComparison {
	return Chained(query.OpLT, min, v, max)
}

// RangeInclusive creates an inclusive range check: min <= v <= max
//
// Example:
//
//	qb.RangeInclusive(1, rating, 5)  // 1 <= ?rating <= 5
func RangeInclusive(min interface{}, v *Var, max interface{}) *ChainedComparison {
	return Chained(query.OpLTE, min, v, max)
}

// toClause converts ChainedComparison to a query.Clause
func (c *ChainedComparison) toClause() query.Clause {
	terms := make([]query.Term, len(c.terms))
	for i, t := range c.terms {
		terms[i] = toTerm(t)
	}
	return &query.ChainedComparison{
		Op:    c.op,
		Terms: terms,
	}
}
