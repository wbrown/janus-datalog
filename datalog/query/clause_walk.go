package query

import "fmt"

// WalkClauses visits every clause in clauses in order. When visit returns
// true for a compound clause, the walk descends into its child clauses —
// NOT/NOT-JOIN bodies and OR/OR-JOIN/OR-DEFAULT branches; returning false
// skips the children without stopping the walk. Subquery inner queries are
// never descended into implicitly: a subquery's :where is its own scope, and
// visitors that need it descend explicitly.
//
// The clause taxonomy is closed (unexported marker method), and the switch
// below enumerates it completely — leaves included — so adding a clause type
// without deciding its traversal here panics instead of being silently
// skipped by every consumer at once.
func WalkClauses(clauses []Clause, visit func(Clause) bool) {
	for _, clause := range clauses {
		if !visit(clause) {
			continue
		}
		switch c := clause.(type) {
		case *NotClause:
			WalkClauses(c.Clauses, visit)
		case *NotJoinClause:
			WalkClauses(c.Clauses, visit)
		case *OrClause:
			for _, branch := range c.Branches {
				WalkClauses(branch, visit)
			}
		case *OrJoinClause:
			for _, branch := range c.Branches {
				WalkClauses(branch, visit)
			}
		case *OrDefaultClause:
			for _, branch := range c.Branches {
				WalkClauses(branch, visit)
			}
		case *OrDefaultJoinClause:
			for _, branch := range c.Branches {
				WalkClauses(branch, visit)
			}
		case *DataPattern, *Expression, *SubqueryPattern,
			*Comparison, *ChainedComparison, *NotEqualPredicate,
			*GroundPredicate, *MissingPredicate, *StrStartsWithPredicate,
			*FunctionPredicate, *DatabaseFunctionPredicate, *TxRangePredicate:
			// Leaves: no child clauses.
		default:
			panic(fmt.Sprintf("BUG: unknown clause type %T in WalkClauses", clause))
		}
	}
}
