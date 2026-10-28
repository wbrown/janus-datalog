package query

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// validateConstants refuses a constant outside the value domain at every
// position a query's :where holds one: a pattern element, a predicate term,
// a function's argument or ground value, a get-else default and a subquery
// argument, at the top level and inside every form. An inner query's
// constants are its own ValidateQuery's, which SubqueryPattern.Validate
// runs, and a find pull's defaults are PullPattern.Validate's. The walk
// enumerates the clause, function and element kinds as the hash does and
// panics on a kind it does not know, so a kind added without a decision
// here fails loudly.
func validateConstants(q *Query) error {
	return validateClauseConstants(q.Where)
}

func validateBranchConstants(branches [][]Clause) error {
	for _, branch := range branches {
		if err := validateClauseConstants(branch); err != nil {
			return err
		}
	}
	return nil
}

func validateClauseConstants(clauses []Clause) error {
	for _, clause := range clauses {
		var err error
		switch c := clause.(type) {
		case *DataPattern:
			err = validateElementConstants(c.Elements)
		case *Comparison:
			err = validateTermConstants(c.Left, c.Right)
		case *ChainedComparison:
			err = validateTermConstants(c.Terms...)
		case *NotEqualPredicate:
			err = validateTermConstants(c.Left, c.Right)
		case *GroundPredicate, *MissingPredicate, *TxRangePredicate:
		case *StrStartsWithPredicate:
			err = validateTermConstants(c.Value, c.Prefix)
		case *FunctionPredicate:
			err = validateElementConstants(c.Args)
		case *DatabaseFunctionPredicate:
			err = validateDatabaseFunctionConstants(c.Function)
		case *Expression:
			err = validateFunctionConstants(c.Function)
		case *NotClause:
			err = validateClauseConstants(c.Clauses)
		case *NotJoinClause:
			err = validateClauseConstants(c.Clauses)
		case *OrClause:
			err = validateBranchConstants(c.Branches)
		case *OrJoinClause:
			err = validateBranchConstants(c.Branches)
		case *OrDefaultClause:
			err = validateBranchConstants(c.Branches)
		case *OrDefaultJoinClause:
			err = validateBranchConstants(c.Branches)
		case *SubqueryPattern:
			err = validateElementConstants(c.Inputs)
		case *RuleInvocation:
			err = validateElementConstants(c.Args)
		default:
			panic(fmt.Sprintf("query constants: %T is not a clause kind", clause))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func validateFunctionConstants(fn Function) error {
	switch f := functionValue(fn).(type) {
	case ArithmeticFunction:
		return validateTermConstants(f.Args...)
	case StringConcatFunction:
		return validateTermConstants(f.Terms...)
	case TimeExtractionFunction:
		return validateTermConstants(f.TimeTerm)
	case GroundFunction:
		if err := datalog.ValidateValue(f.Value); err != nil {
			return fmt.Errorf("ground %s: %w", FormatValueEDN(f.Value), err)
		}
		return nil
	case IdentityFunction:
		return validateTermConstants(f.Arg)
	case ComparisonFunction:
		return validateTermConstants(f.Comparison.Left, f.Comparison.Right)
	case ChainedComparisonFunction:
		return validateTermConstants(f.ChainedComparison.Terms...)
	case AndFunction:
		return nil
	case CustomFunction:
		return validateTermConstants(f.Args...)
	case NthFunction:
		return validateTermConstants(f.VecTerm, f.IndexTerm)
	case FirstFunction:
		return validateTermConstants(f.VecTerm)
	case LastFunction:
		return validateTermConstants(f.VecTerm)
	case LengthFunction:
		return validateTermConstants(f.VecTerm)
	case ContainsFunction:
		return validateTermConstants(f.VecTerm, f.ValueTerm)
	case IndexOfFunction:
		return validateTermConstants(f.VecTerm, f.ValueTerm)
	case SubvecFunction:
		return validateTermConstants(f.VecTerm, f.StartTerm, f.EndTerm)
	case EnumerateFunction:
		return validateTermConstants(f.VecTerm)
	case *GetElseFunction:
		return validateDatabaseFunctionConstants(f)
	case *MissingFunction:
		return validateDatabaseFunctionConstants(f)
	case *GetSomeFunction:
		return validateDatabaseFunctionConstants(f)
	default:
		panic(fmt.Sprintf("query constants: %T is not a function kind", fn))
	}
}

func validateDatabaseFunctionConstants(fn DatabaseFunction) error {
	switch f := fn.(type) {
	case *GetElseFunction:
		if err := validateTermConstants(f.Entity); err != nil {
			return err
		}
		if err := datalog.ValidateValue(f.Default); err != nil {
			return fmt.Errorf("get-else default of %s: %w", f.Attr, err)
		}
		return nil
	case *MissingFunction:
		return validateTermConstants(f.Entity)
	case *GetSomeFunction:
		return validateTermConstants(f.Entity)
	default:
		panic(fmt.Sprintf("query constants: %T is not a database function kind", fn))
	}
}

// validateTermConstants checks every constant term. A term's value is a
// pattern element when the parser wrapped a vector literal or a blank in a
// function's argument as the term's value, and is a value of the domain
// otherwise.
func validateTermConstants(terms ...Term) error {
	for _, term := range terms {
		constant, ok := termValue(term).(ConstantTerm)
		if !ok {
			continue
		}
		if elem, isElement := constant.Value.(PatternElement); isElement {
			if err := validateElementConstants([]PatternElement{elem}); err != nil {
				return err
			}
			continue
		}
		if err := datalog.ValidateValue(constant.Value); err != nil {
			return fmt.Errorf("constant %s: %w", constant, err)
		}
	}
	return nil
}

func validateElementConstants(elements []PatternElement) error {
	for _, elem := range elements {
		switch e := elem.(type) {
		case Constant:
			if err := datalog.ValidateValue(e.Value); err != nil {
				return fmt.Errorf("constant %s: %w", e, err)
			}
		case VectorConstant:
			if err := datalog.ValidateValue(e.Values); err != nil {
				return fmt.Errorf("constant %s: %w", e, err)
			}
		}
	}
	return nil
}
