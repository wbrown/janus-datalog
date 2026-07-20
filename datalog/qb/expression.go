package qb

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Expression represents an expression clause that binds a computed result.
type Expression struct {
	fn      query.Function
	binding *Var
}

// toClause converts Expression to a query.Clause
func (e *Expression) toClause() query.Clause {
	return &query.Expression{
		Function: e.fn,
		Binding:  e.binding.Symbol(),
	}
}

// ArithBuilder builds an arithmetic expression.
type ArithBuilder struct {
	op   query.Symbol
	args []interface{}
}

// Add creates an addition expression.
// Call .As(resultVar) to bind the result.
//
// Example:
//
//	total := qb.NewVar("total")
//	qb.Add(price, tax).As(total)  // [(+ ?price ?tax) ?total]
func Add(first interface{}, rest ...interface{}) *ArithBuilder {
	return &ArithBuilder{op: datalog.SymAdd, args: append([]interface{}{first}, rest...)}
}

// Sub creates a subtraction expression.
// Call .As(resultVar) to bind the result.
func Sub(first interface{}, rest ...interface{}) *ArithBuilder {
	return &ArithBuilder{op: datalog.SymSubtract, args: append([]interface{}{first}, rest...)}
}

// Mul creates a multiplication expression.
// Call .As(resultVar) to bind the result.
func Mul(first interface{}, rest ...interface{}) *ArithBuilder {
	return &ArithBuilder{op: datalog.SymMultiply, args: append([]interface{}{first}, rest...)}
}

// Div creates a division expression.
// Call .As(resultVar) to bind the result.
func Div(first interface{}, rest ...interface{}) *ArithBuilder {
	return &ArithBuilder{op: datalog.SymDivide, args: append([]interface{}{first}, rest...)}
}

// As binds the arithmetic result to a variable, completing the expression.
func (a *ArithBuilder) As(result *Var) *Expression {
	terms := make([]query.Term, len(a.args))
	for i, argument := range a.args {
		terms[i] = toTerm(argument)
	}
	return &Expression{
		fn: query.ArithmeticFunction{
			Op:   a.op,
			Args: terms,
		},
		binding: result,
	}
}

// StrBuilder builds a string concatenation expression.
type StrBuilder struct {
	parts []interface{}
}

// Str creates a string concatenation expression.
// Call .As(resultVar) to bind the result.
//
// Example:
//
//	fullName := qb.NewVar("fullName")
//	qb.Str(firstName, " ", lastName).As(fullName)
func Str(parts ...interface{}) *StrBuilder {
	return &StrBuilder{parts: parts}
}

// As binds the string concatenation result to a variable.
func (s *StrBuilder) As(result *Var) *Expression {
	terms := make([]query.Term, len(s.parts))
	for i, p := range s.parts {
		terms[i] = toTerm(p)
	}
	return &Expression{
		fn:      query.StringConcatFunction{Terms: terms},
		binding: result,
	}
}

// GroundBuilder builds a ground expression (constant binding).
type GroundBuilder struct {
	value interface{}
}

// Ground creates a ground expression that binds a constant value.
// Call .As(resultVar) to bind the result.
//
// Example:
//
//	taxRate := qb.NewVar("taxRate")
//	qb.Ground(0.08).As(taxRate)  // [(ground 0.08) ?taxRate]
func Ground(value interface{}) *GroundBuilder {
	return &GroundBuilder{value: normalizeConstant(value)}
}

// As binds the ground value to a variable.
func (g *GroundBuilder) As(result *Var) *Expression {
	return &Expression{
		fn:      &query.GroundFunction{Value: g.value},
		binding: result,
	}
}

// TupleGroundBuilder builds a tuple ground expression.
type TupleGroundBuilder struct {
	values []interface{}
}

// TupleGround creates a tuple ground expression that binds multiple constants.
// Call .As(vars...) to bind to multiple variables.
//
// Example:
//
//	a, b, c := qb.NewVar("a"), qb.NewVar("b"), qb.NewVar("c")
//	qb.TupleGround(0, 0, 0).As(a, b, c)  // [(ground [0 0 0]) [?a ?b ?c]]
func TupleGround(values ...interface{}) *TupleGroundBuilder {
	normalized := make([]interface{}, len(values))
	for i, v := range values {
		normalized[i] = normalizeConstant(v)
	}
	return &TupleGroundBuilder{values: normalized}
}

// As binds the tuple values to multiple variables.
func (g *TupleGroundBuilder) As(vars ...*Var) *TupleExpression {
	return &TupleExpression{
		fn:       &query.GroundFunction{Value: g.values},
		bindings: vars,
	}
}

// TupleExpression represents an expression with tuple binding.
type TupleExpression struct {
	fn       query.Function
	bindings []*Var
}

// toClause converts TupleExpression to a query.Clause
func (e *TupleExpression) toClause() query.Clause {
	vars := make([]query.Symbol, len(e.bindings))
	for i, v := range e.bindings {
		vars[i] = v.Symbol()
	}
	return &query.Expression{
		Function: e.fn,
		Binding:  query.TupleBinding{Variables: vars},
	}
}

// IdentityBuilder builds an identity expression (pass-through binding).
type IdentityBuilder struct {
	arg interface{}
}

// Identity creates an identity expression that passes a value through.
// Useful for binding an existing variable to a new name.
func Identity(arg interface{}) *IdentityBuilder {
	return &IdentityBuilder{arg: arg}
}

// As binds the identity result to a variable.
func (i *IdentityBuilder) As(result *Var) *Expression {
	return &Expression{
		fn:      query.IdentityFunction{Arg: toTerm(i.arg)},
		binding: result,
	}
}

// TimeBuilder builds a time extraction expression.
type TimeBuilder struct {
	field   query.Symbol
	timeVar *Var
}

// Year extracts the year from a time variable.
// Call .As(resultVar) to bind the result.
//
// Example:
//
//	year := qb.NewVar("year")
//	qb.Year(createdAt).As(year)
func Year(timeVar *Var) *TimeBuilder {
	return &TimeBuilder{field: datalog.SymYear, timeVar: timeVar}
}

// Month extracts the month from a time variable.
func Month(timeVar *Var) *TimeBuilder {
	return &TimeBuilder{field: datalog.SymMonth, timeVar: timeVar}
}

// Day extracts the day from a time variable.
func Day(timeVar *Var) *TimeBuilder {
	return &TimeBuilder{field: datalog.SymDay, timeVar: timeVar}
}

// Hour extracts the hour from a time variable.
func Hour(timeVar *Var) *TimeBuilder {
	return &TimeBuilder{field: datalog.SymHour, timeVar: timeVar}
}

// Minute extracts the minute from a time variable.
func Minute(timeVar *Var) *TimeBuilder {
	return &TimeBuilder{field: datalog.SymMinute, timeVar: timeVar}
}

// Second extracts the second from a time variable.
func Second(timeVar *Var) *TimeBuilder {
	return &TimeBuilder{field: datalog.SymSecond, timeVar: timeVar}
}

// As binds the time extraction result to a variable.
func (t *TimeBuilder) As(result *Var) *Expression {
	return &Expression{
		fn: query.TimeExtractionFunction{
			Field:    t.field,
			TimeTerm: query.VariableTerm{Symbol: t.timeVar.Symbol()},
		},
		binding: result,
	}
}
