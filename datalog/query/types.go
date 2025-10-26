package query

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// Tuple represents a row of values in a relation
// It's used throughout the executor and storage layers for query results
type Tuple []interface{}

// Symbol represents a variable in a query (e.g., ?x, ?name)
type Symbol string

// IsVariable returns true if this is a variable symbol (starts with ?)
func (s Symbol) IsVariable() bool {
	return len(s) > 0 && s[0] == '?'
}

// String returns the string representation
func (s Symbol) String() string {
	return string(s)
}

// PatternElement represents an element in a pattern
// It can be a concrete value, a variable, or a blank
type PatternElement interface {
	IsVariable() bool
	IsBlank() bool
	String() string
}

// Variable represents a query variable (e.g., ?x)
type Variable struct {
	Name Symbol
}

func (v Variable) IsVariable() bool { return true }
func (v Variable) IsBlank() bool    { return false }
func (v Variable) String() string   { return v.Name.String() }

// Blank represents a blank/wildcard (_)
type Blank struct{}

func (b Blank) IsVariable() bool { return false }
func (b Blank) IsBlank() bool    { return true }
func (b Blank) String() string   { return "_" }

// Constant represents a concrete value in a pattern
type Constant struct {
	Value interface{} // Can be Entity, Attribute, Value, etc.
}

func (c Constant) IsVariable() bool { return false }
func (c Constant) IsBlank() bool    { return false }
func (c Constant) String() string   { return fmt.Sprintf("%v", c.Value) }

// Pattern represents a single pattern in a where clause
type Pattern interface {
	String() string
}

// DataPattern represents a data pattern [e a v] or [e a v t]
type DataPattern struct {
	Elements []PatternElement
}

// SubqueryPattern represents a nested query pattern [(q <query> <inputs...>) <binding>]
type SubqueryPattern struct {
	Query   *Query           // The nested query
	Inputs  []PatternElement // Variables/constants to pass as inputs
	Binding BindingForm      // How to bind results
}

func (*SubqueryPattern) clause() {} // Implements Clause interface

// BindingForm describes how subquery results are bound
type BindingForm interface {
	isBindingForm()
	String() string
}

// TupleBinding binds a single row: [[?a ?b]]
type TupleBinding struct {
	Variables []Symbol
}

func (t TupleBinding) isBindingForm() {}
func (t TupleBinding) String() string {
	result := "[["
	for i, v := range t.Variables {
		if i > 0 {
			result += " "
		}
		result += v.String()
	}
	result += "]]"
	return result
}

// CollectionBinding binds all rows: ?coll
type CollectionBinding struct {
	Variable Symbol
}

func (c CollectionBinding) isBindingForm() {}
func (c CollectionBinding) String() string {
	return c.Variable.String()
}

// RelationBinding binds as relation: [[?a ?b] ...]
type RelationBinding struct {
	Variables []Symbol
}

func (r RelationBinding) isBindingForm() {}
func (r RelationBinding) String() string {
	result := "[["
	for i, v := range r.Variables {
		if i > 0 {
			result += " "
		}
		result += v.String()
	}
	result += "] ...]"
	return result
}

// String returns a string representation of the data pattern
func (p DataPattern) String() string {
	result := "["
	for i, elem := range p.Elements {
		if i > 0 {
			result += " "
		}
		result += elem.String()
	}
	result += "]"
	return result
}

// String returns a string representation of the subquery pattern
func (p SubqueryPattern) String() string {
	return p.formatWithIndent("")
}

// formatWithIndent formats the subquery pattern with proper indentation
func (p SubqueryPattern) formatWithIndent(indent string) string {
	result := "[(q "

	// Format the nested query with proper indentation
	baseIndent := indent + "        "   // 8 spaces to match pattern alignment
	nestedIndent := baseIndent + "    " // 4 more spaces for "[(q "
	formattedNested := p.Query.formatWithIndent(nestedIndent)
	result += formattedNested

	// Add newline and indent for inputs and binding
	result += "\n" + nestedIndent

	// Format inputs
	for i, input := range p.Inputs {
		if i > 0 {
			result += " "
		}
		result += input.String()
	}
	result += ") " + p.Binding.String() + "]"

	return result
}

// GetE returns the entity element if it exists
func (p DataPattern) GetE() PatternElement {
	if len(p.Elements) > 0 {
		return p.Elements[0]
	}
	return nil
}

// GetA returns the attribute element if it exists
func (p DataPattern) GetA() PatternElement {
	if len(p.Elements) > 1 {
		return p.Elements[1]
	}
	return nil
}

// GetV returns the value element if it exists
func (p DataPattern) GetV() PatternElement {
	if len(p.Elements) > 2 {
		return p.Elements[2]
	}
	return nil
}

// GetT returns the transaction/time element if it exists
func (p DataPattern) GetT() PatternElement {
	if len(p.Elements) > 3 {
		return p.Elements[3]
	}
	return nil
}

// Symbols returns the symbols (variables) bound by this pattern
// In relational theory, these become the attributes of the resulting relation
func (p *DataPattern) Symbols() []Symbol {
	var symbols []Symbol

	// Check E position
	if v, ok := p.GetE().(Variable); ok {
		symbols = append(symbols, v.Name)
	}

	// Check A position
	if v, ok := p.GetA().(Variable); ok {
		// Avoid duplicates
		found := false
		for _, sym := range symbols {
			if sym == v.Name {
				found = true
				break
			}
		}
		if !found {
			symbols = append(symbols, v.Name)
		}
	}

	// Check V position
	if v, ok := p.GetV().(Variable); ok {
		found := false
		for _, sym := range symbols {
			if sym == v.Name {
				found = true
				break
			}
		}
		if !found {
			symbols = append(symbols, v.Name)
		}
	}

	// Check T position if present
	if len(p.Elements) > 3 {
		if v, ok := p.GetT().(Variable); ok {
			found := false
			for _, sym := range symbols {
				if sym == v.Name {
					found = true
					break
				}
			}
			if !found {
				symbols = append(symbols, v.Name)
			}
		}
	}

	return symbols
}

// ExtractColumns is deprecated, use Symbols() instead
// Kept for backward compatibility
func (p *DataPattern) ExtractColumns() []Symbol {
	return p.Symbols()
}

// Query represents a Datalog query
type Query struct {
	Find    []FindElement   // Elements to return (variables or aggregates)
	In      []InputSpec     // Input specifications (database and parameters)
	Where   []Clause        // Clauses in WHERE (DataPattern, Predicate, Expression, Subquery)
	OrderBy []OrderByClause // Optional ordering of results
}

// InputSpec represents an input specification in the :in clause
type InputSpec interface {
	isInputSpec()
	String() string
}

// DatabaseInput represents the database input ($)
type DatabaseInput struct{}

func (d DatabaseInput) isInputSpec()   {}
func (d DatabaseInput) String() string { return "$" }

// ScalarInput represents a single value input (?x)
type ScalarInput struct {
	Symbol Symbol
}

func (s ScalarInput) isInputSpec()   {}
func (s ScalarInput) String() string { return s.Symbol.String() }

// CollectionInput represents a collection input [?x ...]
type CollectionInput struct {
	Symbol Symbol
}

func (c CollectionInput) isInputSpec()   {}
func (c CollectionInput) String() string { return "[" + c.Symbol.String() + " ...]" }

// TupleInput represents a tuple input [[?x ?y]]
type TupleInput struct {
	Symbols []Symbol
}

func (t TupleInput) isInputSpec() {}
func (t TupleInput) String() string {
	result := "[["
	for i, sym := range t.Symbols {
		if i > 0 {
			result += " "
		}
		result += sym.String()
	}
	result += "]]"
	return result
}

// RelationInput represents a relation input [[?x ?y] ...]
type RelationInput struct {
	Symbols []Symbol
}

func (r RelationInput) isInputSpec() {}
func (r RelationInput) String() string {
	result := "[["
	for i, sym := range r.Symbols {
		if i > 0 {
			result += " "
		}
		result += sym.String()
	}
	result += "] ...]"
	return result
}

// FindElement represents an element in the find clause
type FindElement interface {
	String() string
	IsAggregate() bool
}

// FindVariable is a simple variable in the find clause
type FindVariable struct {
	Symbol Symbol
}

func (f FindVariable) String() string {
	return f.Symbol.String()
}

func (f FindVariable) IsAggregate() bool {
	return false
}

// FindAggregate represents an aggregate function in the find clause
type FindAggregate struct {
	Function  string // "sum", "avg", "count", "min", "max"
	Arg       Symbol // Variable to aggregate
	Predicate Symbol // Optional: predicate variable for conditional aggregates (e.g., min-if, max-if)
}

// IsConditional returns true if this is a conditional aggregate (has a predicate)
func (f FindAggregate) IsConditional() bool {
	return f.Predicate != ""
}

func (f FindAggregate) String() string {
	// Note: Predicate field is for internal query rewriting only
	// Users never write conditional aggregate syntax explicitly
	return fmt.Sprintf("(%s %s)", f.Function, f.Arg)
}

func (f FindAggregate) IsAggregate() bool {
	return true
}

// String returns a string representation of the query
func (q Query) String() string {
	// Import cycle prevents using parser.FormatQuery directly
	// So we implement a simplified version here that matches the parser's format
	return q.formatWithIndent("")
}

// formatWithIndent formats the query with proper indentation
func (q Query) formatWithIndent(indent string) string {
	result := "[:find"
	for _, elem := range q.Find {
		result += " " + elem.String()
	}

	// Add :in clause if present
	if len(q.In) > 0 {
		result += "\n" + indent + " :in"
		for _, input := range q.In {
			result += " " + input.String()
		}
	}

	result += "\n" + indent + " :where"

	// Format patterns with proper indentation
	patternIndent := indent + "        " // 8 spaces to align with :where text
	for i, pattern := range q.Where {
		if i == 0 {
			result += " "
		} else {
			result += "\n" + patternIndent
		}

		// Special handling for subqueries to preserve formatting
		if subq, ok := pattern.(*SubqueryPattern); ok {
			result += subq.formatWithIndent(indent)
		} else {
			result += pattern.String()
		}
	}

	// Add :order-by clause if present
	if len(q.OrderBy) > 0 {
		result += "\n" + indent + " :order-by ["
		for i, clause := range q.OrderBy {
			if i > 0 {
				result += " "
			}
			result += clause.String()
		}
		result += "]"
	}

	result += "]"
	return result
}

// Result represents a query result tuple
type Result []interface{}

// ResultSet represents a set of query results
type ResultSet struct {
	Columns []Symbol // Column names (from Find clause)
	Rows    []Result // Result tuples
}

// ToMap converts a result row to a map using column names
func (rs ResultSet) ToMap(row int) map[Symbol]interface{} {
	if row < 0 || row >= len(rs.Rows) {
		return nil
	}
	result := make(map[Symbol]interface{})
	for i, col := range rs.Columns {
		if i < len(rs.Rows[row]) {
			result[col] = rs.Rows[row][i]
		}
	}
	return result
}

// Relation represents an intermediate relation during query execution
type Relation struct {
	Columns []Symbol
	Tuples  [][]interface{}
}

// IsEmpty returns true if the relation has no tuples
func (r Relation) IsEmpty() bool {
	return len(r.Tuples) == 0
}

// Size returns the number of tuples
func (r Relation) Size() int {
	return len(r.Tuples)
}

// ColumnIndex returns the index of a column, or -1 if not found
func (r Relation) ColumnIndex(col Symbol) int {
	for i, c := range r.Columns {
		if c == col {
			return i
		}
	}
	return -1
}

// CommonColumns returns the columns that appear in both relations
func (r Relation) CommonColumns(other Relation) []Symbol {
	common := []Symbol{}
	for _, col := range r.Columns {
		if other.ColumnIndex(col) >= 0 {
			common = append(common, col)
		}
	}
	return common
}

// TimeRange represents a time range for temporal queries
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// TxInstant represents a specific transaction instant
type TxInstant uint64

// TxLatest represents a query for only the latest values
type TxLatest struct{}

// OrderByClause represents a single ordering specification
type OrderByClause struct {
	Variable  Symbol
	Direction OrderDirection
}

// OrderDirection specifies ascending or descending order
type OrderDirection string

const (
	OrderAsc  OrderDirection = "asc"
	OrderDesc OrderDirection = "desc"
)

// String returns the string representation of an OrderByClause
func (o OrderByClause) String() string {
	if o.Direction == "" || o.Direction == OrderAsc {
		return string(o.Variable)
	}
	return fmt.Sprintf("[%s :%s]", o.Variable, o.Direction)
}

// DatomToTuple converts a datom to a tuple based on the pattern and requested columns.
// This is used by both executor and storage packages to extract values from datoms
// in the order specified by the columns.
func DatomToTuple(datom datalog.Datom, pattern *DataPattern, columns []Symbol) Tuple {
	if len(columns) == 0 {
		return nil
	}

	// Build column to value mapping
	values := make(map[Symbol]interface{})

	// Map E position
	if v, ok := pattern.GetE().(Variable); ok {
		values[v.Name] = datom.E
	}

	// Map A position
	if v, ok := pattern.GetA().(Variable); ok {
		values[v.Name] = datom.A
	}

	// Map V position
	if v, ok := pattern.GetV().(Variable); ok {
		values[v.Name] = datom.V
	}

	// Map T position
	if len(pattern.Elements) > 3 {
		if v, ok := pattern.GetT().(Variable); ok {
			values[v.Name] = datom.Tx
		}
	}

	// Build tuple in column order
	tuple := make(Tuple, len(columns))
	for i, col := range columns {
		if val, found := values[col]; found {
			tuple[i] = val
		}
	}

	return tuple
}
