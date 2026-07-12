package query

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// FormatValueEDN formats a Go value as a parseable EDN string.
// This is the canonical way to render values in query String() methods
// so that the output round-trips through the parser.
func FormatValueEDN(v interface{}) string {
	switch val := v.(type) {
	case datalog.Keyword:
		return val.String()
	case datalog.Identity:
		return `#identity "` + val.String() + `"`
	case string:
		var sb strings.Builder
		sb.WriteRune('"')
		for _, r := range val {
			switch r {
			case '"':
				sb.WriteString(`\"`)
			case '\\':
				sb.WriteString(`\\`)
			case '\n':
				sb.WriteString(`\n`)
			case '\r':
				sb.WriteString(`\r`)
			case '\t':
				sb.WriteString(`\t`)
			default:
				sb.WriteRune(r)
			}
		}
		sb.WriteRune('"')
		return sb.String()
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'g', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case time.Time:
		return `#inst "` + val.UTC().Format(time.RFC3339Nano) + `"`
	case []byte:
		return fmt.Sprintf("#bytes \"%x\"", val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Tuple represents a tuple of values in a relation
// It's used throughout the executor and storage layers for query results
type Tuple []interface{}

// Symbol is an interned pointer type for query variables, source names, etc.
// Alias for datalog.Symbol — construction via datalog.NewSymbol("?x").
type Symbol = datalog.Symbol

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
func (c Constant) String() string   { return FormatValueEDN(c.Value) }

// VectorConstant represents a vector of constant values in a pattern
// Used for tuple ground: [(ground [1 2 3]) [?a ?b ?c]]
type VectorConstant struct {
	Values []interface{}
}

func (v VectorConstant) IsVariable() bool { return false }
func (v VectorConstant) IsBlank() bool    { return false }
func (v VectorConstant) String() string {
	result := "["
	for i, val := range v.Values {
		if i > 0 {
			result += " "
		}
		result += FormatValueEDN(val)
	}
	result += "]"
	return result
}

// Pattern represents a single pattern in a where clause
type Pattern interface {
	String() string
}

// DataPattern represents a data pattern [e a v] or [e a v t].
// For multi-source queries, Source identifies which data source to query
// (e.g., Symbol("$users")). Empty Source means the default source ($).
type DataPattern struct {
	Source   Symbol // Source identifier (e.g., "$users"); empty for default
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

// TupleBinding binds a single tuple: [[?a ?b]]
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

// CollectionBinding binds a single symbol from all tuples: [?coll ...]
// This collects all values from a single-symbol result into a collection.
type CollectionBinding struct {
	Variable Symbol
}

func (c CollectionBinding) isBindingForm() {}
func (c CollectionBinding) String() string {
	return "[" + c.Variable.String() + " ...]"
}

// ScalarBinding binds a single value to a single variable: ?var
// Expects exactly one tuple with one symbol. Used with scalar find spec (.:find ... .)
type ScalarBinding struct {
	Variable Symbol
}

func (s ScalarBinding) isBindingForm() {}
func (s ScalarBinding) String() string {
	return s.Variable.String()
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
	if p.Source != nil {
		result += p.Source.String() + " "
	}
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
	Find         []FindElement   // Elements to return (variables or aggregates)
	In           []InputSpec     // Input specifications (database and parameters)
	Where        []Clause        // Clauses in WHERE (DataPattern, Predicate, Expression, Subquery)
	OrderBy      []OrderByClause // Optional ordering of results
	Limit        *int            // Optional cap on result rows (nil = unbounded); applied after OrderBy and aggregation
	ScalarReturn bool            // If true, return scalar value (find spec ends with .)
}

// SingleDataPattern returns the sole data pattern in a physical query fragment.
// PatternMatcher implementations use this invariant while receiving the full
// Datalog fragment so they can inspect safe ordering and limit requirements.
func (q *Query) SingleDataPattern() (*DataPattern, error) {
	if q == nil || len(q.Where) != 1 {
		count := 0
		if q != nil {
			count = len(q.Where)
		}
		return nil, fmt.Errorf("pattern query requires exactly one where clause, got %d", count)
	}
	pattern, ok := q.Where[0].(*DataPattern)
	if !ok {
		return nil, fmt.Errorf("pattern query requires a DataPattern, got %T", q.Where[0])
	}
	return pattern, nil
}

// PatternQuery wraps a DataPattern as the minimal Datalog query fragment
// accepted by PatternMatcher.
func PatternQuery(pattern *DataPattern) *Query {
	return &Query{Where: []Clause{pattern}}
}

// InputSpec represents an input specification in the :in clause
type InputSpec interface {
	isInputSpec()
	String() string
}

// DatabaseInput represents a database input ($ or $name)
// For single-source queries, Name is Symbol("$").
// For multi-source queries, Name identifies the source (e.g., Symbol("$users")).
type DatabaseInput struct {
	Name Symbol
}

func (d DatabaseInput) isInputSpec()   {}
func (d DatabaseInput) String() string { return d.Name.String() }

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
	return f.Predicate != nil
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

	// Add :limit clause if present
	if q.Limit != nil {
		result += "\n" + indent + " :limit " + strconv.Itoa(*q.Limit)
	}

	result += "]"
	return result
}

// Result represents a query result tuple
type Result []interface{}

// ResultSet represents a set of query results
type ResultSet struct {
	Symbols []Symbol // Symbol names (from Find clause)
	Tuples  []Result // Result tuples
}

// ToMap converts a result tuple to a map using symbol names
func (rs ResultSet) ToMap(tupleIdx int) map[Symbol]interface{} {
	if tupleIdx < 0 || tupleIdx >= len(rs.Tuples) {
		return nil
	}
	result := make(map[Symbol]interface{})
	for i, sym := range rs.Symbols {
		if i < len(rs.Tuples[tupleIdx]) {
			result[sym] = rs.Tuples[tupleIdx][i]
		}
	}
	return result
}

// Relation represents an intermediate relation during query execution
type Relation struct {
	Symbols []Symbol
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

// SymbolIndex returns the index of a symbol, or -1 if not found
func (r Relation) SymbolIndex(sym Symbol) int {
	for i, s := range r.Symbols {
		if s == sym {
			return i
		}
	}
	return -1
}

// CommonSymbols returns the symbols that appear in both relations
func (r Relation) CommonSymbols(other Relation) []Symbol {
	common := []Symbol{}
	for _, sym := range r.Symbols {
		if other.SymbolIndex(sym) >= 0 {
			common = append(common, sym)
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
		return o.Variable.String()
	}
	return fmt.Sprintf("[%s :%s]", o.Variable, o.Direction)
}

// DatomToTuple converts a datom to a tuple based on the pattern and requested symbols.
// This is used by both executor and storage packages to extract values from datoms
// in the order specified by the symbols.
func DatomToTuple(datom datalog.Datom, pattern *DataPattern, symbols []Symbol) Tuple {
	if len(symbols) == 0 {
		return nil
	}

	// Build symbol to value mapping
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

	// Build tuple in symbol order
	tuple := make(Tuple, len(symbols))
	for i, sym := range symbols {
		if val, found := values[sym]; found {
			tuple[i] = val
		}
	}

	return tuple
}
