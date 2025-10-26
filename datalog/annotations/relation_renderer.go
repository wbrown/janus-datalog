package annotations

import (
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// RelationInfo represents the basic info about a relation for rendering
type RelationInfo struct {
	Attrs      []query.Symbol
	TupleCount int
}

// RelationRenderer provides pretty-printing for relations
type RelationRenderer struct {
	useColor bool
}

// NewRelationRenderer creates a new relation renderer
func NewRelationRenderer(useColor bool) *RelationRenderer {
	return &RelationRenderer{useColor: useColor}
}

// RenderRelation renders a single relation as a string
func (r *RelationRenderer) RenderRelation(rel RelationInfo) string {
	// Build attribute list
	attrStrs := make([]string, len(rel.Attrs))
	for i, attr := range rel.Attrs {
		attrStrs[i] = string(attr)
	}
	attrList := strings.Join(attrStrs, " ")

	if r.useColor {
		return fmt.Sprintf("%s%s%s%s%s%s",
			color.BlueString("Relation(["),
			color.CyanString(attrList),
			color.BlueString("]"),
			color.BlueString(", "),
			r.colorizeCount("Tuples", rel.TupleCount),
			color.BlueString(")"))
	}

	return fmt.Sprintf("Relation([%s], %d Tuples)", attrList, rel.TupleCount)
}

// RenderRelations renders multiple relations as a string
func (r *RelationRenderer) RenderRelations(rels []RelationInfo) string {
	parts := make([]string, len(rels))
	for i, rel := range rels {
		parts[i] = r.RenderRelation(rel)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// RenderRelationWithAttrs renders with just attrs and tuple count
func (r *RelationRenderer) RenderRelationWithAttrs(attrs []string, tupleCount int) string {
	attrList := strings.Join(attrs, " ")

	if r.useColor {
		result := fmt.Sprintf("%s%s%s",
			color.BlueString("Relation(["),
			color.CyanString(attrList),
			color.BlueString("]"))

		if tupleCount >= 0 {
			result += fmt.Sprintf("%s%s%s",
				color.BlueString(", "),
				r.colorizeCount("Tuples", tupleCount),
				color.BlueString(")"))
		} else {
			result += color.BlueString(")")
		}
		return result
	}

	if tupleCount >= 0 {
		return fmt.Sprintf("Relation([%s], %d Tuples)", attrList, tupleCount)
	}
	return fmt.Sprintf("Relation([%s])", attrList)
}

// colorizeCount formats a count with color based on size
func (r *RelationRenderer) colorizeCount(label string, count int) string {
	if !r.useColor {
		return fmt.Sprintf("%d %s", count, label)
	}

	countStr := fmt.Sprintf("%d", count)

	// Color based on size
	switch {
	case count == 0:
		countStr = color.RedString(countStr)
	case count < 100:
		countStr = color.GreenString(countStr)
	case count < 10000:
		countStr = color.YellowString(countStr)
	default:
		countStr = color.RedString(countStr)
	}

	return fmt.Sprintf("%s %s", countStr, label)
}

// RenderJoin renders a join operation
func (r *RelationRenderer) RenderJoin(leftAttrs []string, leftCount int, rightAttrs []string, rightCount int, resultAttrs []string, resultCount int) string {
	left := r.RenderRelationWithAttrs(leftAttrs, leftCount)
	right := r.RenderRelationWithAttrs(rightAttrs, rightCount)
	result := r.RenderRelationWithAttrs(resultAttrs, resultCount)

	joinOp := " × "
	if r.useColor {
		joinOp = color.YellowString(" × ")
	}

	return fmt.Sprintf("%s%s%s → %s", left, joinOp, right, result)
}

// RenderQuery renders a query with color
func (r *RelationRenderer) RenderQuery(queryStr string) []string {
	lines := strings.Split(queryStr, "\n")
	if len(lines) == 0 {
		return lines
	}

	// First line with "Query: " prefix
	result := []string{}
	if r.useColor {
		result = append(result, color.BlueString("Query: ")+lines[0])
	} else {
		result = append(result, "Query: "+lines[0])
	}

	// Remaining lines indented
	for i := 1; i < len(lines); i++ {
		result = append(result, "       "+lines[i])
	}

	return result
}
