package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A group sharing a variable with the pattern that reports no size is read
// in windows; a group held in memory, and a group sharing no variable with
// the pattern, bind the pattern through the matcher.
func TestStreamsThroughReportsAGroupSharingAVariableThatReportsNoSize(t *testing.T) {
	entity, value, other := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?other")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity}, query.Constant{Value: datalog.NewKeyword(":t/a")}, query.Variable{Name: value},
	}}
	held := NewMaterializedRelation([]query.Symbol{entity}, nil)
	streaming := NewStreamingRelation([]query.Symbol{entity}, NewMaterializedRelation([]query.Symbol{entity}, nil).Iterator())
	unrelated := NewStreamingRelation([]query.Symbol{other}, NewMaterializedRelation([]query.Symbol{other}, nil).Iterator())
	assert.True(t, streamsThrough(pattern.Symbols(), Relations{held, streaming}), "a streaming group sharing the entity")
	assert.False(t, streamsThrough(pattern.Symbols(), Relations{held}), "a group held in memory")
	assert.False(t, streamsThrough(pattern.Symbols(), Relations{unrelated}), "a streaming group sharing no variable")
}
