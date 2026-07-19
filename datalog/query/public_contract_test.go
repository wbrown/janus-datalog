package query_test

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

var (
	_ query.Function = query.ArithmeticFunction{
		Op: datalog.SymAdd,
		Args: []query.Term{
			query.ConstantTerm{Value: int64(1)},
			query.ConstantTerm{Value: int64(2)},
		},
	}
	_ query.EntityLookup                             = (*executor.SourceRouter)(nil)
	_ func(*query.DataPattern) *query.Query          = query.PatternQuery
	_ func(*query.Query) (*query.DataPattern, error) = (*query.Query).SingleDataPattern
	_ datalog.Value                                  = int64(1)
)
