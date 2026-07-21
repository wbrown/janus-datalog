package executor

import (
	"github.com/wbrown/janus-datalog/datalog/query"
)

// RegisterCustomFunction registers a user-defined function for use in
// queries: predicate position ([(my/fn ?x)]) and expression position
// ([(my/fn ?x) ?y]). The registry lives with the function namespace in
// query.DefaultRegistry; register before parsing queries that use the name.
func RegisterCustomFunction(name string, fn func([]interface{}) (interface{}, error)) {
	query.DefaultRegistry.RegisterImplementation(name, fn)
}

// CallCustomFunction calls a registered user-defined function if it exists.
func CallCustomFunction(name string, args []interface{}) (interface{}, bool, error) {
	fn, ok := query.DefaultRegistry.Implementation(name)
	if !ok {
		return nil, false, nil
	}
	result, err := fn(args)
	return result, true, err
}
