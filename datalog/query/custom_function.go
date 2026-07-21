package query

import (
	"fmt"
	"strings"

	"github.com/wbrown/janus-datalog/datalog"
)

// CustomFunction invokes a user-defined function in expression position:
// [(my/fn ?x "lit") ?y]. Eval consults DefaultRegistry for the registered
// implementation (RegisterImplementation); the parser constructs this form
// only for names with a registered implementation, and an unregistered name
// at evaluation time (registration raced or bypassed the parser) errors
// loudly. Results are normalized into the engine's canonical value types —
// a registered function returning a Go int must not diverge from stored
// int64 data.
type CustomFunction struct {
	Fn   string
	Args []Term
}

func (c CustomFunction) RequiredSymbols() []Symbol {
	var symbols []Symbol
	for _, argument := range c.Args {
		symbols = append(symbols, argument.RequiredSymbols()...)
	}
	return symbols
}

func (c CustomFunction) Eval(bindings map[Symbol]interface{}) (interface{}, error) {
	impl, ok := DefaultRegistry.Implementation(c.Fn)
	if !ok {
		return nil, fmt.Errorf("unsupported function: %s", c.Fn)
	}
	args := make([]interface{}, len(c.Args))
	for i, argument := range c.Args {
		value, resolved := argument.Resolve(bindings)
		if !resolved {
			return nil, fmt.Errorf("function %s: cannot resolve argument %s", c.Fn, argument)
		}
		args[i] = value
	}
	result, err := impl(args)
	if err != nil {
		return nil, fmt.Errorf("function %s: %w", c.Fn, err)
	}
	return datalog.NormalizeValue(result), nil
}

func (c CustomFunction) String() string {
	parts := make([]string, len(c.Args))
	for i, argument := range c.Args {
		parts[i] = argument.String()
	}
	return fmt.Sprintf("(%s %s)", c.Fn, strings.Join(parts, " "))
}

func (c CustomFunction) ReturnType() string {
	return "any"
}
