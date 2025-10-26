package executor

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// customFunctions is a registry for custom functions (mainly for testing)
var customFunctions = make(map[string]func([]interface{}) (interface{}, error))
var customFuncMutex sync.RWMutex

// RegisterCustomFunction registers a custom function for use in expressions
func RegisterCustomFunction(name string, fn func([]interface{}) (interface{}, error)) {
	customFuncMutex.Lock()
	defer customFuncMutex.Unlock()
	customFunctions[name] = fn
}

// CallCustomFunction calls a custom function if it exists
func CallCustomFunction(name string, args []interface{}) (interface{}, bool, error) {
	customFuncMutex.RLock()
	defer customFuncMutex.RUnlock()

	if fn, ok := customFunctions[name]; ok {
		result, err := fn(args)
		return result, true, err
	}
	return nil, false, nil
}

// addValues adds numeric values (variadic)
func addValues(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("+ requires at least one argument")
	}

	// Check if all args are integers
	allInts := true
	for _, arg := range args {
		switch arg.(type) {
		case int, int64:
			// still int
		case float64:
			allInts = false
		default:
			return nil, fmt.Errorf("+ requires numeric arguments, got %T", arg)
		}
	}

	if allInts {
		var sum int64
		for _, arg := range args {
			switch v := arg.(type) {
			case int:
				sum += int64(v)
			case int64:
				sum += v
			}
		}
		return sum, nil
	} else {
		var sum float64
		for _, arg := range args {
			switch v := arg.(type) {
			case int:
				sum += float64(v)
			case int64:
				sum += float64(v)
			case float64:
				sum += v
			}
		}
		return sum, nil
	}
}

// subtractValues subtracts numeric values (variadic: a - b - c - ...)
func subtractValues(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("- requires at least one argument")
	}

	// Check if all args are integers
	allInts := true
	for _, arg := range args {
		switch arg.(type) {
		case int, int64:
			// still int
		case float64:
			allInts = false
		default:
			return nil, fmt.Errorf("- requires numeric arguments, got %T", arg)
		}
	}

	if allInts {
		var result int64
		for i, arg := range args {
			switch v := arg.(type) {
			case int:
				if i == 0 {
					result = int64(v)
				} else {
					result -= int64(v)
				}
			case int64:
				if i == 0 {
					result = v
				} else {
					result -= v
				}
			}
		}
		return result, nil
	} else {
		var result float64
		for i, arg := range args {
			switch v := arg.(type) {
			case int:
				if i == 0 {
					result = float64(v)
				} else {
					result -= float64(v)
				}
			case int64:
				if i == 0 {
					result = float64(v)
				} else {
					result -= float64(v)
				}
			case float64:
				if i == 0 {
					result = v
				} else {
					result -= v
				}
			}
		}
		return result, nil
	}
}

// multiplyValues multiplies numeric values (variadic)
func multiplyValues(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("* requires at least one argument")
	}

	// Check if all args are integers
	allInts := true
	for _, arg := range args {
		switch arg.(type) {
		case int, int64:
			// still int
		case float64:
			allInts = false
		default:
			return nil, fmt.Errorf("* requires numeric arguments, got %T", arg)
		}
	}

	if allInts {
		var product int64 = 1
		for _, arg := range args {
			switch v := arg.(type) {
			case int:
				product *= int64(v)
			case int64:
				product *= v
			}
		}
		return product, nil
	} else {
		var product float64 = 1.0
		for _, arg := range args {
			switch v := arg.(type) {
			case int:
				product *= float64(v)
			case int64:
				product *= float64(v)
			case float64:
				product *= v
			}
		}
		return product, nil
	}
}

// divideValues divides two numeric values
func divideValues(a, b interface{}) (interface{}, error) {
	// Convert to float64 for division
	var aFloat, bFloat float64

	switch v := a.(type) {
	case int:
		aFloat = float64(v)
	case int64:
		aFloat = float64(v)
	case float64:
		aFloat = v
	default:
		return nil, fmt.Errorf("/ requires numeric arguments, got %T", a)
	}

	switch v := b.(type) {
	case int:
		bFloat = float64(v)
	case int64:
		bFloat = float64(v)
	case float64:
		bFloat = v
	default:
		return nil, fmt.Errorf("/ requires numeric arguments, got %T", b)
	}

	if bFloat == 0 {
		return nil, fmt.Errorf("division by zero")
	}

	return aFloat / bFloat, nil
}

// concatenateStrings concatenates string values
func concatenateStrings(args []interface{}) string {
	parts := make([]string, len(args))
	for i, arg := range args {
		parts[i] = fmt.Sprintf("%v", arg)
	}
	return strings.Join(parts, "")
}

// extractTimeComponent extracts a component from a time value
func extractTimeComponent(component string, value interface{}) (interface{}, error) {
	t, ok := value.(time.Time)
	if !ok {
		return nil, fmt.Errorf("%s requires a time argument, got %T", component, value)
	}

	switch component {
	case "year":
		return int64(t.Year()), nil
	case "month":
		return int64(t.Month()), nil
	case "day":
		return int64(t.Day()), nil
	case "hour":
		return int64(t.Hour()), nil
	case "minute":
		return int64(t.Minute()), nil
	case "second":
		return int64(t.Second()), nil
	default:
		return nil, fmt.Errorf("unknown time component: %s", component)
	}
}
