package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// Helper to create bindings
func makeBindings(pairs ...interface{}) map[Symbol]interface{} {
	if len(pairs)%2 != 0 {
		panic("makeBindings requires key-value pairs")
	}
	bindings := make(map[Symbol]interface{})
	for i := 0; i < len(pairs); i += 2 {
		key := datalog.NewSymbol(pairs[i].(string))
		bindings[key] = pairs[i+1]
	}
	return bindings
}

// TestNth tests the nth function
func TestNth(t *testing.T) {
	vec := []interface{}{"a", "b", "c", "d", "e"}

	t.Run("valid index", func(t *testing.T) {
		fn := NthFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			IndexTerm: VariableTerm{Symbol: datalog.NewSymbol("?idx")},
		}
		bindings := makeBindings("?vec", vec, "?idx", int64(2))

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, "c", result)
	})

	t.Run("first element", func(t *testing.T) {
		fn := NthFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			IndexTerm: ConstantTerm{Value: int64(0)},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, "a", result)
	})

	t.Run("last element", func(t *testing.T) {
		fn := NthFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			IndexTerm: ConstantTerm{Value: int64(4)},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, "e", result)
	})
}

func TestNthOutOfBounds(t *testing.T) {
	vec := []interface{}{"a", "b", "c"}

	t.Run("negative index", func(t *testing.T) {
		fn := NthFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			IndexTerm: ConstantTerm{Value: int64(-1)},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Nil(t, result, "negative index should return nil")
	})

	t.Run("index equals length", func(t *testing.T) {
		fn := NthFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			IndexTerm: ConstantTerm{Value: int64(3)},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Nil(t, result, "index == length should return nil")
	})

	t.Run("index exceeds length", func(t *testing.T) {
		fn := NthFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			IndexTerm: ConstantTerm{Value: int64(100)},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Nil(t, result, "index > length should return nil")
	})
}

// TestContains tests the contains? function
func TestContains(t *testing.T) {
	vec := []interface{}{"warrior", "mage", "archer"}

	t.Run("finds existing value", func(t *testing.T) {
		fn := ContainsFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			ValueTerm: ConstantTerm{Value: "mage"},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})

	t.Run("finds first element", func(t *testing.T) {
		fn := ContainsFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			ValueTerm: ConstantTerm{Value: "warrior"},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})

	t.Run("finds last element", func(t *testing.T) {
		fn := ContainsFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			ValueTerm: ConstantTerm{Value: "archer"},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, true, result)
	})
}

func TestContainsNotFound(t *testing.T) {
	vec := []interface{}{"warrior", "mage", "archer"}

	t.Run("value not in vector", func(t *testing.T) {
		fn := ContainsFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			ValueTerm: ConstantTerm{Value: "healer"},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, false, result)
	})

	t.Run("empty vector", func(t *testing.T) {
		fn := ContainsFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			ValueTerm: ConstantTerm{Value: "anything"},
		}
		bindings := makeBindings("?vec", []interface{}{})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, false, result)
	})
}

// TestVectorLength tests the length function
func TestVectorLength(t *testing.T) {
	t.Run("non-empty vector", func(t *testing.T) {
		fn := LengthFunction{
			VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
		}
		bindings := makeBindings("?vec", []interface{}{"a", "b", "c", "d", "e"})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, int64(5), result)
	})

	t.Run("single element", func(t *testing.T) {
		fn := LengthFunction{
			VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
		}
		bindings := makeBindings("?vec", []interface{}{"only"})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, int64(1), result)
	})
}

func TestVectorLengthEmpty(t *testing.T) {
	fn := LengthFunction{
		VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
	}
	bindings := makeBindings("?vec", []interface{}{})

	result, err := fn.Eval(bindings)
	require.NoError(t, err)
	assert.Equal(t, int64(0), result)
}

// TestFirst tests the first function
func TestFirst(t *testing.T) {
	t.Run("non-empty vector", func(t *testing.T) {
		fn := FirstFunction{
			VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
		}
		bindings := makeBindings("?vec", []interface{}{"alpha", "beta", "gamma"})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, "alpha", result)
	})

	t.Run("single element", func(t *testing.T) {
		fn := FirstFunction{
			VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
		}
		bindings := makeBindings("?vec", []interface{}{"only"})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, "only", result)
	})
}

func TestFirstEmpty(t *testing.T) {
	fn := FirstFunction{
		VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
	}
	bindings := makeBindings("?vec", []interface{}{})

	result, err := fn.Eval(bindings)
	require.NoError(t, err)
	assert.Nil(t, result, "first of empty vector should be nil")
}

// TestLast tests the last function
func TestLast(t *testing.T) {
	t.Run("non-empty vector", func(t *testing.T) {
		fn := LastFunction{
			VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
		}
		bindings := makeBindings("?vec", []interface{}{"alpha", "beta", "gamma"})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, "gamma", result)
	})

	t.Run("single element", func(t *testing.T) {
		fn := LastFunction{
			VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
		}
		bindings := makeBindings("?vec", []interface{}{"only"})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, "only", result)
	})
}

func TestLastEmpty(t *testing.T) {
	fn := LastFunction{
		VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
	}
	bindings := makeBindings("?vec", []interface{}{})

	result, err := fn.Eval(bindings)
	require.NoError(t, err)
	assert.Nil(t, result, "last of empty vector should be nil")
}

// TestIndexOf tests the index-of function
func TestIndexOf(t *testing.T) {
	vec := []interface{}{"a", "b", "c", "d", "e"}

	t.Run("finds first element", func(t *testing.T) {
		fn := IndexOfFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			ValueTerm: ConstantTerm{Value: "a"},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, int64(0), result)
	})

	t.Run("finds middle element", func(t *testing.T) {
		fn := IndexOfFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			ValueTerm: ConstantTerm{Value: "c"},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, int64(2), result)
	})

	t.Run("finds last element", func(t *testing.T) {
		fn := IndexOfFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			ValueTerm: ConstantTerm{Value: "e"},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, int64(4), result)
	})
}

func TestIndexOfNotFound(t *testing.T) {
	t.Run("value not in vector", func(t *testing.T) {
		fn := IndexOfFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			ValueTerm: ConstantTerm{Value: "z"},
		}
		bindings := makeBindings("?vec", []interface{}{"a", "b", "c"})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), result)
	})

	t.Run("empty vector", func(t *testing.T) {
		fn := IndexOfFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			ValueTerm: ConstantTerm{Value: "anything"},
		}
		bindings := makeBindings("?vec", []interface{}{})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), result)
	})
}

// TestSubvec tests the subvec function
func TestSubvec(t *testing.T) {
	vec := []interface{}{"a", "b", "c", "d", "e"}

	t.Run("middle slice", func(t *testing.T) {
		fn := SubvecFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			StartTerm: ConstantTerm{Value: int64(1)},
			EndTerm:   ConstantTerm{Value: int64(4)},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{"b", "c", "d"}, result)
	})

	t.Run("from start", func(t *testing.T) {
		fn := SubvecFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			StartTerm: ConstantTerm{Value: int64(0)},
			EndTerm:   ConstantTerm{Value: int64(3)},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{"a", "b", "c"}, result)
	})

	t.Run("to end", func(t *testing.T) {
		fn := SubvecFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			StartTerm: ConstantTerm{Value: int64(2)},
			EndTerm:   ConstantTerm{Value: int64(5)},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{"c", "d", "e"}, result)
	})

	t.Run("single element", func(t *testing.T) {
		fn := SubvecFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			StartTerm: ConstantTerm{Value: int64(2)},
			EndTerm:   ConstantTerm{Value: int64(3)},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{"c"}, result)
	})
}

func TestSubvecBounds(t *testing.T) {
	vec := []interface{}{"a", "b", "c"}

	t.Run("negative start clamped", func(t *testing.T) {
		fn := SubvecFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			StartTerm: ConstantTerm{Value: int64(-5)},
			EndTerm:   ConstantTerm{Value: int64(2)},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{"a", "b"}, result, "negative start should clamp to 0")
	})

	t.Run("end exceeds length clamped", func(t *testing.T) {
		fn := SubvecFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			StartTerm: ConstantTerm{Value: int64(1)},
			EndTerm:   ConstantTerm{Value: int64(100)},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{"b", "c"}, result, "end should clamp to length")
	})

	t.Run("start >= end returns empty", func(t *testing.T) {
		fn := SubvecFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			StartTerm: ConstantTerm{Value: int64(2)},
			EndTerm:   ConstantTerm{Value: int64(1)},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{}, result, "start >= end should return empty")
	})

	t.Run("start == end returns empty", func(t *testing.T) {
		fn := SubvecFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			StartTerm: ConstantTerm{Value: int64(2)},
			EndTerm:   ConstantTerm{Value: int64(2)},
		}
		bindings := makeBindings("?vec", vec)

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{}, result, "start == end should return empty")
	})

	t.Run("empty vector", func(t *testing.T) {
		fn := SubvecFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			StartTerm: ConstantTerm{Value: int64(0)},
			EndTerm:   ConstantTerm{Value: int64(10)},
		}
		bindings := makeBindings("?vec", []interface{}{})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{}, result, "empty vec subvec should return empty")
	})
}

// TestEnumerate tests the enumerate function
func TestEnumerate(t *testing.T) {
	t.Run("non-empty vector", func(t *testing.T) {
		fn := EnumerateFunction{
			VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
		}
		bindings := makeBindings("?vec", []interface{}{"a", "b", "c"})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)

		tuples, ok := result.([][]interface{})
		require.True(t, ok, "enumerate should return [][]interface{}")
		require.Len(t, tuples, 3)

		assert.Equal(t, []interface{}{int64(0), "a"}, tuples[0])
		assert.Equal(t, []interface{}{int64(1), "b"}, tuples[1])
		assert.Equal(t, []interface{}{int64(2), "c"}, tuples[2])
	})

	t.Run("single element", func(t *testing.T) {
		fn := EnumerateFunction{
			VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
		}
		bindings := makeBindings("?vec", []interface{}{"only"})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)

		tuples, ok := result.([][]interface{})
		require.True(t, ok)
		require.Len(t, tuples, 1)
		assert.Equal(t, []interface{}{int64(0), "only"}, tuples[0])
	})
}

func TestEnumerateEmpty(t *testing.T) {
	fn := EnumerateFunction{
		VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
	}
	bindings := makeBindings("?vec", []interface{}{})

	result, err := fn.Eval(bindings)
	require.NoError(t, err)

	tuples, ok := result.([][]interface{})
	require.True(t, ok)
	assert.Len(t, tuples, 0, "enumerate of empty vector should return empty slice")
}

// Test error handling
func TestVectorFunctionErrors(t *testing.T) {
	t.Run("nth with non-vector", func(t *testing.T) {
		fn := NthFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			IndexTerm: ConstantTerm{Value: int64(0)},
		}
		bindings := makeBindings("?vec", "not a vector")

		_, err := fn.Eval(bindings)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be a vector")
	})

	t.Run("contains? with non-vector", func(t *testing.T) {
		fn := ContainsFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			ValueTerm: ConstantTerm{Value: "x"},
		}
		bindings := makeBindings("?vec", 42)

		_, err := fn.Eval(bindings)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be a vector")
	})

	t.Run("unbound variable", func(t *testing.T) {
		fn := FirstFunction{
			VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?unbound")},
		}
		bindings := makeBindings("?other", []interface{}{"a"})

		_, err := fn.Eval(bindings)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot resolve")
	})
}

// Test type coercion for different slice types
func TestVectorTypeCoercion(t *testing.T) {
	t.Run("[]string", func(t *testing.T) {
		fn := LengthFunction{
			VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
		}
		bindings := makeBindings("?vec", []string{"a", "b", "c"})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, int64(3), result)
	})

	t.Run("[]int64", func(t *testing.T) {
		fn := FirstFunction{
			VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
		}
		bindings := makeBindings("?vec", []int64{10, 20, 30})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, int64(10), result)
	})

	t.Run("[]float64", func(t *testing.T) {
		fn := LastFunction{
			VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
		}
		bindings := makeBindings("?vec", []float64{1.1, 2.2, 3.3})

		result, err := fn.Eval(bindings)
		require.NoError(t, err)
		assert.Equal(t, float64(3.3), result)
	})
}

// Test String() methods for debugging
func TestVectorFunctionStrings(t *testing.T) {
	t.Run("nth", func(t *testing.T) {
		fn := NthFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			IndexTerm: ConstantTerm{Value: int64(2)},
		}
		assert.Equal(t, "(nth ?vec 2)", fn.String())
	})

	t.Run("contains?", func(t *testing.T) {
		fn := ContainsFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			ValueTerm: ConstantTerm{Value: "x"},
		}
		assert.Equal(t, "(contains? ?vec x)", fn.String())
	})

	t.Run("subvec", func(t *testing.T) {
		fn := SubvecFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			StartTerm: ConstantTerm{Value: int64(1)},
			EndTerm:   ConstantTerm{Value: int64(4)},
		}
		assert.Equal(t, "(subvec ?vec 1 4)", fn.String())
	})

	t.Run("enumerate", func(t *testing.T) {
		fn := EnumerateFunction{
			VecTerm: VariableTerm{Symbol: datalog.NewSymbol("?vec")},
		}
		assert.Equal(t, "(enumerate ?vec)", fn.String())
	})
}

// Test RequiredSymbols methods
func TestVectorFunctionRequiredSymbols(t *testing.T) {
	t.Run("nth requires both args", func(t *testing.T) {
		fn := NthFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			IndexTerm: VariableTerm{Symbol: datalog.NewSymbol("?idx")},
		}
		symbols := fn.RequiredSymbols()
		assert.Contains(t, symbols, datalog.NewSymbol("?vec"))
		assert.Contains(t, symbols, datalog.NewSymbol("?idx"))
	})

	t.Run("nth with constant index", func(t *testing.T) {
		fn := NthFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			IndexTerm: ConstantTerm{Value: int64(0)},
		}
		symbols := fn.RequiredSymbols()
		assert.Contains(t, symbols, datalog.NewSymbol("?vec"))
		assert.Len(t, symbols, 1, "constant should not add required symbols")
	})

	t.Run("subvec requires vec and indices", func(t *testing.T) {
		fn := SubvecFunction{
			VecTerm:   VariableTerm{Symbol: datalog.NewSymbol("?vec")},
			StartTerm: VariableTerm{Symbol: datalog.NewSymbol("?start")},
			EndTerm:   VariableTerm{Symbol: datalog.NewSymbol("?end")},
		}
		symbols := fn.RequiredSymbols()
		assert.Contains(t, symbols, datalog.NewSymbol("?vec"))
		assert.Contains(t, symbols, datalog.NewSymbol("?start"))
		assert.Contains(t, symbols, datalog.NewSymbol("?end"))
	})
}

// Test ReturnType methods
func TestVectorFunctionReturnTypes(t *testing.T) {
	assert.Equal(t, "any", NthFunction{}.ReturnType())
	assert.Equal(t, "any", FirstFunction{}.ReturnType())
	assert.Equal(t, "any", LastFunction{}.ReturnType())
	assert.Equal(t, "number", LengthFunction{}.ReturnType())
	assert.Equal(t, "boolean", ContainsFunction{}.ReturnType())
	assert.Equal(t, "number", IndexOfFunction{}.ReturnType())
	assert.Equal(t, "vector", SubvecFunction{}.ReturnType())
	assert.Equal(t, "tuples", EnumerateFunction{}.ReturnType())
}
