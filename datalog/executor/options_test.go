package executor

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/annotations"
)

// TestExecutorOptionsPopulated pins ExecutorOptions.populated() to the struct's
// actual field set. populated() cannot be written as a comparison against the
// zero value — Handler is a func type, and a struct containing one is not
// comparable with == — so it enumerates fields by hand. This test walks the
// struct reflectively and asserts that setting any single field to a non-zero
// value makes populated() true, so adding a field without accounting for it here
// fails rather than silently reading as "no options".
func TestExecutorOptionsPopulated(t *testing.T) {
	require.False(t, ExecutorOptions{}.populated(),
		"the zero value carries nothing and must not read as populated")

	structType := reflect.TypeOf(ExecutorOptions{})
	require.NotZero(t, structType.NumField(), "ExecutorOptions has no fields")

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		t.Run(field.Name, func(t *testing.T) {
			value := reflect.New(structType).Elem()
			target := value.Field(i)

			switch field.Type.Kind() {
			case reflect.Bool:
				target.SetBool(true)
			case reflect.Int:
				target.SetInt(1)
			case reflect.Func:
				target.Set(reflect.ValueOf(annotations.Handler(func(annotations.Event) {})))
			default:
				t.Fatalf("field %s has kind %s, which populated() has no case for — "+
					"add one there and a case here", field.Name, field.Type.Kind())
			}

			require.True(t, value.Interface().(ExecutorOptions).populated(),
				"populated() does not account for %s", field.Name)
		})
	}
}
