package reflect

import (
	goreflect "reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func TestGoTypeToSchemaTypeSymbol(t *testing.T) {
	symbolType := goreflect.TypeOf((datalog.Symbol)(nil))
	valueType, err := GoTypeToSchemaType(symbolType)
	require.NoError(t, err)
	require.Equal(t, schema.TypeSymbol, valueType)
}
