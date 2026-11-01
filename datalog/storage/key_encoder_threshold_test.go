package storage

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// A CompressionThreshold option reads one way wherever a database's encoder is
// built: unset is the 512-byte default, a positive value is the threshold, and
// a negative one stores every value raw. The encoder shows it in the tier it
// gives a 1200-byte string.
func TestNewBinaryKeyEncoder_ReadsTheThresholdOption(t *testing.T) {
	long := strings.Repeat("the quire is sewn and folded. ", 40)
	cases := []struct {
		name   string
		option int
		tag    datalog.ValueType
	}{
		{"unset, the 512-byte default", 0, datalog.TypeCompressedString},
		{"a threshold below the value", 256, datalog.TypeCompressedString},
		{"a threshold past the value", 2048, datalog.TypeString},
		{"compression off", -1, datalog.TypeString},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vBytes, _ := newBinaryKeyEncoder(c.option).EncodeValueBytes(long)
			require.Equal(t, c.tag, datalog.ValueType(vBytes[0]), "the tier the option gives the value")
		})
	}
}
