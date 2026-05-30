package datalog

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// assertValueOrderPreserving checks, for values given in ascending numeric order,
// that ValueBytes sorts them the same way (bytes.Compare matches CompareValues)
// and that ValueFromBytes inverts ValueBytes exactly. This is the invariant the
// AVET/VAET key layout depends on: value bytes share a single byte-sorted
// keyspace, so range scans / index min-max / ordered iteration are only correct
// when encoded order matches numeric order.
func assertValueOrderPreserving(t *testing.T, ascending []Value) {
	t.Helper()
	for i, v := range ascending {
		got, err := ValueFromBytes(Type(v), ValueBytes(v))
		require.NoErrorf(t, err, "decode %v", v)
		require.Truef(t, ValuesEqual(v, got), "round trip changed %v -> %v", v, got)

		if i == 0 {
			continue
		}
		prev := ascending[i-1]
		require.Negativef(t, bytes.Compare(ValueBytes(prev), ValueBytes(v)),
			"byte order: enc(%v) must sort before enc(%v)", prev, v)
		require.Negativef(t, CompareValues(prev, v),
			"sanity: %v must compare less than %v", prev, v)
	}
}

func TestValueBytes_Int64_OrderPreserving(t *testing.T) {
	assertValueOrderPreserving(t, []Value{
		int64(math.MinInt64),
		int64(-1 << 40),
		int64(-1000),
		int64(-1),
		int64(0),
		int64(1),
		int64(1000),
		int64(1 << 40),
		int64(math.MaxInt64),
	})
}

func TestValueBytes_Float64_OrderPreserving(t *testing.T) {
	assertValueOrderPreserving(t, []Value{
		-math.MaxFloat64,
		-1e10,
		-2.0,
		-1.0,
		-0.5,
		0.0,
		0.5,
		1.0,
		2.0,
		1e10,
		math.MaxFloat64,
	})
}

func TestValueBytes_Time_OrderPreserving(t *testing.T) {
	mk := func(y int) Value { return time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC) }
	assertValueOrderPreserving(t, []Value{
		mk(1900), // pre-1970: negative UnixNano
		mk(1950),
		time.Unix(0, 0).UTC(), // epoch
		mk(2000),
		mk(2026),
	})
}
