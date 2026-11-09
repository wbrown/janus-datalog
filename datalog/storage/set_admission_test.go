package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// setAdmissionSlices is one slice per element kind a caller can hand Set at a
// cardinality-many or vector attribute, beside the elements Add is handed one
// at a time and the values a datom holds for them, at a long attribute and at
// an attribute declared with no value type. The kinds are the integer widths
// datalog.NormalizeValue widens, typed and in a []interface{}, a float32, which
// no datom holds, a nil element, and strings. A nil want is a slice Add
// refuses an element of.
var setAdmissionSlices = []struct {
	name        string
	slice       interface{}
	elements    []interface{}
	wantLong    []interface{}
	wantUntyped []interface{}
}{
	{"[]int", []int{1, 2}, []interface{}{1, 2},
		[]interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)}},
	{"[]int8", []int8{1, 2}, []interface{}{int8(1), int8(2)},
		[]interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)}},
	{"[]int16", []int16{1, 2}, []interface{}{int16(1), int16(2)},
		[]interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)}},
	{"[]int32", []int32{1, 2}, []interface{}{int32(1), int32(2)},
		[]interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)}},
	{"[]int64", []int64{1, 2}, []interface{}{int64(1), int64(2)},
		[]interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)}},
	{"[]uint", []uint{1, 2}, []interface{}{uint(1), uint(2)},
		[]interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)}},
	{"[]uint16", []uint16{1, 2}, []interface{}{uint16(1), uint16(2)},
		[]interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)}},
	{"[]uint32", []uint32{1, 2}, []interface{}{uint32(1), uint32(2)},
		[]interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)}},
	{"[]uint64", []uint64{1, 2}, []interface{}{uint64(1), uint64(2)},
		[]interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)}},
	{"[]float32", []float32{1.5, 2.5}, []interface{}{float32(1.5), float32(2.5)},
		nil, nil},
	{"[]interface{} of int32", []interface{}{int32(1), int32(2)}, []interface{}{int32(1), int32(2)},
		[]interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)}},
	{"[]interface{} of uint16", []interface{}{uint16(1), uint16(2)}, []interface{}{uint16(1), uint16(2)},
		[]interface{}{int64(1), int64(2)}, []interface{}{int64(1), int64(2)}},
	{"[]interface{} holding nil", []interface{}{int64(1), nil}, []interface{}{int64(1), nil},
		nil, nil},
	{"[]string", []string{"x", "y"}, []interface{}{"x", "y"},
		nil, []interface{}{"x", "y"}},
}

// setAdmissionValues is one value per kind a caller can hand Set at a
// cardinality-one attribute, beside the value a datom holds for it at a long
// attribute and at an attribute the schema leaves undefined. A nil want is a
// value Add refuses.
var setAdmissionValues = []struct {
	name           string
	value          interface{}
	wantLong       interface{}
	wantUndeclared interface{}
}{
	{"int32", int32(1), int64(1), int64(1)},
	{"uint16", uint16(1), int64(1), int64(1)},
	{"float32", float32(1.5), nil, nil},
	{"nil", nil, nil, nil},
	{"a string", "x", nil, "x"},
	{"a vector", []int64{1}, nil, nil},
}

// Transaction.Set admits each value it writes as Transaction.Add admits it:
// each element of the slice at a cardinality-many or vector attribute, and the
// value at a cardinality-one attribute. Where Add refuses a value, Set refuses
// it with Add's error and writes nothing. Where Add admits a value, Set writes
// the value Add writes, an integer of any width as an int64. Neither panics.
// Each case reads the write's error and the datoms the transaction holds
// before its commit.
func TestSetWritesEachValueAsAddWritesIt(t *testing.T) {
	e := datalog.NewIdentity("set-admission")
	s := schema.NewBuilder().
		Attribute(":set-admission/long-many").Type(schema.TypeLong).Many().Add().
		Attribute(":set-admission/long-vector").Type(schema.TypeLong).Vector().Add().
		Attribute(":set-admission/many").Many().Add().
		Attribute(":set-admission/vector").Vector().Add().
		Attribute(":set-admission/long-one").Type(schema.TypeLong).One().Add().
		MustBuild()

	held := func(tx *Transaction) []datalog.Datom {
		var datoms []datalog.Datom
		for _, datom := range tx.datoms {
			if datom.E == e {
				datoms = append(datoms, datom)
			}
		}
		return datoms
	}
	values := func(datoms []datalog.Datom) []interface{} {
		vs := make([]interface{}, len(datoms))
		for i, datom := range datoms {
			vs[i] = datom.V
		}
		return vs
	}
	// addEach adds each element at a on a transaction of its own, and answers
	// the datoms the transaction holds and Add's first error.
	addEach := func(t *testing.T, d *Database, a datalog.Keyword, elements []interface{}) ([]datalog.Datom, error) {
		tx := d.NewTransaction()
		var err error
		require.NotPanics(t, func() {
			for _, element := range elements {
				if err = tx.Add(e, a, element); err != nil {
					return
				}
			}
		}, "Transaction.Add")
		datoms := held(tx)
		require.NoError(t, tx.Rollback())
		return datoms, err
	}
	// setOn sets v at a on a transaction of its own, and answers the datoms the
	// transaction holds and Set's error.
	setOn := func(t *testing.T, d *Database, a datalog.Keyword, v interface{}) ([]datalog.Datom, error) {
		tx := d.NewTransaction()
		var err error
		require.NotPanics(t, func() { err = tx.Set(e, a, v) }, "Transaction.Set")
		datoms := held(tx)
		require.NoError(t, tx.Rollback())
		return datoms, err
	}

	collections := []struct {
		name   string
		attr   datalog.Keyword
		typed  bool
		vector bool
	}{
		{"long many", kw(":set-admission/long-many"), true, false},
		{"long vector", kw(":set-admission/long-vector"), true, true},
		{"untyped many", kw(":set-admission/many"), false, false},
		{"untyped vector", kw(":set-admission/vector"), false, true},
	}
	ones := []struct {
		name     string
		attr     datalog.Keyword
		declared bool
	}{
		{"long one", kw(":set-admission/long-one"), true},
		{"undeclared", kw(":set-admission/undeclared"), false},
	}

	for _, mode := range legsFor(optimizerModes) {
		for _, collection := range collections {
			for _, c := range setAdmissionSlices {
				want := c.wantUntyped
				if collection.typed {
					want = c.wantLong
				}
				t.Run(mode.name+"/"+collection.name+"/"+c.name, func(t *testing.T) {
					d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
					added, addErr := addEach(t, d, collection.attr, c.elements)
					written, setErr := setOn(t, d, collection.attr, c.slice)
					if want == nil {
						require.Error(t, addErr, "Add refuses an element of %v", c.slice)
						require.EqualError(t, setErr, addErr.Error(), "Set refuses the slice with Add's error")
						require.Empty(t, written, "Set writes nothing of a slice it refuses")
						return
					}
					require.NoError(t, addErr)
					require.Equal(t, want, values(added), "Add writes each element as the value its datom holds")
					require.NoError(t, setErr, "Set admits each element Add admits")
					op := datalog.OpCRDTAdd
					if collection.vector {
						op = datalog.OpRGAInsert
						require.Equal(t, want, values(written), "Set writes each element as the value Add writes, in order")
					} else {
						require.ElementsMatch(t, want, values(written), "Set writes each element as the value Add writes")
					}
					for _, datom := range written {
						require.Equal(t, op, datom.Op)
					}
				})
			}
		}

		// Over a committed vector whose one element is the slice's first
		// element as Add writes it, Set keeps that element and writes the rest.
		for _, collection := range collections {
			if !collection.vector {
				continue
			}
			for _, c := range setAdmissionSlices {
				want := c.wantUntyped
				if collection.typed {
					want = c.wantLong
				}
				if want == nil {
					continue
				}
				t.Run(mode.name+"/"+collection.name+" over a committed first element/"+c.name, func(t *testing.T) {
					d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
					committing := d.NewTransaction()
					require.NoError(t, committing.Add(e, collection.attr, want[0]))
					_, err := committing.Commit()
					require.NoError(t, err)
					written, setErr := setOn(t, d, collection.attr, c.slice)
					require.NoError(t, setErr, "Set admits each element Add admits")
					require.Equal(t, want[1:], values(written), "Set keeps the committed element its first element equals and writes the rest as Add writes them")
					for _, datom := range written {
						require.Equal(t, datalog.OpRGAInsert, datom.Op, "Set withdraws no element of the common prefix")
					}
				})
			}
		}

		for _, one := range ones {
			for _, c := range setAdmissionValues {
				want := c.wantUndeclared
				if one.declared {
					want = c.wantLong
				}
				t.Run(mode.name+"/"+one.name+"/"+c.name, func(t *testing.T) {
					d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
					added, addErr := addEach(t, d, one.attr, []interface{}{c.value})
					written, setErr := setOn(t, d, one.attr, c.value)
					if want == nil {
						require.Error(t, addErr, "Add refuses %v", c.value)
						require.EqualError(t, setErr, addErr.Error(), "Set refuses the value with Add's error")
						require.Empty(t, written, "Set writes nothing of a value it refuses")
						return
					}
					require.NoError(t, addErr)
					require.Equal(t, []interface{}{want}, values(added), "Add writes the value its datom holds")
					require.NoError(t, setErr, "Set admits the value Add admits")
					require.Equal(t, []interface{}{want}, values(written), "Set writes the value Add writes")
				})
			}
		}
	}
}
