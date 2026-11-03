package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// windowItems writes n entities carrying attr and returns their datoms and
// the subject tuples over entity, in entity order.
func windowItems(n int, attr datalog.Keyword) ([]datalog.Datom, []Tuple) {
	var datoms []datalog.Datom
	var tuples []Tuple
	for i := 1; i <= n; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("window-item%d", i))
		datoms = append(datoms, datalog.Datom{E: e, A: attr, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(i), ReplicaID: 1}})
		tuples = append(tuples, Tuple{e})
	}
	return datoms, tuples
}
