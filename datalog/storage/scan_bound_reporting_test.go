package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestScanBoundErrorNamesItsIndex pins that the loud failure the typed bound
// added is readable. IndexType has no String method, so %v renders the integer
// and the operator is told "scan bound on 5" for what indexName already calls
// AVET.
func TestScanBoundErrorNamesItsIndex(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	_, err := encoder.EncodeScanBound(ScanBound{
		Index:  AVET,
		Prefix: []datalog.Value{"not-a-keyword"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), AVET.String(),
		"the error must name the index the way the rest of the engine does; got %q", err.Error())
}

// TestBoundAnnotationKeyHasOneType pins that one key in one event family
// carries one payload type. addBoundFields writes "bound" as the run's bound
// positions ([]string) on v-validation/open-scan, while two sibling events in
// the same family write the same key as the rendered bound value (string). A
// handler filtering v-validation/* has to type-switch to learn which it got.
func TestBoundAnnotationKeyHasOneType(t *testing.T) {
	var events []annotations.Event
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:              t.TempDir(),
		AnnotationHandler: func(e annotations.Event) { events = append(events, e) },
	})
	require.NoError(t, err)
	defer db.Close()

	name := datalog.NewKeyword(":person/name")
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(datalog.NewIdentity("person:alice"), name, "Alice"))
	_, err = tx.Commit()
	require.NoError(t, err)

	result, err := db.Query(`[:find ?e :where [?e :person/name "Alice"]]`)
	require.NoError(t, err)
	_, err = executor.CollectTuples(result, nil)
	require.NoError(t, err)

	seen := map[string]bool{}
	for _, e := range events {
		raw, ok := e.Data["bound"]
		if !ok {
			continue
		}
		switch raw.(type) {
		case []string:
			seen["positions"] = true
		case string:
			seen["value"] = true
		default:
			t.Fatalf("event %s carries an unexpected type for \"bound\": %T", e.Name, raw)
		}
	}

	require.NotEmpty(t, seen, "the V-validation path must report its bound")
	require.Len(t, seen, 1,
		`one key, one type: the "bound" key carries both bound positions and a rendered `+
			`bound value across the v-validation family; saw %v`, seen)
}
