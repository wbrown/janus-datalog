package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// A database given a handler on its planner options and a handler at open
// hands every event to both: each receives the same events in the same order.
func TestBothHandlersGivenAtOpenReceiveEveryEvent(t *testing.T) {
	name := datalog.NewKeyword(":person/name")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: name, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})

	var onOptions, atOpen []string
	opts := storage.DefaultPlannerOptions()
	opts.Handler = func(e annotations.Event) { onOptions = append(onOptions, e.Name) }
	d, err := db.OpenMemory(db.WithSchema(s), db.WithPlannerOptions(opts), db.WithAnnotationHandler(func(e annotations.Event) { atOpen = append(atOpen, e.Name) }))
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	tx := d.NewTransaction()
	require.NoError(t, tx.Add(datalog.NewIdentity("person:ada"), name, "Ada"))
	_, err = tx.Commit()
	require.NoError(t, err)
	_, err = executor.CollectTuples(d.Query(`[:find ?n :where [?e :person/name ?n]]`))
	require.NoError(t, err)

	require.NotEmpty(t, atOpen, "the query emits events")
	require.Equal(t, atOpen, onOptions, "the handler on the planner options receives every event the handler given at open does")
}
