package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

const (
	historyOrderEntities = 100
	historyOrderVersions = 100
	historyOrderDatoms   = historyOrderEntities * historyOrderVersions
)

type historyOrderScanCapture struct {
	mu      sync.Mutex
	scanned int
	index   string
}

func (c *historyOrderScanCapture) handler(event annotations.Event) {
	if event.Name != "pattern/storage-scan" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if scanned, ok := event.Data["datoms.scanned"].(int); ok {
		c.scanned += scanned
	}
	if index, ok := event.Data["index"].(string); ok {
		c.index = index
	}
}

func (c *historyOrderScanCapture) reset() {
	c.mu.Lock()
	c.scanned = 0
	c.index = ""
	c.mu.Unlock()
}

func (c *historyOrderScanCapture) snapshot() (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.scanned, c.index
}

func openHistoryOrderDatabase(tb testing.TB, capture *historyOrderScanCapture) *Database {
	tb.Helper()
	valueAttr := datalog.NewKeyword(":event/value")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       valueAttr,
		ValueType:   schema.TypeLong,
		Cardinality: schema.CardinalityOne,
	})

	options := DatabaseOptions{
		Path:   tb.TempDir(),
		Schema: s,
	}
	if capture != nil {
		options.AnnotationHandler = capture.handler
	}
	db, err := NewDatabaseWithOptions(options)
	require.NoError(tb, err)
	tb.Cleanup(func() { db.Close() })

	entities := make([]datalog.Identity, historyOrderEntities)
	for i := range entities {
		entities[i] = datalog.NewIdentity(fmt.Sprintf("history-order:%d", i))
	}
	for version := 0; version < historyOrderVersions; version++ {
		tx := db.NewTransaction()
		for _, entity := range entities {
			require.NoError(tb, tx.Set(entity, valueAttr, int64(version)))
		}
		_, err := tx.Commit()
		require.NoError(tb, err)
	}
	return db
}

func historyOrderedLimitQuery(limit int) string {
	return fmt.Sprintf(
		`[:find ?e ?v ?tx
		  :where [?e :event/value ?v ?tx]
		  :order-by [[?tx :desc] [?e :asc]]
		  :limit %d]`,
		limit,
	)
}

func historyTransactionOrderedLimitQuery(limit int) string {
	return fmt.Sprintf(
		`[:find ?e ?a ?v ?tx
		  :where [?e ?a ?v ?tx]
		  :order-by [[?tx :desc] [?a :asc] [?e :asc]]
		  :limit %d]`,
		limit,
	)
}

func TestHistoryOrderedLimitUsesATEV(t *testing.T) {
	const limit = 10
	capture := &historyOrderScanCapture{}
	db := openHistoryOrderDatabase(t, capture)

	capture.reset()
	result, err := db.History().Query(historyOrderedLimitQuery(limit))
	require.NoError(t, err)
	rows, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, rows, limit)
	for i := 1; i < len(rows); i++ {
		previousTx, ok := datalog.DerefElementID(rows[i-1][2])
		require.True(t, ok)
		currentTx, ok := datalog.DerefElementID(rows[i][2])
		require.True(t, ok)
		require.GreaterOrEqual(t, previousTx.Compare(currentTx), 0)
		if previousTx.Equal(currentTx) {
			previousEntity := rows[i-1][0].(datalog.Identity)
			currentEntity := rows[i][0].(datalog.Identity)
			require.LessOrEqual(t, previousEntity.Compare(currentEntity), 0)
		}
	}

	scanned, index := capture.snapshot()
	require.Equal(t, "ATEV", index)
	require.LessOrEqual(t, scanned, limit,
		"history ATEV ordering must stop after the requested rows")
}

func TestLatestOrderedLimitDeclinesATEV(t *testing.T) {
	capture := &historyOrderScanCapture{}
	db := openHistoryOrderDatabase(t, capture)

	capture.reset()
	result, err := db.Query(historyOrderedLimitQuery(10))
	require.NoError(t, err)
	rows, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, rows, 10)

	_, index := capture.snapshot()
	require.NotEqual(t, "ATEV", index,
		"latest CRDT resolution must not use Tx-primary ATEV")
}

func TestHistoryTransactionOrderedLimitUsesTAEV(t *testing.T) {
	const limit = 10
	capture := &historyOrderScanCapture{}
	db := openHistoryOrderDatabase(t, capture)

	capture.reset()
	result, err := db.History().Query(historyTransactionOrderedLimitQuery(limit))
	require.NoError(t, err)
	rows, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, rows, limit)
	for i := 1; i < len(rows); i++ {
		previousTx, ok := datalog.DerefElementID(rows[i-1][3])
		require.True(t, ok)
		currentTx, ok := datalog.DerefElementID(rows[i][3])
		require.True(t, ok)
		require.GreaterOrEqual(t, previousTx.Compare(currentTx), 0)
		if previousTx.Equal(currentTx) {
			previousAttr := rows[i-1][1].(datalog.Keyword)
			currentAttr := rows[i][1].(datalog.Keyword)
			require.LessOrEqual(t, previousAttr.Compare(currentAttr), 0)
			if previousAttr.Compare(currentAttr) == 0 {
				previousEntity := rows[i-1][0].(datalog.Identity)
				currentEntity := rows[i][0].(datalog.Identity)
				require.LessOrEqual(t, previousEntity.Compare(currentEntity), 0)
			}
		}
	}

	scanned, index := capture.snapshot()
	require.Equal(t, "TAEV", index)
	require.LessOrEqual(t, scanned, limit,
		"history TAEV ordering must stop after the requested rows")
}

func TestLatestTransactionOrderedLimitDeclinesTAEV(t *testing.T) {
	capture := &historyOrderScanCapture{}
	db := openHistoryOrderDatabase(t, capture)

	capture.reset()
	result, err := db.Query(historyTransactionOrderedLimitQuery(10))
	require.NoError(t, err)
	rows, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, rows, 10)

	_, index := capture.snapshot()
	require.NotEqual(t, "TAEV", index,
		"latest CRDT resolution must not use global Tx-primary TAEV")
}

func TestHistoryATEVPropertiesRequireSafeDatalogShape(t *testing.T) {
	entity := datalog.NewSymbol("?e")
	tx := datalog.NewSymbol("?tx")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity},
		query.Constant{Value: datalog.NewKeyword(":event/value")},
		query.Variable{Name: datalog.NewSymbol("?v")},
		query.Variable{Name: tx},
	}}
	limit := 10
	q := &query.Query{
		Where: []query.Clause{pattern},
		OrderBy: []query.OrderByClause{
			{Variable: tx, Direction: query.OrderDesc},
			{Variable: entity, Direction: query.OrderAsc},
		},
		Limit: &limit,
	}
	fragmentPattern, err := q.SingleDataPattern()
	require.NoError(t, err)
	require.Same(t, pattern, fragmentPattern)

	properties, ok := historyATEVProperties(q, pattern, true)
	require.True(t, ok)
	require.Equal(t, executor.RelationProperties{Ordering: q.OrderBy}, properties)

	_, ok = historyATEVProperties(q, pattern, false)
	require.False(t, ok, "latest/as-of modes must decline Tx-primary ATEV")

	q.OrderBy[0].Direction = query.OrderAsc
	_, ok = historyATEVProperties(q, pattern, true)
	require.False(t, ok, "forward ATEV cannot satisfy ascending Tx")
}

func TestHistoryTAEVPropertiesRequireSafeDatalogShape(t *testing.T) {
	entity := datalog.NewSymbol("?e")
	attribute := datalog.NewSymbol("?a")
	value := datalog.NewSymbol("?v")
	tx := datalog.NewSymbol("?tx")
	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity},
		query.Variable{Name: attribute},
		query.Variable{Name: value},
		query.Variable{Name: tx},
	}}
	limit := 10
	q := &query.Query{
		Where: []query.Clause{pattern},
		OrderBy: []query.OrderByClause{
			{Variable: tx, Direction: query.OrderDesc},
			{Variable: attribute, Direction: query.OrderAsc},
			{Variable: entity, Direction: query.OrderAsc},
		},
		Limit: &limit,
	}

	properties, ok := historyTAEVProperties(q, pattern, true)
	require.True(t, ok)
	require.Equal(t, executor.RelationProperties{Ordering: q.OrderBy}, properties)

	fullOrder := append([]query.OrderByClause(nil), q.OrderBy...)
	for prefixLength := 1; prefixLength <= len(fullOrder); prefixLength++ {
		q.OrderBy = fullOrder[:prefixLength]
		properties, ok = historyTAEVProperties(q, pattern, true)
		require.True(t, ok, "physical TAEV order prefix of length %d should be accepted", prefixLength)
		require.Equal(t, executor.RelationProperties{Ordering: q.OrderBy}, properties)
	}
	q.OrderBy = fullOrder

	_, ok = historyTAEVProperties(q, pattern, false)
	require.False(t, ok, "latest/as-of modes must decline global Tx-primary TAEV")

	q.Limit = nil
	_, ok = historyTAEVProperties(q, pattern, true)
	require.False(t, ok, "TAEV order is useful for finalization only with a limit")
	q.Limit = &limit

	q.OrderBy[0].Direction = query.OrderAsc
	_, ok = historyTAEVProperties(q, pattern, true)
	require.False(t, ok, "forward TAEV cannot satisfy ascending Tx")
	q.OrderBy[0].Direction = query.OrderDesc

	pattern.Elements[1] = query.Constant{Value: datalog.NewKeyword(":event/value")}
	_, ok = historyTAEVProperties(q, pattern, true)
	require.False(t, ok, "filtered patterns are outside the exact-N global TAEV shape")
}
