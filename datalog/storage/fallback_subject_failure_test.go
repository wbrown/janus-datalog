package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// With the EA arm serving a fallback's branch, a failure reading the subject
// or closing it is the query's error: the arm reads the window's run, and the
// subject is read by the window reader.
func TestFallbackSurfacesTheSubjectsFailureUnderTheEAArm(t *testing.T) {
	// The subject is the scan of :t/under, the 300 entities under a root,
	// read as the windows are, under a cap of 128: windows of 1, 2, 4, 8,
	// 16, 32, 64, 128 and 45 tuples. The first 150 entities alone carry :t/a,
	// so the scan of :t/under is the one scan that reaches 200 datoms.
	const subjectSize, carrying = 300, 150
	const seventhWindow, eighthWindow, ninthWindow = 64, 128, 45
	s, err := schema.NewBuilder().
		Attribute(":t/root").Type(schema.TypeBoolean).One().Add().
		Attribute(":t/under").Type(schema.TypeRef).One().Add().
		Attribute(":t/a").Type(schema.TypeLong).One().Add().
		Build()
	require.NoError(t, err)
	const text = `[:find ?e ?v :where [?e :t/under ?r] (or-default-join [[?e] ?v] [?e :t/a ?v] [(ground 0) ?v])]`
	// The read fails inside the eighth window, and the window reader builds no
	// window over a failed read. The subject's read reaches its end and the
	// ninth window is built, then the subject's close fails, so the close
	// failure is the query's error after every window.
	cases := []struct {
		name                      string
		failAfter, closeFailAfter int
		expected                  error
		lookups                   int // the entities of the windows the arm built before the failure
	}{
		{"read", 200, 0, errInjectedScanFault, seventhWindow},
		{"close", subjectSize + 1, 200, errInjectedCloseFault, seventhWindow + eighthWindow + ninthWindow},
	}
	for _, mode := range legsWhere(optimizerModes, func(m optimizerMode) bool { return m.prefetch && !m.fusion }) {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				var mu sync.Mutex
				var triggers, lookups int
				base, err := mode.backend.Open(t.TempDir(), &BinaryKeyEncoder{})
				require.NoError(t, err)
				store := &scanFaultStore{Store: base, failAfter: c.failAfter, closeFailAfter: c.closeFailAfter}
				opts := mode.plannerOptions()
				opts.ScopeWindowCap = 128
				d, err := NewDatabaseWithOptions(DatabaseOptions{Store: store, Schema: s, PlannerOptions: &opts, AnnotationHandler: func(e annotations.Event) {
					mu.Lock()
					defer mu.Unlock()
					switch e.Name {
					case annotations.PrefetchTrigger:
						if e.Data[annotations.KeyForm] == "or-default-join" {
							triggers++
						}
					case annotations.StorageResolveComplete:
						_, served := e.Data[annotations.KeyValuesServed]
						_, ofPattern := e.Data[annotations.KeyPattern]
						if served && !ofPattern && e.Data[annotations.KeyAttribute] == kw(":t/a") {
							lookups++
						}
					}
				}})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, d.Close()) })
				root := datalog.NewIdentity("subject-failure:root")
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(root, kw(":t/root"), true))
				for i := 1; i <= subjectSize; i++ {
					e := datalog.NewIdentity(fmt.Sprintf("subject-failure:e%d", i))
					require.NoError(t, tx.Add(e, kw(":t/under"), root))
					if i <= carrying {
						require.NoError(t, tx.Add(e, kw(":t/a"), int64(i)))
					}
				}
				_, err = tx.Commit()
				require.NoError(t, err)
				store.arm()

				_, err = executor.CollectTuples(d.Query(text))
				require.ErrorContains(t, err, c.expected.Error())
				mu.Lock()
				defer mu.Unlock()
				require.Positive(t, triggers, "a window was prefetched before the failure, so the arm served the branch")
				require.Equal(t, c.lookups, lookups, "the arm built every window after the trigger from the cache")
			})
		}
	}
}
