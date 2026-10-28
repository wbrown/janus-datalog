package db_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A rule body's constants are a query's constants: one outside the value
// domain is refused, given as rule text or built, as the same constant in the
// query's own :where is, and never reaches an answer or a panic.
func TestARuleBodyConstantOutsideTheDomainIsRefusedAsAQueryConstantIs(t *testing.T) {
	alice := datalog.NewIdentity("rule-constant-alice")
	d, err := db.OpenMemory()
	require.NoError(t, err)
	defer d.Close()
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(alice, datalog.NewKeyword(":t/name"), "alice"))
	_, err = tx.Commit()
	require.NoError(t, err)

	x := qb.NewVar("x")
	cases := []struct {
		name  string
		rules func() (interface{}, error)
		where string
	}{
		{"text, a NaN ground", func() (interface{}, error) {
			return `[[(r ?x) [(ground ##NaN) ?x]]]`, nil
		}, `[(ground ##NaN) ?x]`},
		{"built, a float32 ground", func() (interface{}, error) {
			return qb.RuleSet(qb.DefineRule("r", x).Where(qb.Ground(float32(2)).As(x)))
		}, ""},
		{"built, an unsigned integer above int64's range", func() (interface{}, error) {
			return qb.RuleSet(qb.DefineRule("r", x).Where(qb.Ground(uint64(math.MaxInt64) + 1).As(x)))
		}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.where != "" {
				_, topErr := d.Query(`[:find ?x :where ` + c.where + `]`)
				require.Error(t, topErr, "the constant in the query's own :where is refused, so the rule body's is held to the same domain")
			}
			var answered [][]interface{}
			var refusal error
			require.NotPanics(t, func() {
				rules, err := c.rules()
				if err != nil {
					refusal = err
					return
				}
				if text, ok := rules.(string); ok {
					answered, refusal = executor.CollectTuples(d.Query(`[:find ?x :in $ % :where (r ?x)]`, text))
					return
				}
				answered, refusal = executor.CollectTuples(d.Query(`[:find ?x :in $ % :where (r ?x)]`, rules.(*query.RuleSet)))
			})
			require.Error(t, refusal, "the rule body's constant is refused; answered %v", answered)
		})
	}
}
