package algebra

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The get-else rewrite reads the source the get-else names: its scan is a
// pattern on that source, and the default branch the join decompiles to is
// the get-else itself, reading the same source.
func TestGetElseScanRewrite_ReadsTheSourceTheGetElseNames(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?v :in $ $src :where [$src ?e :t/n _] [(get-else $src ?e :t/a "d") ?v]]`)
	require.NoError(t, err)
	root, err := Compile(q, nil)
	require.NoError(t, err)
	optimized, err := NewOptimizer(GetElseScanRewritePass(nil)).Optimize(root)
	require.NoError(t, err)
	clauses, err := Decompile(optimized)
	require.NoError(t, err)

	src := datalog.NewSymbol("$src")
	attr := datalog.NewKeyword(":t/a")
	var scans []*query.DataPattern
	var defaults []*query.GetElseFunction
	query.WalkClauses(clauses, func(clause query.Clause) bool {
		switch c := clause.(type) {
		case *query.DataPattern:
			if constant, ok := c.GetA().(query.Constant); ok && constant.Value == attr {
				scans = append(scans, c)
			}
		case *query.Expression:
			if ge, ok := c.Function.(*query.GetElseFunction); ok {
				defaults = append(defaults, ge)
			}
		}
		return true
	})
	require.Len(t, scans, 1, "the rewrite scans :t/a once")
	require.Equal(t, src, scans[0].Source, "the scan reads $src")
	require.Len(t, defaults, 1, "the default branch is a get-else")
	require.Equal(t, src, defaults[0].ReadsFrom(), "the default branch's get-else reads $src")
}
