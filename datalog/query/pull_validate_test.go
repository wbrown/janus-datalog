package query

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// A pull pattern holding a limit below zero or a default outside the value
// domain, at its top level or under a map spec, is refused by
// PullPattern.Validate, the one check a find pull gets from ValidateQuery and
// a direct pull gets from the database, and the refusal names the spec's
// attribute. A pattern of non-negative limits and domain defaults is accepted.
func TestPullPatternValidateRefusesALimitBelowZeroAndADefaultOutsideTheDomain(t *testing.T) {
	e := sym("?e")
	tags := datalog.NewKeyword(":t/tags")
	friend := datalog.NewKeyword(":t/friend")
	scan := scopePattern("?e", "t/a", "?v")

	specs := []struct {
		name  string
		spec  PullAttrSpec
		names []string
	}{
		{"a limit of -1", &PullLimitExpr{Attr: tags, Limit: -1}, []string{"pull limit of :t/tags must be non-negative, got -1"}},
		{"a NaN default", &PullDefaultExpr{Attr: tags, Default: math.NaN()}, []string{"pull default of :t/tags", "NaN"}},
		{"a float32 default", &PullDefaultExpr{Attr: tags, Default: float32(2)}, []string{"pull default of :t/tags", "float32"}},
		{"a vector default holding NaN", &PullDefaultExpr{Attr: tags, Default: []interface{}{int64(1), math.NaN()}}, []string{"pull default of :t/tags", "NaN"}},
	}
	depths := []struct {
		name    string
		pattern func(spec PullAttrSpec) *PullPattern
	}{
		{"at the top level", func(spec PullAttrSpec) *PullPattern {
			return &PullPattern{Specs: []PullAttrSpec{&PullAttribute{Attr: tags}, spec}}
		}},
		{"under a map spec", func(spec PullAttrSpec) *PullPattern {
			return &PullPattern{Specs: []PullAttrSpec{&PullMapSpec{Attr: friend, Pattern: &PullPattern{Specs: []PullAttrSpec{spec}}}}}
		}},
	}
	for _, s := range specs {
		for _, d := range depths {
			t.Run(s.name+" "+d.name, func(t *testing.T) {
				pattern := d.pattern(s.spec)
				err := pattern.Validate()
				require.Error(t, err, "Validate accepted %s", pattern)
				for _, name := range s.names {
					require.ErrorContains(t, err, name, "Validate")
				}
				q := &Query{Find: []FindElement{FindPull{Variable: e, Pattern: pattern}}, Where: []Clause{scan}}
				err = ValidateQuery(q)
				require.Error(t, err, "ValidateQuery accepted %s", q)
				for _, name := range s.names {
					require.ErrorContains(t, err, name, "ValidateQuery")
				}
			})
		}
	}

	t.Run("non-negative limits and domain defaults are accepted", func(t *testing.T) {
		pattern := &PullPattern{Specs: []PullAttrSpec{
			&PullLimitExpr{Attr: tags, Limit: 0},
			&PullDefaultExpr{Attr: tags, Default: []interface{}{int64(1), math.Inf(1)}},
			&PullMapSpec{Attr: friend, Pattern: &PullPattern{Specs: []PullAttrSpec{
				&PullLimitExpr{Attr: tags, Limit: 3},
				&PullDefaultExpr{Attr: tags, Default: "none"},
			}}},
		}}
		require.NoError(t, pattern.Validate())
		q := &Query{Find: []FindElement{FindPull{Variable: e, Pattern: pattern}}, Where: []Clause{scan}}
		require.NoError(t, ValidateQuery(q))
	})
}
