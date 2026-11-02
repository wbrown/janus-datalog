package executor_test

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/executor"
)

func TestScanSharingMatcher_LooksUpAndTypesDefaultsAsTheMatcherItWraps(t *testing.T) {
	d, one, skills := skillsDatabase(t)
	requireAnswersLookupsAsTheMatcherItWraps(t, executor.NewScanSharingMatcher(d.Matcher(), executor.NewScanRegistry(), nil), one, skills)
}
