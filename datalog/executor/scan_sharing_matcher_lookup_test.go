package executor_test

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/executor"
)

func TestScanSharingMatcher_LooksUpAndTypesDefaultsAsTheMatcherItWraps(t *testing.T) {
	d, one, skills := skillsDatabase(t)
	requireAnswersLookupsAsTheMatcherItWraps(t, executor.NewScanSharingMatcher(d.Matcher(), executor.NewScanRegistry(), nil), one, skills)
}

// A scan-sharing matcher over a matcher that cannot look an attribute up
// answers each lookup with an error, never (nil, false, nil), which reads as
// the attribute being absent.
func TestScanSharingMatcher_LookupOverAMatcherThatCannotLookUpIsAnError(t *testing.T) {
	requireRefusesLookupsOverAMatcherThatCannotLookUp(t,
		executor.NewScanSharingMatcher(customSource{valueDatabase(t, "held")}, executor.NewScanRegistry(), nil))
}
