package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkPull_SingleAttribute measures single attribute lookup performance
func BenchmarkPull_SingleAttribute(b *testing.B) {
	// Create test data
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)
	pattern, _ := parser.ParsePullPattern(`[:user/name]`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = puller.Pull(alice, pattern)
	}
}

// BenchmarkPull_MultipleAttributes measures multiple attribute lookup performance
func BenchmarkPull_MultipleAttributes(b *testing.B) {
	alice := datalog.NewIdentity("user:alice")

	datoms := []datalog.Datom{
		{E: alice, A: datalog.NewKeyword(":user/name"), V: "Alice", Tx: 1},
		{E: alice, A: datalog.NewKeyword(":user/age"), V: int64(30), Tx: 1},
		{E: alice, A: datalog.NewKeyword(":user/email"), V: "alice@example.com", Tx: 1},
		{E: alice, A: datalog.NewKeyword(":user/city"), V: "NYC", Tx: 1},
		{E: alice, A: datalog.NewKeyword(":user/country"), V: "USA", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)
	pattern, _ := parser.ParsePullPattern(`[:user/name :user/age :user/email :user/city :user/country]`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = puller.Pull(alice, pattern)
	}
}

// BenchmarkPull_Wildcard measures wildcard pull performance
func BenchmarkPull_Wildcard(b *testing.B) {
	alice := datalog.NewIdentity("user:alice")

	datoms := []datalog.Datom{
		{E: alice, A: datalog.NewKeyword(":user/name"), V: "Alice", Tx: 1},
		{E: alice, A: datalog.NewKeyword(":user/age"), V: int64(30), Tx: 1},
		{E: alice, A: datalog.NewKeyword(":user/email"), V: "alice@example.com", Tx: 1},
		{E: alice, A: datalog.NewKeyword(":user/city"), V: "NYC", Tx: 1},
		{E: alice, A: datalog.NewKeyword(":user/country"), V: "USA", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)
	pattern, _ := parser.ParsePullPattern(`[*]`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = puller.Pull(alice, pattern)
	}
}

// BenchmarkPull_NestedReference measures nested reference following performance
func BenchmarkPull_NestedReference(b *testing.B) {
	alice := datalog.NewIdentity("user:alice")
	region := datalog.NewIdentity("region:us-west")

	datoms := []datalog.Datom{
		{E: alice, A: datalog.NewKeyword(":user/name"), V: "Alice", Tx: 1},
		{E: alice, A: datalog.NewKeyword(":user/region"), V: region, Tx: 1},
		{E: region, A: datalog.NewKeyword(":region/code"), V: "US-W", Tx: 1},
		{E: region, A: datalog.NewKeyword(":region/name"), V: "US West", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)
	pattern, _ := parser.ParsePullPattern(`[:user/name {:user/region [:region/code :region/name]}]`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = puller.Pull(alice, pattern)
	}
}

// BenchmarkPull_DeepNesting measures deeply nested reference performance
func BenchmarkPull_DeepNesting(b *testing.B) {
	nation := datalog.NewIdentity("nation:usa")
	region := datalog.NewIdentity("region:us-west")
	entity := datalog.NewIdentity("entity:apple")

	datoms := []datalog.Datom{
		{E: nation, A: datalog.NewKeyword(":nation/name"), V: "United States", Tx: 1},
		{E: region, A: datalog.NewKeyword(":region/code"), V: "US-W", Tx: 1},
		{E: region, A: datalog.NewKeyword(":region/nation"), V: nation, Tx: 1},
		{E: entity, A: datalog.NewKeyword(":entity/code"), V: "AAPL", Tx: 1},
		{E: entity, A: datalog.NewKeyword(":entity/region"), V: region, Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)
	pattern, _ := parser.ParsePullPattern(`[:entity/code {:entity/region [:region/code {:region/nation [:nation/name]}]}]`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = puller.Pull(entity, pattern)
	}
}

// BenchmarkPullMany measures batch pull performance
func BenchmarkPullMany(b *testing.B) {
	// Create 100 entities
	var entities []datalog.Identity
	var datoms []datalog.Datom

	for i := 0; i < 100; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("user:%d", i))
		entities = append(entities, entity)
		datoms = append(datoms,
			datalog.Datom{E: entity, A: datalog.NewKeyword(":user/name"), V: fmt.Sprintf("User%d", i), Tx: 1},
			datalog.Datom{E: entity, A: datalog.NewKeyword(":user/age"), V: int64(20 + i%50), Tx: 1},
		)
	}

	matcher := NewMemoryPatternMatcher(datoms)
	puller := NewPullExecutor(matcher)
	pattern, _ := parser.ParsePullPattern(`[:user/name :user/age]`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = puller.PullMany(entities, pattern)
	}
}

// BenchmarkPull_VsManualQuery compares Pull vs equivalent manual pattern matching
func BenchmarkPull_VsManualQuery(b *testing.B) {
	alice := datalog.NewIdentity("user:alice")
	nameAttr := datalog.NewKeyword(":user/name")
	ageAttr := datalog.NewKeyword(":user/age")
	emailAttr := datalog.NewKeyword(":user/email")

	datoms := []datalog.Datom{
		{E: alice, A: nameAttr, V: "Alice", Tx: 1},
		{E: alice, A: ageAttr, V: int64(30), Tx: 1},
		{E: alice, A: emailAttr, V: "alice@example.com", Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	b.Run("Pull", func(b *testing.B) {
		puller := NewPullExecutor(matcher)
		pattern, _ := parser.ParsePullPattern(`[:user/name :user/age :user/email]`)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = puller.Pull(alice, pattern)
		}
	})

	b.Run("ManualPatternMatch", func(b *testing.B) {
		// Simulate manual pattern matching for each attribute
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := make(map[string]interface{})

			// Manual pattern match for each attribute
			for _, attr := range []datalog.Keyword{nameAttr, ageAttr, emailAttr} {
				pattern := &query.DataPattern{
					Elements: []query.PatternElement{
						query.Constant{Value: alice},
						query.Constant{Value: attr},
						query.Variable{Name: "?v"},
					},
				}
				rel, err := matcher.Match(pattern, nil)
				if err == nil && rel != nil {
					it := rel.Iterator()
					if it.Next() {
						tuple := it.Tuple()
						if len(tuple) > 0 {
							result[attr.String()[1:]] = tuple[0] // strip leading colon
						}
					}
					it.Close()
				}
			}
			_ = result
		}
	})
}

// BenchmarkPull_ScalingWithAttributes measures how Pull scales with attribute count
func BenchmarkPull_ScalingWithAttributes(b *testing.B) {
	alice := datalog.NewIdentity("user:alice")

	for _, numAttrs := range []int{1, 5, 10, 20, 50} {
		var datoms []datalog.Datom
		var patternAttrs string

		for i := 0; i < numAttrs; i++ {
			attr := datalog.NewKeyword(fmt.Sprintf(":user/attr%d", i))
			datoms = append(datoms, datalog.Datom{E: alice, A: attr, V: fmt.Sprintf("value%d", i), Tx: 1})
			if i > 0 {
				patternAttrs += " "
			}
			patternAttrs += fmt.Sprintf(":user/attr%d", i)
		}

		matcher := NewMemoryPatternMatcher(datoms)
		puller := NewPullExecutor(matcher)
		pattern, _ := parser.ParsePullPattern(fmt.Sprintf("[%s]", patternAttrs))

		b.Run(fmt.Sprintf("Attrs_%d", numAttrs), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = puller.Pull(alice, pattern)
			}
		})
	}
}
