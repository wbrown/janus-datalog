package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestOrSubqueryPlannerPhases debugs how the planner handles the production query
func TestOrSubqueryPlannerPhases(t *testing.T) {
	queryStr := `
[:find ?scenario ?id ?title ?createdAt ?intensity ?pov ?genre ?element ?setting
       ?taskCount ?totalTokens ?totalDuration ?cacheCreation ?cacheRead
       ?complete ?lastKey
 :where
 [?scenario :scenario/id ?id]
 [(get-else $ ?scenario :scenario/title "") ?title]
 [?scenario :scenario/created-at ?createdAt]
 [(get-else $ ?scenario :idea/intensity "") ?intensity]
 [(get-else $ ?scenario :scenario/pov "") ?pov]
 [(get-else $ ?scenario :idea/genre "") ?genre]
 [(get-else $ ?scenario :idea/element "") ?element]
 [(get-else $ ?scenario :idea/setting "") ?setting]

 (or [(q [:find (count ?t) (sum ?tok) (sum ?dur) (sum ?cc) (sum ?cr)
          :in $ ?s
          :where [?t :task/scenario ?s]
                 [?t :task/status :status/complete]
                 [(get-else $ ?t :task/token-count 0) ?tok]
                 [(get-else $ ?t :task/duration 0) ?dur]
                 [(get-else $ ?t :task/cache-creation-tokens 0) ?cc]
                 [(get-else $ ?t :task/cache-read-tokens 0) ?cr]]
        $ ?scenario) [[?taskCount ?totalTokens ?totalDuration ?cacheCreation ?cacheRead]]]
     (and [(ground 0) ?taskCount]
          [(ground 0) ?totalTokens]
          [(ground 0) ?totalDuration]
          [(ground 0) ?cacheCreation]
          [(ground 0) ?cacheRead]))

 (or [(q [:find (count ?t)
          :in $ ?s
          :where [?t :task/scenario ?s]
                 [?t :task/key :task/opening]
                 [?t :task/status :status/complete]]
        $ ?scenario) [[?openingCount]]]
     [(ground 0) ?openingCount])

 [(> ?openingCount 0) ?complete]

 (or [(q [:find ?key
          :in $ ?s
          :where [?t :task/scenario ?s]
                 [?t :task/status :status/complete]
                 [?t :task/completed-at ?ca]
                 [?t :task/key ?key]
                 [(q [:find (max ?ca2)
                      :in $ ?s2
                      :where [?t2 :task/scenario ?s2]
                             [?t2 :task/status :status/complete]
                             [?t2 :task/completed-at ?ca2]]
                    $ ?s) [[?maxCa]]]
                 [(= ?ca ?maxCa)]]
        $ ?scenario) [[?lastKey]]]
     [(ground :none) ?lastKey])]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	p := NewClauseBasedPlanner(nil, PlannerOptions{})
	realized, err := p.Plan(q, nil)
	if err != nil {
		t.Fatalf("Plan error: %v", err)
	}

	t.Logf("Plan has %d phases", len(realized.Phases))

	for i, phase := range realized.Phases {
		t.Logf("Phase %d:", i)
		t.Logf("  Available: %v", phase.Available)
		t.Logf("  Provides: %v", phase.Provides)
		t.Logf("  Keep: %v", phase.Keep)
		t.Logf("  Query WHERE clauses: %d", len(phase.Query.Where))
		for j, clause := range phase.Query.Where {
			t.Logf("    Clause %d: %T - %s", j, clause, clause.String())
		}
	}
}
