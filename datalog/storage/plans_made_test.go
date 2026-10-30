package storage

import (
	"sync"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// plansMade collects the plans the planner reports making, as their queries
// rendered, from the parallel workers the engine does not serialize.
type plansMade struct {
	mu      sync.Mutex
	queries []string
}

func (m *plansMade) handle(e annotations.Event) {
	if e.Name != annotations.PlannerPlanMade {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queries = append(m.queries, e.Data[annotations.KeyQuery].(*query.Query).String())
}

// reset forgets the plans collected so far.
func (m *plansMade) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queries = nil
}

// made is the plans collected so far, rendered one per line.
func (m *plansMade) made() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.queries...)
}
