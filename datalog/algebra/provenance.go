package algebra

import "github.com/wbrown/janus-datalog/datalog/annotations"

// RewriteAction classifies one decision a rewrite pass or transform made.
type RewriteAction string

const (
	// RewriteConsidered records that a pass examined a candidate node and is
	// about to decide; the following record for the same subject carries the
	// decision.
	RewriteConsidered RewriteAction = "considered"
	// RewriteApplied records that the rewrite fired on the subject.
	RewriteApplied RewriteAction = "applied"
	// RewriteDeclined records that the rewrite's preconditions failed;
	// Reason states which one.
	RewriteDeclined RewriteAction = "declined"
)

// RewriteRecord is one decision a rewrite pass or transform made, as a value.
// Records are the source of truth for transform provenance — the planner
// returns them through ExplainAlgebra — while the annotation events emitted
// at the same call sites remain the streaming observability view.
type RewriteRecord struct {
	// Pass names the pass or transform ("decorrelation",
	// "get-else-scan-rewrite", "join-project-insertion").
	Pass   string
	Action RewriteAction
	// Reason states the failed precondition for declined records; empty
	// otherwise.
	Reason string
	// Subject renders the context the decision was made on (the inner query
	// of a lateral join, the expression being rewritten, the terminal
	// symbols of an insertion).
	Subject string
}

// RewriteSink is where a pass's rewrite decisions go. Each recorded decision
// appends a typed RewriteRecord when Collect is set and emits its event form
// through Handler when one is present. Both destinations are optional; a nil
// sink is valid and does nothing, so passes call it unconditionally.
type RewriteSink struct {
	// Handler receives each decision's annotation-event form, plus any
	// diagnostic events that accompany a decision (Emit).
	Handler annotations.Handler
	// Collect enables record accumulation. It is off on the normal query
	// path — records are collected only when a caller will read them
	// (ExplainAlgebra) — so planning pays nothing for provenance nobody asks
	// for.
	Collect bool

	records []RewriteRecord
}

// Record appends the typed decision (when collecting) and emits its event
// form (when a handler is present).
func (s *RewriteSink) Record(rec RewriteRecord, event string, data map[string]interface{}) {
	if s == nil {
		return
	}
	if s.Collect {
		s.records = append(s.records, rec)
	}
	s.Emit(event, data)
}

// Emit forwards a diagnostic event that accompanies a recorded decision —
// detail beyond the record's fields — to the handler, if any.
func (s *RewriteSink) Emit(event string, data map[string]interface{}) {
	if s == nil || s.Handler == nil {
		return
	}
	s.Handler(annotations.Event{Name: event, Data: data})
}

// Records returns the decisions recorded so far, in occurrence order.
func (s *RewriteSink) Records() []RewriteRecord {
	if s == nil {
		return nil
	}
	return s.records
}
