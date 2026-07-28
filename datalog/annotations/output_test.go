package annotations

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRenderBoundPositionsCompactsComponentOrder pins the compact rendering of
// a scan bound's positions: the names arrive in the index's component order and
// concatenate, matching the index-name idiom on the same output line.
func TestRenderBoundPositionsCompactsComponentOrder(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field interface{}
		want  string
	}{
		{"two positions", []string{"A", "V"}, "AV"},
		{"one position", []string{"E"}, "E"},
		{"multi-character position", []string{"A", "Tx"}, "ATx"},
		{"all four", []string{"E", "A", "V", "Tx"}, "EAVTx"},
		// A whole-index scan binds nothing. That is a real answer and it is
		// distinct from the producer reporting no run at all, which the scan
		// line renders "?" — one is a full index read, the other is missing
		// instrumentation, and a trace that renders them alike hides the first.
		{"whole index, nil slice", []string(nil), "none"},
		{"whole index, empty slice", []string{}, "none"},
		{"field absent", nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, renderBoundPositions(tc.field))
		})
	}
}

// producerValue stands in for the typed values real producers carry — a
// storage.IndexType, a *query.DataPattern — neither of which is nameable in
// this package, since both live in packages that import it. The formatter
// reaches them through fmt, so a stand-in that renders the same way exercises
// the same path. Using plain strings here would pin a payload shape no producer
// emits.
type producerValue string

func (p producerValue) String() string { return string(p) }

// TestUniqueLookupLineReportsItsFunnel pins the arm for the sixth event in the
// scan-completion family. It carries the same payload as the other five — a
// pattern and the three counts — so a reader tracing a program whose dominant
// read is LookupByUnique must see it as a scan line rather than a raw map.
//
// The two counts that differ are the ones asserted apart: resolved above
// matched is an index entry whose claimant has since replaced the value, and a
// line collapsing them back together would hide the case the third term was
// added for.
func TestUniqueLookupLineReportsItsFunnel(t *testing.T) {
	var out bytes.Buffer
	f := NewPlainTextFormatter(&out)

	line := f.Format(Event{
		Name:    UniqueLookupComplete,
		Latency: 3 * time.Millisecond,
		Data: map[string]interface{}{
			KeyPattern:        producerValue("[?e :user/email \"a@example.com\"]"),
			KeyDatomsScanned:  12,
			KeyDatomsResolved: 1,
			KeyDatomsMatched:  0,
		},
	})
	require.Contains(t, line, `[?e :user/email "a@example.com"]`,
		"the line must name what was looked up")
	require.Contains(t, line, "0 matched, 1 resolved, 12 scanned",
		"a claimant was resolved and rejected; that is not the same as finding nothing")
	require.Contains(t, line, "[3.0ms]")
	require.NotContains(t, line, "map[",
		"an arm renders; falling through to the default dumps the payload")
}

// TestScanLineRendersFromItsOwnEvent pins that a scan line needs no memory of
// the event before it.
//
// The formatter used to carry lastIndex and lastBound between calls, filled by
// the preceding index-selection event. That is the reporter's state living in
// the consumer, and it cannot survive concurrency: the engine emits from
// parallel workers through one handler, so a worker's scan line could render
// the index another worker had just selected. Serializing the handler would
// have hidden the race and kept the wrong pairing, because the leftovers are
// still whoever wrote last.
//
// The matcher holds the bound at scan time — it hands the same ScanBound to
// emitIndexSelection and to the reader — so the scan event carries it, and this
// formats one with no preceding event at all.
func TestScanLineRendersFromItsOwnEvent(t *testing.T) {
	var out bytes.Buffer
	f := NewPlainTextFormatter(&out)

	line := f.Format(Event{
		Name:    PatternStorageScan,
		Latency: 2 * time.Millisecond,
		Data: map[string]interface{}{
			KeyPattern:        producerValue("[?e :task/scenario ?s]"),
			KeyIndex:          producerValue("AVET"),
			KeyBound:          []string{"A", "V"},
			"bound.values":    []string{":task/scenario", "scenario-alpha"},
			KeyDatomsScanned:  10,
			KeyDatomsResolved: 10,
			"scan.duration":   2 * time.Millisecond,
		},
	})
	require.Contains(t, line, "Scan([[?e :task/scenario ?s]], AVET, bound: AV)")
	require.Contains(t, line, "10 datoms")
	require.NotContains(t, line, "scanned",
		"a scan that returned everything it read says one number, not two")
	// The scan's duration is the line's latency prefix, the mechanism every
	// other timed event uses. It must not also be appended from a data key —
	// nothing writes one, so the line read "in <nil>".
	require.Contains(t, line, "[2.0ms]")
	require.NotContains(t, line, "<nil>")
	require.NotContains(t, line, " in ")
}

// TestScanLineShowsIntakeWhenItExceedsOutput pins the amplification the typed
// bound exists to control: a scan that read ten datoms to return one must say
// so on the line a human reads, or a bound that narrowed and a bound that did
// nothing render identically.
func TestScanLineShowsIntakeWhenItExceedsOutput(t *testing.T) {
	var out bytes.Buffer
	f := NewPlainTextFormatter(&out)

	line := f.Format(Event{
		Name:    PatternStorageScan,
		Latency: time.Millisecond,
		Data: map[string]interface{}{
			KeyPattern:        producerValue("[?e :person/name ?n]"),
			KeyDatomsScanned:  10,
			KeyDatomsResolved: 1,
		},
	})
	require.Contains(t, line, "1 datoms")
	require.Contains(t, line, "(10 scanned)",
		"the index charged ten reads for one datom and the line must show it")
}

// TestScanLineWholeIndexBoundIsNotUnknown separates the two ways a scan line can
// fail to name a bound: a run that bound nothing (a whole-index scan) is "none",
// while a producer that named no run at all is "?". Collapsing them would hide a
// full scan behind the same rendering as missing instrumentation.
//
// Both now turn on what the scan event carries, not on whether an earlier event
// arrived — an absent KeyBound is a producer that reported none, and a present
// but empty one is a run over the whole index.
func TestScanLineWholeIndexBoundIsNotUnknown(t *testing.T) {
	var wholeIndex bytes.Buffer
	f := NewPlainTextFormatter(&wholeIndex)
	require.Contains(t, f.Format(Event{
		Name: PatternStorageScan,
		Data: map[string]interface{}{
			KeyPattern:        producerValue("[?e ?a ?v]"),
			KeyIndex:          producerValue("EATV"),
			KeyBound:          []string(nil),
			"bound.values":    []string(nil),
			KeyDatomsResolved: 500,
		},
	}), "EATV, bound: none")

	var unreported bytes.Buffer
	bare := NewPlainTextFormatter(&unreported)
	require.Contains(t, bare.Format(Event{
		Name: PatternStorageScan,
		Data: map[string]interface{}{
			KeyPattern:        producerValue("[?e ?a ?v]"),
			KeyDatomsResolved: 500,
		},
	}), "?, bound: ?")
}

// TestFormatterHandleWritesScanLine pins that a scan line reaches the writer
// through Handle, not only through Format, and that the index-selection event
// still writes nothing of its own.
//
// It no longer sets anything up for the line that follows. Handle is where a
// formatter carrying state between events would have been fed, so this is the
// path that has to stay stateless: the two events are formatted in order and
// only the scan produces output, from its own payload.
func TestFormatterHandleWritesScanLine(t *testing.T) {
	var out bytes.Buffer
	f := NewPlainTextFormatter(&out)

	f.Handle(Event{
		Name: PatternIndexSelection,
		Data: map[string]interface{}{
			KeyPattern: producerValue("[?e :person/name ?n]"),
			KeyIndex:   producerValue("AETV"),
			KeyBound:   []string{"A"},
		},
	})
	require.Empty(t, out.String(), "the announcement is not a line of its own")

	f.Handle(Event{
		Name: PatternStorageScan,
		Data: map[string]interface{}{
			KeyPattern:        producerValue("[?e :person/name ?n]"),
			KeyIndex:          producerValue("AETV"),
			KeyBound:          []string{"A"},
			KeyDatomsResolved: 3,
		},
	})
	require.Equal(t, 1, strings.Count(out.String(), "\n"))
	require.Contains(t, out.String(), "AETV, bound: A")
}
