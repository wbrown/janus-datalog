package annotations

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/mattn/go-isatty"
)

// OutputFormatter formats events for human-readable display.
//
// It holds no state between events, and must not: the engine emits from
// parallel workers through one handler, so anything remembered from the last
// event is whoever wrote last rather than this line's own run. Every line is
// rendered from the event that produced it, which is why a scan event carries
// the bound its reporter held at scan time instead of relying on the
// announcement that preceded it.
type OutputFormatter struct {
	useColor bool
	writer   io.Writer
	renderer *RelationRenderer
}

// NewOutputFormatter creates a formatter with color support detection.
func NewOutputFormatter(w io.Writer) *OutputFormatter {
	if w == nil {
		w = os.Stdout
	}

	// Auto-detect color support
	useColor := false
	if f, ok := w.(*os.File); ok {
		useColor = isTerminal(f.Fd())
	}

	return &OutputFormatter{
		useColor: useColor,
		writer:   w,
		renderer: NewRelationRenderer(useColor),
	}
}

// NewPlainTextFormatter creates a formatter with color disabled.
func NewPlainTextFormatter(w io.Writer) *OutputFormatter {
	if w == nil {
		w = os.Stdout
	}
	return &OutputFormatter{
		useColor: false,
		writer:   w,
		renderer: NewRelationRenderer(false),
	}
}

// Handle implements the Handler interface - prints events as they occur
func (f *OutputFormatter) Handle(event Event) {
	output := f.Format(event)
	if output != "" {
		fmt.Fprintln(f.writer, output)
	}
}

// Format converts an event to a human-readable string.
func (f *OutputFormatter) Format(event Event) string {
	latency := f.formatLatency(event.Latency)

	switch event.Name {
	case QueryInvoked:
		return fmt.Sprintf("%s Query: %s", latency, event.Data["query"].(string))

	case QueryPlanCreated:
		return fmt.Sprintf("\n%s\n", event.Data["plan"].(string))

	case QueryComplete:
		success := event.Data["success"].(bool)
		if !success {
			return fmt.Sprintf("%s %s Query failed: %v",
				latency,
				f.colorize("✗", color.FgRed),
				event.Data["error"])
		}
		return fmt.Sprintf("%s %s Query done with %s with %s total.",
			latency,
			f.colorize("===", color.FgGreen),
			f.colorizeCount("Relations", event.Data["relations.count"].(int)),
			f.colorizeCount("Tuples", event.Data["tuples.count"].(int)))

	case PhaseBegin:
		return fmt.Sprintf("%s %s Phase %v starting on %v input group(s)",
			latency,
			f.colorize("===", color.FgYellow),
			event.Data["phase"],
			event.Data["input_groups"])

	case PhaseComplete:
		// -1 is a streaming group declining to size itself rather than be
		// consumed to produce the number. Report the decline; printing it as a
		// count would read as "minus one tuples".
		count, ok := event.Data["tuple.count"].(int)
		tuples := f.colorizeCount("tuples", count)
		if !ok || count < 0 {
			tuples = "an unsized stream"
		}
		return fmt.Sprintf("%s Phase %v completed with %s",
			latency,
			event.Data["phase"],
			tuples)

	case JoinHash:
		left := event.Data["left.size"].(int)
		right := event.Data["right.size"].(int)
		result := event.Data["result.size"].(int)

		// Get relation attributes if available
		var leftAttrs, rightAttrs, resultAttrs []string
		if attrs, ok := event.Data["left.attrs"].([]string); ok {
			leftAttrs = attrs
		}
		if attrs, ok := event.Data["right.attrs"].([]string); ok {
			rightAttrs = attrs
		}
		if attrs, ok := event.Data["result.attrs"].([]string); ok {
			resultAttrs = attrs
		}

		// Use relation renderer if we have attributes
		var joinStr string
		if len(leftAttrs) > 0 && len(rightAttrs) > 0 && len(resultAttrs) > 0 {
			joinStr = f.renderer.RenderJoin(leftAttrs, left, rightAttrs, right, resultAttrs, result)
		} else {
			// Fallback to simple format
			joinStr = fmt.Sprintf("%d × %d → %d tuples", left, right, result)
		}

		// Check for explosive joins
		if result > left*right/2 || result > 100000 {
			return fmt.Sprintf("%s %s %s",
				latency,
				f.colorize("⚠️", color.FgYellow),
				joinStr)
		}

		// Normal join
		return fmt.Sprintf("%s %s", latency, joinStr)

	case MatchesToRelations:
		pattern := renderPayloadValue(event.Data[KeyPattern])
		matchCount := event.Data["match.count"].(int)

		// Extract bound symbols from the pattern to determine output symbols
		var outputSymbols []string

		// Check if we have symbol order information
		if symbolOrder, ok := event.Data["symbol.order"].([]string); ok {
			outputSymbols = symbolOrder
		} else if outputBinds, ok := event.Data["output.binds"].(map[string]int); ok && len(outputBinds) > 0 {
			// Fallback - get symbols but without guaranteed order
			for sym := range outputBinds {
				outputSymbols = append(outputSymbols, sym)
			}
		} else if binds, ok := event.Data["binds"].(map[string]int); ok && len(binds) > 0 {
			// Fallback for old format
			for sym := range binds {
				outputSymbols = append(outputSymbols, sym)
			}
		}

		// Format as Pattern(...) → Relation([symbols], count)
		// Apply the same coloring style as relations
		var patternStr string
		if f.useColor {
			patternStr = fmt.Sprintf("%s%s%s",
				color.BlueString("Pattern("),
				color.CyanString(pattern),
				color.BlueString(")"))
		} else {
			patternStr = fmt.Sprintf("Pattern(%s)", pattern)
		}

		relationStr := f.renderer.RenderRelationWithAttrs(outputSymbols, matchCount)

		if f.useColor {
			arrow := color.YellowString(" → ")
			return fmt.Sprintf("%s %s%s%s", latency, patternStr, arrow, relationStr)
		}

		return fmt.Sprintf("%s %s → %s", latency, patternStr, relationStr)

	case PatternIndexSelection:
		// The announcement is not a line. It says which run a scan is about to
		// walk, and the scan's own event repeats that alongside what the run
		// cost — so printing it here would double every scan in the trace.
		return ""

	case StorageScanComplete:
		// One event for every scan performed on a query's behalf. Which
		// strategy performed it is a payload field, so the shapes below
		// dispatch on that rather than on the event name — the same four lines
		// five event names used to produce.
		//
		// Comma-ok rather than assertions throughout, on the same grounds as
		// renderScanFunnel below: this event has more producers than any other,
		// and a producer that omits a key should cost the reader one wrong line
		// rather than panic the formatter in the middle of a trace.
		switch strategy, _ := event.Data[KeyStrategy].(ScanStrategy); strategy {
		case ScanHashJoin, ScanMergeJoin:
			// One scan of the index probed against a binding set. Same three
			// counts as the direct scan line, but all three always shown: on
			// these paths the gap between what was read and what came out is
			// the reason the event exists, so it is not conditional the way the
			// direct line's amplification suffix is.
			//
			// The run is named the same way as on the direct line, and it
			// matters more here: neither path announces a pattern/index-selection
			// beforehand, so this is the only line in the trace saying what was
			// walked.
			index := renderPayloadValue(event.Data[KeyIndex])
			if index == "" {
				index = "?"
			}
			bound := renderBoundPositions(event.Data[KeyBound])
			if bound == "" {
				bound = "?"
			}
			return fmt.Sprintf("%s %s([%v], %s, bound: %s, %v bindings) → %s",
				latency, strategy,
				event.Data[KeyPattern], index, bound, event.Data[KeyBindingSize],
				renderScanFunnel(event.Data))

		case ScanPerBinding:
			// One scan per binding, reported once. No index: chooseIndex runs
			// per binding and can pick a different one each time, so a single
			// index here would name whichever happened to be last. The count of
			// scans is the datum this path owes its reader.
			return fmt.Sprintf("%s %s([%v], %v scans over %v bindings) → %s",
				latency, strategy,
				event.Data[KeyPattern], event.Data[KeyScansOpened], event.Data[KeyBindingSize],
				renderScanFunnel(event.Data))

		case ScanUniqueLookup:
			// Resolution walks AVET for the claimant and then the claimant's
			// own history, so no single run is the one this call addressed and
			// there is no index to name. The funnel is what it owes its reader:
			// resolved above matched is an index entry whose claimant has since
			// replaced the value.
			return fmt.Sprintf("%s %s([%v]) → %s",
				latency, strategy, event.Data[KeyPattern], renderScanFunnel(event.Data))

		case ScanDirect:
			// Falls through to the shared body below.

		default:
			// An unrecognised strategy is a producer this formatter has not
			// been taught, and rendering it as a direct scan would file it
			// silently under the wrong shape. Naming it costs one odd line and
			// says which producer to go look at; panicking mid-trace costs the
			// whole trace.
			return fmt.Sprintf("%s Scan([%v], strategy: %v) → %s",
				latency, event.Data[KeyPattern], event.Data[KeyStrategy],
				renderScanFunnel(event.Data))
		}

		// ScanDirect. Format as Scan([pattern], index, bound) → X datoms. The
		// scan's duration is the line's latency prefix, carried on the event
		// like every other timed event's; there is no separate duration field.
		pattern := renderPayloadValue(event.Data[KeyPattern])
		datoms, _ := event.Data[KeyDatomsResolved].(int)

		// Intake is appended only when it exceeds what came out. A bound that
		// narrowed and a bound that did nothing produce the same line
		// otherwise, which is the reading this count exists to prevent; a scan
		// that returned everything it read stays one number.
		scanned, _ := event.Data[KeyDatomsScanned].(int)
		amplification := ""
		if scanned > datoms {
			amplification = fmt.Sprintf(" (%d scanned)", scanned)
		}

		// Both come from this event. The reporter holds the bound at scan time
		// and puts it here; a formatter that remembered the last one it saw
		// would render another worker's run on this line, since the engine
		// emits from parallel workers through one handler.
		//
		// "?" is a producer that named no run, which is different from a run
		// that bound nothing — renderBoundPositions reports the latter as
		// "none".
		index := renderPayloadValue(event.Data[KeyIndex])
		if index == "" {
			index = "?"
		}
		bound := renderBoundPositions(event.Data[KeyBound])
		if bound == "" {
			bound = "?"
		}

		var scanStr string
		if f.useColor {
			scanStr = fmt.Sprintf("%s%s, %s, bound: %s%s",
				color.BlueString("Scan(["),
				color.CyanString(pattern),
				color.CyanString(index),
				color.YellowString(bound),
				color.BlueString(")"))
		} else {
			scanStr = fmt.Sprintf("Scan([%s], %s, bound: %s)", pattern, index, bound)
		}

		if f.useColor {
			arrow := color.YellowString(" → ")
			return fmt.Sprintf("%s %s%s%s%s",
				latency,
				scanStr,
				arrow,
				f.colorizeCount("datoms", datoms),
				color.RedString(amplification))
		}

		return fmt.Sprintf("%s %s → %d datoms%s", latency, scanStr, datoms, amplification)

	case StorageResolveComplete:
		// Cause and mechanism are read off which fields are populated, per the
		// event's contract: KeyPattern or KeyEntity with KeyAttribute names the
		// subject, and a run appears only where the call read storage to answer.
		// No run means the cache answered — it picks an index per entry by
		// cardinality inside resolution, and a hit walks none.
		var subject string
		if pattern, ok := event.Data[KeyPattern]; ok {
			subject = fmt.Sprintf("[%v]", pattern)
		} else {
			subject = fmt.Sprintf("%v %v", event.Data[KeyEntity], event.Data[KeyAttribute])
		}
		if card, ok := event.Data[KeyCardinality]; ok {
			subject = fmt.Sprintf("%s, %v", subject, card)
		}
		if index, ok := event.Data[KeyIndex]; ok {
			subject = fmt.Sprintf("%s, %v, bound: %v", subject, index, event.Data[KeyBound])
		}
		if bindings, ok := event.Data[KeyBindingSize]; ok {
			subject = fmt.Sprintf("%s, %v bindings", subject, bindings)
		}

		scanned, _ := event.Data[KeyDatomsScanned].(int)

		// A populating read answers nobody, so served and matched would both
		// print zero for work that filled the cache.
		if populated, ok := event.Data[KeyEntriesPopulated].(int); ok {
			return fmt.Sprintf("%s Resolve(%s) → %d entries populated (%d scanned)",
				latency, subject, populated, scanned)
		}

		// Not renderScanFunnel. The three terms narrow, and printing this arm's
		// two counts in that frame reads a hit as resolution producing a value
		// out of an intake of zero — which is what a reader would then go
		// looking for a bug in. Served and matched, with intake named as its
		// own number.
		//
		// Matched only where a pattern was the cause: a read handed an (E, A)
		// matches nothing, and a zero printed for it reads as having found none.
		served, _ := event.Data[KeyValuesServed].(int)
		if matched, ok := event.Data[KeyDatomsMatched].(int); ok {
			return fmt.Sprintf("%s Resolve(%s) → %d matched of %d served (%d scanned)",
				latency, subject, matched, served, scanned)
		}
		return fmt.Sprintf("%s Resolve(%s) → %d served (%d scanned)",
			latency, subject, served, scanned)

	default:
		// Generic format for unknown events
		return fmt.Sprintf("%s %s %v", latency, event.Name, event.Data)
	}
}

// renderScanFunnel renders the three counts a scan's completion event carries,
// in the order the query pays them: intake from the index, what CRDT resolution
// produced from it, what survived the pattern and its constraints.
//
// Not every completion event — a resolve has no middle term and has its own arm
// above. Rendering its two counts in this frame reads a cache hit as resolution
// producing a value out of an intake of zero.
//
// Comma-ok reads rather than assertions: an event that omits one of the three
// should render a zero, not panic the formatter mid-trace.
func renderScanFunnel(data map[string]interface{}) string {
	matched, _ := data[KeyDatomsMatched].(int)
	resolved, _ := data[KeyDatomsResolved].(int)
	scanned, _ := data[KeyDatomsScanned].(int)
	return fmt.Sprintf("%d matched, %d resolved, %d scanned", matched, resolved, scanned)
}

// renderPayloadValue renders a value a producer left typed — an IndexType as
// "AEVT", a *query.DataPattern as its bracket form. Those types belong to
// packages that import this one, so they arrive as interface values and render
// through fmt, which reports a panicking String method inline instead of taking
// the trace down with it. A missing key renders empty, which callers here
// distinguish from a value that is present.
func renderPayloadValue(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

// renderBoundPositions renders a scan event's "bound" field — the positions the
// run binds, in the index's component order — as the compact form the scan line
// carries (E, A, V, Tx concatenated, matching the index-name idiom on the same
// line).
//
// Three answers, and the middle one is why the field is read rather than
// assumed. A run binding no positions is a whole-index scan and renders "none".
// An absent field is a producer that reported no run, which the caller renders
// as "?" — a scan the trace cannot account for, not a scan over everything.
// Those two must not collapse: one is a full index read, the other is missing
// instrumentation, and they look the same in a trace that renders both the same
// way.
//
// The distinction survives because an absent map key yields a nil interface
// while a present empty slice does not, so the type assertion below is reached
// only when something was written.
func renderBoundPositions(field interface{}) string {
	if field == nil {
		return ""
	}
	positions, ok := field.([]string)
	if !ok || len(positions) == 0 {
		return "none"
	}
	return strings.Join(positions, "")
}

// formatLatency formats a duration as [XXXms] or [XXXµs] with color coding.
func (f *OutputFormatter) formatLatency(d time.Duration) string {
	// Use microseconds for sub-millisecond durations
	if d < time.Millisecond {
		us := d.Microseconds()
		s := fmt.Sprintf("[%dµs]", us)
		if !f.useColor {
			return s
		}
		return color.GreenString(s)
	}

	// Use floating-point milliseconds to preserve precision
	ms := float64(d.Microseconds()) / 1000.0
	s := fmt.Sprintf("[%.1fms]", ms)

	if !f.useColor {
		return s
	}

	switch {
	case ms < 50:
		return color.GreenString(s)
	case ms < 200:
		return color.YellowString(s)
	default:
		return color.RedString(s)
	}
}

// colorizeCount formats a count with a label, using color based on the label type.
func (f *OutputFormatter) colorizeCount(label string, count int) string {
	text := fmt.Sprintf("%d %s", count, label)

	if !f.useColor {
		return text
	}

	// Different colors for different types
	switch strings.ToLower(label) {
	case "relations":
		return color.CyanString(text)
	case "tuples":
		return color.MagentaString(text)
	case "datoms":
		return color.BlueString(text)
	default:
		return text
	}
}

// colorize applies color if enabled.
func (f *OutputFormatter) colorize(text string, attrs ...color.Attribute) string {
	if !f.useColor {
		return text
	}
	return color.New(attrs...).Sprint(text)
}

// ConsoleHandler creates a handler that prints formatted events to stdout.
func ConsoleHandler() Handler {
	formatter := NewOutputFormatter(os.Stdout)
	return func(event Event) {
		fmt.Fprintln(formatter.writer, formatter.Format(event))
	}
}

// isTerminal reports whether fd refers to an interactive terminal.
// Used to decide whether to emit ANSI color codes: we only want color
// when the output is going to a tty, not when redirected to a file or
// piped to another process.
func isTerminal(fd uintptr) bool {
	return isatty.IsTerminal(fd)
}
