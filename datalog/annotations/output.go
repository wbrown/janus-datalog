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
type OutputFormatter struct {
	useColor bool
	writer   io.Writer
	renderer *RelationRenderer
	// Temporary storage for combining events
	lastIndex string
	lastBound string
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
		// Store index and bound for the scan event that follows.
		f.lastIndex = renderPayloadValue(event.Data[KeyIndex])
		f.lastBound = renderBoundPositions(event.Data[KeyBound])
		return ""

	case PatternStorageScan:
		// Format as Scan([pattern], index, bound) → X datoms. The scan's
		// duration is the line's latency prefix, carried on the event like
		// every other timed event's; there is no separate duration field.
		//
		// Comma-ok rather than assertions, on the same grounds as
		// renderScanFunnel below: this is the one event with seven producers,
		// and a producer that omits a key should cost the reader one wrong line
		// rather than panic the formatter in the middle of a trace.
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

		// Use stored index info if available. An empty bound means no
		// index-selection event preceded this scan, which is different from a
		// selection that bound nothing — renderBoundPositions reports the
		// latter as "none".
		index := f.lastIndex
		bound := f.lastBound
		if index == "" {
			index = "?"
		}
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

	case PatternHashJoinComplete, PatternMergeJoinComplete:
		// One scan of the index probed against a binding set. Same three counts
		// as the unbound scan line, but all three always shown: on these paths
		// the gap between what was read and what came out is the reason the
		// event exists, so it is not conditional the way the unbound line's
		// amplification suffix is.
		strategy := "HashJoinScan"
		if event.Name == PatternMergeJoinComplete {
			strategy = "MergeJoin"
		}
		return fmt.Sprintf("%s %s([%v], %v, %v bindings) → %s",
			latency, strategy,
			event.Data[KeyPattern], event.Data[KeyIndex], event.Data[KeyBindingSize],
			renderScanFunnel(event.Data))

	case PatternPerBindingScanComplete:
		// One scan per binding, reported once. No index: chooseIndex runs per
		// binding and can pick a different one each time, so a single index
		// here would name whichever happened to be last. The count of scans is
		// the datum this path owes its reader.
		return fmt.Sprintf("%s PerBindingScan([%v], %v scans over %v bindings) → %s",
			latency,
			event.Data[KeyPattern], event.Data[KeyScansOpened], event.Data[KeyBindingSize],
			renderScanFunnel(event.Data))

	case PatternCacheResolveComplete:
		// No index and no bound: the cache picks one by cardinality inside
		// resolution, and a hit reads no index at all. Zero scanned is a hit.
		return fmt.Sprintf("%s CacheResolve([%v], %v) → %s",
			latency,
			event.Data[KeyPattern], event.Data[KeyCardinality],
			renderScanFunnel(event.Data))

	default:
		// Generic format for unknown events
		return fmt.Sprintf("%s %s %v", latency, event.Name, event.Data)
	}
}

// renderScanFunnel renders the three counts every completion event carries, in
// the order the query pays them: intake from the index, what CRDT resolution
// produced from it, what survived the pattern and its constraints.
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

// renderBoundPositions renders a pattern/index-selection event's "bound" field
// — the positions the scan's run binds, in the index's component order — as the
// compact form the scan line carries (E, A, V, Tx concatenated, matching the
// index-name idiom on the same line).
//
// A selection that bound no positions is a whole-index scan and renders
// "none"; the scan line distinguishes that from "?", which means no selection
// event preceded the scan at all.
func renderBoundPositions(field interface{}) string {
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
