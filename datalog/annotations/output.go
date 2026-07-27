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
		phase := event.Data["phase"]
		delimiter := f.colorize("===", color.FgYellow)
		if count, ok := event.Data["pattern.count"]; ok {
			return fmt.Sprintf("%s %s %s starting with %d patterns",
				latency,
				delimiter,
				phase,
				count)
		}
		return fmt.Sprintf("%s %s %s starting",
			latency,
			delimiter,
			phase)

	case PhaseComplete:
		return fmt.Sprintf("%s %s completed with %s",
			latency,
			event.Data["phase"],
			f.colorizeCount("tuples", event.Data["tuple.count"].(int)))

	case CombineRelsBegin:
		old := event.Data["relations/count-old"].(int)
		new := event.Data["relations/count-new"].(int)
		return fmt.Sprintf("%s Combining %s with %s",
			latency,
			f.colorizeCount("Relations", old),
			f.colorizeCount("Relations", new))

	case CombineRelsCollapsed:
		reduction := event.Data["reduction"].(float64)
		after := event.Data["relations/after"].([]map[string]interface{})
		return fmt.Sprintf("%s Collapsed to %s (%.1f%% reduction)",
			latency,
			f.colorizeCount("Relations", len(after)),
			(1.0-reduction)*100)

	case JoinHash, JoinNested, JoinMerge:
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

	case RelationIndexing:
		return fmt.Sprintf("%s Indexing relation with %s on %v",
			latency,
			f.colorizeCount("tuples", event.Data["relation.size"].(int)),
			event.Data["relation.attrs"])

	case RelationIndexed:
		return fmt.Sprintf("%s Indexed with %s strategy",
			latency,
			event.Data["index.type"])

	case PatternsToRelationsBegin:
		return fmt.Sprintf("%s Converting %d patterns to relations",
			latency,
			event.Data["pattern.count"])

	case PatternsToRelationsRealized:
		return fmt.Sprintf("%s Realized %s with %s",
			latency,
			f.colorizeCount("relations", event.Data["relation.count"].(int)),
			f.colorizeCount("tuples", event.Data["tuple.count"].(int)))

	case MatchesToRelations:
		pattern := event.Data["pattern"].(string)
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
		f.lastIndex, _ = event.Data["index"].(string)
		f.lastBound = renderBoundPositions(event.Data["bound"])
		return ""

	case PatternStorageScan:
		// Format as Scan([pattern], index, bound) → X datoms. The scan's
		// duration is the line's latency prefix, carried on the event like
		// every other timed event's; there is no separate duration field.
		pattern := event.Data["pattern"].(string)
		datoms := event.Data["datoms.resolved"].(int)

		// Intake is appended only when it exceeds what came out. A bound that
		// narrowed and a bound that did nothing produce the same line
		// otherwise, which is the reading this count exists to prevent; a scan
		// that returned everything it read stays one number.
		scanned, _ := event.Data["datoms.scanned"].(int)
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

	case PatternFiltering:
		// Skip - filtering info is redundant with Pattern output
		return ""

	case PatternToRelation:
		// Skip - convert info is redundant
		return ""

	default:
		// Generic format for unknown events
		return fmt.Sprintf("%s %s %v", latency, event.Name, event.Data)
	}
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
