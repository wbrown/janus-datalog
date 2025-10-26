package annotations

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
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
		return fmt.Sprintf("%s Query: %s", latency, truncateQuery(event.Data["query"].(string)))

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

	case "pattern/multi-match":
		pattern := event.Data["pattern"].(string)
		bindingTuples := event.Data["binding.tuples"].(int)
		bindingColumns := event.Data["binding.columns"].([]string)
		totalMatches := event.Data["match.total"].(int)
		scansPerformed := event.Data["scans.performed"].(int)
		datomsScanned := event.Data["datoms.scanned"].(int)

		// Format the binding relation
		bindingRelStr := f.renderer.RenderRelationWithAttrs(bindingColumns, bindingTuples)

		// Format like: MultiMatch([pattern]) with binding Relation → Y tuples (Z scans, W datoms)
		var matchStr string
		if f.useColor {
			matchStr = fmt.Sprintf("%s%s%s",
				color.BlueString("MultiMatch(["),
				color.CyanString(pattern),
				color.BlueString(")"))
		} else {
			matchStr = fmt.Sprintf("MultiMatch([%s])", pattern)
		}

		if f.useColor {
			arrow := color.YellowString(" → ")
			scanInfo := color.RedString(fmt.Sprintf(" (%d scans, %d datoms scanned)", scansPerformed, datomsScanned))
			return fmt.Sprintf("%s %s with binding %s%s%s%s",
				latency,
				matchStr,
				bindingRelStr,
				arrow,
				f.colorizeCount("tuples", totalMatches),
				scanInfo)
		}

		return fmt.Sprintf("%s %s with binding %s → %d tuples (%d scans, %d datoms scanned)",
			latency, matchStr, bindingRelStr, totalMatches, scansPerformed, datomsScanned)

	case "pattern/multi-match-filter":
		pattern := event.Data["pattern"].(string)
		primarySymbol := event.Data["primary.symbol"].(string)
		primaryCount := event.Data["primary.count"].(int)
		totalMatches := event.Data["match.total"].(int)

		// Show all bindings if available
		bindingsStr := fmt.Sprintf("%s=%d", primarySymbol, primaryCount)
		if allBindings, ok := event.Data["all.bindings"].(map[string]int); ok && len(allBindings) > 1 {
			var parts []string
			for sym, count := range allBindings {
				parts = append(parts, fmt.Sprintf("%s=%d", sym, count))
			}
			bindingsStr = fmt.Sprintf("{%s}", strings.Join(parts, ", "))
		}

		// Format like: MultiMatch([pattern]) with bindings {...} → Y tuples
		var matchStr string
		if f.useColor {
			matchStr = fmt.Sprintf("%s%s%s",
				color.BlueString("MultiMatch(["),
				color.CyanString(pattern),
				color.BlueString(")"))
		} else {
			matchStr = fmt.Sprintf("MultiMatch([%s])", pattern)
		}

		if f.useColor {
			arrow := color.YellowString(" → ")
			return fmt.Sprintf("%s %s with bindings %s%s%s",
				latency,
				matchStr,
				bindingsStr,
				arrow,
				f.colorizeCount("tuples", totalMatches))
		}

		return fmt.Sprintf("%s %s with bindings %s → %d tuples",
			latency, matchStr, bindingsStr, totalMatches)

	case MatchesToRelations:
		pattern := event.Data["pattern"].(string)
		matchCount := event.Data["match.count"].(int)

		// Extract bound symbols from the pattern to determine output columns
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
		// Store index info for the next scan event
		f.lastIndex = event.Data["index"].(string)

		// Build bound string from individual fields if needed
		if bound, ok := event.Data["bound"].(string); ok {
			f.lastBound = bound
		} else {
			// Build from individual bound fields
			boundParts := []string{}
			if e, ok := event.Data["bound.e"].(bool); ok {
				boundParts = append(boundParts, fmt.Sprintf("E=%v", e))
			}
			if a, ok := event.Data["bound.a"].(bool); ok {
				boundParts = append(boundParts, fmt.Sprintf("A=%v", a))
			}
			if v, ok := event.Data["bound.v"].(bool); ok {
				boundParts = append(boundParts, fmt.Sprintf("V=%v", v))
			}
			if t, ok := event.Data["bound.t"].(bool); ok {
				boundParts = append(boundParts, fmt.Sprintf("T=%v", t))
			}
			f.lastBound = strings.Join(boundParts, " ")
		}
		return ""

	case PatternStorageScan:
		// Format as Scan([pattern], index, bound) → X datoms in Yms
		pattern := event.Data["pattern"].(string)
		datoms := event.Data["datoms.scanned"].(int)
		duration := event.Data["scan.duration"]

		// Use stored index info if available
		index := f.lastIndex
		bound := f.lastBound
		if index == "" {
			index = "?"
		}
		if bound == "" {
			bound = "?"
		} else {
			// Convert "E=false A=true V=true T=false" to "AV"
			boundParts := []string{}
			if strings.Contains(bound, "E=true") {
				boundParts = append(boundParts, "E")
			}
			if strings.Contains(bound, "A=true") {
				boundParts = append(boundParts, "A")
			}
			if strings.Contains(bound, "V=true") {
				boundParts = append(boundParts, "V")
			}
			if strings.Contains(bound, "T=true") {
				boundParts = append(boundParts, "T")
			}
			if len(boundParts) > 0 {
				bound = strings.Join(boundParts, "")
			} else {
				bound = "none"
			}
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
			return fmt.Sprintf("%s %s%s%s in %v",
				latency,
				scanStr,
				arrow,
				f.colorizeCount("datoms", datoms),
				duration)
		}

		return fmt.Sprintf("%s %s → %d datoms in %v",
			latency, scanStr, datoms, duration)

	case PatternFiltering:
		// Skip - filtering info is redundant with Pattern output
		return ""

	case PatternToRelation:
		// Skip - convert info is redundant
		return ""

	case "pattern/match":
		// Format pattern match with result information
		pattern := event.Data["pattern"].(string)
		matchCount := 0
		if count, ok := event.Data["match.count"].(int); ok {
			matchCount = count
		}

		// Build symbol list if available
		var symbols []string
		if syms, ok := event.Data["symbol.order"].([]string); ok {
			symbols = syms
		}

		// Format as Pattern(...) → Relation([symbols], count)
		var patternStr string
		if f.useColor {
			patternStr = fmt.Sprintf("%s%s%s",
				color.BlueString("Pattern("),
				color.CyanString(pattern),
				color.BlueString(")"))
		} else {
			patternStr = fmt.Sprintf("Pattern(%s)", pattern)
		}

		relationStr := f.renderer.RenderRelationWithAttrs(symbols, matchCount)

		if f.useColor {
			arrow := color.YellowString(" → ")
			return fmt.Sprintf("%s %s%s%s", latency, patternStr, arrow, relationStr)
		}

		return fmt.Sprintf("%s %s → %s", latency, patternStr, relationStr)

	case "pattern/match-with-bindings":
		// Format pattern match that has input bindings
		pattern := event.Data["pattern"].(string)
		bindingCols := event.Data["binding.columns"].([]string)
		bindingSize := event.Data["binding.size"].(int)

		// Format the binding relation
		bindingRelStr := f.renderer.RenderRelationWithAttrs(bindingCols, bindingSize)

		// Format as Pattern(...) with binding Relation([cols], N tuples)
		var patternStr string
		if f.useColor {
			patternStr = fmt.Sprintf("%s%s%s %s %s",
				color.BlueString("Pattern("),
				color.CyanString(pattern),
				color.BlueString(")"),
				"with binding",
				bindingRelStr)
		} else {
			patternStr = fmt.Sprintf("Pattern(%s) with binding %s",
				pattern, bindingRelStr)
		}

		return fmt.Sprintf("%s %s", latency, patternStr)

	case "badger/match-with-bindings":
		// Debug output to see if BadgerMatcher is being used
		pattern := event.Data["pattern"].(string)
		bindings := event.Data["bindings"].(int)
		return fmt.Sprintf("%s BadgerMatcher.MatchWithRelation([%s], %d bindings)",
			latency, pattern, bindings)

	case "expression/evaluate":
		// Format as Expression(...) on X Tuples → X Tuples
		expr := event.Data["expression"].(string)
		inputSize := event.Data["input.size"].(int)
		resultSize := event.Data["result.size"].(int)

		var exprStr string
		if f.useColor {
			exprStr = fmt.Sprintf("%s%s%s",
				color.BlueString("Expression("),
				color.CyanString(expr),
				color.BlueString(")"))
		} else {
			exprStr = fmt.Sprintf("Expression(%s)", expr)
		}

		if f.useColor {
			arrow := color.YellowString(" → ")
			return fmt.Sprintf("%s %s on %s%s%s",
				latency,
				exprStr,
				f.colorizeCount("Tuples", inputSize),
				arrow,
				f.colorizeCount("Tuples", resultSize))
		}

		return fmt.Sprintf("%s %s on %d Tuples → %d Tuples",
			latency, exprStr, inputSize, resultSize)

	case "filter/predicate":
		// Format as Predicate(...) on X Tuples → Y Tuples (filtered Z)
		pred := event.Data["predicate"].(string)
		inputSize := event.Data["input.size"].(int)
		outputSize := event.Data["output.size"].(int)
		filtered := event.Data["filtered"].(int)
		selectivity := event.Data["selectivity"].(float64)

		var predStr string
		if f.useColor {
			predStr = fmt.Sprintf("%s%s%s",
				color.BlueString("Predicate("),
				color.CyanString(pred),
				color.BlueString(")"))
		} else {
			predStr = fmt.Sprintf("Predicate(%s)", pred)
		}

		if f.useColor {
			arrow := color.YellowString(" → ")
			filterInfo := color.RedString(fmt.Sprintf(" (filtered %d, %.1f%% selectivity)", filtered, selectivity*100))
			return fmt.Sprintf("%s %s on %s%s%s%s",
				latency,
				predStr,
				f.colorizeCount("Tuples", inputSize),
				arrow,
				f.colorizeCount("Tuples", outputSize),
				filterInfo)
		}

		return fmt.Sprintf("%s %s on %d Tuples → %d Tuples (filtered %d, %.1f%% selectivity)",
			latency, predStr, inputSize, outputSize, filtered, selectivity*100)

	default:
		// Generic format for unknown events
		return fmt.Sprintf("%s %s %v", latency, event.Name, event.Data)
	}
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

// truncateQuery shortens long queries for display.
func truncateQuery(query string) string {
	// Remove extra whitespace
	query = strings.Join(strings.Fields(query), " ")

	const maxLen = 80
	if len(query) <= maxLen {
		return query
	}

	return query[:maxLen-3] + "..."
}

// ConsoleHandler creates a handler that prints formatted events to stdout.
func ConsoleHandler() Handler {
	formatter := NewOutputFormatter(os.Stdout)
	return func(event Event) {
		fmt.Fprintln(formatter.writer, formatter.Format(event))
	}
}

// isTerminal checks if the file descriptor is a terminal.
// This is a simplified version - in production you'd use a proper terminal detection library.
func isTerminal(fd uintptr) bool {
	// This is platform-specific. For a real implementation,
	// use golang.org/x/term or similar.
	return fd == uintptr(1) || fd == uintptr(2) // stdout or stderr
}
