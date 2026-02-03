package storage

import (
	"sort"

	"github.com/wbrown/janus-datalog/datalog"
)

// ReconstructRGA builds an ordered list from RGA elements.
//
// The algorithm:
//  1. Build a map from AfterRef -> []elements (ALL elements, including tombstoned)
//  2. Sort each child list by ElementID for deterministic order
//  3. DFS from HEAD, emitting values only for non-tombstoned elements
//
// Tombstoned elements are kept in the tree structure so their children remain
// reachable. For example, if elem2 is tombstoned but elem3 has AfterRef=elem2.ID,
// elem3 must still appear in the output.
//
// Time complexity: O(n log n) where n = number of elements
// Space complexity: O(n) for the children map and result slice
//
// CONCURRENT WRITE BEHAVIOR:
//
// When two replicas concurrently insert elements at the same position (same AfterRef),
// BOTH elements are preserved. The order is determined by ElementID comparison:
// lower (Lamport, ReplicaID) comes first.
//
// Example: Two replicas append to an empty vector concurrently:
//
//	Replica A: Add("stealth") → AfterRef=HEAD, ID=(L=5, R=100)
//	Replica B: Add("magic")   → AfterRef=HEAD, ID=(L=5, R=200)
//
// After merge, children[HEAD] = [{stealth, (5,100)}, {magic, (5,200)}]
// Sorted by ElementID: stealth first (100 < 200)
// Result: ["stealth", "magic"]
//
// This is NOT last-writer-wins. All concurrent writes are preserved and
// deterministically ordered. The order is based on ElementID, not wall-clock time.
//
// For same-position updates via Set(), both new values appear adjacent in the
// result because both have the same AfterRef (the element before the changed position).
func ReconstructRGA(elements []RGAElement) []any {
	// Build children map: afterRef -> []elements
	// Include ALL elements (even tombstoned) to maintain tree structure
	children := make(map[datalog.ElementID][]RGAElement)
	for _, e := range elements {
		children[e.AfterRef] = append(children[e.AfterRef], e)
	}

	// Sort each child list by ElementID for deterministic order
	// Lower ElementID comes first (preserves causal ordering within single replica,
	// uses ReplicaID as tiebreaker for concurrent cross-replica inserts)
	for k := range children {
		sort.Slice(children[k], func(i, j int) bool {
			return children[k][i].ID.Less(children[k][j].ID)
		})
	}

	// DFS from HEAD to build ordered result
	// Only emit values for non-tombstoned elements
	var result []any
	var walk func(id datalog.ElementID)
	walk = func(id datalog.ElementID) {
		for _, child := range children[id] {
			if child.Tombstone == nil {
				result = append(result, child.Value)
			}
			// Always walk children, even if this element is tombstoned
			walk(child.ID)
		}
	}
	walk(HEAD) // HEAD is the zero ElementID

	return result
}

// RGAElementWithPosition pairs an element with its position in the reconstructed list.
// Used for building position indexes.
type RGAElementWithPosition struct {
	Element  RGAElement
	Position int
}

// ReconstructRGAWithIDs builds an ordered list with element IDs preserved.
// This is used for building position indexes (position -> ElementID mapping).
//
// Returns a slice of (ElementID, Value) pairs in the correct order.
// Tombstoned elements are excluded from the result but kept in the tree structure
// so their children remain reachable.
func ReconstructRGAWithIDs(elements []RGAElement) []RGAElementWithPosition {
	// Build children map: afterRef -> []elements
	// Include ALL elements (even tombstoned) to maintain tree structure
	children := make(map[datalog.ElementID][]RGAElement)
	for _, e := range elements {
		children[e.AfterRef] = append(children[e.AfterRef], e)
	}

	// Sort each child list by ElementID for deterministic order
	for k := range children {
		sort.Slice(children[k], func(i, j int) bool {
			return children[k][i].ID.Less(children[k][j].ID)
		})
	}

	// DFS from HEAD to build ordered result with positions
	// Only emit non-tombstoned elements
	var result []RGAElementWithPosition
	var walk func(id datalog.ElementID)
	walk = func(id datalog.ElementID) {
		for _, child := range children[id] {
			if child.Tombstone == nil {
				result = append(result, RGAElementWithPosition{
					Element:  child,
					Position: len(result),
				})
			}
			// Always walk children, even if this element is tombstoned
			walk(child.ID)
		}
	}
	walk(HEAD)

	return result
}

// FindMaxElementID returns the highest ElementID among the given elements.
// Used for cache version tracking.
func FindMaxElementID(elements []RGAElement) datalog.ElementID {
	var maxID datalog.ElementID
	for _, elem := range elements {
		if !elem.ID.Less(maxID) {
			maxID = elem.ID
		}
		// Also check tombstone IDs (they may be newer)
		if elem.Tombstone != nil && !elem.Tombstone.Less(maxID) {
			maxID = *elem.Tombstone
		}
	}
	return maxID
}

// RGAStats provides statistics about an RGA collection.
type RGAStats struct {
	TotalElements   int // Total including tombstones
	LiveElements    int // Non-tombstoned elements
	TombstoneCount  int // Number of tombstoned elements
	MaxID           datalog.ElementID
	UniqueAfterRefs int // Number of unique insertion points
}

// ComputeRGAStats calculates statistics for debugging and monitoring.
func ComputeRGAStats(elements []RGAElement) RGAStats {
	stats := RGAStats{TotalElements: len(elements)}

	afterRefs := make(map[datalog.ElementID]bool)

	for _, e := range elements {
		if e.Tombstone == nil {
			stats.LiveElements++
		} else {
			stats.TombstoneCount++
		}

		if !e.ID.Less(stats.MaxID) {
			stats.MaxID = e.ID
		}
		if e.Tombstone != nil && !e.Tombstone.Less(stats.MaxID) {
			stats.MaxID = *e.Tombstone
		}

		afterRefs[e.AfterRef] = true
	}

	stats.UniqueAfterRefs = len(afterRefs)
	return stats
}
