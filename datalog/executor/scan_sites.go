package executor

import (
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// scanSites is, by fingerprint, the sites at which a query's plan reads an
// unbound scan. A site is a data pattern of a planned phase, the pointer the
// executor passes to the matcher at every run of that phase. A site under a
// frame that runs once per window of its form's subject repeats: it is read in
// every window until the outermost form on its path ends. The listing a query
// shares holds the fingerprints read at two or more sites or at a repeating
// site: a scan read once at one site is not shared.
type scanSites struct {
	buckets map[uint64][]scanSitesEntry
	// owners counts, per frame plan, the forms the plan holds it as the
	// outermost form's frame of: the plan cache serves one frame plan to two
	// forms of one text, and each form's end counts.
	owners map[*planner.RealizedPlan]int
	// existence is, per pattern the plan reads in windows, the site of its
	// existence check: a copy of the pattern, which the windowed route
	// matches alone before it reads the group (existenceSite).
	existence map[*query.DataPattern]*query.DataPattern
}

type scanSitesEntry struct {
	fingerprint ScanFingerprint
	sites       []*query.DataPattern
	// repeats holds each repeating site's scopes, the outermost forms' frames
	// on its paths; a site it does not hold is read once.
	repeats map[*query.DataPattern][]*planner.RealizedPlan
}

// sharedScanSites lists the sites of plan, its forms' frames
// (RealizedPlan.Frames) and its evaluations' bodies, at every depth: each
// pattern whose plan binds none of its variables at the entity, attribute,
// value or transaction position (PatternPlan.BoundMask), under the
// fingerprint of the query the executor matches it under (patternMatchQuery).
// It keeps the fingerprints read at two or more sites or at a repeating site.
func sharedScanSites(plan *planner.RealizedPlan) scanSites {
	all := scanSites{buckets: map[uint64][]scanSitesEntry{}, owners: map[*planner.RealizedPlan]int{}, existence: map[*query.DataPattern]*query.DataPattern{}}
	all.walk(plan, nil, false)
	shared := scanSites{buckets: map[uint64][]scanSitesEntry{}, owners: all.owners, existence: all.existence}
	for hash, entries := range all.buckets {
		for _, entry := range entries {
			if len(entry.sites) > 1 || len(entry.repeats) > 0 {
				shared.buckets[hash] = append(shared.buckets[hash], entry)
			}
		}
	}
	return shared
}

// walk lists the unbound sites of plan's phases and recurses into the plans
// they run: each form's frames and each evaluation's bodies. A phase's
// unbound sites are its patterns whose plan binds none of their variables,
// and the existence check of each pattern the plan reads in windows, at the
// site the walk makes for it (existenceSite), which the executor matches with
// no bindings under no order or limit. owner is the frame of the outermost
// form on the path,
// nil in the query's own plan, and repeats says a frame on the path runs once
// per window, its branch reading what it is given (FramePlan.ReadsNothing
// false), so a site under it repeats until owner's form ends.
func (s scanSites) walk(plan *planner.RealizedPlan, owner *planner.RealizedPlan, repeats bool) {
	var scope *planner.RealizedPlan
	if repeats {
		scope = owner
	}
	for _, phase := range plan.Phases {
		for _, analyzed := range phase.Patterns {
			pattern, ok := analyzed.Pattern.(*query.DataPattern)
			if !ok {
				continue
			}
			// The mask counts a constant as bound, and a constant is part of
			// the fingerprint, so only a variable bound at the entity,
			// attribute, value or transaction position makes the match bound.
			_, e := pattern.GetE().(query.Variable)
			_, a := pattern.GetA().(query.Variable)
			_, v := pattern.GetV().(query.Variable)
			_, tx := pattern.GetT().(query.Variable)
			mask := analyzed.BoundMask
			if (mask.E && e) || (mask.A && a) || (mask.V && v) || (mask.T && tx) {
				continue
			}
			s.add(FingerprintScan(patternMatchQuery(phase.Query, pattern), pattern), pattern, scope)
		}
		for _, clause := range phase.Query.Where {
			if pattern, ok := clause.(*query.DataPattern); ok {
				if plan.Windows[clause] {
					site := s.existenceSite(pattern)
					s.add(FingerprintScan(&query.Query{}, site), site, scope)
				}
			}
			for _, frame := range plan.Frames[clause] {
				frameOwner := owner
				if frameOwner == nil {
					frameOwner = frame.Plan
					s.owners[frame.Plan]++
				}
				s.walk(frame.Plan, frameOwner, repeats || !frame.ReadsNothing)
			}
		}
	}
	for _, evaluation := range plan.Evaluations {
		for _, stratum := range evaluation.Strata {
			for _, bodies := range [][]planner.PlannedBody{stratum.Bodies.Plain, stratum.Bodies.Delta, stratum.Bodies.Lower} {
				for _, body := range bodies {
					s.walk(body.Plan, owner, repeats)
				}
			}
		}
	}
}

// existenceSite answers the site of a windowed pattern's existence check, a
// copy of the pattern made once, so every walk of the pattern and the route
// that reads it share one site.
func (s scanSites) existenceSite(pattern *query.DataPattern) *query.DataPattern {
	site, made := s.existence[pattern]
	if !made {
		copied := *pattern
		site = &copied
		s.existence[pattern] = site
	}
	return site
}

// add lists site under fingerprint once, with scope among its scopes when
// scope is a repeating site's outermost form's frame; a nil scope adds none.
func (s scanSites) add(fingerprint ScanFingerprint, site *query.DataPattern, scope *planner.RealizedPlan) {
	entries := s.buckets[fingerprint.hash]
	at := -1
	for i := range entries {
		if entries[i].fingerprint.Equal(fingerprint) {
			at = i
			break
		}
	}
	if at < 0 {
		entries = append(entries, scanSitesEntry{fingerprint: fingerprint, repeats: map[*query.DataPattern][]*planner.RealizedPlan{}})
		at = len(entries) - 1
		s.buckets[fingerprint.hash] = entries
	}
	entry := &entries[at]
	listed := false
	for _, known := range entry.sites {
		if known == site {
			listed = true
			break
		}
	}
	if !listed {
		entry.sites = append(entry.sites, site)
	}
	if scope == nil {
		return
	}
	for _, known := range entry.repeats[site] {
		if known == scope {
			return
		}
	}
	entry.repeats[site] = append(entry.repeats[site], scope)
}
