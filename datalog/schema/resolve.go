package schema

import "github.com/wbrown/janus-datalog/datalog/query"

// ResolvePullPattern annotates a PullPattern with schema info (cardinality, ref status)
// Schema lookups happen once during resolution, not per-entity during execution.
// If schema is nil or doesn't define an attribute, defaults are used (cardinality-one, not ref).
func ResolvePullPattern(pattern *query.PullPattern, s SchemaProvider) *query.ResolvedPullPattern {
	if pattern == nil {
		return nil
	}
	resolved := &query.ResolvedPullPattern{
		Specs: make([]query.ResolvedPullAttrSpec, 0, len(pattern.Specs)),
	}
	for _, spec := range pattern.Specs {
		resolved.Specs = append(resolved.Specs, resolveSpec(spec, s))
	}
	return resolved
}

// resolveSpec converts a single PullAttrSpec to its resolved form
func resolveSpec(spec query.PullAttrSpec, s SchemaProvider) query.ResolvedPullAttrSpec {
	switch sp := spec.(type) {
	case *query.PullAttribute:
		isMany := s != nil && s.IsMany(sp.Attr)
		isRef := s != nil && s.IsRef(sp.Attr)
		return &query.ResolvedPullAttribute{
			Attr:   sp.Attr,
			IsMany: isMany,
			IsRef:  isRef,
		}

	case *query.PullWildcard:
		return &query.ResolvedPullWildcard{}

	case *query.PullMapSpec:
		isMany := s != nil && s.IsMany(sp.Attr)
		// Map specs are always refs (they follow a reference)
		return &query.ResolvedPullMapSpec{
			Attr:    sp.Attr,
			IsMany:  isMany,
			Pattern: ResolvePullPattern(sp.Pattern, s),
		}

	case *query.PullLimitExpr:
		isMany := s != nil && s.IsMany(sp.Attr)
		isRef := s != nil && s.IsRef(sp.Attr)
		return &query.ResolvedPullLimitExpr{
			Attr:   sp.Attr,
			Limit:  sp.Limit,
			IsMany: isMany,
			IsRef:  isRef,
		}

	case *query.PullDefaultExpr:
		isMany := s != nil && s.IsMany(sp.Attr)
		isRef := s != nil && s.IsRef(sp.Attr)
		return &query.ResolvedPullDefaultExpr{
			Attr:    sp.Attr,
			Default: sp.Default,
			IsMany:  isMany,
			IsRef:   isRef,
		}

	default:
		// Unknown spec type, return nil (shouldn't happen)
		return nil
	}
}
