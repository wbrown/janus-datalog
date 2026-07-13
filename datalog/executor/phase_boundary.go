package executor

// materializePhaseBoundary makes a non-final phase result reusable while
// preserving its exact symbol sequence, relation properties, tuple-copy
// contract, deferred iterator error, and close error.
func materializePhaseBoundary(group Relation) Relation {
	var tuples []Tuple
	boundaryErr := collectTuplesInto(&tuples, group)
	materialized := NewMaterializedRelationWithProperties(
		group.Symbols(),
		tuples,
		group.Options(),
		group.Properties(),
	)
	materialized.err = boundaryErr
	return materialized
}
