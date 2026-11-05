package query

// UnderCallSources is an inner query read under the sources its call passes,
// and the arguments the rebuilt call passes. The clauses are renamed by the
// call's source substitution, as a framed scope's are. Each :in source is the
// source standing for it, a source standing at two places declared once, and
// the call passes each under its own name. Any other :in entry keeps its
// place and the arguments of its positions, args being the call's arguments,
// one per input position of the inner :in (InputPositions).
func UnderCallSources(inner *Query, sources Substitution, args []PatternElement) (*Query, []PatternElement) {
	renamed := inner.WithWhere(RenameSources(inner.Where, sources))
	if len(inner.In) == 0 {
		return renamed, args
	}
	renamed.In = nil
	declared := make(map[Symbol]bool, len(inner.In))
	var inputs []PatternElement
	position := 0
	for _, spec := range inner.In {
		width := InputArity([]InputSpec{spec})
		source, isSource := spec.(DatabaseInput)
		if !isSource {
			renamed.In = append(renamed.In, spec)
			inputs = append(inputs, args[position:position+width]...)
			position += width
			continue
		}
		position += width
		name := source.Name
		if standing, renamedHere := sources.Of(name); renamedHere {
			name = standing
		}
		if declared[name] {
			continue
		}
		declared[name] = true
		renamed.In = append(renamed.In, DatabaseInput{Name: name})
		inputs = append(inputs, Constant{Value: name})
	}
	return renamed, inputs
}
