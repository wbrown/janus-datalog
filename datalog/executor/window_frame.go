package executor

import "github.com/wbrown/janus-datalog/datalog/query"

// The frames of a windowed form (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md
// §4.3): one frame per distinct required list among the form's branch plans,
// each resolved against the subject's symbols and the environment's tuple,
// giving every window the run the branches framed alike are built over. A
// frame reads nothing of the form, of a branch's clauses or of any branch's
// result.

// windowFrame is a branch frame resolved against the subject and the
// environment: for each required symbol of the frame, its position in the
// subject tuple, or the environment's value when the environment alone
// binds it. A frame carrying no subject symbol has one run for every
// window, the environment's tuple or the unit relation, so its branches
// are built at the first window alone. ran holds the keys the frame's
// windows have run, over the subject positions, so a key a later window
// carries again is left out of its run; run is the window's run, computed
// once per window for the branches framed alike. expandPos marks the frame
// positions bound to one member per element of their value, a subquery's
// collection inputs, which a run carries as one key per member, and
// probeExpandPos the subject positions those of them the subject carries
// read, at which a subject tuple's probe keys expand the same way.
type windowFrame struct {
	required       []query.Symbol
	subjectPos     []int // the subject tuple's positions of the required symbols it carries
	framePos       []int // the frame-run position of each subjectPos entry
	envValues      Tuple // the environment's value per frame position, nil where the subject carries the symbol
	expandPos      []int // the frame positions whose value's members are the keys
	probeExpandPos []int // the subject positions the expanding frame positions read
	ran            *TupleKeyMap
	run            *MaterializedRelation
	runWindow      int
}

// runSymbols orders what a form's branches are given against the subject:
// the given symbols the subject carries lead, in the subject's order, and
// are a built branch's probe key, the second result; the symbols the subject
// lacks, which the environment alone binds, follow.
func runSymbols(given, outerSyms []query.Symbol) (runSyms, joinSyms []query.Symbol) {
	for _, sym := range outerSyms {
		if query.ContainsSymbol(given, sym) {
			runSyms = append(runSyms, sym)
		}
	}
	joinSyms = append([]query.Symbol(nil), runSyms...)
	for _, sym := range given {
		if !query.ContainsSymbol(outerSyms, sym) {
			runSyms = append(runSyms, sym)
		}
	}
	return runSyms, joinSyms
}

// environmentRelation is the environment's tuple over the run symbols the
// environment alone binds, the symbols the subject lacks; nil when the
// context carries no environment or the environment binds none of them.
func environmentRelation(ctx *Context, runSyms, outerSyms []query.Symbol) (Relation, error) {
	env := ctx.Environment()
	if env == nil {
		return nil, nil
	}
	var fromEnvironment []query.Symbol
	for _, sym := range runSyms {
		if !query.ContainsSymbol(outerSyms, sym) && query.ContainsSymbol(env.Symbols(), sym) {
			fromEnvironment = append(fromEnvironment, sym)
		}
	}
	if len(fromEnvironment) == 0 {
		return nil, nil
	}
	return env.Project(fromEnvironment)
}

// windowFrames resolves each distinct frame of plans against the framing's
// subject and its environment relation, and returns the frames with each
// branch's frame by index. A frame symbol is found in the subject by the
// symbol the framing's substitution says it stands for, itself when the
// substitution does not name it, and a symbol the subject lacks takes the
// environment relation's value under its own name. The branches of one form
// share one substitution, so branches framed over one symbol list share one
// frame.
func windowFrames(plans []branchRun, f framing) ([]windowFrame, []int) {
	outerSyms := f.subject.Symbols()
	var frames []windowFrame
	frameOf := make([]int, len(plans))
	for i, plan := range plans {
		required := plan.frame.Required
		found := -1
		for j := range frames {
			if symbolsMatch(frames[j].required, required) {
				found = j
				break
			}
		}
		if found >= 0 {
			frameOf[i] = found
			continue
		}
		frame := windowFrame{required: required, envValues: make(Tuple, len(required))}
		for pos, sym := range required {
			expands := query.ContainsSymbol(f.members, sym)
			if expands {
				frame.expandPos = append(frame.expandPos, pos)
			}
			standsFor := sym
			if to, ok := f.subst.Of(sym); ok {
				standsFor = to
			}
			if outerPos := query.SymbolIndex(outerSyms, standsFor); outerPos >= 0 {
				frame.subjectPos = append(frame.subjectPos, outerPos)
				frame.framePos = append(frame.framePos, pos)
				if expands {
					frame.probeExpandPos = append(frame.probeExpandPos, outerPos)
				}
				continue
			}
			frame.envValues[pos] = f.environment.Get(0)[query.SymbolIndex(f.environment.Symbols(), sym)]
		}
		if len(frame.subjectPos) > 0 {
			frame.ran = NewTupleKeyMap()
		}
		frameOf[i] = len(frames)
		frames = append(frames, frame)
	}
	return frames, frameOf
}

// environmentRun is the run of a frame carrying no subject symbol: the
// environment's tuple over the frame's symbols, one tuple per member at an
// expanding position, or nil, the unit relation, for a frame with none.
func (f *windowFrame) environmentRun(opts ExecutorOptions) *MaterializedRelation {
	if len(f.required) == 0 {
		return nil
	}
	return newMaterializedRelationFromSet(f.required, expandKeys(f.envValues, f.expandPos), opts, deduplicatedProperties(f.required))
}

// windowRun is the frame's run over a window: the window's distinct
// projection onto the frame's symbols, each tuple decorated with the
// environment's values and expanded to one key per member at an expanding
// position, less the keys an earlier window ran, computed once per window
// for the branches framed alike.
func (f *windowFrame) windowRun(window []Tuple, ordinal int, opts ExecutorOptions) *MaterializedRelation {
	if f.runWindow == ordinal {
		return f.run
	}
	var tuples []Tuple
	for _, tuple := range window {
		if f.ran.PutIfAbsentPositions(tuple, f.subjectPos, true) {
			continue
		}
		key := make(Tuple, len(f.required))
		copy(key, f.envValues)
		for i, pos := range f.subjectPos {
			key[f.framePos[i]] = tuple[pos]
		}
		tuples = append(tuples, expandKeys(key, f.expandPos)...)
	}
	f.run = newMaterializedRelationFromSet(f.required, tuples, opts, deduplicatedProperties(f.required))
	f.runWindow = ordinal
	return f.run
}

// probeKeys is the keys a subject tuple stands for at the probe: the tuple
// itself for a frame reading no collection, and otherwise the tuple expanded
// at the subject positions the expanding frame positions read, one key per
// member at each, as the run's keys are expanded.
func (f *windowFrame) probeKeys(tuple Tuple) []Tuple {
	return expandKeys(tuple, f.probeExpandPos)
}

// expandKeys is the keys a tuple stands for: the tuple itself with no
// expanding position, and otherwise one key per member at each expanding
// position, the members of a vector value being its elements and a value of
// any other kind its own one member, as a collection input binds a value.
func expandKeys(tuple Tuple, positions []int) []Tuple {
	keys := []Tuple{tuple}
	for _, pos := range positions {
		var expanded []Tuple
		for _, k := range keys {
			members, isVector := query.VectorElements(k[pos])
			if !isVector {
				members = []interface{}{k[pos]}
			}
			for _, member := range members {
				next := make(Tuple, len(k))
				copy(next, k)
				next[pos] = member
				expanded = append(expanded, next)
			}
		}
		keys = expanded
	}
	return keys
}
