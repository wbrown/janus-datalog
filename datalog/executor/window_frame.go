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
// windows have run, in the key tuples the runs carried, each probed by a
// subject tuple at the subject positions and read at the frame positions
// (docs/proposals/FRAME_RUN_RECORD_PROOF.md), so a key a later window
// carries again is left out of its run; run is the window's run, computed
// once per window for the branches framed alike.
type windowFrame struct {
	required   []query.Symbol
	subjectPos []int // the subject tuple's positions of the required symbols it carries
	framePos   []int // the frame-run position of each subjectPos entry
	envValues  Tuple // the environment's value per frame position, nil where the subject carries the symbol
	ran        *TupleSet
	run        *MaterializedRelation
	runWindow  int
}

// keySymbols is a built branch's probe key: the given symbols the subject
// carries, in the subject's order.
func keySymbols(given, outerSyms []query.Symbol) []query.Symbol {
	var key []query.Symbol
	for _, sym := range outerSyms {
		if query.ContainsSymbol(given, sym) {
			key = append(key, sym)
		}
	}
	return key
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
// symbol the framing says it stands for; a symbol standing for nothing, a
// constant argument's, and one standing for a symbol the subject lacks take
// the environment relation's value under their own name. The branches of
// one form share one standing, so branches framed over one symbol list
// share one frame.
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
			if standsFor := f.standing(sym); standsFor != nil {
				if outerPos := query.SymbolIndex(outerSyms, standsFor); outerPos >= 0 {
					frame.subjectPos = append(frame.subjectPos, outerPos)
					frame.framePos = append(frame.framePos, pos)
					continue
				}
			}
			frame.envValues[pos] = f.environment.Get(0)[query.SymbolIndex(f.environment.Symbols(), sym)]
		}
		if len(frame.subjectPos) > 0 {
			frame.ran = NewTupleSet(frame.subjectPos, frame.framePos, 0)
		}
		frameOf[i] = len(frames)
		frames = append(frames, frame)
	}
	return frames, frameOf
}

// environmentRun is the run of a frame carrying no subject symbol: the
// environment's tuple over the frame's symbols, or nil, the unit relation,
// for a frame with none.
func (f *windowFrame) environmentRun(opts ExecutorOptions) *MaterializedRelation {
	if len(f.required) == 0 {
		return nil
	}
	return newMaterializedRelationFromSet(f.required, []Tuple{f.envValues}, opts, deduplicatedProperties(f.required))
}

// windowRun is the frame's run over a window: the window's distinct
// projection onto the frame's symbols, each tuple decorated with the
// environment's values, less the keys an earlier window ran, computed once
// per window for the branches framed alike.
func (f *windowFrame) windowRun(window []Tuple, ordinal int, opts ExecutorOptions) *MaterializedRelation {
	if f.runWindow == ordinal {
		return f.run
	}
	tuples := make([]Tuple, 0, len(window))
	for _, tuple := range window {
		if f.ran.Probe(tuple) {
			continue
		}
		key := make(Tuple, len(f.required))
		copy(key, f.envValues)
		for i, pos := range f.subjectPos {
			key[f.framePos[i]] = tuple[pos]
		}
		f.ran.Add(key)
		tuples = append(tuples, key)
	}
	f.run = newMaterializedRelationFromSet(f.required, tuples, opts, deduplicatedProperties(f.required))
	f.runWindow = ordinal
	return f.run
}
