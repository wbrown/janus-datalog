package executor

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
)

// The window reader of a windowed form
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.2, §4.3): it reads the
// subject's iterator into windows of one tuple, then two, doubling to the
// cap, each window holding its tuples and nothing of the subject beyond
// them, in a workspace grown with the window and never past the cap, so a
// result abandoned after its first tuples has paid for the windows read and
// nothing more. It reports each subject tuple read, each window with its ordinal, its tuple
// count and its distinct key count over the key positions, and the subject's
// exhaustion, each under the form it reads for, and it surfaces the subject's
// failure. It reads nothing of the form's branches.

// windowReader reads a subject into windows. window is the current window;
// size is the next window's size, growing by growth to cap; ordinal counts
// the windows read, the window event's ordinal; keyPos is the positions in
// the subject tuple of the required run's symbols it carries, which the
// event's distinct key count is taken over; copies records that the subject
// reuses its tuple workspace, so a tuple is copied on entry; exhausted
// records that the subject is read to its end; err is the subject's failure.
type windowReader struct {
	subject   Iterator
	form      string
	copies    bool
	keyPos    []int
	options   ExecutorOptions
	window    []Tuple
	size      int
	cap       int
	growth    int
	ordinal   int
	exhausted bool
	err       error
}

// newWindowReader is a reader over subject for form, copying each tuple when
// copies, counting the window event's distinct keys over keyPos, under the
// cap and growth of options.
func newWindowReader(subject Iterator, form string, copies bool, keyPos []int, options ExecutorOptions) *windowReader {
	return &windowReader{
		subject: subject,
		form:    form,
		copies:  copies,
		keyPos:  keyPos,
		options: options,
		size:    1,
		cap:     options.windowCap(),
		growth:  options.windowGrowth(),
	}
}

// next reads the next window of the subject, up to size tuples, into the
// workspace grown to size, and reports it. It returns false at the subject's
// end, reporting the exhaustion, and on the subject's failure, with err set.
func (w *windowReader) next() bool {
	if cap(w.window) < w.size {
		w.window = make([]Tuple, 0, w.size)
	} else {
		w.window = w.window[:0]
	}
	for !w.exhausted && len(w.window) < w.size {
		if !w.subject.Next() {
			w.exhausted = true
			if err := w.subject.Error(); err != nil {
				w.err = err
				return false
			}
			break
		}
		tuple := w.subject.Tuple()
		if w.copies {
			tuple = copyTuple(tuple)
		}
		w.reportTuple(tuple)
		w.window = append(w.window, tuple)
	}
	if len(w.window) == 0 {
		w.reportExhausted()
		return false
	}
	w.ordinal++
	w.reportWindow()
	if w.size < w.cap {
		w.size *= w.growth
		if w.size > w.cap {
			w.size = w.cap
		}
	}
	return true
}

// close closes the subject's iterator.
func (w *windowReader) close() error {
	return w.subject.Close()
}

// reportTuple emits the event for a subject tuple read into a window.
func (w *windowReader) reportTuple(tuple Tuple) {
	if w.options.Handler == nil {
		return
	}
	w.options.Handler(annotations.Event{
		Name:  annotations.ScopeSubjectTuple,
		Start: time.Now(),
		Data: map[string]interface{}{
			annotations.KeyForm: w.form,
			"tuple":             fmt.Sprintf("%v", tuple),
		},
	})
}

// reportExhausted emits the event for the subject read to its end.
func (w *windowReader) reportExhausted() {
	if w.options.Handler == nil {
		return
	}
	w.options.Handler(annotations.Event{
		Name:  annotations.ScopeSubjectExhausted,
		Start: time.Now(),
		Data:  map[string]interface{}{annotations.KeyForm: w.form},
	})
}

// reportWindow emits the window event: the form, the window's ordinal, its
// tuple count, and its distinct key count over the key positions.
func (w *windowReader) reportWindow() {
	if w.options.Handler == nil {
		return
	}
	keys := NewTupleKeyMapWithCapacity(len(w.window))
	distinct := 0
	for _, tuple := range w.window {
		if !keys.PutIfAbsentPositions(tuple, w.keyPos, true) {
			distinct++
		}
	}
	w.options.Handler(annotations.Event{
		Name:  annotations.ScopeWindow,
		Start: time.Now(),
		Data: map[string]interface{}{
			annotations.KeyForm: w.form,
			"ordinal":           w.ordinal,
			"tuple_count":       len(w.window),
			"distinct_keys":     distinct,
		},
	})
}
