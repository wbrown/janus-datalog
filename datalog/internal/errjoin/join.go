// Package errjoin combines a function's own error with a second one, a
// Close's or a Rollback's, for the packages under datalog.
package errjoin

import "errors"

// Join answers a function's own error and a second error, a Close's or a
// Rollback's, as one: either alone when the other is nil, and both joined
// when both failed. errors.Join wraps a lone error in a join of one, which
// hides a join the caller enumerates, so a lone error is answered as itself.
func Join(first, second error) error {
	if second == nil {
		return first
	}
	if first == nil {
		return second
	}
	return errors.Join(first, second)
}
