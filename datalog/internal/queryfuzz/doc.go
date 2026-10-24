// Package queryfuzz draws query text at random over the whole grammar:
// patterns with a source and a transaction position, predicates, expressions
// and grounds under every binding form, rule invocations, every nested form
// with and without a header, subqueries with every input form, a scalar or an
// aggregate find, :order-by and :limit, and constants of every type, floats of
// integral magnitude among them. A draw from one seed renders the same texts
// every time, so a test names its seeds and the queries follow.
//
// The parser's round-trip test reads it to check that every text the parser
// accepts renders back to itself through every phase, and the storage package's
// mode-parity test reads it to check that the two optimizer paths answer every
// text alike.
package queryfuzz
