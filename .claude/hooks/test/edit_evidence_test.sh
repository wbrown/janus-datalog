#!/bin/bash
# edit_evidence_test.sh — deterministic tests for the evidence computations in
# lib/edit_evidence.sh. No LLM: each function is driven with crafted Edit
# content and asserted against its output. Targets bash 3.2 (macOS /bin/bash).
#
# The functions run under review-edit.sh's `set -euo pipefail`, so this file
# sets the same options: grep returning 1 on no match, and comm on empty input,
# are where a check like this aborts instead of reporting nothing.
#
# Coverage is organised by the rule each function serves:
#   compute_test_evidence            Rule 6/7, sibling *_test.go on disk
#   compute_comment_evidence         Rule 9, comment text this change ADDS
#   compute_deleted_comment_evidence Rule 9's reciprocal, text it REMOVES and
#                                    whether the survivor still reads as prose

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HOOKS_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
# shellcheck source=../lib/edit_evidence.sh
source "$HOOKS_DIR/lib/edit_evidence.sh"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

pass=0
fail=0

ok()  { printf '  PASS  %s\n' "$1"; pass=$((pass + 1)); }
bad() {
    printf '  FAIL  %s\n' "$1"
    printf '        expected: [%s]\n' "$2"
    printf '        actual:   [%s]\n' "$3"
    fail=$((fail + 1))
}
assert_empty()    { if [ -z "$2" ]; then ok "$1"; else bad "$1" "(empty)" "$2"; fi; }
assert_contains() {
    case "$2" in
        *"$3"*) ok "$1" ;;
        *)      bad "$1" "substring: $3" "$2" ;;
    esac
}
assert_not_contains() {
    case "$2" in
        *"$3"*) bad "$1" "must NOT contain: $3" "$2" ;;
        *)      ok "$1" ;;
    esac
}

# --- compute_test_evidence -------------------------------------------------
printf '\ncompute_test_evidence (Rule 6/7)\n'

GO_OLD='package p

func Existing() int { return 1 }
'
GO_NEW='package p

func Existing() int { return 1 }

func Added() int { return 2 }

func (r Recv) Method() int { return 3 }
'

assert_empty "non-Go path is not scanned" \
    "$(compute_test_evidence "$TMP/notes.txt" "$GO_OLD" "$GO_NEW")"

assert_empty "a test file is not scanned" \
    "$(compute_test_evidence "$TMP/p_test.go" "$GO_OLD" "$GO_NEW")"

assert_empty "no added symbols is quiet" \
    "$(compute_test_evidence "$TMP/p.go" "$GO_OLD" "$GO_OLD")"

# A sibling test naming one of the two added symbols: one found, one missing.
printf 'package p\n\nfunc TestAdded(t *testing.T) { Added() }\n' > "$TMP/p_test.go"
TE=$(compute_test_evidence "$TMP/p.go" "$GO_OLD" "$GO_NEW")
assert_contains "covered symbol is reported as found"   "$TE" "already referenced by a sibling test file: Added"
assert_contains "uncovered symbol is reported missing"  "$TE" "NO sibling test-file reference for these newly-added symbols: Method"
assert_contains "the receiver is stripped from a method name" "$TE" "Method"
assert_not_contains "the receiver type is not reported as a symbol" "$TE" "Recv)"

# --- compute_comment_evidence ----------------------------------------------
printf '\ncompute_comment_evidence (Rule 9, added text)\n'

assert_contains "an added defense-of-a-choice is reported" \
    "$(compute_comment_evidence \
'func f() int {
	return 1
}' \
'// A struct rather than two adjacent ints.
func f() int {
	return 1
}')" "rather than two adjacent ints"

assert_empty "an added comment with no tell is quiet" \
    "$(compute_comment_evidence \
'func f() int {
	return 1
}' \
'// f answers one.
func f() int {
	return 1
}')"

assert_empty "a code-only change leaves the comment alone" \
    "$(compute_comment_evidence \
'// A struct rather than two adjacent ints.
func f() int {
	return 1
}' \
'// A struct rather than two adjacent ints.
func f() int {
	return 2
}')"

assert_empty "no comments on either side is quiet" \
    "$(compute_comment_evidence 'x := 1' 'x := 2')"

assert_contains "a Write to a new file reports its tells" \
    "$(compute_comment_evidence \
'(new file — no prior contents)' \
'// This read require.Positive previously.
func f() {}')" "This read require.Positive previously."

assert_contains "block-comment continuation lines are scanned" \
    "$(compute_comment_evidence \
'func f() {}' \
'/*
 * The handler arrives per call rather than being stored.
 */
func f() {}')" "arrives per call rather than being stored"

assert_empty "a tell inside a string literal is not a comment" \
    "$(compute_comment_evidence \
'func f() string { return "a" }' \
'func f() string { return "used to be a" }')"

# --- compute_deleted_comment_evidence --------------------------------------
printf '\ncompute_deleted_comment_evidence (Rule 9 reciprocal, removed text)\n'

# A clause cut mid-sentence: the survivor resumes lowercase on the same line.
DC=$(compute_deleted_comment_evidence \
'// Deliberately no count of the producers. One was written here and had gone
// stale within the round, which is the same rot the event names were centralised
// to end — a number in prose is a claim about the tree that nothing rechecks.' \
'// Deliberately no count of the producers. a number in prose is a claim about
// the tree that nothing rechecks.')
assert_contains "mid-line resume: removal is reported" "$DC" "COMMENT-DELETION CHECK"
assert_contains "mid-line resume: fragment is flagged" "$DC" "resumes lowercase mid-line"

# A clause cut so the next comment line begins lowercase after a full stop.
DC=$(compute_deleted_comment_evidence \
'// Which strategy performed the scan, on a StorageScanComplete. This is what
// used to be the difference between five event names, moved into the payload
// so one name covers the family and a new strategy is a new value rather
// than a name every consumer must learn.' \
'// Which strategy performed the scan, on a StorageScanComplete.
// one name covers the family and a new strategy is a new value rather
// than a name every consumer must learn.')
assert_contains "next-line resume: removal is reported" "$DC" "COMMENT-DELETION CHECK"
assert_contains "next-line resume: fragment is flagged" "$DC" "resumes lowercase after a completed sentence"

# A dangling contrast term. Semantic, not lexical — the reviewer has to read it,
# so the removal prints and the fragment scan stays quiet. A quiet scan is not
# clearance, which is why these two assertions travel together.
DC=$(compute_deleted_comment_evidence \
'// slot in workerResults — no cross-worker synchronization on output.
//
// Earlier shape: one goroutine per input tuple. The current shape
// uses numWorkers goroutines regardless of input size, one channel' \
'// slot in workerResults — no cross-worker synchronization on output.
//
// The current shape
// uses numWorkers goroutines regardless of input size, one channel')
assert_contains     "dangling contrast: removal is reported" "$DC" "COMMENT-DELETION CHECK"
assert_not_contains "dangling contrast: no lexical fragment" "$DC" "read as fragments"

# A "still" that has lost its antecedent. Semantic as well.
DC=$(compute_deleted_comment_evidence \
'// InternIdentity returns an interned identity instance. All constructors
// intern, so this is normally the identity function.
// It will still intern if somehow given an uninterned identity.' \
'// InternIdentity returns an interned identity instance.
// It will still intern if somehow given an uninterned identity.')
assert_contains     "dangling still: removal is reported" "$DC" "COMMENT-DELETION CHECK"
assert_not_contains "dangling still: no lexical fragment" "$DC" "read as fragments"

# A whole standalone comment removed leaves nothing to break.
DC=$(compute_deleted_comment_evidence \
'	// Materialized mode (original implementation)
	// Use efficient TupleKeyMap for deduplication' \
'	// Materialized mode
	// Use efficient TupleKeyMap for deduplication')
assert_contains     "whole-comment deletion is reported" "$DC" "original implementation"
assert_not_contains "whole-comment deletion leaves no fragment" "$DC" "read as fragments"

assert_empty "a change that removes no comment text is quiet" \
    "$(compute_deleted_comment_evidence \
'	x := 1
	// unchanged comment' \
'	x := 2
	// unchanged comment')"

# An abbreviation must not read as a sentence boundary.
DC=$(compute_deleted_comment_evidence \
'// Applies to the streaming relations. Removed sentence here.
// Callers pass a bound, e.g. the AEVT prefix, and read the run.' \
'// Applies to the streaming relations.
// Callers pass a bound, e.g. the AEVT prefix, and read the run.')
assert_not_contains "an abbreviation is not a fragment" "$DC" "read as fragments"

# A wrapped sentence legitimately resumes lowercase after a colon.
DC=$(compute_deleted_comment_evidence \
'// Pure function of the immutable hash. Cached once upon a time.
// Not cached: every caller is rendering — String, export, the CLI —
// or parse-time.' \
'// Pure function of the immutable hash.
// Not cached: every caller is rendering — String, export, the CLI —
// or parse-time.')
assert_not_contains "a wrapped continuation is not a fragment" "$DC" "read as fragments"

# --- summary ---------------------------------------------------------------
printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
