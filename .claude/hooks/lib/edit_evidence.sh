#!/bin/bash
# edit_evidence.sh — deterministic evidence computations for review-edit.sh.
#
# Each function turns a proposed Edit/Write into ground truth the reviewing
# model cannot get from the change text alone, and prints it for inclusion in
# the review prompt. All of them print EVIDENCE, never verdicts: the reviewer
# still applies the rule to what it is handed.
#
# Sourced by review-edit.sh and by test/edit_evidence_test.sh. Nothing here
# reads stdin, sets shell options, or exits, so it is safe to source. Targets
# bash 3.2 (macOS /bin/bash).

# extract_func_names: read Go source on stdin, print one top-level func/method
# name per line ("func Name(" and "func (recv Type) Name(" both reduce to
# "Name"). Plain awk — portable to bash 3.2 / BSD awk, no gawk extensions.
extract_func_names() {
    awk '/^func / {
        line = $0
        sub(/^func /, "", line)
        if (line ~ /^\(/) {
            sub(/^\([^)]*\) /, "", line)
        }
        sub(/\(.*/, "", line)
        if (line != "") print line
    }'
}

# compute_test_evidence: deterministic ground truth for Rule 6/7 ("write
# tests first"). The reviewer sees only a BOUNDED tail of the conversation
# transcript, so on a long session the turn that wrote the test has scrolled
# out of view and correct test-first work reads as "no test written".
#
# The check therefore reads the one thing that is complete and current
# regardless of session length: the sibling *_test.go files on disk. Diff old
# against new content for newly-added top-level func/method names, then grep
# the file's own directory for each.
#
# Purely additive to the transcript signal: this never suppresses a real
# violation, it only hands the reviewer verified evidence for what the
# transcript window misses.
compute_test_evidence() {
    local file_path="$1" old_content="$2" new_content="$3"

    case "$file_path" in
        *_test.go)
            return 0
            ;;
        *.go)
            ;;
        *)
            return 0
            ;;
    esac

    local new_funcs old_funcs added_funcs
    new_funcs=$(printf '%s\n' "$new_content" | extract_func_names | sort -u)
    old_funcs=$(printf '%s\n' "$old_content" | extract_func_names | sort -u)
    added_funcs=$(comm -23 <(printf '%s\n' "$new_funcs") <(printf '%s\n' "$old_funcs"))

    if [ -z "$added_funcs" ]; then
        return 0
    fi

    local dir found missing hit tf fn
    dir=$(dirname "$file_path")
    found=""
    missing=""
    while IFS= read -r fn; do
        if [ -z "$fn" ]; then
            continue
        fi
        hit=0
        for tf in "$dir"/*_test.go; do
            if [ -f "$tf" ]; then
                if grep -q -- "$fn" "$tf" 2>/dev/null; then
                    hit=1
                    break
                fi
            fi
        done
        if [ "$hit" = "1" ]; then
            found="$found $fn"
        else
            missing="$missing $fn"
        fi
    done <<< "$added_funcs"

    if [ -n "$found" ]; then
        printf 'DETERMINISTIC TEST-EXISTENCE CHECK (verified against the actual %s/*_test.go files on disk right now, not conversation memory): these newly-added symbols are already referenced by a sibling test file:%s. Treat Rule 6/7 as satisfied for them regardless of whether their test-writing turn is visible in the reasoning context below.\n' "$dir" "$found"
    fi
    if [ -n "$missing" ]; then
        printf 'DETERMINISTIC TEST-EXISTENCE CHECK found NO sibling test-file reference for these newly-added symbols:%s -- Rule 6 applies to them on its own merits.\n' "$missing"
    fi

    return 0
}

# compute_comment_evidence: deterministic ground truth for Rule 9 ("comments
# written for a diff reviewer"). The reviewer receives the whole NEW CODE block
# and can miss a narrating comment buried in a large, otherwise-correct edit, so
# hand it the candidate lines directly.
#
# Only lines this change ADDS are considered; comment text already present is
# not this change's doing. A match is evidence, not a verdict: these words also
# appear in legitimate invariant statements, and the reviewer still applies the
# test in Rule 9.
#
# The vocabulary deliberately includes "rather than" and "instead of", the two
# most common terms, because they are where the violation concentrates —
# contrasting the code with a rejected implementation. They also appear in
# legitimate contract prose, so this check is only worth running against a tree
# that has already been swept; before a sweep the base rate makes every edit
# match and the signal is worthless.
compute_comment_evidence() {
    local old_content="$1" new_content="$2"

    local tells='rather than|instead of|not merely|unlike the|used to|no longer'
    tells="$tells|previously|this read|until 20|would break|would hold|would have"
    tells="$tells|could not tell|cannot tell|still passes|which is how"
    tells="$tells|caught by test|found while implementing|DEPRECATED|TODO"
    tells="$tells|from [0-9]+ .* to [0-9]+"

    local old_comments new_comments added matched
    old_comments=$(printf '%s\n' "$old_content" | grep -E '^[[:space:]]*(//|\*)' | sort -u) \
        || old_comments=""
    new_comments=$(printf '%s\n' "$new_content" | grep -E '^[[:space:]]*(//|\*)' | sort -u) \
        || new_comments=""

    added=$(comm -23 <(printf '%s\n' "$new_comments") <(printf '%s\n' "$old_comments"))
    if [ -z "$added" ]; then
        return 0
    fi

    matched=$(printf '%s\n' "$added" | grep -iE -- "$tells") || matched=""
    if [ -z "$matched" ]; then
        return 0
    fi

    printf 'DETERMINISTIC COMMENT CHECK: these comment lines are added by this change (absent from the prior content) and match the diff-narration vocabulary. Rule 9 applies to them on its own merits — apply its test, do not treat a match as a verdict:\n%s\n' "$matched"

    return 0
}

# compute_deleted_comment_evidence: the counterpart to compute_comment_evidence,
# serving Rule 9's reciprocal. The reviewer needs two things the NEW CODE block
# alone cannot give it: what comment text this change removes, and whether what
# it leaves behind still reads as prose.
#
# Clause-level excision is where the damage concentrates. Removing a whole
# comment is clean; cutting a clause out of the middle leaves a fragment nobody
# re-reads. Two fragment shapes are lexically detectable — a sentence resuming
# in lowercase after a full stop, either on the same line or at the start of the
# next comment line. The rest are semantic (a contrast whose other half went, a
# "still" with no antecedent) and only the reviewer sees them, so a quiet
# fragment scan is not clearance: the removed text prints either way.
#
# Abbreviations are stripped before the mid-line scan so "e.g. the" is not read
# as a sentence boundary. A wrapped sentence resuming after a colon or comma
# never trips the next-line scan, which fires only when the prior line ended a
# sentence.
compute_deleted_comment_evidence() {
    local old_content="$1" new_content="$2"

    local old_comments new_comments removed
    old_comments=$(printf '%s\n' "$old_content" | grep -E '^[[:space:]]*(//|\*)' | sort -u) \
        || old_comments=""
    new_comments=$(printf '%s\n' "$new_content" | grep -E '^[[:space:]]*(//|\*)' | sort -u) \
        || new_comments=""

    removed=$(comm -13 <(printf '%s\n' "$new_comments") <(printf '%s\n' "$old_comments"))
    if [ -z "$removed" ]; then
        return 0
    fi

    printf 'DETERMINISTIC COMMENT-DELETION CHECK: this change removes the comment text below. Rule 9'"'"'s reciprocal governs it — approve if what went argued the change was correct or described a prior state; BLOCK if it stated an invariant, a contract, a derivation, a non-obvious mechanism, or a present-tense fact that should have been restated in the present rather than cut:\n%s\n' "$removed"

    local fragments
    fragments=$(printf '%s\n' "$new_content" | awk '
        /^[[:space:]]*(\/\/|\*)/ {
            text = $0
            sub(/^[[:space:]]*(\/\/|\*)[[:space:]]*/, "", text)
            if (prev_ended && text ~ /^[a-z]/) {
                print "  resumes lowercase after a completed sentence: " $0
            }
            scan = text
            gsub(/[Ee]\.[Gg]\.|[Ii]\.[Ee]\.|[Ee][Tt][Cc]\.|[Vv][Ss]\.|[Cc][Ff]\./, "", scan)
            if (scan ~ /[.!?] [a-z]/) {
                print "  sentence resumes lowercase mid-line: " $0
            }
            prev_ended = (text ~ /[.!?]$/)
            next
        }
        { prev_ended = 0 }
    ')

    if [ -n "$fragments" ]; then
        printf '\nAnd these surviving comment lines read as fragments, the signature of a clause cut out of the middle of a comment. Judge the survivor as well as the deletion:\n%s\n' "$fragments"
    fi

    return 0
}
