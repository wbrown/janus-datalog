#!/bin/bash
# run.sh — run the .claude/hooks test harness.
#
#   Tier 1 (review_common_test.sh): deterministic tests of the verdict channel
#           in lib/review_common.sh. No LLM; fast and hermetic.
#   Tier 2 (verdict_quality_test.sh): end-to-end tests that run the real review
#           hooks against the claude CLI to check verdict quality (block/allow).
#
# Both tiers run; the overall exit code is nonzero if either fails. Targets
# bash 3.2 (macOS /bin/bash). Invoke via `make test-hooks`.

set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

rc=0

printf '=== Tier 1: verdict channel (deterministic, no LLM) ===\n'
bash "$SCRIPT_DIR/review_common_test.sh" || rc=1

printf '\n=== Tier 2: verdict quality (real claude CLI) ===\n'
bash "$SCRIPT_DIR/verdict_quality_test.sh" || rc=1

printf '\n'
if [ "$rc" -eq 0 ]; then
    printf 'ALL HOOK TESTS PASSED\n'
else
    printf 'HOOK TESTS FAILED\n'
fi
exit "$rc"
