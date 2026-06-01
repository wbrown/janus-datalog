#!/bin/bash
# gate-reset.sh — clears the per-turn gate markers so the next round starts
# fresh. Wired to:
#   - PostToolBatch  (primary: fires once after every batch resolves, including
#                     single-call batches, before the next model call — verified)
#   - UserPromptSubmit, Stop  (safety nets at turn/response boundaries in case
#                     PostToolBatch is ever missed)
#
# Removes both markers used by gate-pretooluse.sh. rmdir only removes a marker if
# present; harmless when already clear.

set -euo pipefail

HOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCK_ROOT="$(dirname "$HOOK_DIR")/.gate-locks"

cat >/dev/null  # drain stdin

rmdir "$LOCK_ROOT/.batch" 2>/dev/null || true
rmdir "$LOCK_ROOT/.cmd" 2>/dev/null || true
exit 0
