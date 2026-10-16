#!/usr/bin/env bash
# Verify one session end to end: device export (ramp), cloud copy, offline vs online results.
S="${1:?usage: tools/verify.sh SESSION_ID}"
cd "$(dirname "$0")/.."
echo "== device export: ramp values, no silent gaps =="
pipeline export --session "$S" --from device | python3 tools/check_export.py
echo "== cloud copy vs device copy =="
if diff <(pipeline export --session "$S" --from device) <(pipeline export --session "$S" --from cloud) > /tmp/verify.diff; then
  echo "IDENTICAL: the cloud has every sample exactly once"
else
  echo "DIFFERENT: $(grep -c '^<' /tmp/verify.diff) lines only on device, $(grep -c '^>' /tmp/verify.diff) only in cloud"
fi
echo "== offline (device) vs online (cloud) results =="
pipeline results --session "$S" --from cloud | python3 tools/compare_results.py
