#!/usr/bin/env python3
"""Check a `pipeline export` the way check_ground_truth would, for ramp mode.

Usage:  pipeline export --from device | python tools/check_export.py

Checks every line is in the spec format, every sample value matches the ramp formula,
no index appears twice, and for each sensor the samples plus recorded losses cover
0..end with no gap and no overlap (no silent loss).
"""
import json
import sys

MOD = 8_388_608
WIDTH = {1: 3, 2: 2}
STEP = {1: 3, 2: 2}

indexes = {1: [], 2: []}
losses = {1: [], 2: []}
problems = []

for n, line in enumerate(sys.stdin, 1):
    try:
        rec = json.loads(line)
    except ValueError:
        problems.append(f"line {n}: not valid JSON")
        continue
    s = rec.get("sensor")
    if s not in WIDTH:
        problems.append(f"line {n}: bad sensor {s!r}")
    elif set(rec) == {"sensor", "index", "values"}:
        i, v = rec["index"], rec["values"]
        expected = [(STEP[s] * i + c) % MOD for c in range(1, WIDTH[s] + 1)]
        if v != expected:
            problems.append(f"line {n}: sensor {s} index {i} is {v}, expected {expected}")
        indexes[s].append(i)
    elif set(rec) == {"sensor", "from_index", "to_index", "reason"}:
        losses[s].append((rec["from_index"], rec["to_index"], rec["reason"]))
    else:
        problems.append(f"line {n}: unexpected fields {sorted(rec)}")

for s in (1, 2):
    idx = sorted(indexes[s])
    spans = []
    if idx:
        start = prev = idx[0]
        for i in idx[1:]:
            if i == prev:
                problems.append(f"sensor {s}: index {i} appears more than once")
                continue
            if i != prev + 1:
                spans.append((start, prev + 1, "data"))
                start = i
            prev = i
        spans.append((start, prev + 1, "data"))
    open_end = []
    for f, t, reason in losses[s]:
        if t is None:
            open_end.append(f)
        else:
            spans.append((f, t + 1, "lost"))
    spans.sort()
    pos = 0
    for a, b, kind in spans:
        if a > pos:
            problems.append(f"sensor {s}: SILENT GAP {pos}..{a - 1}")
        elif a < pos:
            problems.append(f"sensor {s}: OVERLAP {a}..{min(b, pos) - 1}")
        pos = max(pos, b)
    lost = sum(t - f + 1 for f, t, _ in losses[s] if t is not None)
    tail = f", then unknown-end loss from {open_end[0]}" if open_end else ""
    print(f"sensor {s}: {len(idx)} samples, {lost} lost in {len(losses[s])} ranges, "
          f"covers 0..{pos - 1}{tail}")

for p in problems[:20]:
    print("  " + p)
if len(problems) > 20:
    print(f"  ... and {len(problems) - 20} more problems")
print("ALL OK" if not problems else f"PROBLEMS FOUND: {len(problems)}")
sys.exit(0 if not problems else 1)
