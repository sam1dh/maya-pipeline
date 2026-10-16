#!/usr/bin/env python3
"""Phase 1 self-check of the device database.

For every session and sensor it verifies that each index below next_index is
covered exactly once: by stored data or by a recorded loss (no gaps, no overlaps).
With --ramp it also checks every stored value against the ramp formula in the
spec, which proves nothing was changed.
"""
import argparse
import sqlite3
import struct

MOD = 8_388_608
WIDTH = {1: 3, 2: 2}
STEP = {1: 3, 2: 2}

ap = argparse.ArgumentParser()
ap.add_argument("--db", default="data/device.db")
ap.add_argument("--ramp", action="store_true", help="also verify values against the ramp formula")
args = ap.parse_args()

db = sqlite3.connect(args.db)
all_ok = True
for sid, sensor, nxt in db.execute(
        "SELECT session_id, sensor, next_index FROM cursors ORDER BY session_id, sensor").fetchall():
    spans, stored, bad_values = [], 0, 0
    for fi, cnt, blob in db.execute(
            "SELECT first_index, count, data FROM chunks WHERE session_id=? AND sensor=? "
            "ORDER BY first_index", (sid, sensor)):
        spans.append((fi, fi + cnt, "data"))
        stored += cnt
        if args.ramp:
            w = WIDTH[sensor]
            vals = struct.unpack(f"<{len(blob) // 4}i", blob)
            for k in range(cnt):
                i = fi + k
                if list(vals[k * w:(k + 1) * w]) != [(STEP[sensor] * i + c) % MOD for c in range(1, w + 1)]:
                    bad_values += 1
    losses = db.execute(
        "SELECT from_index, to_index, reason FROM losses WHERE session_id=? AND sensor=? "
        "ORDER BY from_index", (sid, sensor)).fetchall()
    lost = 0
    for f, t, reason in losses:
        if t is not None:
            spans.append((f, t + 1, "lost"))
            lost += t - f + 1
    spans.sort()
    problems, pos = [], 0
    for a, b, kind in spans:
        if a > pos:
            problems.append(f"GAP {pos}..{a - 1} (neither stored nor recorded as lost)")
        if a < pos:
            problems.append(f"OVERLAP at {a}..{min(b, pos) - 1} ({kind})")
        pos = max(pos, b)
    if pos != nxt:
        problems.append(f"cursor says {nxt} but coverage ends at {pos}")
    if bad_values:
        problems.append(f"{bad_values} samples do not match the ramp formula")
    all_ok = all_ok and not problems
    status = "OK " if not problems else "BAD"
    print(f"[{status}] session {sid} sensor {sensor}: next_index={nxt}, stored={stored}, lost={lost}")
    for f, t, reason in losses:
        print(f"        loss {f}..{t if t is not None else 'unknown end'}: {reason}")
    for p in problems:
        print(f"        {p}")
print("ALL OK" if all_ok else "PROBLEMS FOUND")
