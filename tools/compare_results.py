#!/usr/bin/env python3
"""Prove that offline (device) and online (cloud) processing give identical results.

Usage:  pipeline results --from cloud | python tools/compare_results.py
"""
import json
import sys

by = {}
for line in sys.stdin:
    r = json.loads(line)
    by.setdefault((r["session"], r["window"]), {})[r["mode"]] = r

pairs = [k for k, m in by.items() if len(m) == 2]
diffs = []
for k in pairs:
    a, b = dict(by[k]["offline"]), dict(by[k]["online"])
    for d in (a, b):
        d.pop("mode", None)
        d.pop("processed_at", None)
    if a != b:
        diffs.append(k[1])

n_online = sum("online" in m for m in by.values())
n_offline = sum("offline" in m for m in by.values())
top = max((w for _, w in by), default=-1)
no_online = [w for w in range(top + 1) if not any(s_w == (s, w) and "online" in by[s_w]
                                                   for s_w in by for s in [s_w[0]])][:10]
print(f"windows: {len(by)}  online results: {n_online}  offline results: {n_offline}  "
      f"processed both ways: {len(pairs)}")
print(f"offline vs online differences: {len(diffs)} {sorted(diffs)[:10]}")
print("ALL OK" if not diffs else "PROBLEMS FOUND")
