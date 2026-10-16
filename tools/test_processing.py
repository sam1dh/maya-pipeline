#!/usr/bin/env python3
"""Instant test of the Section 3 processing logic (no need to wait 8 real minutes).

Generates 8 minutes of signal-mode data with the stand-in mock's own generator, then:
  1. prints heart_rate in each period (expect ~60, ~90, ~72, then ~60 again);
  2. compares EVERY window's heart_rate with a straightforward full-session peak scan
     (the literal reading of the spec) to prove the windowed algorithm is exact;
  3. removes some samples and checks missing counts, `complete` and heart_rate = null.
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

from pipeline.processing import PEAK_MIN_GAP, RATE, bpm, candidates, compute_window  # noqa: E402
from stand_in_mock import signal  # noqa: E402

SECONDS = 8 * 60
N = SECONDS * RATE
print(f"generating {SECONDS} s of signal-mode data ...")
s1 = [signal(1, i, 7) for i in range(N)]
s2 = [signal(2, i, 7) for i in range(N)]


def make_get(a, b):
    def get(sensor, lo, hi):
        src = a if sensor == 1 else b
        return [src[i] if 0 <= i < N else None for i in range(lo, hi)]
    return get


get = make_get(s1, s2)

# literal reading of the spec: one greedy scan over the whole session
ecg = [s[1] for s in s1]
full_peaks, last = [], None
for c in candidates(ecg, 0, 0, N - 1):
    if last is None or c - last >= PEAK_MIN_GAP:
        full_peaks.append(c)
        last = c


def full_hr(w):
    if w < 4:
        return None
    pk = [p for p in full_peaks if RATE * (w - 4) <= p < RATE * (w + 1)]
    return bpm(pk) if len(pk) >= 2 else None


print("\n1) heart rate per period")
for w in (3, 4, 30, 100, 121, 123, 125, 180, 239, 243, 300, 360, 420):
    r = compute_window(w, get, "offline")
    print(f"   window {w:3d} ({w // 60}:{w % 60:02d})  heart_rate={r['heart_rate']}")

print("\n   full result for window 30:")
print("   " + json.dumps(compute_window(30, get, "offline")))

print("\n2) windowed algorithm vs full-session scan, all windows")
mismatches = [w for w in range(SECONDS)
              if compute_window(w, get, "offline")["heart_rate"] != full_hr(w)]
print(f"   {SECONDS} windows compared, mismatches: {len(mismatches)} {mismatches[:10]}")

print("\n3) missing data")
s1_gap = list(s1)
for i in range(RATE * 200 + 10, RATE * 200 + 50):     # remove 40 samples of sensor 1 in window 200
    s1_gap[i] = None
s2_gap = list(s2)
for i in range(RATE * 300, RATE * 301):               # remove all of sensor 2 in window 300
    s2_gap[i] = None
g = make_get(s1_gap, s2_gap)
for w in (199, 200, 204, 205, 206):
    r = compute_window(w, g, "offline")
    print(f"   window {w}: missing_1={r['missing_1']} complete={r['complete']} heart_rate={r['heart_rate']}")
r = compute_window(300, g, "offline")
print(f"   window 300: missing_2={r['missing_2']} mean_2={r['mean_2']} magnitude={r['magnitude']}")

print("\nALL OK" if not mismatches else "\nPROBLEMS FOUND")
