#!/usr/bin/env python3
"""How fast did online results appear in the cloud? (criterion 4: within 5 s)

Usage:  pipeline results --from cloud | python tools/check_latency.py

lag = processed_at - (session start + end of the window). Session start = when the device first
saw the session, so start `pipeline run` BEFORE the mock for an accurate figure. Windows that also
have an offline result fell inside a network outage and are reported separately (catch-up).
"""
import datetime
import json
import os
import sqlite3
import statistics
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
start = dict(sqlite3.connect(os.path.join(ROOT, "data", "device.db"))
             .execute("SELECT session_id, first_seen FROM sessions"))
online, outage = {}, set()
for line in sys.stdin:
    r = json.loads(line)
    key = (r["session"], r["window"])
    if r["mode"] == "offline":
        outage.add(key)
    else:
        done = datetime.datetime.fromisoformat(r["processed_at"]).timestamp()
        online[key] = done - (start[r["session"]] + r["window"] + 1)

live = sorted((lag, k[1]) for k, lag in online.items() if k not in outage)
if live:
    lags = [lag for lag, _ in live]
    late = sorted(w for lag, w in live if lag > 5)
    print(f"live windows: {len(lags)}   median lag {statistics.median(lags):.2f} s   "
          f"95th percentile {lags[int(0.95 * (len(lags) - 1))]:.2f} s   max {lags[-1]:.2f} s")
    print(f"live windows over 5 s: {len(late)} {late[:15]}")
catchup = [lag for k, lag in online.items() if k in outage]
if catchup:
    print(f"outage windows (also processed offline on the device), done during catch-up: "
          f"{len(catchup)}, max lag {max(catchup):.0f} s (expected)")
