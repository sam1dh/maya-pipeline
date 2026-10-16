#!/usr/bin/env python3
"""Resource use of the pipeline service since it (re)started, from systemd accounting.

Usage (on the device):  python3 tools/resources.py [unit]
"""
import subprocess
import sys
import time

unit = sys.argv[1] if len(sys.argv) > 1 else "maya-pipeline"
props = ["CPUUsageNSec", "MemoryPeak", "IOWriteBytes", "IPEgressBytes", "IPIngressBytes",
         "ActiveEnterTimestampMonotonic"]
out = subprocess.run(["systemctl", "show", unit, "--property=" + ",".join(props)],
                     capture_output=True, text=True, check=True).stdout
v = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)


def num(key):
    try:
        n = int(v.get(key, ""))
    except ValueError:
        return None
    return None if n >= 2 ** 63 else n           # systemd prints 2^64-1 for "not available"


elapsed = time.monotonic() - num("ActiveEnterTimestampMonotonic") / 1e6
cpu = num("CPUUsageNSec")
print(f"unit: {unit}   measured over {elapsed / 60:.1f} minutes")
if cpu is not None:
    pct = cpu / 1e9 / elapsed * 100
    print(f"average CPU:          {pct:.2f} % of one core ({pct / 4:.2f} % of the 4-core device)")
for key, label in (("MemoryPeak", "peak memory:         "), ("IOWriteBytes", "written to disk:     "),
                   ("IPEgressBytes", "sent over network:   "), ("IPIngressBytes", "received over network:")):
    n = num(key)
    print(f"{label} {n / 1e6:.1f} MB" if n is not None else f"{label} not available ({v.get(key)})")
