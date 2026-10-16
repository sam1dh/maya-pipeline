# Test evidence

All tests use the stand-in mock (`tools/stand_in_mock.py`). Laptop: Arch Linux x86-64. Device: QEMU ARM64 VM
(Cortex-A72, 4 cores, 2 GB, Debian 13). Cloud: Render + Neon.

## 1. Device storage (laptop, ramp mode)

| Test | Result |
|---|---|
| Normal run | ALL OK, lost = 0 |
| Crash (`kill -9`), restart within 5 s | ALL OK, lost = 0 |
| Crash, restart after 15 s | ALL OK, ~5 s per sensor recorded as `buffer_overflow` |
| Mock restart + `--faults` (corrupt, duplicate, stalled replies) | ALL OK; corrupt replies discarded and re-read; old session recorded lost with unknown end |
| Export check (`check_export.py`, ~80,000 samples) | ALL OK |

## 2. Processing (`tools/test_processing.py`)

(paste output)

## 3. Online mode and network cut (laptop)

- Live result lag: median 0.31 s, 95th percentile 2.01 s.
- During a 60 s outage the device processed 62 windows offline; the cloud's results for the same windows: 0 differences.
- Cloud copy after catch-up: identical to the device copy (`diff`).

## 4. Failure tests on the ARM64 VM

(filled in Phase 5B: network cut, process kill, power cut, reboot, disk full)

## 5. Resource figures (1 hour, online mode)

(paste `python3 tools/resources.py` output)

