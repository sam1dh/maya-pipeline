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

Live check on the device (signal mode): windows 0-3 heart_rate null (w < 4); windows 4-5 = 60.0 bpm (minutes 0-2); windows 212-213 = 90.0 bpm (minutes 2-4); channels 1 and 3 = channel 2 x 0.6 and x 1.3. `python tools/test_processing.py` also compares every window with a full-session peak scan.

## 3. Online mode and network cut (laptop)

- Live result lag: median 0.31 s, 95th percentile 2.01 s.
- During a 60 s outage the device processed 62 windows offline; the cloud's results for the same windows: 0 differences.
- Cloud copy after catch-up: identical to the device copy (`diff`).

## 4. Failure tests on the ARM64 VM

All three are shown in the demo video.

- **Network cut** (QEMU `set_link net0 off`): `MODE -> OFFLINE` about 4-5 s after the cut (14:11:27); back to `MODE -> ONLINE` within seconds of `set_link net0 on`, then catch-up.
- **Process kill** (`kill -9`): systemd restarted the pipeline within about 1 s; it resumed from its committed position with no loss.
- **Power cut** (QEMU process killed; also happened twice unplanned when the laptop overheated): `PRAGMA integrity_check` = ok; the pipeline started by itself at boot, detected the new mock session and went online within 13 s; the unread end of the old session was recorded as `{"to_index": null, "reason": "mock_restarted: unread data of this session"}`; afterwards the cloud copy of that session was IDENTICAL to the device copy (session 94649cd4).
- `tools/verify.sh` on the session that went through all three failures (cec1af43) is shown at the end of the video.

## 5. Resource figures (1 hour, online mode)

```
unit: maya-pipeline   measured over 50.0 minutes
average CPU:          46.64 % of one core (11.66 % of the 4-core device)
peak memory:          67.6 MB
written to disk:      407.8 MB
sent over network:    27.8 MB
received over network: 63.2 MB
```
Environment: QEMU ARM64 VM (Cortex-A72, 4 cores, 2 GB, Debian 13) emulated on an x86 laptop. 'Received' includes the local sensor feed from the mock over loopback.

