# Design note: Never Lose a Heartbeat

## 1. Architecture

    biosignal mock --TCP--> collector --+--> SQLite (WAL, fsync per commit) <-- processor (offline mode)
                                        |                ^
                                        |                +-- uploader --HTTPS--> cloud app (Render)
                                                                                     |-- Postgres (Neon)
                                                                                     +-- processing (online mode)

One process (`pipeline run`, a systemd service restarted 1 s after any failure) runs three threads, each with
its own database connection. The Section 3 logic lives in one module, `pipeline/processing.py`, imported by both
the device and the cloud, so online and offline results cannot diverge (verified: 62 windows processed both ways
during an outage, 0 differences).

**Collector.** Every request asks for data from the collector's own durable position (`from_index` = first index
not yet stored or recorded lost). After a crash, reboot or corrupt reply it therefore re-reads exactly what is
missing, as long as the mock still holds it (10 s). The CRC-32 is checked on the raw bytes of the `data` array as
received, never on re-serialised JSON. Data older than the position is a duplicate and skipped; data that jumps
ahead without a loss report is re-requested (recorded as `gap_not_reported_by_mock` only after 3 attempts).
A new `session_id` records the unread rest of the old session as lost with `to_index: null`.

**Storage.** SQLite in WAL mode with `synchronous=FULL`. Each reply's samples, losses and new position are
written in one transaction, and the COMMIT returns only after fsync. The database can never claim a position
without the data behind it. Samples are stored as packed int32 chunks (one row per reply).

**Processing.** Window *w* is processed once both sensors are settled (stored or recorded lost) up to sample
400(*w*+1), because the peak rule needs sample *i*+1. Rule 4 (100 samples after the previous peak) makes every peak
depend on earlier ones; instead of rescanning the session, the scan starts at a point with no peak candidate in
the 100 samples before it, after which history cannot change the answer. `tools/test_processing.py` shows this
equals a full-session scan for all 480 windows of an 8-minute test.

**Uploader and modes.** Every 0.5 s it sends the newest not-yet-uploaded chunks (~20 s) plus the oldest backlog
(120 chunks), losses and offline results. A chunk is marked uploaded only after the cloud confirms its commit;
the cloud's primary key (session, sensor, first_index) ignores re-sent chunks, so retries never duplicate.
Mode is ONLINE after any successful request and OFFLINE after 4 s without one; a separate watchdog thread makes
that decision so a request hanging on a dead network cannot delay it. The cloud's address is resolved in the
background (Python cannot time out DNS lookups) and requests connect to the last known address. While offline
the device processes windows itself; on going offline it re-processes the last 15 windows, covering the seconds
before the loss was detected.

**Cloud.** Ingest stores everything idempotently and marks affected windows pending. A worker processes pending
windows newest first (live results before backlog), each only when every sample it needs is stored or recorded
lost, and commits each result immediately. The worker sleeps between ingests, letting the free database suspend.

## 2. What is guaranteed

1. Every sample received with a valid CRC is on disk, once and unchanged, before the next request is sent.
2. Every index not on disk is recorded with sensor, range and reason. Values are never invented.
3. Committed data survives process kills, reboots and power cuts (WAL + fsync; atomic transactions).
4. The cloud receives every stored sample and loss exactly once, after any outage (no outage length limit; limited
   only by disk space).
5. Online and offline results are computed by the same code from the same samples.
6. Timing uses monotonic clocks, so system clock changes do not affect collection, uploads or mode switching.

## 3. What is not guaranteed

- Samples the mock discards while the collector is down for more than 10 s are lost (recorded, not silent).
- Durability relies on the storage honouring fsync; some SD cards acknowledge writes early.
- If the disk fills, nothing can be stored until space returns; the overflow is recorded once writing works again.
- Results for windows around a network cut may exist in both modes (offline and online, identical values).
- Cloud free tiers: Render has 0.1 CPU (catch-up processing of very long outages takes minutes) and spins down
  without traffic (mitigated by an uptime monitor); Neon allows 100 compute-hours per month.
- Losing the device's storage itself loses data not yet uploaded.

## 4. Maximum data loss on a power cut: 1 second of samples per sensor

A power cut can only lose samples that were **received but not yet committed**. Each reply is committed (with
fsync) before the next request, and a reply holds at most 1 second of data (spec), typically 0.5 s at our
0.5 s polling rate. So the stated maximum is **1 second per sensor**, usually 0.5 s. Everything committed
survives, because WAL replays complete transactions and discards a partial one. When the board powers up, the
collector resumes from its committed position: if the mock session continues, missing data still in its 10 s
buffer is re-read (loss 0); if the mock restarted with the board, the unread remainder of the old session is
listed as lost with an unknown end, as Section 6.3 requires. The uploader then sends everything the cloud lacks.

## 5. Interpretations and assumptions

- Per-channel statistics are output as `mean_1`/`min_1`/`max_1` (sensor 1, three values) and
  `mean_2`/`min_2`/`max_2` (sensor 2), following the spec's `missing_1`/`missing_2` naming.
- Rounding uses Python's `round(x, 1)`. Heart rate's "any sample missing" rule is applied to sensor 1.
- Result lines also carry `session`; online results add `processed_at` (cloud commit time) as latency evidence.
- Ramp channels are assumed numbered from 1 (affects only the checker, not the pipeline).
- The stand-in mock follows Section 6; the official mock may differ in formatting or signal shape.

## 6. Resource use (1 hour, online mode)

Measured with systemd accounting for the pipeline service only, on the QEMU ARM64 VM described in the README:

| Average CPU | Peak memory | Written to disk | Sent over network |
|---|---|---|---|
| (fill in) | (fill in) | (fill in) | (fill in) |

The VM emulates ARM on x86 (QEMU TCG), which is several times slower than a real Raspberry Pi 4, so CPU use on
the real board should be lower.
