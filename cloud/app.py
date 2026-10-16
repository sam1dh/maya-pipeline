"""Cloud side of the pipeline (runs on Render, stores data in Neon Postgres).

It keeps exactly one copy of every chunk (primary keys make re-sent data harmless) and
processes every second with the SAME code the device uses offline (pipeline/processing.py),
marking those results "online".

Endpoints (all except /health need the header  Authorization: Bearer <CLOUD_TOKEN>):
  GET  /health             liveness only (does not wake the database)
  POST /ingest             chunks, losses, session info and offline results from the device
  GET  /status             what the cloud holds
  GET  /export?session=    samples as JSON lines, then one line per loss (same format as the device)
  GET  /results?session=   one JSON line per window and mode
"""
import base64
import json
import logging
import os
import struct
import sys
import threading
import time
import zlib

import psycopg
from fastapi import Body, FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipeline.processing import RATE, compute_window  # noqa: E402  (shared with the device)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cloud")

DATABASE_URL = os.environ["DATABASE_URL"]
TOKEN = os.environ["CLOUD_TOKEN"]
WIDTH = {1: 3, 2: 2}
NEED_BEFORE = 4400                        # window w's heart rate reads sensor 1 from 400w - 4400
AFFECTS_AHEAD = NEED_BEFORE // RATE + 1   # so one sample can matter for up to ~12 later windows

SCHEMA = """
CREATE TABLE IF NOT EXISTS chunks (
    session_id  TEXT     NOT NULL,
    sensor      SMALLINT NOT NULL,
    first_index BIGINT   NOT NULL,
    n           INTEGER  NOT NULL,
    data        BYTEA    NOT NULL,              -- little-endian int32, n x channels
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, sensor, first_index)
);
CREATE TABLE IF NOT EXISTS losses (
    session_id TEXT     NOT NULL,
    sensor     SMALLINT NOT NULL,
    from_index BIGINT   NOT NULL,
    to_index   BIGINT,                          -- NULL = end unknown
    reason     TEXT     NOT NULL,
    PRIMARY KEY (session_id, sensor, from_index)
);
CREATE TABLE IF NOT EXISTS sessions (
    session_id  TEXT PRIMARY KEY,
    final_index BIGINT                          -- set once no more data will come
);
CREATE TABLE IF NOT EXISTS results (
    session_id   TEXT   NOT NULL,
    win          BIGINT NOT NULL,
    mode         TEXT   NOT NULL,
    result       TEXT   NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, win, mode)
);
CREATE TABLE IF NOT EXISTS pending (
    session_id TEXT   NOT NULL,
    win        BIGINT NOT NULL,
    PRIMARY KEY (session_id, win)
);
"""

COVERED_SQL = """
SELECT
  (SELECT COALESCE(SUM(LEAST(first_index + n, %(b)s) - GREATEST(first_index, %(a)s)), 0)
     FROM chunks
    WHERE session_id = %(sid)s AND sensor = %(sensor)s
      AND first_index >= %(a)s - 400 AND first_index < %(b)s AND first_index + n > %(a)s)
  +
  (SELECT COALESCE(SUM(LEAST(COALESCE(to_index + 1, %(b)s), %(b)s) - GREATEST(from_index, %(a)s)), 0)
     FROM losses
    WHERE session_id = %(sid)s AND sensor = %(sensor)s
      AND from_index < %(b)s AND COALESCE(to_index + 1, %(b)s) > %(a)s)
"""


def connect():
    return psycopg.connect(DATABASE_URL, prepare_threshold=None, connect_timeout=10)


app = FastAPI(title="Never Lose a Heartbeat - cloud")
ingest_lock = threading.Lock()
ingest_conn = None
work = threading.Event()


def check(authorization):
    if authorization != f"Bearer {TOKEN}":
        raise HTTPException(status_code=401, detail="bad token")


# ---------------------------------------------------------------- processing
class CloudSamples:
    """Sample access for pipeline/processing.py, backed by Postgres."""
    def __init__(self, cur, sid):
        self.cur, self.sid = cur, sid

    def get(self, sensor, lo, hi):
        out = [None] * (hi - lo)
        w = WIDTH[sensor]
        self.cur.execute("SELECT first_index, n, data FROM chunks WHERE session_id=%s AND sensor=%s "
                         "AND first_index >= %s AND first_index < %s", (self.sid, sensor, lo - RATE, hi))
        for fi, n, data in self.cur.fetchall():
            vals = struct.unpack(f"<{n * w}i", zlib.decompress(data))
            for k in range(n):
                if lo <= fi + k < hi:
                    out[fi + k - lo] = list(vals[k * w:(k + 1) * w])
        return out


def covered(cur, sid, sensor, a, b):
    """True if every index in [a, b) is either stored or recorded as lost."""
    if b <= a:
        return True
    cur.execute(COVERED_SQL, {"sid": sid, "sensor": sensor, "a": a, "b": b})
    return cur.fetchone()[0] >= b - a


def ready(cur, sid, w, final):
    end = RATE * (w + 1) + 1                  # the peak rule also needs sample 400(w+1)
    if final is not None:
        end = min(end, final)                 # nothing will ever come after the session's end
    return (covered(cur, sid, 1, max(0, RATE * w - NEED_BEFORE), end)
            and covered(cur, sid, 2, RATE * w, min(RATE * (w + 1), end)))


def process_ready(conn, scan=60):
    """Process ready pending windows, newest first (live results before backlog)."""
    done = 0
    with conn.cursor() as cur:
        cur.execute("SELECT p.session_id, p.win, s.final_index FROM pending p "
                    "LEFT JOIN sessions s USING (session_id) ORDER BY p.win DESC LIMIT %s", (scan,))
        for sid, w, final in cur.fetchall():
            if final is not None and RATE * w >= final:
                cur.execute("DELETE FROM pending WHERE session_id=%s AND win=%s", (sid, w))
                conn.commit()
                continue
            if not ready(cur, sid, w, final):
                continue
            result = {"session": sid, **compute_window(w, CloudSamples(cur, sid).get, "online")}
            cur.execute("INSERT INTO results (session_id, win, mode, result) VALUES (%s, %s, 'online', %s) "
                        "ON CONFLICT DO NOTHING", (sid, w, json.dumps(result)))
            cur.execute("DELETE FROM pending WHERE session_id=%s AND win=%s", (sid, w))
            conn.commit()                     # commit each window: live results appear immediately
            done += 1
    conn.commit()
    return done


def worker():
    conn = None
    while True:
        work.wait()                           # woken by every ingest; sleeps (and lets Neon sleep) otherwise
        work.clear()
        try:
            if conn is None or conn.closed:
                conn = connect()
            while process_ready(conn):
                pass
        except Exception:
            log.exception("worker error")
            try:
                conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(2)
            work.set()


@app.on_event("startup")
def startup():
    with connect() as conn:
        conn.execute(SCHEMA)
    threading.Thread(target=worker, daemon=True).start()
    work.set()                                # finish anything left pending before a restart
    log.info("cloud ready")


# ---------------------------------------------------------------- endpoints
@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    return {"ok": True}


def windows_touched(first, last):
    """Windows whose result can depend on samples first..last."""
    return range(max(0, first // RATE - 1), last // RATE + AFFECTS_AHEAD + 1)


@app.post("/ingest")
def ingest(body: dict = Body(...), authorization: str = Header(default="")):
    global ingest_conn
    check(authorization)
    chunks = [(c["session"], c["sensor"], c["first_index"], c["count"],
               base64.b64decode(c["data"])) for c in body.get("chunks", [])]
    losses = [(x["session"], x["sensor"], x["from_index"], x["to_index"], x["reason"])
              for x in body.get("losses", [])]
    results = [(r["session"], r["window"], json.dumps(r)) for r in body.get("results", [])]
    sessions = [(s["session"], s.get("final_index")) for s in body.get("sessions", [])]

    pending = set()
    for sid, _, fi, n, _ in chunks:
        pending.update((sid, w) for w in windows_touched(fi, fi + n - 1))
    for sid, _, f, t, _ in losses:
        pending.update((sid, w) for w in windows_touched(f, f if t is None else t))
    for sid, final in sessions:
        if final:
            pending.update((sid, w) for w in windows_touched(max(0, final - NEED_BEFORE), final - 1))

    with ingest_lock:
        try:
            if ingest_conn is None or ingest_conn.closed:
                ingest_conn = connect()
            with ingest_conn.cursor() as cur:
                if sessions:
                    cur.executemany(
                        "INSERT INTO sessions (session_id, final_index) VALUES (%s, %s) "
                        "ON CONFLICT (session_id) DO UPDATE "
                        "SET final_index = EXCLUDED.final_index "
                        "WHERE EXCLUDED.final_index IS NOT NULL AND sessions.final_index IS DISTINCT FROM EXCLUDED.final_index", sessions)
                if chunks:
                    cur.executemany("INSERT INTO chunks (session_id, sensor, first_index, n, data) "
                                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", chunks)
                if losses:
                    cur.executemany("INSERT INTO losses (session_id, sensor, from_index, to_index, reason) "
                                    "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", losses)
                if results:
                    cur.executemany("INSERT INTO results (session_id, win, mode, result) "
                                    "VALUES (%s, %s, 'offline', %s) ON CONFLICT DO NOTHING", results)
                if pending:
                    cur.executemany("INSERT INTO pending (session_id, win) VALUES (%s, %s) "
                                    "ON CONFLICT DO NOTHING", sorted(pending))
            ingest_conn.commit()
        except psycopg.Error as e:
            log.error("ingest failed: %s", e)
            try:
                ingest_conn.close()
            except Exception:
                pass
            ingest_conn = None
            raise HTTPException(status_code=503, detail="database unavailable")
    work.set()
    return {"ok": True, "chunks": len(chunks), "losses": len(losses), "results": len(results)}


@app.get("/status")
def status(authorization: str = Header(default="")):
    check(authorization)
    with connect() as conn:
        by_session = dict(conn.execute(
            "SELECT session_id, COALESCE(SUM(n), 0) FROM chunks GROUP BY session_id").fetchall())
        by_mode = dict(conn.execute("SELECT mode, COUNT(*) FROM results GROUP BY mode").fetchall())
        pending = conn.execute("SELECT COUNT(*) FROM pending").fetchone()[0]
    return {"samples_in_cloud": int(sum(by_session.values())),
            "samples_by_session": {k: int(v) for k, v in by_session.items()},
            "results_by_mode": by_mode, "pending_windows": pending}


@app.get("/export")
def export(session: str, authorization: str = Header(default="")):
    check(authorization)

    def lines():
        with connect() as conn:
            for sensor in (1, 2):
                w = WIDTH[sensor]
                with conn.cursor(name=f"export{sensor}") as cur:
                    cur.execute("SELECT first_index, n, data FROM chunks WHERE session_id=%s AND sensor=%s "
                                "ORDER BY first_index", (session, sensor))
                    for fi, n, data in cur:
                        vals = struct.unpack(f"<{n * w}i", zlib.decompress(data))
                        yield "".join('{"sensor": %d, "index": %d, "values": [%s]}\n'
                                      % (sensor, fi + k, ", ".join(map(str, vals[k * w:(k + 1) * w])))
                                      for k in range(n))
            for sensor, f, t, reason in conn.execute(
                    "SELECT sensor, from_index, to_index, reason FROM losses WHERE session_id=%s "
                    "ORDER BY sensor, from_index", (session,)):
                yield json.dumps({"sensor": sensor, "from_index": f, "to_index": t, "reason": reason}) + "\n"

    return StreamingResponse(lines(), media_type="application/x-ndjson")


@app.get("/results")
def results(session: str, authorization: str = Header(default="")):
    check(authorization)

    def lines():
        with connect() as conn:
            for result, mode, processed_at in conn.execute(
                    "SELECT result, mode, processed_at FROM results WHERE session_id=%s ORDER BY win, mode",
                    (session,)):
                r = json.loads(result)
                if mode == "online":
                    r["processed_at"] = processed_at.isoformat()
                yield json.dumps(r) + "\n"

    return StreamingResponse(lines(), media_type="application/x-ndjson")
