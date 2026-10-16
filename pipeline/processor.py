"""Offline processing on the device: computes Section 3 results for every finished window
and stores them with mode "offline". (From Phase 4, while online, the cloud does this.)"""
import json
import logging
import time

from .processing import RATE, compute_window
from .storage import WIDTH, Storage, unpack

log = logging.getLogger("processor")

SCHEMA = """
CREATE TABLE IF NOT EXISTS results (
    session_id TEXT NOT NULL,
    win        INTEGER NOT NULL,
    mode       TEXT NOT NULL,
    result     TEXT NOT NULL,        -- the JSON line printed by `pipeline results`
    created_at REAL NOT NULL,
    PRIMARY KEY (session_id, win, mode)
);
CREATE TABLE IF NOT EXISTS progress (
    session_id  TEXT PRIMARY KEY,
    next_window INTEGER NOT NULL     -- windows below this are done (or left to the cloud)
);
"""


class DeviceSamples:
    """Sample access for the processing module, backed by the device database."""
    def __init__(self, db, session_id):
        self.db, self.sid = db, session_id

    def get(self, sensor, lo, hi):
        out = [None] * (hi - lo)
        width = WIDTH[sensor]
        for fi, blob in self.db.execute(
                "SELECT first_index, data FROM chunks WHERE session_id=? AND sensor=? "
                "AND first_index >= ? AND first_index < ?", (self.sid, sensor, lo - RATE, hi)):
            for k, s in enumerate(unpack(blob, width)):
                if lo <= fi + k < hi:
                    out[fi + k - lo] = s
        return out


class Processor:
    def __init__(self, path, poll=0.5, batch=200):
        self.st = Storage(path)
        self.db = self.st.db
        self.db.executescript(SCHEMA)
        self.poll, self.batch = poll, batch
        self.last_report = 0.0

    def finished_windows(self, sid, current):
        """How many windows of this session are final (their samples can never change)."""
        settled = min(self.st.next_index(sid, 1), self.st.next_index(sid, 2))
        ended = self.db.execute("SELECT ended FROM sessions WHERE session_id=?", (sid,)).fetchone()[0]
        if ended or sid != current:
            return (settled + RATE - 1) // RATE      # no more data will come for this session
        return max(0, (settled - 1) // RATE)         # window w also needs sample 400(w+1)

    def step(self):
        meta = dict(self.db.execute("SELECT key, value FROM meta"))
        current, online = meta.get("current_session"), meta.get("mode") == "online"
        done = 0
        for (sid,) in self.db.execute("SELECT session_id FROM sessions ORDER BY first_seen").fetchall():
            row = self.db.execute("SELECT next_window FROM progress WHERE session_id=?", (sid,)).fetchone()
            nxt = row[0] if row else 0
            end = self.finished_windows(sid, current)
            if nxt >= end:
                continue
            if online:                               # Phase 4: the cloud processes these windows
                with self.st.tx():
                    self.db.execute("INSERT OR REPLACE INTO progress VALUES (?, ?)", (sid, end))
                continue
            src = DeviceSamples(self.db, sid)
            stop = min(end, nxt + self.batch)
            rows = []
            for w in range(nxt, stop):
                result = {"session": sid, **compute_window(w, src.get, "offline")}
                rows.append((sid, w, "offline", json.dumps(result), time.time()))
            with self.st.tx():
                self.db.executemany("INSERT OR REPLACE INTO results VALUES (?, ?, ?, ?, ?)", rows)
                self.db.execute("INSERT OR REPLACE INTO progress VALUES (?, ?)", (sid, stop))
            done += len(rows)
            if time.monotonic() - self.last_report >= 10:
                self.last_report = time.monotonic()
                last = json.loads(rows[-1][3])
                log.info("session %s: processed up to window %d (heart_rate=%s, complete=%s)",
                         sid, stop - 1, last["heart_rate"], last["complete"])
        return done

    def run(self):
        log.info("offline processor started")
        while True:
            if self.step() == 0:
                time.sleep(self.poll)
