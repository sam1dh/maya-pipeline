"""Crash-safe storage on the device.

SQLite in WAL mode with synchronous=FULL: every COMMIT is flushed to disk (fsync)
before we continue, so anything committed survives crashes, reboots and power cuts.
Each reply is stored in ONE transaction together with the cursor update, so the
database can never say "stored up to X" without the data actually being there.
"""
import os
import sqlite3
import struct
import time
from contextlib import contextmanager

WIDTH = {1: 3, 2: 2}

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id  TEXT PRIMARY KEY,
    first_seen  REAL NOT NULL,             -- wall clock, informational only
    ended       INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS cursors (
    session_id  TEXT NOT NULL,
    sensor      INTEGER NOT NULL,
    next_index  INTEGER NOT NULL,          -- every index below this is stored or recorded as lost
    PRIMARY KEY (session_id, sensor)
);
CREATE TABLE IF NOT EXISTS chunks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL,
    sensor      INTEGER NOT NULL,
    first_index INTEGER NOT NULL,
    count       INTEGER NOT NULL,
    data        BLOB NOT NULL,             -- little-endian int32 values, count x channels
    UNIQUE (session_id, sensor, first_index)
);
CREATE TABLE IF NOT EXISTS losses (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL,
    sensor      INTEGER NOT NULL,
    from_index  INTEGER NOT NULL,
    to_index    INTEGER,                   -- NULL = end unknown
    reason      TEXT NOT NULL,
    UNIQUE (session_id, sensor, from_index)
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""


def pack(samples):
    flat = [v for s in samples for v in s]
    return struct.pack(f"<{len(flat)}i", *flat)


def unpack(blob, width):
    vals = struct.unpack(f"<{len(blob) // 4}i", blob)
    return [list(vals[i:i + width]) for i in range(0, len(vals), width)]


class Storage:
    def __init__(self, path):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None, timeout=10)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(SCHEMA)

    @contextmanager
    def tx(self):
        """One atomic, durable transaction: all of it is saved, or none of it."""
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise

    def current_session(self):
        row = self.db.execute("SELECT value FROM meta WHERE key='current_session'").fetchone()
        return row[0] if row else None

    def next_index(self, session_id, sensor):
        row = self.db.execute(
            "SELECT next_index FROM cursors WHERE session_id=? AND sensor=?",
            (session_id, sensor)).fetchone()
        return row[0] if row else 0

    def begin_session(self, session_id, previous):
        """The mock has a new session. Unread data of the previous one is lost (end unknown)."""
        with self.tx():
            if previous is not None:
                row = self.db.execute(
                    "SELECT ended FROM sessions WHERE session_id=?", (previous,)).fetchone()
                if row and not row[0]:
                    for sensor in WIDTH:
                        self.db.execute(
                            "INSERT OR IGNORE INTO losses(session_id, sensor, from_index, to_index, reason) "
                            "VALUES (?, ?, ?, NULL, ?)",
                            (previous, sensor, self.next_index(previous, sensor),
                             "mock_restarted: unread data of this session"))
            self.db.execute("INSERT OR IGNORE INTO sessions VALUES (?, ?, 0)", (session_id, time.time()))
            for sensor in WIDTH:
                self.db.execute("INSERT OR IGNORE INTO cursors VALUES (?, ?, 0)", (session_id, sensor))
            self.db.execute("INSERT OR REPLACE INTO meta VALUES ('current_session', ?)", (session_id,))

    def commit(self, session_id, sensor, losses, store_at, samples, next_index, ended):
        """Save one reply's losses + samples + new cursor in a single durable transaction."""
        with self.tx():
            for f, t, reason in losses:
                self.db.execute(
                    "INSERT OR IGNORE INTO losses(session_id, sensor, from_index, to_index, reason) "
                    "VALUES (?, ?, ?, ?, ?)", (session_id, sensor, f, t, reason))
            if samples:
                self.db.execute(
                    "INSERT OR IGNORE INTO chunks(session_id, sensor, first_index, count, data) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (session_id, sensor, store_at, len(samples), pack(samples)))
            self.db.execute(
                "UPDATE cursors SET next_index=? WHERE session_id=? AND sensor=?",
                (next_index, session_id, sensor))
            if ended:
                self.db.execute("UPDATE sessions SET ended=1 WHERE session_id=?", (session_id,))
