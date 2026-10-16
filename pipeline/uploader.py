"""Uploader: streams the device's data to the cloud and decides online/offline mode.

* Every 0.5 s it sends what the cloud does not have yet: the NEWEST chunks first (so the
  cloud can process the current second within 5 s), plus a block of the OLDEST backlog
  (catch-up after an outage). Lost-sample records and offline results are sent too.
* A chunk counts as "in the cloud" only after the cloud confirms its commit. The cloud
  ignores anything it already has, so re-sending after a timeout never duplicates data.
* Mode: ONLINE as soon as a request succeeds; OFFLINE when nothing has succeeded for 4 s.
  A watchdog thread makes that decision, so a request stuck on the network can't delay it.
* DNS lookups cannot be given a timeout in Python, so the cloud's address is resolved in
  the background and every request connects to the last known address.
"""
import base64
import http.client
import json
import logging
import os
import socket
import ssl
import threading
import time
import urllib.parse
import zlib

from .processor import SCHEMA as RESULTS_SCHEMA
from .storage import Storage

log = logging.getLogger("uploader")

LIVE_CHUNKS = 80          # newest not-yet-uploaded chunks per request (~20 s of data)
BACKLOG_CHUNKS = 120      # oldest not-yet-uploaded chunks per request (~30 s of data)
OFFLINE_AFTER = 4.0       # seconds without a successful request -> offline
INTERVAL = 0.5            # seconds between requests when caught up


class _PinnedHTTPS(http.client.HTTPSConnection):
    """HTTPS to `host`, connecting to an already-resolved IP (no DNS lookup on this path)."""
    def __init__(self, host, ip, **kw):
        super().__init__(host, **kw)
        self._ip = ip

    def connect(self):
        sock = socket.create_connection((self._ip, self.port), self.timeout)
        self.sock = self._context.wrap_socket(sock, server_hostname=self.host)


class CloudClient:
    def __init__(self, url, token):
        u = urllib.parse.urlsplit(url)
        self.host, self.port, self.token = u.hostname, u.port or 443, token
        self.ctx = ssl.create_default_context()
        self.ip = None
        threading.Thread(target=self._resolve_loop, name="dns", daemon=True).start()

    def _resolve_loop(self):
        while True:
            try:
                self.ip = socket.getaddrinfo(self.host, self.port, socket.AF_INET,
                                             socket.SOCK_STREAM)[0][4][0]
                time.sleep(300)
            except OSError:
                time.sleep(2)

    def post(self, path, body, timeout):
        if self.ip is None:
            raise ConnectionError("cloud address not resolved yet")
        conn = _PinnedHTTPS(self.host, self.ip, port=self.port, timeout=timeout, context=self.ctx)
        try:
            conn.request("POST", path, body=body, headers={
                "Content-Type": "application/json", "Authorization": f"Bearer {self.token}"})
            resp = conn.getresponse()
            data = resp.read()
            if resp.status != 200:
                raise ConnectionError(f"cloud answered HTTP {resp.status}")
            return json.loads(data)
        finally:
            conn.close()


class Uploader:
    def __init__(self, path, url, token):
        self.path, self.url = path, url
        self.st = Storage(path)
        self.db = self.st.db
        self.db.executescript(RESULTS_SCHEMA +
                              "CREATE TABLE IF NOT EXISTS uploaded (chunk_id INTEGER PRIMARY KEY);")
        self.cloud = CloudClient(url, token)
        self.lock = threading.Lock()
        self.mode = "offline"                  # until the cloud answers
        self.last_ok = float("-inf")
        self.floor = 0                         # every chunk id below this is already in the cloud
        self.last_note = 0.0

    # ---------- mode ----------
    def set_mode(self, st, mode, why):
        if mode == self.mode:
            return
        self.mode = mode
        with st.tx():
            st.db.execute("INSERT OR REPLACE INTO meta VALUES ('mode', ?)", (mode,))
        log.warning("MODE -> %s (%s)", mode.upper(), why)

    def watchdog(self):
        try:
            st = Storage(self.path)
            while True:
                time.sleep(0.5)
                with self.lock:
                    if self.mode == "online" and time.monotonic() - self.last_ok > OFFLINE_AFTER:
                        self.set_mode(st, "offline", f"no successful upload for {OFFLINE_AFTER:.0f} s")
        except Exception:
            log.exception("watchdog crashed")
            os._exit(1)

    # ---------- what to send ----------
    def pending_chunks(self, order, limit, floor=0):
        return self.db.execute(
            "SELECT c.id, c.session_id, c.sensor, c.first_index, c.count, c.data FROM chunks c "
            "LEFT JOIN uploaded u ON u.chunk_id = c.id WHERE u.chunk_id IS NULL AND c.id >= ? "
            f"ORDER BY c.id {order} LIMIT ?", (floor, limit)).fetchall()

    def build(self, with_backlog):
        rows = {r[0]: r for r in self.pending_chunks("DESC", LIVE_CHUNKS)}
        more = len(rows) == LIVE_CHUNKS
        if with_backlog:
            back = self.pending_chunks("ASC", BACKLOG_CHUNKS, self.floor)
            more = more or len(back) == BACKLOG_CHUNKS
            rows.update((r[0], r) for r in back)
        meta = dict(self.db.execute("SELECT key, value FROM meta"))
        loss_wm = int(meta.get("upload_loss_id", 0))
        res_wm = int(meta.get("upload_result_rowid", 0))
        losses = self.db.execute("SELECT id, session_id, sensor, from_index, to_index, reason FROM losses "
                                 "WHERE id > ? ORDER BY id LIMIT 1000", (loss_wm,)).fetchall()
        results = self.db.execute("SELECT rowid, result FROM results WHERE rowid > ? ORDER BY rowid "
                                  "LIMIT 2000", (res_wm,)).fetchall()
        current = meta.get("current_session")
        sessions = []
        for sid, ended in self.db.execute("SELECT session_id, ended FROM sessions").fetchall():
            final = None
            if ended or sid != current:        # no more data will come for this session
                final = min(self.st.next_index(sid, 1), self.st.next_index(sid, 2))
            sessions.append({"session": sid, "final_index": final})
        payload = {
            "chunks": [{"session": s, "sensor": sen, "first_index": fi, "count": n,
                        "data": base64.b64encode(zlib.compress(d)).decode()}
                       for (_, s, sen, fi, n, d) in rows.values()],
            "losses": [{"session": s, "sensor": sen, "from_index": f, "to_index": t, "reason": r}
                       for (_, s, sen, f, t, r) in losses],
            "results": [json.loads(text) for _, text in results],
            "sessions": sessions,
        }
        ack = {"ids": list(rows), "samples": sum(r[4] for r in rows.values()),
               "loss_wm": losses[-1][0] if losses else loss_wm,
               "res_wm": results[-1][0] if results else res_wm}
        return payload, ack, more

    def acknowledge(self, ack):
        with self.st.tx():
            self.db.executemany("INSERT OR IGNORE INTO uploaded VALUES (?)", [(i,) for i in ack["ids"]])
            self.db.execute("INSERT OR REPLACE INTO meta VALUES ('upload_loss_id', ?)", (str(ack["loss_wm"]),))
            self.db.execute("INSERT OR REPLACE INTO meta VALUES ('upload_result_rowid', ?)", (str(ack["res_wm"]),))
            self.db.execute("INSERT INTO meta VALUES ('samples_in_cloud', ?) ON CONFLICT(key) DO UPDATE "
                            "SET value = CAST(value AS INTEGER) + CAST(excluded.value AS INTEGER)",
                            (str(ack["samples"]),))
        self.floor = self.db.execute(
            "SELECT COALESCE(MIN(c.id), (SELECT COALESCE(MAX(id), 0) + 1 FROM chunks)) FROM chunks c "
            "LEFT JOIN uploaded u ON u.chunk_id = c.id WHERE u.chunk_id IS NULL AND c.id >= ?",
            (self.floor,)).fetchone()[0]

    # ---------- main loop ----------
    def run(self):
        threading.Thread(target=self.watchdog, name="watchdog", daemon=True).start()
        log.info("uploader started (cloud: %s)", self.url)
        while True:
            payload, ack, more = self.build(with_backlog=self.mode == "online")
            body = json.dumps(payload).encode()
            try:
                self.cloud.post("/ingest", body, timeout=3.0 + len(body) / 200_000)
            except Exception as e:
                if time.monotonic() - self.last_note > 10:
                    self.last_note = time.monotonic()
                    log.warning("cloud unreachable: %s: %s", type(e).__name__, e)
                time.sleep(1.0)
                continue
            self.acknowledge(ack)
            with self.lock:
                self.last_ok = time.monotonic()
                self.set_mode(self.st, "online", "cloud reachable")
            if more and time.monotonic() - self.last_note > 10:
                self.last_note = time.monotonic()
                saved = self.db.execute("SELECT COALESCE(SUM(count), 0) FROM chunks").fetchone()[0]
                in_cloud = int(dict(self.db.execute("SELECT key, value FROM meta")).get("samples_in_cloud", 0))
                log.info("catching up: %d samples still to upload", saved - in_cloud)
            if not more:
                time.sleep(INTERVAL)
