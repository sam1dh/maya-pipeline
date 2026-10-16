"""Collector: reads both sensors from the mock and stores every sample crash-safely.

Rules (spec Section 6):
  * Always request data from OUR next durable index (from_index). After a crash, reboot
    or corrupt reply we therefore re-read exactly what is missing.
  * Only accept data that continues exactly at next_index. Older data is a duplicate;
    newer data without an explanation is a gap we ask about again.
  * Losses are recorded only when the mock reports them (or after repeated unexplained
    gaps). Gaps are never filled with invented values.
"""
import argparse
import json
import logging
import socket
import sqlite3
import time

from .protocol import ProtocolError, parse_reply
from .storage import Storage

log = logging.getLogger("collector")

MAX_REPLY = 400        # one second of samples: a full reply means we are behind
READ_TIMEOUT = 5.0     # seconds to wait for a reply before calling the mock silent
MAX_SILENT = 3         # silent replies in a row before reconnecting
GAP_RETRIES = 3        # unexplained gaps before recording them as lost


class Collector:
    def __init__(self, host, port, storage, poll):
        self.host, self.port, self.st, self.poll = host, port, storage, poll
        self.sock = None
        self.buf = bytearray()
        self.session = storage.current_session()
        self.gaps = {}
        self.disk_full = False
        self.corrupt = 0
        self.ended = set()
        self.last_report = time.monotonic()

    # ---------- network ----------
    def connect(self):
        self.close()
        self.sock = socket.create_connection((self.host, self.port), timeout=READ_TIMEOUT)
        self.buf = bytearray()
        log.info("connected to mock at %s:%s", self.host, self.port)

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None

    def request(self, sensor):
        req = {"cmd": "read", "sensor": sensor}
        if self.session is not None:
            req["from_index"] = self.st.next_index(self.session, sensor)
        self.sock.sendall((json.dumps(req) + "\n").encode())

    def read_lines(self):
        """Wait for at least one full line, then also take any extra lines already waiting
        (the mock may send the same reply twice)."""
        deadline = time.monotonic() + READ_TIMEOUT
        while b"\n" not in self.buf:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return []
            self.sock.settimeout(remaining)
            try:
                chunk = self.sock.recv(65536)
            except TimeoutError:
                return []
            if not chunk:
                raise ConnectionError("mock closed the connection")
            self.buf += chunk
        self.sock.setblocking(False)
        try:
            while True:
                chunk = self.sock.recv(65536)
                if not chunk:
                    break
                self.buf += chunk
        except BlockingIOError:
            pass
        finally:
            self.sock.setblocking(True)
        *lines, rest = self.buf.split(b"\n")
        self.buf = bytearray(rest)
        return [bytes(l) for l in lines if l.strip()]

    # ---------- reply handling ----------
    def handle(self, line):
        """Process one reply line. Returns how many samples it carried."""
        try:
            msg = parse_reply(line)
        except ProtocolError as e:
            self.corrupt += 1
            log.warning("discarding reply: %s", e)
            return 0

        sid, sensor = msg["session_id"], msg["sensor"]
        if sid != self.session:
            self.st.begin_session(sid, previous=self.session)
            log.info("new mock session %s (previous session: %s)", sid, self.session)
            self.session = sid

        old = self.st.next_index(sid, sensor)
        cursor = old
        losses = []

        # 1) losses reported by the mock
        for item in sorted(msg["lost"], key=lambda d: d.get("from_index") or 0):
            f, t = item.get("from_index"), item.get("to_index")
            if f is None or (t is not None and t < cursor):
                continue
            reason = str(item.get("reason", "unknown"))
            if self.disk_full:
                reason += " (device disk was full)"
            f = max(f, cursor)
            losses.append((f, t, reason))
            if f == cursor and t is not None:
                cursor = t + 1

        # 2) the samples
        fi, data = msg["first_index"], msg["data"]
        store_at, new = cursor, []
        if fi + len(data) <= cursor:
            pass                                    # duplicate: already stored
        elif fi <= cursor:
            new = data[cursor - fi:]                # keep only the part we don't have yet
        else:
            key = (sid, sensor)                     # unexplained gap: ask again first
            seen_at, times = self.gaps.get(key, (None, 0))
            times = times + 1 if seen_at == cursor else 1
            self.gaps[key] = (cursor, times)
            if times < GAP_RETRIES:
                log.warning("sensor %d: got index %d but expected %d; asking again", sensor, fi, cursor)
            else:
                losses.append((cursor, fi - 1, "gap_not_reported_by_mock"))
                store_at, new = fi, data
                del self.gaps[key]

        new_cursor = store_at + len(new) if new else cursor
        ended = bool(msg.get("session_end")) and new_cursor >= fi + len(data) and sid not in self.ended
        if not losses and not new and not ended and new_cursor == old:
            return len(data)                        # nothing new to save

        try:
            self.st.commit(sid, sensor, losses, store_at, new, new_cursor, ended)
        except sqlite3.OperationalError as e:
            if "full" not in str(e).lower():
                raise
            if not self.disk_full:
                log.error("DISK FULL: cannot save. Waiting for space; samples that overflow the "
                          "mock's 10 s buffer will be recorded as lost with the reason.")
            self.disk_full = True
            time.sleep(1)
            return 0

        if self.disk_full:
            log.info("disk space available again, saving resumed")
            self.disk_full = False
        if ended:
            self.ended.add(sid)
            log.info("session %s ended", sid)
        for f, t, r in losses:
            log.warning("sensor %d: recorded loss %s..%s (%s)", sensor, f, "?" if t is None else t, r)
        return len(data)

    # ---------- main loop ----------
    def loop(self):
        silent = 0
        while True:
            behind = False
            for sensor in (1, 2):
                self.request(sensor)
                lines = self.read_lines()
                if not lines:
                    silent += 1
                    log.warning("mock did not answer within %.0f s (%d in a row)", READ_TIMEOUT, silent)
                    if silent >= MAX_SILENT:
                        raise ConnectionError("mock silent too long, reconnecting")
                    continue
                silent = 0
                for line in lines:
                    if self.handle(line) >= MAX_REPLY:
                        behind = True
            if time.monotonic() - self.last_report >= 10 and self.session:
                self.last_report = time.monotonic()
                log.info("saved up to: sensor1=%d sensor2=%d (corrupt replies discarded: %d)",
                         self.st.next_index(self.session, 1), self.st.next_index(self.session, 2),
                         self.corrupt)
            if not behind:
                time.sleep(self.poll)

    def run(self):
        backoff = 0.5
        while True:
            try:
                self.connect()
                backoff = 0.5
                self.loop()
            except OSError as e:          # includes ConnectionError and timeouts
                log.warning("connection problem: %s; retrying in %.1f s", e, backoff)
                self.close()
                time.sleep(backoff)
                backoff = min(backoff * 2, 2.0)


def main():
    ap = argparse.ArgumentParser(description="Collect sensor data from the mock into the device database.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7100)
    ap.add_argument("--db", default="data/device.db")
    ap.add_argument("--poll", type=float, default=0.5, help="seconds between read rounds when caught up")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        Collector(args.host, args.port, Storage(args.db), args.poll).run()
    except KeyboardInterrupt:
        log.info("stopped")


if __name__ == "__main__":
    main()
