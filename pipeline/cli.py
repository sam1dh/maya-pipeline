"""The `pipeline` command: the device-side interface required by spec Section 4.

  pipeline collect                              run the collector
  pipeline status                               one JSON line: mode, samples saved, in cloud, backlog
  pipeline sessions                             one JSON line per session on the device
  pipeline export --session ID --from device    samples as JSON lines, then one line per loss
  pipeline results --session ID                 per-window results (added in Phase 3)
"""
import argparse
import datetime
import json
import logging
import os
import sqlite3
import struct
import sys

from .storage import WIDTH

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB = os.environ.get("PIPELINE_DB", os.path.join(ROOT, "data", "device.db"))


def open_db(path):
    """Open the device database for reading, or return None if it does not exist yet."""
    if not os.path.exists(path):
        return None
    return sqlite3.connect(path, isolation_level=None, timeout=10)


def read_meta(db):
    return dict(db.execute("SELECT key, value FROM meta"))


def resolve_session(db, wanted):
    if wanted:
        if not db.execute("SELECT 1 FROM sessions WHERE session_id=?", (wanted,)).fetchone():
            sys.exit(f"pipeline: unknown session '{wanted}' (see: pipeline sessions)")
        return wanted
    current = read_meta(db).get("current_session")
    if not current:
        sys.exit("pipeline: no session recorded yet")
    return current


# ---------------------------------------------------------------- commands
def cmd_collect(args):
    from .collector import Collector
    from .storage import Storage
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        Collector(args.host, args.port, Storage(args.db), args.poll).run()
    except KeyboardInterrupt:
        logging.getLogger("collector").info("stopped")


def cmd_status(args):
    db = open_db(args.db)
    if db is None:
        print(json.dumps({"mode": "offline", "session": None, "samples_saved": 0,
                          "samples_in_cloud": 0, "backlog": 0, "samples_lost": 0}))
        return 0
    db.execute("BEGIN")                       # one consistent snapshot while the collector writes
    meta = read_meta(db)
    watermark = int(meta.get("cloud_chunk_watermark", 0))   # set by the uploader (Phase 4)
    saved = db.execute("SELECT COALESCE(SUM(count), 0) FROM chunks").fetchone()[0]
    in_cloud = db.execute("SELECT COALESCE(SUM(count), 0) FROM chunks WHERE id <= ?",
                          (watermark,)).fetchone()[0]
    lost = db.execute("SELECT COALESCE(SUM(to_index - from_index + 1), 0) FROM losses "
                      "WHERE to_index IS NOT NULL").fetchone()[0]
    db.execute("COMMIT")
    print(json.dumps({"mode": meta.get("mode", "offline"), "session": meta.get("current_session"),
                      "samples_saved": saved, "samples_in_cloud": in_cloud,
                      "backlog": saved - in_cloud, "samples_lost": lost}))
    return 0


def cmd_sessions(args):
    db = open_db(args.db)
    if db is None:
        return 0
    rows = db.execute("SELECT session_id, first_seen, ended FROM sessions ORDER BY first_seen").fetchall()
    for sid, first_seen, ended in rows:
        saved = db.execute("SELECT COALESCE(SUM(count), 0) FROM chunks WHERE session_id=?",
                           (sid,)).fetchone()[0]
        lost = db.execute("SELECT COALESCE(SUM(to_index - from_index + 1), 0) FROM losses "
                          "WHERE session_id=? AND to_index IS NOT NULL", (sid,)).fetchone()[0]
        print(json.dumps({
            "session": sid,
            "first_seen": datetime.datetime.fromtimestamp(first_seen).isoformat(timespec="seconds"),
            "ended": bool(ended), "samples_saved": saved, "samples_lost": lost}))
    return 0


def cmd_export(args):
    if args.source == "cloud":
        sys.exit("pipeline: export --from cloud is not available yet (cloud side comes in Phase 4)")
    db = open_db(args.db)
    if db is None:
        sys.exit("pipeline: no device database yet")
    sid = resolve_session(db, args.session)
    write = sys.stdout.write
    db.execute("BEGIN")                       # consistent snapshot
    for sensor in (1, 2):
        w = WIDTH[sensor]
        for fi, cnt, blob in db.execute(
                "SELECT first_index, count, data FROM chunks WHERE session_id=? AND sensor=? "
                "ORDER BY first_index", (sid, sensor)):
            vals = struct.unpack(f"<{cnt * w}i", blob)
            write("".join(
                '{"sensor": %d, "index": %d, "values": [%s]}\n'
                % (sensor, fi + k, ", ".join(map(str, vals[k * w:(k + 1) * w])))
                for k in range(cnt)))
    for sensor, f, t, reason in db.execute(
            "SELECT sensor, from_index, to_index, reason FROM losses WHERE session_id=? "
            "ORDER BY sensor, from_index", (sid,)):
        write(json.dumps({"sensor": sensor, "from_index": f, "to_index": t, "reason": reason}) + "\n")
    db.execute("COMMIT")
    return 0


def cmd_results(args):
    print("pipeline: results are added in Phase 3", file=sys.stderr)
    return 2


# ---------------------------------------------------------------- entry point
def build_parser():
    ap = argparse.ArgumentParser(prog="pipeline", description="Never Lose a Heartbeat: device pipeline")
    ap.add_argument("--db", default=DEFAULT_DB, help=f"device database (default: {DEFAULT_DB})")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("collect", help="read both sensors from the mock and store every sample")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=7100)
    p.add_argument("--poll", type=float, default=0.5)
    p.set_defaults(func=cmd_collect)

    sub.add_parser("status", help="mode, samples saved, in cloud, backlog (one JSON line)") \
        .set_defaults(func=cmd_status)
    sub.add_parser("sessions", help="list sessions on the device").set_defaults(func=cmd_sessions)

    p = sub.add_parser("export", help="samples as JSON lines, then one line per loss")
    p.add_argument("--session", help="session id (default: current session)")
    p.add_argument("--from", dest="source", choices=["device", "cloud"], default="device")
    p.set_defaults(func=cmd_export)

    p = sub.add_parser("results", help="per-window results (Phase 3)")
    p.add_argument("--session")
    p.set_defaults(func=cmd_results)
    return ap


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        sys.exit(args.func(args) or 0)
    except BrokenPipeError:                   # e.g. `pipeline export | head`
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(0)
