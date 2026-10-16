#!/usr/bin/env python3
"""
stand_in_mock.py - temporary stand-in for biosignal-mock, written from
Section 6 of the assessment spec. NOT the official tool: the official mock
download link was blank in the assessment document. Re-test everything with
the real mock if it becomes available.
"""
import argparse, asyncio, json, math, random, secrets, time, zlib

RATE = 400           # samples per second per sensor
BUFFER = 10 * RATE   # mock keeps only the last 10 seconds
MAX_REPLY = RATE     # at most 1 second of data per reply
MOD = 8_388_608      # 2**23

def ramp(sensor, i):
    # Assumption: channels are numbered from 1
    if sensor == 1:
        return [(3 * i + c) % MOD for c in (1, 2, 3)]
    return [(2 * i + c) % MOD for c in (1, 2)]

def beats_since_start(t):
    """Heartbeats since t=0: 60 bpm, then 90, then 72, 2 minutes each, repeating."""
    n, r = divmod(t, 360)
    phase = n * 444                      # beats in one 6-minute cycle
    for bpm in (60, 90, 72):
        seg = min(r, 120)
        phase += seg * bpm / 60
        r -= seg
        if r <= 0:
            break
    return phase

def signal(sensor, i, seed):
    t = i / RATE
    rng = random.Random(seed * 1_000_003 + sensor * 7_919 + i)
    if sensor == 1:
        p = beats_since_start(t)
        f = p - round(p)                 # distance to the nearest beat
        ecg = 1_000_000 * math.exp(-(f / 0.012) ** 2)
        ch2 = int(50_000 + ecg + rng.uniform(-3000, 3000))
        return [int(ch2 * 0.6), ch2, int(ch2 * 1.3)]
    ch1 = int(400_000 + 20_000 * math.sin(2 * math.pi * t / 30) + rng.uniform(-500, 500))
    ch2 = int(150_000 + 10_000 * math.sin(2 * math.pi * t / 45) + rng.uniform(-500, 500))
    return [ch1, ch2]

class Mock:
    def __init__(self, args):
        self.args = args
        self.session = secrets.token_hex(4)
        self.start = time.monotonic()
        self.end_index = args.duration * RATE
        self.cursor = {1: 0, 2: 0}
        self.busy = False

    def produced(self):
        return min(int((time.monotonic() - self.start) * RATE), self.end_index)

    def value(self, sensor, i):
        if self.args.mode == "ramp":
            return ramp(sensor, i)
        return signal(sensor, i, self.args.seed)

    def reply(self, sensor, from_index=None):
        produced = self.produced()
        oldest = max(0, produced - BUFFER)
        start = self.cursor[sensor] if from_index is None else from_index
        lost = []
        if start < oldest:
            lost.append({"from_index": start, "to_index": oldest - 1,
                         "reason": "buffer_overflow"})
            start = oldest
        start = min(start, produced)
        count = min(produced - start, MAX_REPLY)
        data = [self.value(sensor, i) for i in range(start, start + count)]
        self.cursor[sensor] = start + count

        data_text = json.dumps(data)
        crc = format(zlib.crc32(data_text.encode()) & 0xFFFFFFFF, "08x")
        if self.args.faults and random.random() < 0.03:
            crc = format(random.getrandbits(32), "08x")      # corrupt reply

        head = {"session_id": self.session, "sensor": sensor,
                "first_index": start, "count": count, "lost": lost,
                "session_end": self.cursor[sensor] >= self.end_index,
                "crc32": crc}
        return json.dumps(head)[:-1] + ', "data": ' + data_text + "}\n"

async def handle(mock, reader, writer):
    if mock.busy:                         # one client at a time
        writer.close()
        return
    mock.busy = True
    try:
        while line := await reader.readline():
            try:
                req = json.loads(line)
                if req.get("cmd") != "read" or req.get("sensor") not in (1, 2):
                    raise ValueError
            except (ValueError, AttributeError):
                writer.write(b'{"error": "bad request"}\n')
                await writer.drain()
                continue
            if mock.args.faults and random.random() < 0.01:
                await asyncio.sleep(random.uniform(3, 12))   # stop answering
            out = mock.reply(req["sensor"], req.get("from_index")).encode()
            writer.write(out)
            if mock.args.faults and random.random() < 0.03:
                writer.write(out)                             # same reply twice
            await writer.drain()
    except ConnectionError:
        pass
    finally:
        mock.busy = False
        writer.close()

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["signal", "ramp"], default="signal")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--duration", type=int, default=3600)
    p.add_argument("--port", type=int, default=7100)
    p.add_argument("--faults", action="store_true",
                   help="inject corrupt, duplicate and stalled replies")
    args = p.parse_args()
    mock = Mock(args)
    server = await asyncio.start_server(
        lambda r, w: handle(mock, r, w), "0.0.0.0", args.port)
    print(f"stand_in_mock: session {mock.session}, mode {args.mode}, port {args.port}",
          flush=True)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("stand_in_mock: stopped")
