"""Section 3 processing logic: ONE implementation, used by the device (offline mode)
and by the cloud (online mode), so both always produce identical results.

Window w holds samples 400w .. 400w+399 of each sensor.
`get(sensor, lo, hi)` must return a list with one entry per index lo..hi-1:
the sample (list of channel values), or None if that sample is not available.
"""
import math
from collections import deque

RATE = 400
PEAK_LOOKBACK = 800        # a peak needs the 800 samples before it to be available
PEAK_MIN_GAP = 100         # and must be at least 100 samples after the previous peak
HR_WINDOWS = 5             # heart rate uses the peaks in the last 5 seconds
SCAN_MARGIN = 2000         # extra history scanned to reproduce the previous-peak rule exactly


def r1(x):
    """Round to 1 decimal place (Python's round(), which rounds exact halves to even)."""
    return None if x is None else round(x, 1)


def channel_stats(samples, width):
    """mean / min / max per channel over the available samples (all None if none)."""
    avail = [s for s in samples if s is not None]
    if not avail:
        return [None] * width, [None] * width, [None] * width
    cols = list(zip(*avail))
    return ([r1(sum(c) / len(c)) for c in cols],
            [min(c) for c in cols],
            [max(c) for c in cols])


def candidates(values, base, a, b):
    """Indexes i in [a, b) that meet peak rules 1-3: the 800 samples before i and sample i+1
    are available, the value is larger than both neighbours, and larger than half of the
    maximum of the 800 samples before it. values[k] is the sample at index base + k."""
    lo = a - PEAK_LOOKBACK
    seg = [values[i - base] if 0 <= i - base < len(values) else None for i in range(lo, b + 1)]
    missing = [0]
    for v in seg:
        missing.append(missing[-1] + (v is None))
    out, dq = [], deque()                     # dq: positions of a running maximum
    for k in range(len(seg) - 1):
        v = seg[k]
        if k >= PEAK_LOOKBACK and v is not None:
            nxt = seg[k + 1]
            if (nxt is not None and missing[k] - missing[k - PEAK_LOOKBACK] == 0
                    and v > seg[k - 1] and v > nxt and 2 * v > seg[dq[0]]):
                out.append(lo + k)
        if v is not None:
            while dq and seg[dq[-1]] <= v:
                dq.pop()
            dq.append(k)
        while dq and dq[0] <= k - PEAK_LOOKBACK:
            dq.popleft()
    return out


def peaks_in(get_ecg, p0, p1):
    """Peaks with index in [p0, p1), exactly as one greedy scan from the session start finds them.

    Rule 4 (>= 100 samples after the previous peak) makes each peak depend on earlier ones.
    We start the scan at a reset point r <= p0 with no candidate in [r-100, r): the previous
    accepted peak is then more than 100 samples before r, so it cannot affect anything after r."""
    margin = SCAN_MARGIN
    while True:
        start = p0 - margin
        lo = start - PEAK_LOOKBACK
        cands = candidates(get_ecg(lo, p1 + 1), lo, start, p1)
        r = p0
        while True:
            blocking = [c for c in cands if r - PEAK_MIN_GAP <= c < r]
            if not blocking:
                break
            r = blocking[0]
        if r - PEAK_MIN_GAP >= start or start <= 0:
            break
        margin *= 2                           # very rare: look further back
    peaks, last = [], None
    for c in cands:
        if c >= r and (last is None or c - last >= PEAK_MIN_GAP):
            peaks.append(c)
            last = c
    return [p for p in peaks if p >= p0]


def bpm(peaks):
    mean_gap = (peaks[-1] - peaks[0]) / (len(peaks) - 1)     # in samples
    return r1(60 / (mean_gap / RATE))


def heart_rate(w, get):
    if w < HR_WINDOWS - 1:
        return None
    p0, p1 = RATE * (w - HR_WINDOWS + 1), RATE * (w + 1)
    if any(s is None for s in get(1, p0, p1)):
        return None
    ecg = lambda lo, hi: [None if s is None else s[1] for s in get(1, lo, hi)]   # sensor 1, channel 2
    peaks = peaks_in(ecg, p0, p1)
    return bpm(peaks) if len(peaks) >= 2 else None


def compute_window(w, get, mode):
    lo, hi = RATE * w, RATE * (w + 1)
    s1, s2 = get(1, lo, hi), get(2, lo, hi)
    mean_1, min_1, max_1 = channel_stats(s1, 3)
    mean_2, min_2, max_2 = channel_stats(s2, 2)
    mags = [math.sqrt(a * a + b * b) for a, b in (s for s in s2 if s is not None)]
    missing_1 = sum(s is None for s in s1)
    missing_2 = sum(s is None for s in s2)
    return {"window": w, "missing_1": missing_1, "missing_2": missing_2,
            "complete": missing_1 == 0 and missing_2 == 0,
            "mean_1": mean_1, "min_1": min_1, "max_1": max_1,
            "mean_2": mean_2, "min_2": min_2, "max_2": max_2,
            "magnitude": r1(sum(mags) / len(mags)) if mags else None,
            "heart_rate": heart_rate(w, get), "mode": mode}
