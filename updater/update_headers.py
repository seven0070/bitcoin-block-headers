#!/usr/bin/env python3
"""
update_headers.py - keep bitcoin_block_headers.sqlite filled as new blocks are mined.

Python 3.8+ standard library only (no pip installs). Works on Windows, macOS and Linux.

What one run does:
  1. Opens the database and reads the highest mined height (the stored tip).
  2. Asks several public Electrum servers (SSL JSON-RPC) and mempool.space /
     blockstream.info (Esplora REST) for the current chain tip.
  3. Checks the stored tip is still on the best chain. If it is not (a reorg), it
     finds the fork point, and the stale rows are rolled back and refilled.
  4. Downloads the new headers and verifies every one: prev-hash link, double-SHA256
     block hash, proof of work vs bits, difficulty retarget rule at every 2016-block
     boundary, median-time-past rule, version >= 4, time <= now + 2 h.
  5. Cross-checks the new tip hash against at least 2 other independent sources.
  6. In ONE transaction: writes the real header fields into those rows (mined = 1,
     constraint columns NULL), then recomputes the constraint representation of every
     unmined row (next-block class, known bits, time_min, estimated time,
     header_constraints, difficulty_periods) and the metadata tip.
     If anything fails, nothing is written.

Usage:
  python update_headers.py                      # one update
  python update_headers.py --watch              # update every 10 minutes until Ctrl+C
  python update_headers.py --db path/to/bitcoin_block_headers.sqlite
"""
import argparse
import bisect
import datetime
import hashlib
import json
import math
import os
import random
import socket
import ssl
import sqlite3
import struct
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

SCRIPT_VERSION = "1.0"
LAST_HEIGHT = 6930000             # last row in the database
U32 = 2 ** 32 - 1                 # nTime ceiling (2106-02-07 06:28:15 UTC)
RETARGET = 2016
TARGET_TIMESPAN = 14 * 24 * 3600  # 1,209,600 s
POW_LIMIT_BITS = 0x1D00FFFF
GENESIS_HASH = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
MIN_CONFIRMATIONS = 2             # other sources that must report the same tip hash
MAX_REORG_DEPTH = 2016
TIMEOUT = 20

ELECTRUM_SERVERS = [
    ("electrum.blockstream.info", 50002),
    ("fortress.qtornado.com", 443),
    ("bitcoin.aranguren.org", 50002),
    ("electrum.bitaroo.net", 50002),
    ("electrum.acinq.co", 50002),
    ("electrum.loyce.club", 50002),
    ("bolt.schulzemic.net", 50002),
    ("electrum.petrkr.net", 50002),
    ("blackie.c3-soft.com", 57002),
    ("electrum1.bluewallet.io", 443),
    ("electrum3.bluewallet.io", 443),
    ("e.keff.org", 50002),
    ("electrum.qtornado.com", 50002),
    ("electrum.hodlister.co", 50002),
]
ESPLORA_APIS = [
    ("mempool.space", "https://mempool.space/api"),
    ("blockstream.info", "https://blockstream.info/api"),
]


class UpdateError(Exception):
    pass


# ----------------------------------------------------------------------------- helpers
def sha256d(b):
    return hashlib.sha256(hashlib.sha256(b).digest()).digest()


def bits_to_target(bits):
    exp = bits >> 24
    mant = bits & 0x007FFFFF
    if exp <= 3:
        return mant >> (8 * (3 - exp))
    return mant << (8 * (exp - 3))


def get_compact(n):
    size = (n.bit_length() + 7) // 8
    if size <= 3:
        comp = n << (8 * (3 - size))
    else:
        comp = n >> (8 * (size - 3))
    if comp & 0x00800000:
        comp >>= 8
        size += 1
    return comp | (size << 24)


def trunc(n):
    return bits_to_target(get_compact(n)) if n > 0 else 0


POW_LIMIT = bits_to_target(POW_LIMIT_BITS)
DIFF1 = POW_LIMIT


def retarget_bits(prev_bits, first_time, last_time):
    """Bitcoin Core CalculateNextWorkRequired (mainnet)."""
    span = last_time - first_time
    span = max(TARGET_TIMESPAN // 4, min(TARGET_TIMESPAN * 4, span))
    new = bits_to_target(prev_bits) * span // TARGET_TIMESPAN
    if new > POW_LIMIT:
        new = POW_LIMIT
    return get_compact(new)


def iso(t):
    return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def l2(n):
    if n <= 0:
        return None
    b = n.bit_length()
    return (b - 53) + math.log2(n >> (b - 53)) if b > 53 else math.log2(n)


def hx(n):
    return "%064x" % n


def parse_header(hb):
    ver, = struct.unpack("<i", hb[0:4])
    t, bits, nonce = struct.unpack("<III", hb[68:80])
    return {"raw": hb, "version": ver, "prev": hb[4:36][::-1].hex(), "merkle": hb[36:68][::-1].hex(),
            "time": t, "bits": bits, "nonce": nonce, "hash": sha256d(hb)[::-1].hex()}


def work(bits):
    return (1 << 256) // (bits_to_target(bits) + 1)


# ----------------------------------------------------------------------------- sources
def _unverified_ctx():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


class ElectrumSource:
    """Electrum protocol over SSL. Most public servers use self-signed certificates, so the
    certificate is not checked; every header is verified by proof of work and cross-checked."""
    kind = "electrum"

    def __init__(self, host, port):
        self.host, self.port = host, port
        self.name = host
        self.sock = None
        self.f = None
        self.next_id = 0
        self.lock = threading.Lock()

    def _connect(self):
        raw = socket.create_connection((self.host, self.port), timeout=TIMEOUT)
        self.sock = _unverified_ctx().wrap_socket(raw, server_hostname=self.host)
        self.sock.settimeout(TIMEOUT)
        self.f = self.sock.makefile("rwb")
        self._call("server.version", ["update_headers.py " + SCRIPT_VERSION, "1.4"])

    def _call(self, method, params):
        self.next_id += 1
        rid = self.next_id
        self.f.write((json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params}) + "\n").encode())
        self.f.flush()
        while True:
            line = self.f.readline()
            if not line:
                raise UpdateError("%s closed the connection" % self.host)
            msg = json.loads(line)
            if msg.get("id") != rid:
                continue  # subscription notification
            if msg.get("error"):
                raise UpdateError("%s: %s" % (self.host, msg["error"]))
            return msg["result"]

    def call(self, method, params):
        with self.lock:
            if self.sock is None:
                self._connect()
            try:
                return self._call(method, params)
            except Exception:
                self.close()
                raise

    def close(self):
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        self.sock = None

    def tip(self):
        r = self.call("blockchain.headers.subscribe", [])
        return r["height"], parse_header(bytes.fromhex(r["hex"]))["hash"]

    def hash_at(self, height):
        return parse_header(bytes.fromhex(self.call("blockchain.block.header", [height])))["hash"]

    def headers(self, start, count):
        out = []
        while len(out) < count:
            n = min(RETARGET, count - len(out))
            r = self.call("blockchain.block.headers", [start + len(out), n])
            raw = bytes.fromhex(r["hex"])
            got = len(raw) // 80
            if got == 0:
                raise UpdateError("%s returned no headers at %d" % (self.host, start + len(out)))
            out.extend(raw[i * 80:(i + 1) * 80] for i in range(got))
        return out[:count]


class EsploraSource:
    kind = "esplora"

    def __init__(self, name, base):
        self.name, self.base = name, base
        self.ctx = None  # system certificates; falls back to unverified if the store is missing

    def _get(self, path):
        url = self.base + path
        req = urllib.request.Request(url, headers={"User-Agent": "update_headers.py/" + SCRIPT_VERSION})
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=TIMEOUT, context=self.ctx) as r:
                    return r.read().decode()
            except urllib.error.URLError as e:
                if isinstance(getattr(e, "reason", None), ssl.SSLCertVerificationError) and self.ctx is None:
                    self.ctx = _unverified_ctx()  # e.g. macOS python.org build without certificates
                    continue
                if isinstance(e, urllib.error.HTTPError) and e.code not in (429, 500, 502, 503, 504):
                    raise
                if attempt == 2:
                    raise
                time.sleep(2 * (attempt + 1))

    def tip(self):
        h = int(self._get("/blocks/tip/height"))
        return h, None

    def hash_at(self, height):
        return self._get("/block-height/%d" % height).strip()

    def headers(self, start, count):
        got = {}
        end = start + count - 1
        h = end
        while h >= start:
            for b in json.loads(self._get("/blocks/%d" % h)):
                hb = struct.pack("<I", b["version"] & 0xFFFFFFFF) + bytes.fromhex(b["previousblockhash"])[::-1] \
                    + bytes.fromhex(b["merkle_root"])[::-1] + struct.pack("<III", b["timestamp"], b["bits"], b["nonce"])
                if sha256d(hb)[::-1].hex() != b["id"]:
                    hb = bytes.fromhex(self._get("/block/%s/header" % b["id"]).strip())
                got[b["height"]] = hb
            h -= 10
        return [got[x] for x in range(start, end + 1)]

    def close(self):
        pass


def make_sources():
    el = [ElectrumSource(h, p) for h, p in ELECTRUM_SERVERS]
    random.shuffle(el)
    return el + [EsploraSource(n, b) for n, b in ESPLORA_APIS]


# ----------------------------------------------------------------------------- database reads
def db_tip(con):
    r = con.execute("SELECT MIN(height) FROM blocks WHERE mined=0").fetchone()[0]
    tip = (r - 1) if r is not None else con.execute("SELECT MAX(height) FROM blocks").fetchone()[0]
    h = con.execute("SELECT block_hash FROM blocks WHERE height=?", (tip,)).fetchone()[0]
    return tip, h


def db_ancestors(con, first, last):
    rows = con.execute("SELECT height, block_hash, time_unix, bits FROM blocks WHERE height BETWEEN ? AND ? AND mined=1 "
                       "ORDER BY height", (first, last)).fetchall()
    return {h: {"hash": bh, "time": t, "bits": int(b, 16)} for h, bh, t, b in rows}


# ----------------------------------------------------------------------------- verification
def verify_chain(start, raw_headers, anc, now):
    """anc: dict height -> {hash,time,bits} for (at least) heights start-2016 .. start-1."""
    chain = dict(anc)
    parsed = []
    for i, hb in enumerate(raw_headers):
        h = start + i
        if len(hb) != 80:
            raise UpdateError("header %d is %d bytes, not 80" % (h, len(hb)))
        p = parse_header(hb)
        parent = chain[h - 1]
        if p["prev"] != parent["hash"]:
            raise UpdateError("height %d: prev-hash does not link to %d" % (h, h - 1))
        if int.from_bytes(bytes.fromhex(p["hash"]), "big") > bits_to_target(p["bits"]):
            raise UpdateError("height %d: proof of work fails (hash above target)" % h)
        if h % RETARGET == 0:
            want = retarget_bits(parent["bits"], chain[h - RETARGET]["time"], parent["time"])
        else:
            want = parent["bits"]
        if p["bits"] != want:
            raise UpdateError("height %d: bits %08x, retarget rule requires %08x" % (h, p["bits"], want))
        mtp = sorted(chain[h - k]["time"] for k in range(1, 12))[5]
        if p["time"] <= mtp:
            raise UpdateError("height %d: time %d not above median-time-past %d" % (h, p["time"], mtp))
        if p["time"] > now + 7200:
            raise UpdateError("height %d: time is more than 2 h ahead of this computer's clock "
                              "(check your clock)" % h)
        if p["version"] < 4:
            raise UpdateError("height %d: version %d < 4" % (h, p["version"]))
        chain[h] = {"hash": p["hash"], "time": p["time"], "bits": p["bits"]}
        p["height"] = h
        parsed.append(p)
    return parsed


# ----------------------------------------------------------------------------- constraint representation
VERSION_RULE = ("nVersion is a signed 32-bit integer. Consensus (BIP34/66/65, buried since height 388,381): blocks with nVersion < 4 are rejected ('bad-version'), "
                "so valid values are 4..2,147,483,647; any value with the top bit set is negative and invalid. Not consensus: BIP9 version-bits convention sets the top 3 bits to 001 "
                "(0x20000000-0x3FFFFFFF) with bits 0-28 as soft-fork signals; BIP320 leaves bits 13-28 free for miners' version rolling (extra nonce space).")
MERKLE_RULE = ("Any 256-bit value (0..2^256-1). It is the double-SHA256 Merkle root of the block's transactions, which the miner chooses; the first must be a coinbase whose scriptSig "
               "begins with the block height (BIP34) and, with SegWit, carries the witness commitment. No 256-bit value is excluded a priori.")
TIME_RULE = ("nTime (unsigned 32-bit seconds) must be strictly greater than the median-time-past (median of the previous 11 blocks' times) and no more than 2 hours (7200 s) ahead of "
             "the validating node's clock (historically network-adjusted time). The 2-hour rule is contextual (depends on when a node sees the block), so the only fixed upper bound is "
             "the field's ceiling 4,294,967,295 = 2106-02-07 06:28:15 UTC. Per-row blocks.time_min is the exact lowest possible nTime at that height if the chain extends the current tip: "
             "found by giving every future block the minimum allowed time (MTP+1); any real chain's times are at or above it.")
NONCE_RULE = "Any unsigned 32-bit value."
VALIDITY = ("Valid header iff all field rules above hold AND SHA256d(80-byte header), read as a little-endian 256-bit integer, <= target(bits), where target(bits) = mantissa * "
            "256^(exponent-3) from the compact nBits. (Block validity additionally needs a valid transaction set matching merkle_root.)")
RULE_FORMULA = ("version_count x prev_block_hash_count x 2^256 (merkle) x (4294967295 - time_min + 1) x bits_value_count x 2^32 (nonce), evaluated at first_height's time_min "
                "(across the class time_min moves the total by < 0.001 in log2)")
FUTURE_PREV_RULE = ("Must equal SHA256d of the header at height-1 (unknown until mined). That parent must meet its own target, so the value is <= the parent's maximum possible "
                    "target (per period in difficulty_periods; never above the proof-of-work limit 0x00000000FFFF0000...0000). prev_block_hash_count here is the loosest bound (powLimit+1).")
FUTURE_BITS_RULE = ("Not yet known. Rule: retarget every 2016 blocks (heights divisible by 2016). new_target = old_target x actual_timespan / 1,209,600 s, with actual_timespan "
                    "clamped to [302,400 s, 4,838,400 s] (so at most x4 or /4 per period), capped at the proof-of-work limit target(0x1d00ffff), then rounded down to compact 32-bit "
                    "nBits. So after k periods the target lies within about [T0/4^k, min(powLimit, T0*4^k)]; exact bounds per period in difficulty_periods. bits_value_count counts "
                    "the distinct compact values allowed (from 1 up to powLimit: 217,317,375).")


def count_repr(lo, hi):
    """Distinct targets T in [lo, hi] representable as a normalized compact nBits."""
    if lo > hi:
        return 0
    tot = 0
    s = 0
    while True:
        blo = 1 if s == 0 else 1 << (23 + 8 * (s - 1))
        bhi = (1 << (23 + 8 * s)) - 1
        if blo > hi:
            break
        a = max(lo, blo)
        b = min(hi, bhi)
        if a <= b:
            step = 256 ** s
            tot += b // step - (a - 1) // step
        s += 1
    return tot


def time_min_model(last11, tip):
    """Lowest possible nTime for every future height (each block takes MTP+1).
    Returns (explicit dict height->t for the first heights, h0, base[6]) with
    t(h) = base[(h-h0) % 6] + (h-h0)//6 for h >= h0. The closed form is checked against a
    direct simulation before it is used."""
    win = list(last11)
    srt = sorted(win)
    sim = {}
    steps = 4000
    for i in range(steps):
        h = tip + 1 + i
        t = srt[5] + 1
        sim[h] = t
        old = win.pop(0)
        srt.pop(bisect.bisect_left(srt, old))
        win.append(t)
        bisect.insort(srt, t)
    h0 = tip + 1 + 64
    base = [sim[h0 + k] for k in range(6)]
    for h in range(h0, tip + 1 + steps):
        if sim[h] != base[(h - h0) % 6] + (h - h0) // 6:
            raise UpdateError("time_min closed form check failed")
    explicit = {h: t for h, t in sim.items() if h < h0}
    return explicit, h0, base


def refresh_representation(con):
    """Recompute every derived column/table for the unmined rows from the stored tip."""
    TIP, TIPHASH = db_tip(con)
    tip_time, tip_bits_hex = con.execute("SELECT time_unix, bits FROM blocks WHERE height=?", (TIP,)).fetchone()
    tip_bits = int(tip_bits_hex, 16)
    last11 = [r[0] for r in con.execute("SELECT time_unix FROM blocks WHERE height BETWEEN ? AND ? ORDER BY height",
                                        (TIP - 10, TIP))]
    explicit, h0, base = time_min_model(last11, TIP)

    def tm(h):
        return explicit[h] if h < h0 else base[(h - h0) % 6] + (h - h0) // 6

    c1h = TIP + 1
    P0 = c1h // RETARGET
    period_start = P0 * RETARGET
    period_end = period_start + RETARGET - 1
    if c1h % RETARGET == 0:
        first_t = con.execute("SELECT time_unix FROM blocks WHERE height=?", (TIP - RETARGET + 1,)).fetchone()[0]
        KB = retarget_bits(tip_bits, first_t, tip_time)
        bits_why = ("Exactly known: difficulty period %d (heights %s-%s) starts at this height; its bits were fixed by the retarget rule from period %d's timestamps "
                    "(computed from heights %s and %s)." % (P0, format(period_start, ","), format(period_end, ","), P0 - 1,
                                                          format(TIP - RETARGET + 1, ","), format(TIP, ",")))
    else:
        KB = tip_bits
        bits_why = ("Exactly known: still inside difficulty period %d (heights %s-%s); every block in a period carries the period's bits."
                    % (P0, format(period_start, ","), format(period_end, ",")))
    KBH = "%08x" % KB
    T0 = bits_to_target(KB)
    MAXT = POW_LIMIT
    c2h = TIP + 2
    c3h = (P0 + 1) * RETARGET
    EST_CUT = min(TIP + ((U32 - tip_time) // 600) + 1, LAST_HEIGHT + 1)
    V = 2 ** 31 - 4
    N = 2 ** 32
    M = 2 ** 256

    def tcount(h):
        return U32 - tm(min(h, LAST_HEIGHT)) + 1

    # --- difficulty periods
    LASTP = LAST_HEIGHT // RETARGET
    periods = []
    lo = hi = T0
    for p in range(P0, LASTP + 1):
        if p > P0:
            lo = trunc(lo // 4)
            hi = trunc(min(MAXT, hi * 4) if hi * 4 <= MAXT else MAXT)
        first = max(p * RETARGET, TIP + 1)
        last = min(p * RETARGET + RETARGET - 1, LAST_HEIGHT)
        periods.append(dict(period=p, first_height=first, last_height=last, k=p - P0, lo=lo, hi=hi,
                            count=count_repr(max(lo, 1), hi)))
    dp_rows = []
    prev_hi_prev = T0
    for q in periods:
        lo_ = max(q["lo"], 1)
        hi_ = q["hi"]
        parent_hi = T0 if q["k"] <= 1 else prev_hi_prev
        bc = q["count"]
        rs = V * (hi_ + 1) * M * tcount(q["first_height"]) * bc * N if q["k"] > 0 else None
        dp_rows.append(dict(period=q["period"], first_height=q["first_height"], last_height=q["last_height"],
                            periods_after_current=q["k"], bits_known=1 if q["k"] == 0 else 0,
                            bits=KBH if q["k"] == 0 else None,
                            target_min_hex=hx(lo_), target_max_hex=hx(hi_), target_min_log2=l2(lo_), target_max_log2=l2(hi_),
                            bits_min="%08x" % get_compact(lo_), bits_max="%08x" % get_compact(hi_),
                            bits_value_count=str(bc), first_height_prev_hash_max_hex=hx(parent_hi if q["k"] > 0 else T0),
                            other_heights_prev_hash_max_hex=hx(hi_),
                            valid_fraction_log2_min=l2(lo_ + 1) - 256, valid_fraction_log2_max=l2(hi_ + 1) - 256,
                            rule_space_log2=l2(rs) if rs else None))
        prev_hi_prev = hi_

    # --- constraint classes
    classes = []

    def mk(cid, name, first, last, prev_rule, prev_known, prev_max, prev_count, bits_rule, bits_known, bcount,
           tgt_lo, tgt_hi, exact, note):
        rule = V * prev_count * M * tcount(first) * bcount * N
        if tgt_lo == tgt_hi:
            f_lo = f_hi = l2(tgt_lo + 1) - 256
        else:
            f_lo = l2(max(tgt_lo, 1) + 1) - 256
            f_hi = l2(tgt_hi + 1) - 256
        empty = first > last
        classes.append(dict(
            constraint_class_id=cid, name=name, first_height=None if empty else first, last_height=None if empty else last,
            version_rule=VERSION_RULE, version_min=4, version_max=2 ** 31 - 1, version_count=str(V),
            prev_block_hash_rule=prev_rule, prev_block_hash_known=prev_known, prev_block_hash_max_hex=prev_max,
            prev_block_hash_count=str(prev_count),
            merkle_root_rule=MERKLE_RULE, merkle_root_count=str(M),
            time_rule=TIME_RULE, time_max=U32, time_max_utc=iso(U32),
            bits_rule=bits_rule, bits_known=bits_known, target_min_hex=hx(max(tgt_lo, 1)), target_max_hex=hx(tgt_hi),
            target_min_log2=l2(max(tgt_lo, 1)), target_max_log2=l2(tgt_hi), bits_value_count=str(bcount),
            nonce_rule=NONCE_RULE, nonce_min=0, nonce_max=U32, nonce_count=str(N),
            validity_predicate=VALIDITY, raw_space_log2=640.0, raw_space_count=str(2 ** 640),
            rule_space_log2=l2(rule), rule_space_count=str(rule), rule_space_is_exact=exact, rule_space_formula=RULE_FORMULA,
            valid_fraction_log2_min=f_lo, valid_fraction_log2_max=f_hi,
            expected_valid_headers_log2_min=l2(rule) + f_lo if exact else None, expected_valid_headers_log2_max=l2(rule) + f_hi,
            notes=note + (" No heights are in this class right now (the next block is the last of its difficulty period)." if empty else "")))

    mk(1, "next block (exact prev hash, exact bits)", c1h, c1h,
       "Must equal SHA256d of the header at height-1. For this one height that header is already mined (current tip), so the value is fixed.",
       TIPHASH, TIPHASH, 1, bits_why, KBH, 1, T0, T0, 1,
       "Assumes the chain extends the current tip; a reorg would change this. Valid-header count is an expectation (SHA256d behaves as random): rule_space x valid fraction.")
    mk(2, "rest of current difficulty period (bits known)", c2h, period_end,
       "Must equal SHA256d of the header at height-1, which is not mined yet. Because that parent must itself meet the period's target, the value lies in 0..target "
       "(prev_block_hash_max_hex), i.e. target+1 possibilities.",
       None, hx(T0), T0 + 1,
       "Exactly known: 0x%s, the bits of difficulty period %d, which runs to height %s. The next retarget is at %s = %d x 2016."
       % (KBH, P0, format(period_end, ","), format(c3h, ","), P0 + 1),
       KBH, 1, T0, T0, 1,
       "Same assumptions as class 1. Expected valid count = rule_space x (target+1)/2^256.")
    mk(3, "future difficulty periods", c3h, EST_CUT - 1, FUTURE_PREV_RULE, None, hx(MAXT), MAXT + 1, FUTURE_BITS_RULE,
       None, 217317375, 0, MAXT, 0,
       "bits is not known yet but is NOT free: it is fixed deterministically by the previous period's timestamps. Per-period bounds are in difficulty_periods. Counts here are upper bounds across all periods.")
    mk(4, "future periods past the 2106 timestamp ceiling at 10-min pace", EST_CUT, LAST_HEIGHT, FUTURE_PREV_RULE, None, hx(MAXT), MAXT + 1,
       FUTURE_BITS_RULE, None, 217317375, 0, MAXT, 0,
       "Consensus rules identical to class 3. Difference is practical: at 10-minute blocks these heights would arrive after 2106-02-07 06:28:15 UTC, when nTime runs out of 32 bits. "
       "Under today's rules such headers are still mathematically valid only if their timestamps lag far behind real time (see time_min); in practice reaching them needs a protocol "
       "change to the time field.")

    # --- write tables
    for table, rows in (("header_constraints", classes), ("difficulty_periods", dp_rows)):
        cols = [r[1] for r in con.execute("PRAGMA table_info(%s)" % table)]
        con.execute("DELETE FROM %s" % table)
        con.executemany("INSERT INTO %s (%s) VALUES (%s)" % (table, ",".join(cols), ",".join("?" * len(cols))),
                        [tuple(r[c] for c in cols) for r in rows])

    # --- per-row columns for every unmined row
    con.execute("DROP TABLE IF EXISTS temp.tmin_head")
    con.execute("CREATE TEMP TABLE tmin_head(height INTEGER PRIMARY KEY, t INTEGER)")
    con.executemany("INSERT INTO temp.tmin_head VALUES (?,?)", sorted(explicit.items()))
    base_case = "CASE (height-%d)%%6 %s END" % (h0, " ".join("WHEN %d THEN %d" % (k, base[k]) for k in range(6)))
    con.execute("""UPDATE blocks SET
        constraint_class_id = CASE WHEN height=%(c1)d THEN 1 WHEN height<=%(pe)d THEN 2 WHEN height<%(cut)d THEN 3 ELSE 4 END,
        known_bits = CASE WHEN height<=%(pe)d THEN '%(kb)s' END,
        target_known = CASE WHEN height<=%(pe)d THEN 1 ELSE 0 END,
        time_min = CASE WHEN height<%(h0)d THEN (SELECT t FROM temp.tmin_head WHERE temp.tmin_head.height=blocks.height)
                        ELSE %(bc)s + (height-%(h0)d)/6 END,
        estimated_time_utc = strftime('%%Y-%%m-%%dT%%H:%%M:%%SZ', %(tt)d + 600*(height-%(tip)d), 'unixepoch')
        WHERE height > %(tip)d""" % dict(c1=c1h, pe=period_end, cut=EST_CUT, kb=KBH, h0=h0, bc=base_case, tt=tip_time, tip=TIP))
    con.execute("DROP TABLE temp.tmin_head")

    meta = {
        "tip_height": str(TIP), "tip_hash": TIPHASH, "tip_time_utc": iso(tip_time),
        "mined_rows": str(TIP + 1), "unmined_rows": str(LAST_HEIGHT - TIP),
        "estimated_time_utc": "For unmined rows only: an ESTIMATE = tip time (%s) + 10 minutes per block. Real timing will drift." % iso(tip_time),
        "header_constraints": ("Class 1 = next block %s: prev_block_hash is exactly the tip hash, bits exactly %s. Class 2 = %s: bits exactly %s (same difficulty period), "
                               "prev hash any value <= that target. Class 3 = %s-%s: bits not yet known, bounded by the retarget rule (x4 / /4 per 2016 blocks, capped at "
                               "0x1d00ffff); see difficulty_periods. Class 4 = %s-6,930,000: same rules as class 3, but at 10-minute pace these heights fall after the 32-bit "
                               "time limit (2106-02-07 06:28:15 UTC)."
                               % (format(c1h, ","), KBH,
                                  ("%s-%s" % (format(c2h, ","), format(period_end, ","))) if c2h <= period_end else "empty right now",
                                  KBH, format(c3h, ","), format(EST_CUT - 1, ","), format(EST_CUT, ","))),
    }
    con.executemany("INSERT OR REPLACE INTO metadata VALUES (?,?)", meta.items())
    return {"tip": TIP, "tip_hash": TIPHASH, "next_bits": KBH, "period_end": period_end, "est_cut": EST_CUT}


# ----------------------------------------------------------------------------- one update
def parallel(fn, items, workers=16):
    def safe(x):
        try:
            return x, fn(x), None
        except Exception as e:
            return x, None, e
    with ThreadPoolExecutor(min(workers, max(1, len(items)))) as ex:
        return list(ex.map(safe, items))


def update_once(db_path, verbose=False):
    t_start = time.time()
    if not os.path.exists(db_path):
        raise UpdateError("database not found: %s" % db_path)
    con = sqlite3.connect(db_path, timeout=120, isolation_level=None)
    con.execute("PRAGMA cache_size=-262144")
    sources = make_sources()
    try:
        H, H_hash = db_tip(con)
        if verbose:
            print("stored tip %d %s" % (H, H_hash))

        # 1. network tips
        tips = {}
        for src, res, err in parallel(lambda s: s.tip(), sources):
            if err is None:
                tips[src] = res
            elif verbose:
                print("  %s unreachable: %s" % (src.name, err))
        live = [s for s in sources if s in tips]
        if len(live) < 1 + MIN_CONFIRMATIONS:
            raise UpdateError("only %d public sources reachable (need %d) - check the internet connection"
                              % (len(live), 1 + MIN_CONFIRMATIONS))
        heights = sorted((tips[s][0] for s in live), reverse=True)
        target = heights[MIN_CONFIRMATIONS]          # highest height at least 3 sources have reached
        primaries = [s for s in live if tips[s][0] >= target]
        primaries.sort(key=lambda s: s.kind != "electrum")

        # 2. fetch (with reorg detection), trying sources in turn
        last_err = None
        for prim in primaries:
            try:
                result = fetch_and_verify(con, prim, H, H_hash, target, live, tips, verbose)
                break
            except UpdateError as e:
                last_err = e
                if verbose:
                    print("  %s failed: %s" % (prim.name, e))
                if "disagree" in str(e) or "changed while" in str(e):
                    raise
            except (OSError, ValueError, KeyError, ssl.SSLError, json.JSONDecodeError) as e:
                last_err = e
                if verbose:
                    print("  %s failed: %s" % (prim.name, e))
        else:
            raise UpdateError("could not fetch headers from any source (last error: %s)" % last_err)

        if result is None:
            return "no new blocks - tip %d %s (network %d) | %.1fs" % (H, H_hash[:20] + "...", heights[0], time.time() - t_start)

        fork, parsed, src_name, confirmers = result
        # 3. write everything in one transaction
        con.execute("BEGIN IMMEDIATE")
        try:
            cur_tip, cur_hash = db_tip(con)
            if (cur_tip, cur_hash) != (H, H_hash):
                raise UpdateError("database changed while updating (another copy of the script running?)")
            rolled = H - fork
            if rolled > 0:
                con.execute("""UPDATE blocks SET mined=0, block_hash=NULL, version=NULL, version_hex=NULL, prev_block_hash=NULL,
                               merkle_root=NULL, time_unix=NULL, time_utc=NULL, bits=NULL, nonce=NULL, difficulty=NULL,
                               raw_header_hex=NULL WHERE height > ? AND height <= ?""", (fork, H))
            con.executemany("""UPDATE blocks SET mined=1, block_hash=?, version=?, version_hex=?, prev_block_hash=?, merkle_root=?,
                               time_unix=?, time_utc=?, bits=?, nonce=?, difficulty=?, raw_header_hex=?, estimated_time_utc=NULL,
                               constraint_class_id=NULL, known_bits=NULL, target_known=NULL, time_min=NULL WHERE height=?""",
                            [(p["hash"], p["version"], "0x%08x" % (p["version"] & 0xFFFFFFFF), p["prev"], p["merkle"], p["time"],
                              iso(p["time"]), "%08x" % p["bits"], p["nonce"], DIFF1 / bits_to_target(p["bits"]), p["raw"].hex(),
                              p["height"]) for p in parsed])
            new_tip = parsed[-1]["height"]
            chk_tip, chk_hash = db_tip(con)
            if chk_tip != new_tip or chk_hash != parsed[-1]["hash"]:
                raise UpdateError("post-write check failed")
            info = refresh_representation(con)
            stamp = now_iso()
            first_new = parsed[0]["height"]
            line = ("heights %d-%d (%d) from %s; verified prev-hash links, PoW, bits/retarget, median-time-past, version; "
                    "tip hash confirmed by %s" % (first_new, new_tip, len(parsed), src_name, ", ".join(confirmers)))
            if rolled:
                line += "; reorg: rolled back %d block(s) above %d" % (rolled, fork)
            con.executemany("INSERT OR REPLACE INTO metadata VALUES (?,?)", [
                ("snapshot_taken_utc", stamp),
                ("updater", "update_headers.py %s keeps this file current: it fetches new headers from public Electrum servers "
                            "(fallback mempool.space / blockstream.info), verifies each one (prev-hash link, double-SHA256, proof of work, "
                            "retarget rule, median-time-past), cross-checks the tip hash with at least 2 other sources, then fills the rows "
                            "and recomputes the constraint representation of the unmined rows in one transaction." % SCRIPT_VERSION),
                ("updater_last_run_utc", stamp),
                ("updater_last_change", line),
            ])
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
        msg = "+%d block%s %d-%d" % (len(parsed), "" if len(parsed) == 1 else "s", first_new, new_tip)
        if rolled:
            msg = "REORG rolled back %d above %d, " % (rolled, fork) + msg
        return ("%s | tip %d %s | verified link/PoW/bits/MTP | tip confirmed by %d other sources | %.1fs"
                % (msg, new_tip, parsed[-1]["hash"], len(confirmers), time.time() - t_start))
    finally:
        for s in sources:
            s.close()
        con.close()


def fetch_and_verify(con, prim, H, H_hash, target, live, tips, verbose):
    # reorg check: is our stored tip still on the source's chain?
    fork = H
    if prim.hash_at(H) != H_hash:
        lo = max(0, H - MAX_REORG_DEPTH + 1)
        theirs = prim.headers(lo, H - lo + 1)
        ours = {h: r[0] for h, r in zip(range(lo, H + 1),
                con.execute("SELECT block_hash FROM blocks WHERE height BETWEEN ? AND ? ORDER BY height", (lo, H)))}
        fork = None
        for h in range(H, lo - 1, -1):
            if parse_header(theirs[h - lo])["hash"] == ours[h]:
                fork = h
                break
        if fork is None:
            raise UpdateError("stored tip is not on %s's chain and no common block in the last %d" % (prim.name, MAX_REORG_DEPTH))
        if verbose:
            print("  reorg: fork point %d" % fork)
    if target <= fork or (fork == H and target <= H):
        return None
    start = fork + 1
    raw = prim.headers(start, target - start + 1)
    anc = db_ancestors(con, start - RETARGET, fork)
    if len(anc) < min(RETARGET, start):
        raise UpdateError("database is missing ancestor rows below %d" % start)
    parsed = verify_chain(start, raw, anc, time.time())
    if fork < H:
        old_work = sum(work(int(r[0], 16)) for r in con.execute("SELECT bits FROM blocks WHERE height > ? AND height <= ?", (fork, H)))
        new_work = sum(work(p["bits"]) for p in parsed)
        if new_work < old_work:
            raise UpdateError("reorg branch from %s has no more work than the stored chain - ignored" % prim.name)

    # cross-check the tip hash with other sources; step back a block or two if the very tip is contested
    others = [s for s in live if s is not prim]
    for back in range(0, 3):
        if len(parsed) - back <= 0:
            break
        cand = parsed[len(parsed) - 1 - back]
        res = parallel(lambda s: s.hash_at(cand["height"]), [s for s in others if tips[s][0] >= cand["height"]])
        agree = [s.name for s, v, e in res if e is None and v == cand["hash"]]
        disagree = [s.name for s, v, e in res if e is None and v != cand["hash"]]
        if len(agree) >= MIN_CONFIRMATIONS and not disagree:
            return fork, parsed[:len(parsed) - back], prim.name, agree
        if verbose:
            print("  height %d: %d agree, %d disagree" % (cand["height"], len(agree), len(disagree)))
    raise UpdateError("sources disagree about the new tip (or fewer than %d could confirm it) - nothing written, will retry"
                      % MIN_CONFIRMATIONS)


# ----------------------------------------------------------------------------- main
def default_db():
    name = "bitcoin_block_headers.sqlite"
    here = os.path.dirname(os.path.abspath(__file__))
    for d in (os.getcwd(), here):
        p = os.path.join(d, name)
        if os.path.exists(p):
            return p
    return os.path.join(os.getcwd(), name)


def stamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def main():
    ap = argparse.ArgumentParser(description="Fill bitcoin_block_headers.sqlite with newly mined, verified block headers.")
    ap.add_argument("--db", default=None, help="path to bitcoin_block_headers.sqlite (default: current folder, then the script's folder)")
    ap.add_argument("--watch", action="store_true", help="keep running, updating every --interval minutes")
    ap.add_argument("--interval", type=float, default=10.0, help="minutes between runs with --watch (default 10)")
    ap.add_argument("--verbose", action="store_true", help="print details for each source")
    ap.add_argument("--refresh-constraints", action="store_true", help="only recompute the unmined-row constraint columns from the stored tip")
    args = ap.parse_args()
    db = args.db or default_db()

    if args.refresh_constraints:
        con = sqlite3.connect(db, timeout=120, isolation_level=None)
        con.execute("BEGIN IMMEDIATE")
        try:
            info = refresh_representation(con)
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
        print("%s  constraints refreshed for tip %d" % (stamp(), info["tip"]))
        return 0

    while True:
        code = 0
        try:
            print("%s  %s" % (stamp(), update_once(db, args.verbose)), flush=True)
        except KeyboardInterrupt:
            print("%s  stopped (database unchanged by the interrupted run)" % stamp())
            return 130
        except sqlite3.OperationalError as e:
            code = 2
            print("%s  ERROR: database: %s (close other programs that are writing to it) - nothing written" % (stamp(), e), flush=True)
        except Exception as e:
            code = 1
            print("%s  ERROR: %s - nothing written" % (stamp(), e), flush=True)
        if not args.watch:
            return code
        try:
            time.sleep(max(0.5, args.interval) * 60)
        except KeyboardInterrupt:
            print("%s  stopped" % stamp())
            return 0


if __name__ == "__main__":
    sys.exit(main())
