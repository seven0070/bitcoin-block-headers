# bitcoin-block-headers

A single SQLite database with **one row for every Bitcoin block height from 0 to 6,930,000** (6,930,001 rows), the height where the block subsidy reaches zero.

- **Mined heights (0 to 974,342 in the v1.0 snapshot):** the real, verified 80-byte header for every block. That means hash, previous hash, merkle root, time, bits, nonce, version, difficulty, and the raw header hex.
- **Unmined heights (974,343 to 6,930,000):** no header exists yet, so these rows are not filled with made-up values. Each one stores the **mathematical representation** of every header that could validly sit at that height: the allowed range of each field, plus the proof-of-work condition. Any candidate header can be checked against it.
- **Updater:** a small Python script (standard library only) fetches new blocks as they are mined, verifies each one, and turns unmined rows into mined rows.

| | |
|---|---|
| Rows | 6,930,001 (974,343 mined, 5,955,658 unmined at snapshot) |
| Snapshot tip | height 974,342, hash `000000000000000000010bbce1734d8a71699a9c9ea1b6fb0048cf2718b85e8b` (2026-11-03 04:08:54 UTC) |
| File size | 997 MB as `.sqlite`, 280 MB zipped |

## Download

The database is too large for the repository itself (GitHub caps files in a repo at 100 MB), so it is shipped as a download:

- **GitHub release:** [v1.0 / bitcoin_block_headers.zip](https://github.com/seven0070/bitcoin-block-headers/releases/download/v1.0/bitcoin_block_headers.zip)
- **Mirror:** https://btc-block-headers.sites.tab.bot/

SHA-256 checksums:

```
c836592ca2f498afc42b25fb672dc01abf54abdab4f12f2f881db1c47f7840bb  bitcoin_block_headers.zip
3fc327fffdb42f32150c532edd7287d02236e63b98b78001fcdb49c2f85e7d26  bitcoin_block_headers.sqlite
```

Unzip it and open `bitcoin_block_headers.sqlite` with any SQLite tool: the `sqlite3` CLI, [DB Browser for SQLite](https://sqlitebrowser.org/), Python's `sqlite3` module, and so on.

## Keeping it up to date

[`updater/update_headers.py`](updater/update_headers.py) needs Python 3.8+ and nothing else (no pip installs). It runs on Windows, macOS and Linux.

```
# put update_headers.py next to bitcoin_block_headers.sqlite, then:
python3 update_headers.py            # one update   (Windows: py update_headers.py)
python3 update_headers.py --watch    # keep updating every 10 minutes
```

What one run does:

1. Reads the highest mined height in the database.
2. Asks several public Electrum servers (SSL JSON-RPC), plus mempool.space and blockstream.info as a fallback, for the current chain tip.
3. Checks that the stored tip is still on the best chain. On a reorg it finds the fork point (up to 2,016 blocks back), rolls those rows back and refills them. A replacement chain with less proof of work is rejected.
4. Verifies every new header: the prev-hash link, the double-SHA256 hash, proof of work against `bits`, the difficulty retarget rule at each 2,016-block boundary, median-time-past, version ≥ 4, and time ≤ now + 2 h.
5. Requires at least 2 other independent sources to report the same tip hash.
6. In **one transaction**, writes the headers (`mined = 1`) and recomputes the constraint data for every unmined row. If anything fails, nothing is written.

Scheduling with Task Scheduler or cron is covered in [`updater/README.txt`](updater/README.txt). A run with new blocks takes about 10–60 s and needs roughly 1 GB of free disk while it writes.

## Schema

The full `CREATE` statements are in [`schema.sql`](schema.sql).

### Tables

**`blocks`**: one row per height (`height` is the primary key).

| Column(s) | Mined rows | Unmined rows |
|---|---|---|
| `height`, `mined` | height, `1` | height, `0` |
| `block_hash`, `prev_block_hash`, `merkle_root` | hex, in the usual display (big-endian) order | NULL |
| `version`, `version_hex`, `time_unix`, `time_utc`, `bits`, `nonce`, `difficulty` | header fields | NULL |
| `raw_header_hex` | the exact 80 serialized bytes (160 hex characters) | NULL |
| `halving_epoch`, `block_subsidy_sats`, `block_subsidy_btc`, `cumulative_max_supply_sats` | protocol schedule | protocol schedule |
| `estimated_time_utc` | NULL | estimate: tip time + 10 min per block |
| `constraint_class_id`, `known_bits`, `target_known`, `time_min` | NULL | links to the rules (see below) |

**`header_constraints`**: four rule classes shared by the unmined rows:

| id | Heights (at snapshot) | Meaning |
|---|---|---|
| 1 | 974,343 | the next block: prev hash is exactly the tip hash, bits exactly `1701e5ac` |
| 2 | 974,344 – 975,743 | rest of the current difficulty period: bits known exactly |
| 3 | 975,744 – 5,143,155 | future difficulty periods: bits bounded by the retarget rule (×4 / ÷4 per period, capped at `0x1d00ffff`) |
| 4 | 5,143,156 – 6,930,000 | same as 3, but at a 10-minute pace these heights fall after the 32-bit timestamp limit (2106-02-07) |

Each class stores the rule text and min/max for every field (version, prev hash, merkle root, time, bits, nonce), the validity predicate, and counts of the possibility space (`raw_space`, `rule_space`, `valid_fraction`, `expected_valid_headers`) as log2 values and exact decimal strings.

**`difficulty_periods`**: one row per 2,016-block window: whether its bits are known, the minimum and maximum possible target and bits, and the matching bounds on the previous block hash.

**`metadata`**: key/value notes: tip height and hash, sources, verification method, updater log, and definitions of every derived column.

### Views

- **`halvings`**: one row per reward era (34 eras): first and last height, subsidy, mined and unmined counts.
- **`header_representation`**: for an unmined height, joins `blocks` + `header_constraints` + `difficulty_periods` into one row with the full set of rules for a valid header at that height.

## How the mined headers were verified

- Headers were downloaded from several independent public Electrum servers (`blockchain.block.headers`) and compared against each other. The tip hash was cross-checked against mempool.space and blockstream.info.
- The genesis hash matches `000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f`.
- For every header: `prev_block_hash` equals the double-SHA256 of the previous header, and the header's own hash meets its `bits` target (proof of work).
- Every `bits` value matches the 2,016-block difficulty retarget rule, and every timestamp satisfies median-time-past.

Because each header commits to the one before it and carries proof of work, the whole chain can be re-checked offline from the `raw_header_hex` column alone.

## Example queries

```sql
-- A real header (the 4th halving block)
SELECT height, block_hash, time_utc, bits, nonce
FROM blocks WHERE height = 840000;

-- Current tip
SELECT value FROM metadata WHERE key IN ('tip_height', 'tip_hash');

-- An unmined height: reward and estimated date
SELECT height, mined, block_subsidy_btc, estimated_time_utc
FROM blocks WHERE height = 1050000;

-- Everything a valid header at the next height must satisfy
SELECT * FROM header_representation WHERE height = 974343;

-- Reward eras
SELECT * FROM halvings;

-- Blocks per year and peak difficulty
SELECT strftime('%Y', time_utc) AS year, COUNT(*) AS blocks, MAX(difficulty) AS peak_difficulty
FROM blocks WHERE mined = 1 GROUP BY year ORDER BY year;

-- Re-check proof of work of one header in Python
-- import hashlib, sqlite3
-- h = bytes.fromhex(raw_header_hex)
-- hashlib.sha256(hashlib.sha256(h).digest()).digest()[::-1].hex() == block_hash
```

## Why every possible header isn't listed

A block header is 80 bytes = 640 bits, so there are **2^640** possible byte strings. That is about 10^192 headers, or roughly 10^194 bytes to write them all out. For scale, the observable universe has about 10^80 atoms, and its total information capacity (the holographic bound) is around 10^122 bits. Even the rules alone leave about 2^448 candidate headers at a single height.

The set is finite, but it can't be enumerated, so this database stores it the only way it can be stored: as **rules**. For any unmined height, `header_representation` says exactly which values each field may take and the proof-of-work condition a header must meet. That makes every possible valid header checkable, and real headers replace the rules as blocks are mined.

## Repository layout

```
README.md                 this file
schema.sql                CREATE statements for all tables, indexes and views
updater/update_headers.py the updater (Python 3 stdlib only)
updater/README.txt        run and scheduling instructions (Windows / Mac / Linux)
LICENSE                   MIT
```

## License

MIT, see [LICENSE](LICENSE). Bitcoin block header data is public.
