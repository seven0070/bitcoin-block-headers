-- bitcoin_block_headers.sqlite schema (sqlite3 .schema)
-- v1.0 snapshot, tip height 974,342

CREATE TABLE blocks(
 height INTEGER PRIMARY KEY,
 mined INTEGER NOT NULL,
 block_hash TEXT,
 version INTEGER,
 version_hex TEXT,
 prev_block_hash TEXT,
 merkle_root TEXT,
 time_unix INTEGER,
 time_utc TEXT,
 bits TEXT,
 nonce INTEGER,
 difficulty REAL,
 raw_header_hex TEXT,
 halving_epoch INTEGER NOT NULL,
 block_subsidy_sats INTEGER NOT NULL,
 block_subsidy_btc REAL NOT NULL,
 cumulative_max_supply_sats INTEGER NOT NULL,
 estimated_time_utc TEXT
, constraint_class_id INTEGER, known_bits TEXT, target_known INTEGER, time_min INTEGER);

CREATE TABLE "difficulty_periods" (
"period" INTEGER,
  "first_height" INTEGER,
  "last_height" INTEGER,
  "periods_after_current" INTEGER,
  "bits_known" INTEGER,
  "bits" TEXT,
  "target_min_hex" TEXT,
  "target_max_hex" TEXT,
  "target_min_log2" REAL,
  "target_max_log2" REAL,
  "bits_min" TEXT,
  "bits_max" TEXT,
  "bits_value_count" TEXT,
  "first_height_prev_hash_max_hex" TEXT,
  "other_heights_prev_hash_max_hex" TEXT,
  "valid_fraction_log2_min" REAL,
  "valid_fraction_log2_max" REAL,
  "rule_space_log2" REAL
);

CREATE TABLE "header_constraints" (
"constraint_class_id" INTEGER,
  "name" TEXT,
  "first_height" INTEGER,
  "last_height" INTEGER,
  "version_rule" TEXT,
  "version_min" INTEGER,
  "version_max" INTEGER,
  "version_count" TEXT,
  "prev_block_hash_rule" TEXT,
  "prev_block_hash_known" TEXT,
  "prev_block_hash_max_hex" TEXT,
  "prev_block_hash_count" TEXT,
  "merkle_root_rule" TEXT,
  "merkle_root_count" TEXT,
  "time_rule" TEXT,
  "time_max" INTEGER,
  "time_max_utc" TEXT,
  "bits_rule" TEXT,
  "bits_known" TEXT,
  "target_min_hex" TEXT,
  "target_max_hex" TEXT,
  "target_min_log2" REAL,
  "target_max_log2" REAL,
  "bits_value_count" TEXT,
  "nonce_rule" TEXT,
  "nonce_min" INTEGER,
  "nonce_max" INTEGER,
  "nonce_count" TEXT,
  "validity_predicate" TEXT,
  "raw_space_log2" REAL,
  "raw_space_count" TEXT,
  "rule_space_log2" REAL,
  "rule_space_count" TEXT,
  "rule_space_is_exact" INTEGER,
  "rule_space_formula" TEXT,
  "valid_fraction_log2_min" REAL,
  "valid_fraction_log2_max" REAL,
  "expected_valid_headers_log2_min" REAL,
  "expected_valid_headers_log2_max" REAL,
  "notes" TEXT
);

CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT);

CREATE UNIQUE INDEX idx_blocks_hash ON blocks(block_hash) WHERE block_hash IS NOT NULL;

CREATE UNIQUE INDEX idx_dp on difficulty_periods(period);

CREATE UNIQUE INDEX idx_hc on header_constraints(constraint_class_id);

CREATE VIEW halvings AS SELECT halving_epoch, MIN(height) first_height, MAX(height) last_height, block_subsidy_btc, SUM(mined) mined_blocks, COUNT(*)-SUM(mined) unmined_blocks FROM blocks GROUP BY halving_epoch;

CREATE VIEW header_representation as
select b.height, b.mined, b.constraint_class_id, c.name as constraint_class,
  b.halving_epoch, b.block_subsidy_sats, b.estimated_time_utc,
  c.version_min, c.version_max, c.version_count, c.version_rule,
  c.prev_block_hash_known,
  case when b.constraint_class_id=1 then c.prev_block_hash_known
       when b.height % 2016 = 0 then d.first_height_prev_hash_max_hex
       else d.other_heights_prev_hash_max_hex end as prev_block_hash_max_hex,
  c.prev_block_hash_rule,
  c.merkle_root_count, c.merkle_root_rule,
  b.time_min, strftime('%Y-%m-%dT%H:%M:%SZ', b.time_min, 'unixepoch') as time_min_utc,
  c.time_max, c.time_max_utc, (c.time_max - b.time_min + 1) as time_value_count, c.time_rule,
  b.height/2016 as difficulty_period, b.known_bits, b.target_known,
  d.bits_min, d.bits_max, d.target_min_hex, d.target_max_hex, d.target_min_log2, d.target_max_log2, d.bits_value_count, c.bits_rule,
  c.nonce_min, c.nonce_max, c.nonce_count, c.nonce_rule,
  c.validity_predicate,
  c.raw_space_log2, c.raw_space_count,
  coalesce(d.rule_space_log2, c.rule_space_log2) as rule_space_log2,
  c.rule_space_count as class_rule_space_count, c.rule_space_is_exact, c.rule_space_formula,
  d.valid_fraction_log2_min, d.valid_fraction_log2_max,
  case when b.target_known=1 then c.rule_space_log2 + d.valid_fraction_log2_min end as expected_valid_headers_log2,
  coalesce(d.rule_space_log2, c.rule_space_log2) + d.valid_fraction_log2_max as valid_headers_log2_upper_bound,
  c.notes
from blocks b join header_constraints c on c.constraint_class_id=b.constraint_class_id
left join difficulty_periods d on d.period = b.height/2016
where b.mined=0;
