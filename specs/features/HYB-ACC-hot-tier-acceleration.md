# HYB-ACC: Hot-tier acceleration (hybrid warehouse over the lake)

| Field | Value |
|-------|-------|
| Status | Draft |
| Owner | RustHouse maintainers |
| Related ADRs | ADR-0001, ADR-0002, ADR-0005 |
| Depends on | LAKE-CAT, LAKE-TXN, LAKE-PQT, LAKE-TT, CORE-STO, CORE-PRN, GOV-ACC, GOV-AUD, API-SQL |
| Milestone | M4 |

## 1. Problem and motivation
ADR-0001 keeps the as-built segment engine (`src/storage/segment.rs`, `src/storage/pruner.rs`)
as a **derived, rebuildable hot tier** on top of lakehouse tables. Risk and fraud dashboards in
a bank need sub-second scans over the current Gold tables; Parquet over the lake is portable and
auditable but slower on small, repeated point queries. The hot tier gives warehouse latency
without giving up the lake as the system of record.

Regulatory posture: the hot tier is a cache, never evidence. Any result served from it MUST be
identical to the lake result for the same snapshot version (BCBS 239 accuracy), governance is
applied before tier selection (ADR-0005), and the whole tier can be deleted and rebuilt (DORA
resilience). A stale or corrupted hot copy must fall back to the lake silently and be reported
via metrics, never served.

## 2. Scope
### In scope
* Materialising one pinned lake version of a table into segments under `hot/<layer>/<table>/v<version>/`.
* `_manifest.json` with lake version, segment list, per-file checksums, build time.
* Tier router integrated into `src/exec/`.
* Staleness on new commit, background refresh, rebuild, eviction.
* Equivalence test suite hot vs lake.
* Metrics.
### Out of scope
* Hot tier for Bronze tables.
* Partial (per-partition) materialisation; the unit is a whole table version.
* Distributed cache.

## 3. Requirements

| ID | Level | Requirement |
|----|-------|-------------|
| HYB-ACC-001 | MUST | `materialise(table, version)` MUST read the lake snapshot at exactly `version` and write it as segments in the as-built format (`SegmentWriter`, `col_N.bin`) under `hot/<layer>/<table>/v<version>/`, plus a `_manifest.json` containing `lake_version`, ordered segment ids, per-segment row counts, SHA-256 of each column file, and `built_at`. |
| HYB-ACC-002 | MUST | The tier router MUST route a scan to the hot tier only if a manifest exists whose `lake_version` equals the snapshot version resolved for the query AND every checksum in the manifest verifies; otherwise it MUST scan the lake. |
| HYB-ACC-003 | MUST | A commit to a lake table MUST mark every hot materialisation of that table whose `lake_version` differs from the new head as stale; stale materialisations MUST NOT be routed to for head queries. |
| HYB-ACC-004 | MUST | For any query and any snapshot, the result from the hot tier MUST be row-for-row identical (same rows, same values, order-insensitive) to the result from the lake scan of the same snapshot. |
| HYB-ACC-005 | MAY | The suite MAY measure lake-scan vs hot-scan latency for the benchmark queries in `benches/` and record the ratio in `specs/traceability.md`; if lake scans are within 1.5x of hot on all benchmarks for two consecutive releases, an ADR MUST be opened to consider retiring the hot tier. |
| HYB-ACC-006 | MUST | Deleting the `hot/` directory (or any file in it) MUST NOT cause any query to fail or return different results; the next refresh MUST rebuild the materialisation from the lake. |
| HYB-ACC-007 | MUST | A time-travel query `AS OF VERSION n` MUST be served from the hot tier only if a materialisation with `lake_version == n` exists; it MUST never be served from a materialisation of another version, including the head. |
| HYB-ACC-008 | MUST | Governance (classification check, grants, row filters, masking, audit intent) MUST be applied to the plan before tier routing; the router MUST receive only a governed plan and MUST NOT expose which tier served the result to the caller other than through metrics and `tracing`. |
| HYB-ACC-009 | MUST | A background worker MUST refresh stale materialisations of hot-eligible tables to the current head, one table at a time, without blocking foreground queries; a refresh MUST write to a temporary directory and rename it into place so a partially built materialisation is never routable. |
| HYB-ACC-010 | MUST | The router MUST expose Prometheus counters `rusthouse_hot_tier_hits_total{table}` and `rusthouse_hot_tier_misses_total{table,reason}` with `reason ∈ {absent, stale, checksum_failed, version_mismatch}`. |
| HYB-ACC-011 | SHOULD | The hot tier SHOULD enforce a byte budget (`hot_tier_max_bytes` in `src/config.rs`) using least-recently-queried eviction at table granularity; eviction SHOULD never remove the materialisation currently being read. |
| HYB-ACC-012 | SHOULD | Hot-tier scans SHOULD use `SegmentPruner` with the governed predicate so that pruning behaviour is identical to the lake's row-group pruning. |
| HYB-ACC-013 | MAY | Materialisation MAY sort rows by a declared clustering key to improve min/max pruning. |

## 4. Design sketch

Module: `src/hot/` (`materialiser.rs`, `manifest.rs`, `router.rs`, `refresh.rs`).

```rust
#[derive(Serialize, Deserialize)]
pub struct HotManifest { pub table: TableRef, pub lake_version: u64, pub segments: Vec<SegmentEntry>, pub built_at: u64 }
pub struct SegmentEntry { pub id: String, pub row_count: usize, pub checksums: BTreeMap<String, String> /* col_N.bin -> sha256 */ }

pub struct Materialiser { lake: Arc<LakeReader>, writer_compression: i32 }
impl Materialiser { pub async fn materialise(&self, t: &TableRef, version: u64, dest: &Path) -> Result<HotManifest>; }

pub enum Tier { Hot(PathBuf), Lake }
pub struct TierRouter { hot_root: PathBuf, metrics: HotMetrics }
impl TierRouter { pub async fn choose(&self, t: &TableRef, snapshot_version: u64) -> Tier; }
```

* Segment build reuses `SegmentWriter::write` and `meta::Segment`/`SegmentStats` from
  `src/meta.rs`; the manifest replaces `metadata.json` as the hot tier's index (the as-built
  `MetadataStore` is no longer authoritative for lake tables).
* Router runs after `Governor` in `Session::execute_governed` (API-SQL) and before `Executor`
  (`src/exec/`), operating on the governed `LogicalPlan` from `src/sql/planner.rs`.
* Refresh worker replaces the compaction sketch in `TECHNICAL.md` Step 7: on each tick, pick the
  oldest stale hot-eligible table, build into `v<version>.tmp/`, rename to `v<version>/`, delete
  older `v*/` directories not currently pinned by a running query (ref-count).
* Checksum verification is cached per (table, version) after first success; cache is invalidated
  by any file modification time change.
* Errors: a hot-tier read error of any kind is converted into a lake fallback plus
  `misses_total{reason="checksum_failed"}`; it is never surfaced to the caller.

## 5. Acceptance scenarios

### HYB-ACC-001: Materialisation layout and manifest
```gherkin
Given gold.exposure_daily at lake version 12 with 2 500 rows
When  materialise(gold.exposure_daily, 12) runs
Then  hot/gold/exposure_daily/v12/_manifest.json exists with lake_version 12
And   every segment listed has col_N.bin files whose SHA-256 matches the manifest
And   the sum of segment row counts is 2 500
```

### HYB-ACC-002: Routing requires version match and valid checksums
```gherkin
Given a valid materialisation of gold.exposure_daily at version 12 and head = 12
When  a query on gold.exposure_daily runs
Then  the router chooses Tier::Hot
When  one byte of col_0.bin is flipped
And   the same query runs
Then  the router chooses Tier::Lake and misses_total{reason="checksum_failed"} increments
```

### HYB-ACC-003: New commit marks hot copy stale
```gherkin
Given a valid materialisation at version 12 and head = 12
When  a new INSERT commits version 13
And   a head query runs
Then  the router chooses Tier::Lake and misses_total{reason="stale"} increments
```

### HYB-ACC-004: Hot and lake results are identical
```gherkin
Given a table with random data (property-based generator) materialised at its head
When  SELECT with random predicates, projections and aggregations is executed against both tiers
Then  the multisets of result rows are equal for every generated query
```

### HYB-ACC-006: Hot tier is disposable
```gherkin
Given a valid materialisation and a query result R obtained through the hot tier
When  the whole hot/ directory is deleted
And   the same query runs
Then  the result equals R and no error is returned
When  the refresh worker ticks
Then  the materialisation exists again with the same lake_version
```

### HYB-ACC-007: Time travel never uses a different version's hot copy
```gherkin
Given a materialisation at version 13 (head) only
When  SELECT ... AS OF VERSION 12 runs
Then  the router chooses Tier::Lake
Given a materialisation at version 12 also exists
When  the same query runs
Then  the router chooses Tier::Hot pointing at v12
```

### HYB-ACC-008: Governance precedes routing
```gherkin
Given principal "analyst" with a row filter region = 'EU' and masking on iban
When  "analyst" queries gold.exposure_daily served by the hot tier
Then  only EU rows are returned and iban is masked
And   the audit intent record was written before the router was invoked (ordering asserted via a recording audit sink)
And   the HTTP response body contains no field indicating the tier
```

### HYB-ACC-009: Refresh is non-blocking and atomic
```gherkin
Given a stale materialisation at version 12 and head = 13
When  the refresh worker starts building v13 and a foreground query runs midway
Then  the query is served from the lake and hot/gold/exposure_daily/v13/ does not yet exist (only v13.tmp/)
When  the build completes
Then  v13/ exists, v13.tmp/ does not, and the next head query is Tier::Hot
```

### HYB-ACC-010: Metrics
```gherkin
Given a fresh process
When  one hot hit and one lake fallback (reason absent) occur
Then  /metrics exposes rusthouse_hot_tier_hits_total{table="gold.exposure_daily"} 1
And   rusthouse_hot_tier_misses_total{table="gold.exposure_daily",reason="absent"} 1
```

## 6. Test plan (write these first)

| Requirement | Test | Location | Kind |
|-------------|------|----------|------|
| HYB-ACC-001 | `fn hyb_acc_001_materialise_writes_segments_and_manifest()` | `tests/spec_hyb_acc.rs` | acceptance |
| HYB-ACC-001 | `fn test_manifest_roundtrip()` | `src/hot/manifest.rs` | unit |
| HYB-ACC-002 | `fn hyb_acc_002_router_picks_hot_on_match()` | `tests/spec_hyb_acc.rs` | acceptance |
| HYB-ACC-002 | `fn hyb_acc_002_router_falls_back_on_checksum_failure()` | `tests/spec_hyb_acc.rs` | acceptance (negative) |
| HYB-ACC-003 | `fn hyb_acc_003_commit_marks_hot_stale()` | `tests/spec_hyb_acc.rs` | acceptance |
| HYB-ACC-004 | `fn hyb_acc_004_hot_and_lake_results_equal()` (proptest) | `tests/spec_hyb_acc.rs` | property |
| HYB-ACC-005 | `fn hyb_acc_005_latency_ratio_recorded()` | `benches/bench_tiers.rs` | benchmark |
| HYB-ACC-006 | `fn hyb_acc_006_delete_hot_dir_no_failure_and_rebuild()` | `tests/spec_hyb_acc.rs` | acceptance |
| HYB-ACC-007 | `fn hyb_acc_007_time_travel_uses_only_matching_version()` | `tests/spec_hyb_acc.rs` | acceptance |
| HYB-ACC-008 | `fn hyb_acc_008_governance_applied_before_routing()` | `tests/spec_hyb_acc.rs` | acceptance |
| HYB-ACC-009 | `fn hyb_acc_009_refresh_builds_in_tmp_and_renames()` | `tests/spec_hyb_acc.rs` | acceptance |
| HYB-ACC-010 | `fn hyb_acc_010_hit_miss_metrics_exposed()` | `tests/spec_hyb_acc.rs` | acceptance |
| HYB-ACC-011 | `fn hyb_acc_011_eviction_respects_budget_and_active_reads()` | `tests/spec_hyb_acc.rs` | acceptance |
| HYB-ACC-012 | `fn hyb_acc_012_hot_scan_prunes_same_segments_as_lake()` | `tests/spec_hyb_acc.rs` | acceptance |
| HYB-ACC-012 | `fn test_router_uses_segment_pruner()` | `src/hot/router.rs` | unit |

## 7. Open questions
* Should materialisation be triggered on first query (lazy) as well as by the worker? Leaning:
  worker only, to keep query latency predictable.
* Nullable columns: the as-built `ColumnData` has no null representation; LAKE-PQT-004 must
  settle the bridge before HYB-ACC-004 can hold for tables with nulls.
* Checksum algorithm: SHA-256 chosen for audit familiarity; BLAKE3 would be faster. Decide in M4.

## 8. Change log
| Date | Change |
|------|--------|
| 2026-09-02 | Created |
