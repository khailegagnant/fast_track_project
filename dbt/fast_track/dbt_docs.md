# fast_track

dbt project for the F1 capstone. This is the **in-platform transformation**
stage of a larger pipeline:

```
F1 API (JSON)  ->  S3 (raw JSON)  ->  Glue/Spark (explode to parquet)
               ->  S3 (parquet)   ->  COPY INTO  ->  F1_CAPSTONE.RAW
               ->  dbt  ->  staging -> silver -> mart
                               |
                               +-> snapshots (SCD Type 2, optional)
```

> **Full model documentation:** [`docs/MODELS.md`](docs/MODELS.md) — layer
> contract, source contract, key strategy, entity relationships, and the grain
> and purpose of every mart table.

Everything upstream of `F1_CAPSTONE.RAW` is handled by Airflow, Glue, and
`COPY INTO`. dbt owns everything from `RAW` onward. `RAW` is an append-style
landing zone: nothing is cleaned, deduplicated, or conformed before dbt.

## Layers

| Layer | Path | Materialization | Purpose |
|---|---|---|---|
| sources | `models/sources/` | – | `F1_RAW` source definitions + source-level tests |
| staging | `models/staging/f1-formula_one/` | view | 1:1 with RAW. **Select and rename only** — no casting, duplicates preserved |
| silver | `models/silver/f1-formula_one/` | table | **Typed**, cleaned, deduplicated, conformed business entities with surrogate keys |
| snapshots | `snapshots/` | table | **SCD Type 2** history of driver/constructor attributes (optional, not read by any fact) |
| mart | `models/mart/` | table | Consumption star schema, periodic-snapshot standings, and season metrics |

There is deliberately **no intermediate layer** — nothing in this domain needs
a shared pre-aggregation step, so adding one would be ceremony without value.

### Naming

Mirrors the ACA reference project: `<layer>_<domain>__<entity>`, with the mart
layer dropping the domain segment since it is the single consumption surface.

* `stg_f1__results`     — staging passthrough
* `slv_f1__results`     — cleaned business *entity* (no `dim_`/`fct_` here)
* `mrt__fct_race_results`, `mrt__dim_drivers` — dimensional model

Dimensional naming (`dim_`/`fct_`) appears **only in the mart**. Silver holds
conformed entities; committing them to a dimension or fact shape is a
consumption concern.

SQL style follows `.github/skills/dbt-sql-style` from the ACA repo: uppercase
keywords and column names, uppercase CTE names, lowercase table aliases,
`ref()`/`source()` only inside import CTEs, and every model ending in
`SELECT * FROM <final_cte>`. All model configuration lives in the adjacent
YAML `config:` block, never in a SQL `{{ config() }}`.

## Models

### Staging (6)
`stg_f1__circuits`, `stg_f1__constructors`, `stg_f1__drivers`,
`stg_f1__races`, `stg_f1__qualifying`, `stg_f1__results`

### Silver (6)
* `slv_f1__circuits`, `slv_f1__constructors`, `slv_f1__drivers`,
  `slv_f1__races` — typed, deduplicated reference entities with surrogate keys
* `slv_f1__results` — classification, status categorisation, pit-lane
  normalisation, lap times in seconds
* `slv_f1__qualifying` — Q1/Q2/Q3 in seconds, best time, segment reached

### Mart (12)
* `mrt__dim_circuits`, `mrt__dim_constructors`, `mrt__dim_drivers`,
  `mrt__dim_races` — presentation dimensions with display labels
* `mrt__fct_race_results` — + gap to winner, fastest-lap gap, field size
* `mrt__fct_qualifying_results` — + gap to pole, % off pole, gap to teammate
* `mrt__fct_driver_standings` — **periodic snapshot**, one row per driver per
  round with cumulative championship state
* `mrt__fct_constructor_standings` — **periodic snapshot**, same per constructor
* `mrt__agg_driver_season_summary`, `mrt__agg_constructor_season_summary` —
  metrics layer, one row per entity per season
* `mrt__dim_drivers_history`, `mrt__dim_constructors_history` — **SCD Type 2**
  full version history, standalone (not joined to any fact)

### SCD Type 2 — driver and constructor history

`snapshots/snap_f1__models.yml` defines two snapshots — `snap_f1__drivers` and
`snap_f1__constructors` — using dbt's YAML-only snapshot syntax (`relation:` +
`config:` in the YAML, no separate `.sql` file needed). They track attribute
history (`DRIVER_CODE`, `PERMANENT_NUMBER`, names, `CONSTRUCTOR_NAME`,
`CONSTRUCTOR_NATIONALITY`) with `strategy: check` against the deduplicated
silver entities.

Explicitly **not** included: a "current champion" flag. That's fact-layer
information (derived from aggregating a season of results, not a descriptive
driver/constructor attribute) and is already modelled correctly as
`IS_CHAMPIONSHIP_LEADER` (per round) and `IS_SEASON_CHAMPION` (per season) in
the standings/aggregate models above.

`strategy: check`, not `timestamp`: the API supplies no genuine `updated_at`
for either entity, and `DBT_LOADED_AT` is stamped from `run_started_at` (same
value for every row in a run), so it can't order anything either. `check`
instead re-compares the listed business columns on every run and opens a new
version only when one of them actually changed.

Snapshot source is **silver**, not staging or RAW: `dbt snapshot` requires
exactly one row per `unique_key`, and staging/RAW still carry the per-season
duplicates with nothing to order them by. Silver is the first point with a
clean 1-to-1 row-per-entity representation.

`mrt__dim_drivers` / `mrt__dim_constructors` are **untouched** by this —
`DRIVER_KEY`/`CONSTRUCTOR_KEY` are hashes of the natural key alone, so every
historical version of an entity shares one key. Merging history into those
tables would break their `unique` tests and fan out every fact join. History
lives only in the two `_history` mart models, which are never joined to a
fact.

Verified during development: manually changing a driver's `PERMANENT_NUMBER`
in RAW, rebuilding silver, and re-running `dbt snapshot` correctly closed the
old version (`DBT_VALID_TO` set) and opened a new one (`DBT_VALID_TO` NULL)
with a new `DBT_SCD_ID` — then reverted. As currently loaded, no driver or
constructor has ever actually had a differing attribute, so `dbt snapshot`
today produces exactly one (current) version per entity. That's expected: the
snapshot is infrastructure for the next time an attribute changes, not a
claim that history already exists.

### Standings design

The two standings models are the only derived subject area — there is no
standings table in `RAW`, so they are computed from `slv_f1__results`.

They are modelled as **periodic snapshot facts**: a spine of every season
entrant crossed with every round guarantees one row per entity per round, even
when that entity did not race. Cumulative measures use
`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` so each snapshot reflects
only rounds up to and including itself.

#### Tie-break (full FIA countback)

`CHAMPIONSHIP_POSITION` implements the official hierarchy in full:

```
points
  -> number of 1st places
  -> number of 2nd places
  -> number of 3rd places
  -> ... through the field ...
  -> number of pole positions
  -> number of 2nd places in qualifying
  -> ... through the field ...
```

Rather than emit one `ORDER BY` term per position, the `f1_countback` macros
fold the season-to-date position counts into two zero-padded, fixed-width
strings — e.g. `001001000000…` for one win and one 2nd place. Because every
count occupies the same width and positions are concatenated best-first,
sorting those strings `DESC` is exactly equivalent to comparing position by
position:

```sql
ORDER BY
    POINTS_TO_DATE DESC,
    RACE_COUNTBACK_KEY DESC,
    QUALIFYING_COUNTBACK_KEY DESC
```

`RACE_COUNTBACK_KEY` and `QUALIFYING_COUNTBACK_KEY` are exposed on both
standings models so a tie can be audited directly, alongside the individual
`SECOND_PLACES_TO_DATE`, `THIRD_PLACES_TO_DATE`, and `POLE_POSITIONS_TO_DATE`
counts. Depth is controlled by the `f1_countback_max_position` var (default
24). For constructors, every car counts — a 1-2 finish contributes one 1st
place and one 2nd place.

A shared position therefore now means the two competitors are identical on
points *and* on every race and qualifying position — a genuine dead heat that
the FIA rule itself cannot resolve. There is exactly one in the current data
(2026 round 2: Antonelli and Russell swapped 1-2 in both races and swapped
pole in both qualifying sessions), and
`assert_one_championship_leader_per_round` surfaces it as a warning.

## Testing

Run everything with `dbt build`. Current state: **22 models, 321 data tests,
14 unit tests — all passing**, with 7 documented warnings.

* **Schema tests** (`data_tests` in model YAML) — `not_null`, `unique`,
  `relationships`, `accepted_values`, `dbt_utils.accepted_range`,
  `dbt_utils.unique_combination_of_columns`, `dbt_utils.expression_is_true`.
  Every model's grain is protected by an explicit combination test.
* **Unit tests** (`*_unit_tests.yml`) — cover the deterministic logic that is
  hardest to spot in production data: constructor and driver deduplication,
  classification from `POSITION_TEXT`, pit-lane start handling, status
  categorisation, best-qualifying-time selection across segments, malformed
  time strings, the standings cumulative/spine logic, and each tier of the
  countback (wins, 2nd places, and qualifying).
* **Singular tests** (`tests/f1-formula_one/`) — cross-model business rules:
  standings reconcile to result totals, cumulative points never decrease,
  round sequences are contiguous, and every race entry has a qualifying row.

Generic test inputs are nested under `arguments:` and runtime options under
`config:`, as required by dbt 1.11+/Fusion.

### Expected warnings

| Warning | Why it is expected |
|---|---|
| `not_null` on `DATE_OF_BIRTH` / `DRIVER_NATIONALITY` (16–23 rows) | Reserve and FP1-only drivers are listed by the API without biographical detail |
| `assert_race_entries_have_qualifying` (6 rows) | A handful of entries have no qualifying row in the source |
| `assert_one_championship_leader_per_round` (1 round) | A genuine dead heat — identical on points and every race and qualifying position |

## Known data characteristics

* **`RAW_CONSTRUCTORS` and `RAW_DRIVERS` contain duplicates.** The API is
  queried per season, so an entity is landed once per season it appears in
  (137 driver rows for 48 drivers; 51 constructor rows for 14 constructors).
  Staging preserves this; silver deduplicates.
* **Deduplication never orders by `DBT_LOADED_AT`.** That column is stamped
  from `run_started_at` and is identical for every row in a run, so it would
  order nothing. All `QUALIFY` clauses order by business columns to stay
  deterministic across runs.
* **`POSITION_TEXT`, not `FINISH_POSITION`, determines classification.** The
  source populates `FINISH_POSITION` even for retirements, so deriving
  classification from it would count retirements as finishers.
* **`GRID_POSITION = 0` means a pit-lane start**, not pole. It is normalised
  to `NULL` so positions-gained stays meaningful.
* **Sprint races are not in `RAW`.** Only main-race results are landed, so
  championship points read lower than official tables for 2021-onward
  seasons. 2022 Verstappen totals 433 here vs. an official 454 — exactly his
  21 sprint points. Closing this gap requires adding the sprint endpoint
  upstream, not changing these models.

## Running

Credentials come from `capstone/.env` and `capstone/.ssh/snowflake_key.p8` via
the helper script one level up:

```bash
cd ..            # capstone/
source dbt_env.sh

cd dbt/fast_track
dbt deps         # first time only, installs dbt_utils
dbt debug        # verify the Snowflake connection
dbt build        # run models + all tests
```

## Modular execution (for Airflow)

`dbt build` runs models and their tests together in DAG order, so one task per
layer is both safe and the smallest useful unit of work. This sequence is
verified to reproduce the monolithic `dbt build` exactly — 389 nodes total:

| # | Task | Command | Nodes |
|---|---|---|---|
| 1 | `dbt_deps` | `dbt deps` | – |
| 2 | `test_sources` | `dbt test --select "source:*"` | 12 |
| 3 | `build_staging` | `dbt build --select staging --exclude "tag:cross_layer"` | 45 |
| 4 | `build_silver` | `dbt build --select silver --exclude "tag:cross_layer"` | 98 |
| 5 | `run_snapshots` | `dbt snapshot` | 2 |
| 6 | `test_snapshots` | `dbt test --select "snap_f1__drivers snap_f1__constructors"` | 10 |
| 7 | `build_mart` | `dbt build --select mart --exclude "tag:cross_layer"` | 220 |
| 8 | `test_reconciliation` | `dbt test --select "tag:cross_layer"` | 2 |

Run them strictly in order; each layer reads the one before it. Steps 5-6 are
optional — nothing in `mart` except `mrt__dim_drivers_history` /
`mrt__dim_constructors_history` depends on the snapshots, so a DAG that
doesn't need SCD2 history can skip straight from step 4 to step 7.

### Why `--exclude tag:cross_layer`

Two singular tests —
`assert_driver_standings_match_result_totals` and
`assert_constructor_standings_match_result_totals` — reconcile **mart against
silver**, so they query tables from both layers. dbt's indirect test selection
pulls them into *both* the silver and the mart selection. Left alone, the
silver task would query a mart table that does not exist yet on a cold run and
fail.

Tagging them `cross_layer`, excluding that tag from the per-layer tasks, and
running them once at the end fixes this and makes the layer counts partition
cleanly: 12 + 39 + 84 + 10 + 202 + 2 = **349 data tests**, exactly matching
the monolithic build, no overlap and no gaps.

### Why snapshots get their own test task

`snap_f1__drivers` and `snap_f1__constructors` are a different dbt resource
type (`snapshot`, not `model`), so the `staging`/`silver`/`mart` path and tag
selectors never match their attached tests (`not_null` on `DBT_SCD_ID`,
`DBT_VALID_FROM`, etc.) — they would silently never run under any of the
other selectors. Selecting them by name (`snap_f1__drivers
snap_f1__constructors`) is what closes that gap; confirmed by diffing every
test selector's output against the full test list for this project.

### Notes for the DAG

* **Exit codes** — dbt exits non-zero on error, zero on warning. The 7
  expected warnings above will not fail a task.
* **No incremental models** — every model is a view or a full table rebuild,
  so tasks are idempotent and `--full-refresh` is never required.
* **Unit tests** need a warehouse connection but no source data, so
  `dbt test --select "test_type:unit"` is a fast pre-flight gate.
* **Environment** — tasks must have `SNOWFLAKE_*` exported (see
  `capstone/dbt_env.sh`) and run with `--project-dir`/`--profiles-dir` pointed
  at this folder. Add `--target prod` once a prod output exists in
  `profiles.yml`.
* **Validation in CI** — `dbt parse --no-partial-parse` catches unused config
  paths and deprecated generic-test syntax without touching the warehouse.

### Other useful selectors

```bash
dbt build --select +mrt__fct_driver_standings   # a model and everything upstream
dbt build --select slv_f1__results+         # a model and everything downstream
dbt build --select tag:snapshot                 # just the periodic-snapshot standings facts
dbt snapshot --select tag:scd2_snapshot          # just the SCD Type 2 snapshots
dbt test  --select "test_type:unit"             # unit tests only
dbt ls    --select mart --resource-type model   # preview what a selector resolves to
```

After editing any model YAML, run `dbt parse --no-partial-parse` to catch
unused config paths and deprecated generic-test syntax.
