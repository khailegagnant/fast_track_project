# fast_track — dbt Model Documentation

Transformation layer of the F1 capstone pipeline. This document covers **only
the dbt project**: the layer contract, the data contract for every model, how
the tables join, and the grain and purpose of each mart table.

---

## 1. Pipeline position

```
F1 API (JSON) ─► S3 (raw JSON) ─► Glue/Spark (explode to parquet)
              ─► S3 (parquet)  ─► COPY INTO ─► F1_CAPSTONE.RAW
                                                    │
                                          ╔═════════▼═════════╗
                                          ║   dbt starts here ║
                                          ╚═══════════════════╝
                              staging ──► silver ──► mart
```

dbt owns everything from `F1_CAPSTONE.RAW` onward. `RAW` is an append-style
landing zone — nothing is cleaned, typed, or deduplicated before dbt.

---

## 2. Layer contract

Each layer has one job. The contract is what makes the layers independently
reviewable and the DAG safe to run task-by-task.

| | **staging** | **silver** | **mart** |
|---|---|---|---|
| **Schema** | `<target>_F1_PIPELINE` | `<target>_F1_PIPELINE` | `<target>_F1_MART` |
| **Materialization** | view | table | table |
| **Prefix** | `stg_f1__` | `slv_f1__` | `mrt__` |
| **Reads from** | `source()` only | staging only | silver + mart |
| **May do** | select, rename | cast, trim, dedupe, derive, surrogate keys | join, aggregate, rank, label |
| **Must NOT do** | cast, clean, dedupe, filter, join | join across entities, aggregate | re-derive keys, re-clean |
| **Grain** | identical to RAW | one row per business entity/event | declared per table |
| **Row count** | = RAW | ≤ RAW (dedupe) | varies |

### Why the split is drawn here

**staging does not cast.** A staging view is a *rename-only* mirror of RAW.
This keeps it trivially auditable against the source — a diff of staging vs.
RAW should only ever show column names. If a cast fails or a `TRIM` changes a
value, that is a transformation, and transformations are reviewable work that
belongs in silver.

**silver owns typing and data quality.** Casting, `NULLIF(TRIM(...))`,
deduplication, bounds-checking, and derived business flags all live here. Each
silver model follows the same CTE skeleton so the stages are obvious:

```
<ENTITY>  →  TYPED  →  FILTERED  →  DEDUPLICATED  →  [DERIVED]  →  CONFORMED
  import     cast &    drop rows     enforce the     business      keys &
  from stg   trim      without a     grain           logic         audit cols
                       usable key
```

**silver holds entities, not dimensions or facts.** `slv_f1__drivers` is "the
clean driver list", not `dim_drivers`. Dimensional modelling is a *consumption*
concern — the same silver entity can legitimately feed a dimension in one mart
and a fact in another. Naming it `dim_` in silver would prematurely commit it
to one shape.

**mart is where dimensional modelling happens.** `mrt__dim_*` and `mrt__fct_*`
are the star schema. This is the only layer a BI tool or analyst should query.

---

## 3. Source contract — `F1_CAPSTONE.RAW`

Declared in [`models/sources/src_f1__raw.yml`](../models/sources/src_f1__raw.yml)
as source `F1_RAW`. Types below are as landed by the Glue/parquet `COPY INTO`.

| Table | Grain | Key columns | Notes |
|---|---|---|---|
| `RAW_CIRCUITS` | one row per circuit | `CIRCUIT_ID` | unique in practice |
| `RAW_CONSTRUCTORS` | one row per constructor **per season pulled** | `CONSTRUCTOR_ID` | **duplicated** — 51 rows / 14 constructors |
| `RAW_DRIVERS` | one row per driver **per season pulled** | `DRIVER_ID` | **duplicated** — 137 rows / 48 drivers |
| `RAW_RACES` | one row per race | `SEASON`, `ROUND` | |
| `RAW_QUALIFYING` | one row per driver per session | `SEASON`, `ROUND`, `DRIVER_ID` | blank Q2/Q3 = eliminated earlier |
| `RAW_RESULTS` | one row per driver per race | `SEASON`, `ROUND`, `DRIVER_ID` | main race only, **no sprints** |

### Source-level guarantees dbt asserts (12 tests)

`not_null` on every natural key and on `SEASON`/`ROUND`; `unique` on
`CIRCUIT_ID`. Run them standalone with `dbt test --select "source:*"` as a
pre-flight gate before transforming.

### Known source characteristics

These are **upstream realities**, documented rather than silently patched:

1. **Per-season duplication** in `RAW_CONSTRUCTORS` and `RAW_DRIVERS`. The API
   is queried once per season, so an entity is re-landed each season it
   appears. Resolved in silver.
2. **`POSITION_TEXT` is the classification authority.** `FINISH_POSITION` is
   populated even for retirements, so it cannot be used to decide who
   finished.
3. **`GRID_POSITION = 0` means a pit-lane start**, not pole.
4. **Reserve/FP1-only drivers** have no `DATE_OF_BIRTH` or `NATIONALITY`.
   These produce warn-level test results, not failures.
5. **No sprint races.** Championship points are main-race only and read lower
   than official tables for 2021+ seasons (2022 Verstappen: 433 here vs. 454
   official — exactly his 21 sprint points). Fixing this requires adding the
   sprint endpoint upstream; no dbt change would be correct.

---

## 4. Key strategy

Every surrogate key is an MD5 hash built by `dbt_utils.generate_surrogate_key`
over column lists centralised in
[`macros/surrogate_key_definition.sql`](../macros/surrogate_key_definition.sql).
Defining them once guarantees that a key computed in silver matches the same
key computed in mart.

| Macro | Columns | Produces |
|---|---|---|
| `circuit_sk()` | `CIRCUIT_ID` | `CIRCUIT_KEY` |
| `constructor_sk()` | `CONSTRUCTOR_ID` | `CONSTRUCTOR_KEY` |
| `driver_sk()` | `DRIVER_ID` | `DRIVER_KEY` |
| `race_sk()` | `SEASON`, `ROUND` | `RACE_KEY` |
| `race_result_sk()` | `SEASON`, `ROUND`, `DRIVER_ID` | `RACE_RESULT_KEY` |
| `qualifying_sk()` | `SEASON`, `ROUND`, `DRIVER_ID` | `QUALIFYING_KEY` |
| `driver_standing_sk()` | `SEASON`, `ROUND`, `DRIVER_ID` | `DRIVER_STANDING_KEY` |
| `constructor_standing_sk()` | `SEASON`, `ROUND`, `CONSTRUCTOR_ID` | `CONSTRUCTOR_STANDING_KEY` |

Natural keys (`DRIVER_ID`, `CONSTRUCTOR_ID`, …) are carried alongside the
surrogate keys in every table, so you can join on either and debug without
decoding a hash.

### Audit columns

Every model ends with `{{ audit_columns() }}`, appending:

| Column | Meaning |
|---|---|
| `DBT_LOADED_AT` | `run_started_at` of the dbt run that built the row |
| `DBT_LOADED_BY` | `CURRENT_USER()` |
| `RUN_ID` | dbt `invocation_id`, or `DBT_ENV_RUN_ID` if set |

> **`DBT_LOADED_AT` is constant within a run.** It is stamped from
> `run_started_at`, so it is identical for every row dbt writes in that
> invocation. It therefore **cannot order a `QUALIFY`**. Every deduplication in
> this project orders by business columns instead. This was a real bug caught
> during development.

---

## 5. Entity relationships

```
                     ┌──────────────────────┐
                     │   mrt__dim_circuits  │
                     │   PK CIRCUIT_KEY     │
                     └──────────▲───────────┘
                                │ CIRCUIT_KEY
                     ┌──────────┴───────────┐
                     │    mrt__dim_races    │
                     │    PK RACE_KEY       │
                     │    (SEASON, ROUND)   │
                     └──────────▲───────────┘
                                │ RACE_KEY
        ┌───────────────┬───────┴────────┬──────────────────┐
        │               │                │                  │
┌───────┴────────┐ ┌────┴─────────┐ ┌────┴──────────┐ ┌─────┴─────────────┐
│ mrt__fct_race_ │ │ mrt__fct_    │ │ mrt__fct_     │ │ mrt__fct_         │
│ results        │ │ qualifying_  │ │ driver_       │ │ constructor_      │
│                │ │ results      │ │ standings     │ │ standings         │
│ PK RACE_       │ │ PK QUALI-    │ │ PK DRIVER_    │ │ PK CONSTRUCTOR_   │
│ RESULT_KEY     │ │ FYING_KEY    │ │ STANDING_KEY  │ │ STANDING_KEY      │
└───────┬────────┘ └────┬─────────┘ └────┬──────────┘ └─────┬─────────────┘
        │               │                │                  │
        │ DRIVER_KEY    │ DRIVER_KEY     │ DRIVER_KEY       │
        └───────┬───────┴────────────────┘                  │
                ▼                                            │
      ┌──────────────────────┐                               │
      │   mrt__dim_drivers   │                               │
      │   PK DRIVER_KEY      │                               │
      └──────────────────────┘                               │
                                                             │
        │ CONSTRUCTOR_KEY (from all four facts) ◄────────────┘
                ▼
      ┌──────────────────────────┐
      │  mrt__dim_constructors   │
      │  PK CONSTRUCTOR_KEY      │
      └──────────────────────────┘

   Aggregates (one row per entity per season, no RACE_KEY):
      mrt__agg_driver_season_summary       → dim_drivers, dim_constructors
      mrt__agg_constructor_season_summary  → dim_constructors
```

### Join reference

| From | Column | To | Column |
|---|---|---|---|
| `mrt__dim_races` | `CIRCUIT_KEY` | `mrt__dim_circuits` | `CIRCUIT_KEY` |
| any `mrt__fct_*` | `RACE_KEY` | `mrt__dim_races` | `RACE_KEY` |
| any `mrt__fct_*` | `DRIVER_KEY` | `mrt__dim_drivers` | `DRIVER_KEY` |
| any `mrt__fct_*` | `CONSTRUCTOR_KEY` | `mrt__dim_constructors` | `CONSTRUCTOR_KEY` |
| `mrt__agg_*` | `DRIVER_KEY` / `CONSTRUCTOR_KEY` | corresponding dim | same |

`mrt__fct_constructor_standings` has **no `DRIVER_KEY`** — it is aggregated
above driver level. Every one of these foreign keys is enforced by a
`relationships` test.

> **Lineage note.** Facts resolve their foreign keys in *silver*, from the
> natural keys present on the source row — not by joining to a mart dimension.
> The mart facts simply carry those keys through. This means a fact row can
> never fail to find its dimension due to join ordering, and the
> `relationships` tests verify that guarantee holds.

---

## 6. Staging layer — 6 models

Views. One per RAW table, 1:1 row count. Select and rename only.

| Model | Source | Renames applied |
|---|---|---|
| `stg_f1__circuits` | `RAW_CIRCUITS` | none |
| `stg_f1__constructors` | `RAW_CONSTRUCTORS` | `NAME`→`CONSTRUCTOR_NAME`, `NATIONALITY`→`CONSTRUCTOR_NATIONALITY` |
| `stg_f1__drivers` | `RAW_DRIVERS` | `NATIONALITY`→`DRIVER_NATIONALITY` |
| `stg_f1__races` | `RAW_RACES` | `RACE_TIMESTAMP_UTC`→`RACE_STARTED_AT` |
| `stg_f1__qualifying` | `RAW_QUALIFYING` | `POSITION`→`QUALIFYING_POSITION` |
| `stg_f1__results` | `RAW_RESULTS` | none |

Renames exist to prevent ambiguity once entities are joined — a bare
`NATIONALITY` or `POSITION` is meaningless three joins deep.

**Duplicates are preserved here by design.** `stg_f1__constructors` and
`stg_f1__drivers` carry the per-season duplication from RAW, so their
`DRIVER_ID`/`CONSTRUCTOR_ID` columns are tested `not_null` but **not**
`unique`.

---

## 7. Silver layer — 6 models

Tables. Typed, cleaned, deduplicated, conformed. **This is where casting
happens.**

| Model | Grain | Rows vs. staging | Adds |
|---|---|---|---|
| `slv_f1__circuits` | 1 per `CIRCUIT_ID` | = | `CIRCUIT_KEY`, bounds-checked coords |
| `slv_f1__constructors` | 1 per `CONSTRUCTOR_ID` | **51 → 14** | `CONSTRUCTOR_KEY` |
| `slv_f1__drivers` | 1 per `DRIVER_ID` | **137 → 48** | `DRIVER_KEY`, `DRIVER_FULL_NAME` |
| `slv_f1__races` | 1 per `SEASON`+`ROUND` | = | `RACE_KEY`, `CIRCUIT_KEY` |
| `slv_f1__qualifying` | 1 per `SEASON`+`ROUND`+`DRIVER_ID` | = | `QUALIFYING_KEY` + 3 FKs, times in seconds |
| `slv_f1__results` | 1 per `SEASON`+`ROUND`+`DRIVER_ID` | = | `RACE_RESULT_KEY` + 3 FKs, classification flags |

### Deduplication rules

Each is deterministic and ordered by business columns:

| Model | Partition | Order (winner first) | Why |
|---|---|---|---|
| `slv_f1__circuits` | `CIRCUIT_ID` | name, locality, latitude | defensive only |
| `slv_f1__constructors` | `CONSTRUCTOR_ID` | name, nationality desc | stable across runs |
| `slv_f1__drivers` | `DRIVER_ID` | **permanent number desc**, code, dob, names | highest number ≈ most recent season |
| `slv_f1__races` | `SEASON`, `ROUND` | started_at, date, name desc | latest schedule wins |
| `slv_f1__results` | `SEASON`, `ROUND`, `DRIVER_ID` | points desc, finish asc, laps desc | best/most complete record wins |
| `slv_f1__qualifying` | `SEASON`, `ROUND`, `DRIVER_ID` | position asc, Q3/Q2/Q1 asc | best classification wins |

### Derived business logic

**`slv_f1__results`** — the most logic-heavy model:

| Column | Rule |
|---|---|
| `IS_CLASSIFIED` | `POSITION_TEXT REGEXP '^[0-9]+$'` — **never** from `FINISH_POSITION` |
| `CLASSIFIED_POSITION` | `FINISH_POSITION` only when classified, else `NULL` |
| `STARTING_GRID_POSITION` | `NULLIF(GRID_POSITION, 0)` — pit-lane start → `NULL` |
| `IS_PIT_LANE_START` | `GRID_POSITION = 0` |
| `POSITIONS_GAINED` | grid − classified position; `NULL` if either unknown |
| `FINISH_STATUS_CATEGORY` | `FINISHED` / `LAPPED` / `DISQUALIFIED` / `ACCIDENT` / `MECHANICAL_OR_OTHER` |
| `FASTEST_LAP_TIME_SECONDS` | `f1_lap_time_to_seconds()` on `M:SS.sss` |
| `IS_RACE_WINNER`, `IS_PODIUM_FINISH` | from `CLASSIFIED_POSITION`, so retirements never count |

**`slv_f1__qualifying`**:

| Column | Rule |
|---|---|
| `Q1/Q2/Q3_TIME_SECONDS` | `f1_lap_time_to_seconds()`; malformed → `NULL`, never an error |
| `BEST_QUALIFYING_TIME_SECONDS` | fastest across segments run. Uses a `999999` sentinel because plain `LEAST()` collapses to `NULL` when any argument is `NULL` |
| `FINAL_SEGMENT_REACHED` | `Q3` / `Q2` / `Q1` / `NO_TIME` |

---

## 8. Mart layer — 12 models

Tables in `<target>_F1_MART`. The consumption surface. Four table types.

### 8.1 Dimensions (4)

Conformed, one row per entity, shared by every fact.

| Model | Grain | PK | Purpose |
|---|---|---|---|
| `mrt__dim_circuits` | 1 per circuit | `CIRCUIT_KEY` | Venue attributes. Adds `CIRCUIT_LABEL` and a `"lat,long"` `GEO_COORDINATES` string for mapping tools. |
| `mrt__dim_constructors` | 1 per constructor | `CONSTRUCTOR_KEY` | Team attributes. Adds `CONSTRUCTOR_LABEL`. |
| `mrt__dim_drivers` | 1 per driver | `DRIVER_KEY` | Driver attributes. Adds `DRIVER_LABEL` (`"Max Verstappen (VER)"`) and `DRIVER_AGE_YEARS`. |
| `mrt__dim_races` | 1 per `SEASON`+`ROUND` | `RACE_KEY` | The date/event dimension. **Denormalised with circuit attributes** so a "races in Italy" filter needs no second join. Adds `ROUNDS_IN_SEASON`, `IS_SEASON_OPENER`, `IS_SEASON_FINALE`. |

> `DRIVER_AGE_YEARS` is computed against `CURRENT_DATE()` at build time, so it
> drifts with wall-clock time. For age *at a given race*, use
> `DATEDIFF('year', DATE_OF_BIRTH, RACE_DATE)` against `mrt__dim_races`.

### 8.2 Transaction facts (2)

One row per real-world event. Add *within-event* analytics that require
comparing a row to its peers — which is exactly why they cannot live in
silver, where each row is independent.

#### `mrt__fct_race_results`
**Grain: one row per driver per race** (`SEASON` + `ROUND` + `DRIVER_ID`).
PK `RACE_RESULT_KEY`. FKs: `RACE_KEY`, `DRIVER_KEY`, `CONSTRUCTOR_KEY`.

*Purpose:* the atomic record of race performance — "how did this driver do in
this race, relative to the rest of the field?"

| Added over silver | Meaning |
|---|---|
| `FIELD_SIZE` | entries classified in this race |
| `RACE_DISTANCE_LAPS` | winner's lap count |
| `GAP_TO_WINNER_MILLIS` | race time − winner's; `NULL` unless both were timed |
| `FASTEST_LAP_GAP_SECONDS` | this driver's fastest lap − the race's fastest |
| `PCT_RACE_DISTANCE_COMPLETED` | laps ÷ race distance × 100 |

#### `mrt__fct_qualifying_results`
**Grain: one row per driver per qualifying session.**
PK `QUALIFYING_KEY`. Same three FKs.

*Purpose:* single-lap pace analysis, independent of race outcome.

| Added over silver | Meaning |
|---|---|
| `QUALIFYING_FIELD_SIZE` | drivers in the session |
| `POLE_TIME_SECONDS` | fastest best-time in the session |
| `GAP_TO_POLE_SECONDS` | this driver's best − pole |
| `PCT_OFF_POLE` | gap as % of pole time — comparable across circuits |
| `GAP_TO_TEAMMATE_SECONDS` | vs. the constructor's best; `NULL` if only one car was timed |

### 8.3 Periodic snapshot facts (2) — championship standings

**The only derived subject area.** There is no standings table in `RAW`; these
are computed from `slv_f1__results`.

#### `mrt__fct_driver_standings`
**Grain: one row per driver per round of their season**
(`SEASON` + `ROUND` + `DRIVER_ID`). PK `DRIVER_STANDING_KEY`.

#### `mrt__fct_constructor_standings`
**Grain: one row per constructor per round** (`SEASON` + `ROUND` +
`CONSTRUCTOR_ID`). PK `CONSTRUCTOR_STANDING_KEY`.

*Purpose:* answer "what did the championship table look like after round N?"
for any N, without re-aggregating.

**Why *periodic snapshot* and not transaction:** a transaction fact only has
rows where something happened. These models build a **dense spine** — every
season entrant × every round — so a competitor who misses a round *still gets
a row*, carrying unchanged cumulative totals and `HAS_RACED_THIS_ROUND =
FALSE`. That density is what makes "position after round N" a simple filter
rather than a window query, and what lets you chart a flat line through a
missed race instead of a gap.

Cumulative measures use:

```sql
SUM(...) OVER (
    PARTITION BY SEASON, DRIVER_ID
    ORDER BY ROUND
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
```

`ROWS`, not `RANGE` — each snapshot must reflect only rounds up to and
including itself.

| Column group | Columns |
|---|---|
| Position | `CHAMPIONSHIP_POSITION`, `IS_CHAMPIONSHIP_LEADER`, `POINTS_BEHIND_LEADER` |
| Cumulative | `POINTS_TO_DATE`, `WINS_TO_DATE`, `PODIUMS_TO_DATE`, `SECOND_PLACES_TO_DATE`, `THIRD_PLACES_TO_DATE`, `POLE_POSITIONS_TO_DATE`, `POINTS_FINISHES_TO_DATE`, `RETIREMENTS_TO_DATE`, `BEST_FINISH_TO_DATE` |
| This round | `ROUND_POINTS`, `ROUND_WINS`, `ROUND_BEST_POSITION`, `HAS_RACED_THIS_ROUND` |
| Countback | `RACE_COUNTBACK_KEY`, `QUALIFYING_COUNTBACK_KEY` |
| Snapshot | `SNAPSHOT_DATE` (= that round's race date) |

Driver standings also carry `CONSTRUCTOR_ID`/`CONSTRUCTOR_KEY`, forward-filled
with `LAST_VALUE(... IGNORE NULLS)` across missed rounds and back-filled with
`FIRST_VALUE` for rounds before a mid-season debut — so the FK is never
`NULL`.

#### Tie-break: the full FIA countback

```
points
  → number of 1st places → 2nd places → 3rd places → … (race)
  → number of poles → 2nd in qualifying → 3rd → …      (qualifying)
```

Implemented in [`macros/f1_countback.sql`](../macros/f1_countback.sql). Rather
than emit ~48 `ORDER BY` terms, per-position counts are folded into one
zero-padded fixed-width string per competitor — `001001000…` = one win, one
2nd. Because each count has equal width and positions concatenate best-first,
a `DESC` sort on the string *is* the position-by-position countback:

```sql
RANK() OVER (
    PARTITION BY SEASON, ROUND
    ORDER BY POINTS_TO_DATE DESC,
             RACE_COUNTBACK_KEY DESC,
             QUALIFYING_COUNTBACK_KEY DESC
)
```

Depth is set by the `f1_countback_max_position` var (default 24). Both keys are
exposed as columns so any tie can be audited directly. For constructors, both
cars count — a 1-2 contributes one 1st and one 2nd.

A shared position now means a genuine dead heat: identical on points *and*
every race *and* every qualifying position. The current data contains exactly
one (2026 R2 — Antonelli and Russell swapped 1-2 in both races and swapped pole
in both sessions), surfaced as a warning by
`assert_one_championship_leader_per_round`.

### 8.4 Aggregates (2) — metrics layer

#### `mrt__agg_driver_season_summary`
**Grain: one row per driver per season** (`SEASON` + `DRIVER_ID`).

#### `mrt__agg_constructor_season_summary`
**Grain: one row per constructor per season** (`SEASON` + `CONSTRUCTOR_ID`).

*Purpose:* pre-computed season totals and rates for leaderboards and
season-over-season comparison, without scanning the round-level facts.

**Design guarantee:** final championship position and totals are **read from
the final round's standings snapshot**, not recomputed:

```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SEASON, DRIVER_ID ORDER BY ROUND DESC
) = 1
```

This makes it structurally impossible for the aggregate to disagree with the
standings. Rate metrics (`AVG_POINTS_PER_RACE`, `PCT_RACES_IN_POINTS`,
`PCT_RACES_RETIRED`, `AVG_QUALIFYING_POSITION`, `AVG_GAP_TO_POLE_SECONDS`, …)
are then layered on from the transaction facts.

---

## 9. SCD Type 2 — snapshot layer

Two dbt `snapshot` resources, defined YAML-only in
[`snapshots/snap_f1__models.yml`](../snapshots/snap_f1__models.yml) (no
separate `.sql` file — `relation:` + `config:` live directly in the YAML).
Not part of the staging → silver → mart spine; a standalone, optional history
layer.

| Snapshot | Source | `unique_key` | `check_cols` |
|---|---|---|---|
| `snap_f1__drivers` | `slv_f1__drivers` | `DRIVER_KEY` | `DRIVER_CODE`, `GIVEN_NAME`, `FAMILY_NAME`, `DRIVER_FULL_NAME`, `DRIVER_NATIONALITY`, `PERMANENT_NUMBER`, `DATE_OF_BIRTH` |
| `snap_f1__constructors` | `slv_f1__constructors` | `CONSTRUCTOR_KEY` | `CONSTRUCTOR_NAME`, `CONSTRUCTOR_NATIONALITY` |

### Why `check`, not `timestamp`

The F1 API supplies no genuine `updated_at` for either entity. `DBT_LOADED_AT`
is stamped from `run_started_at`, so — as established in §4 — it is identical
for every row dbt writes in a run and cannot order anything. `strategy: check`
sidesteps this entirely: it re-compares the listed business columns against
the latest version on every run and opens a new version only when one of them
actually differs, with no dependency on any timestamp column existing.

### Why silver, not staging or RAW

A dbt snapshot requires exactly one row per `unique_key` at snapshot time.
Staging (and RAW) still carry the per-season duplicates documented in §3 —
137 driver rows for 48 drivers, 51 constructor rows for 14 — with no column to
order them by, so a snapshot there would be ambiguous. Silver is the first
point in the pipeline with a clean one-row-per-entity representation, and its
dedup rule is already deterministic (§7), so the same key produces the same
row on every run unless the source genuinely changed.

### Why this is not merged into `mrt__dim_drivers` / `mrt__dim_constructors`

`DRIVER_KEY` and `CONSTRUCTOR_KEY` are hashes of the natural key **alone**
(§4), so every historical version of an entity shares one key. Exposing
`DBT_VALID_FROM`/`DBT_VALID_TO` directly on the existing dimensions would mean
multiple rows per key — breaking their `unique` tests and fanning out every
fact join in the mart (one race result joining to N historical driver rows).

Instead, history is exposed through two standalone mart models that are
**never referenced by a fact**:

| Model | Grain | PK | Purpose |
|---|---|---|---|
| `mrt__dim_drivers_history` | one row per driver version | `DRIVER_VERSION_KEY` (= `DBT_SCD_ID`) | "what was true about this driver as of date X" |
| `mrt__dim_constructors_history` | one row per constructor version | `CONSTRUCTOR_VERSION_KEY` | same, for constructors |

Each adds `IS_CURRENT` (`DBT_VALID_TO IS NULL`) and carries `DRIVER_KEY`/
`CONSTRUCTOR_KEY` for reference only — never as a join key into a fact table.
A `dbt_utils.unique_combination_of_columns` test with `config.where:
"IS_CURRENT"` guards that exactly one current version exists per key.

### Why no "current champion" column anywhere in this layer

Championship status is not a descriptive attribute of a driver or
constructor — it is the output of aggregating a season of race results, and
it changes at *round* granularity (up to ~24 times per season), not at the
rare, discrete points a dimension attribute changes. Modelling it via SCD2
here would mean versioning a dimension on every round, inflating it toward
fact-table cardinality and duplicating logic that is already correct:
`IS_CHAMPIONSHIP_LEADER` (per round) and `IS_SEASON_CHAMPION` (per season) on
`mrt__fct_driver_standings` / `mrt__agg_driver_season_summary` (§8.3, §8.4).

### Current state and how it was verified

As loaded today, no driver or constructor has ever had a differing attribute
across its duplicate RAW rows (checked directly: `COUNT(DISTINCT
PERMANENT_NUMBER)` per `DRIVER_ID`, `COUNT(DISTINCT NAME)` per
`CONSTRUCTOR_ID` — both zero rows with more than one distinct value). The
first `dbt snapshot` run therefore produces exactly one version per entity,
with `DBT_VALID_TO` NULL. This is expected, not a limitation: the snapshot is
infrastructure for the next time an attribute changes, not a claim that
history already exists.

Verified by manually changing `max_verstappen`'s `PERMANENT_NUMBER` in RAW,
rebuilding `slv_f1__drivers`, and re-running `dbt snapshot` — the prior
version's `DBT_VALID_TO` closed correctly and a new version opened with a new
`DBT_SCD_ID`, then the change was reverted and both tables rebuilt from the
restored data.

> This dbt engine (`dbt-core 2.0.0-alpha.2` / Fusion) does not materialize a
> `DBT_IS_DELETED` column for `hard_deletes: invalidate` the way some dbt
> versions do — only `DBT_SCD_ID`, `DBT_VALID_FROM`, and `DBT_VALID_TO` are
> guaranteed present. `hard_deletes: invalidate` is configured for forward
> compatibility but its behavior was not separately verified on this engine.

---

## 10. Testing contract

**349 data tests + 14 unit tests + 2 SCD Type 2 snapshots.** All pass; 7
documented warnings.

| Kind | Where | Covers |
|---|---|---|
| Source tests (12) | `src_f1__raw.yml` | RAW keys present before any transform |
| Schema tests | `*__models.yml` | `not_null`, `unique`, `relationships`, `accepted_values`, `accepted_range`, `unique_combination_of_columns` |
| Unit tests (14) | `*__unit_tests.yml` | deterministic logic with fixed inputs |
| Singular tests (6) | `tests/f1-formula_one/` | cross-model business rules |

**Every model's grain is protected** by an explicit
`dbt_utils.unique_combination_of_columns` test — the single most important
class of test in this project.

### Singular tests

| Test | Asserts | Severity |
|---|---|---|
| `assert_driver_standings_match_result_totals` | final standings = sum of actual results | error |
| `assert_constructor_standings_match_result_totals` | constructor points = sum of its cars' | error |
| `assert_driver_standings_points_never_decrease` | cumulative points are monotonic | error |
| `assert_race_round_sequence_is_contiguous` | no gaps in a season's calendar | error |
| `assert_one_championship_leader_per_round` | exactly one leader per round | **warn** |
| `assert_race_entries_have_qualifying` | every race entry has a qualifying row | **warn** |

The first two are tagged `cross_layer` because they read silver *and* mart —
see the README for why that matters to the Airflow DAG.

### Expected warnings

| Warning | Rows | Why |
|---|---|---|
| `not_null` `DATE_OF_BIRTH` (staging) | 23 | reserve/FP1-only drivers |
| `not_null` `DATE_OF_BIRTH` (silver, mart) | 16 | same, post-dedupe |
| `not_null` `DRIVER_NATIONALITY` (silver, mart) | 16 | same |
| `assert_race_entries_have_qualifying` | 6 | source gaps |
| `assert_one_championship_leader_per_round` | 1 | genuine dead heat |

---

## 11. Macros

| Macro | Purpose |
|---|---|
| `audit_columns(inherit=False)` | `DBT_LOADED_AT` / `DBT_LOADED_BY` / `RUN_ID` |
| `f1_lap_time_to_seconds(col)` | `M:SS.sss` → `NUMBER(9,3)`; malformed → `NULL` |
| `surrogate_key_definition.sql` | the eight `*_sk()` key definitions |
| `f1_countback.sql` | position-count columns, cumulative counts, countback key |
| `drop_renamed_orphans()` | one-off cleanup after the gold→mart rename |

> `drop_renamed_orphans` exists because **dbt does not drop the old relation
> when a model is renamed**. After the `gold`→`mart` and silver `dim_`/`fct_`
> renames, 16 orphaned tables remained. Run
> `dbt run-operation drop_renamed_orphans` once per target, then delete the
> macro.

---

## 12. Common queries

```sql
-- championship table after round 10 of 2024
SELECT CHAMPIONSHIP_POSITION, DRIVER_ID, POINTS_TO_DATE, WINS_TO_DATE,
       POINTS_BEHIND_LEADER
FROM   F1_MART.MRT__FCT_DRIVER_STANDINGS
WHERE  SEASON = 2024 AND ROUND = 10
ORDER  BY CHAMPIONSHIP_POSITION;

-- title race progression, for a line chart
SELECT ROUND, DRIVER_ID, POINTS_TO_DATE
FROM   F1_MART.MRT__FCT_DRIVER_STANDINGS
WHERE  SEASON = 2024 AND CHAMPIONSHIP_POSITION <= 3
ORDER  BY ROUND, CHAMPIONSHIP_POSITION;

-- qualifying pace vs. race pace, by circuit country
SELECT    races.COUNTRY,
          drivers.DRIVER_FULL_NAME,
          AVG(quali.GAP_TO_POLE_SECONDS) AS AVG_QUALI_GAP,
          AVG(results.POSITIONS_GAINED)  AS AVG_POSITIONS_GAINED
FROM      F1_MART.MRT__FCT_QUALIFYING_RESULTS quali
INNER JOIN F1_MART.MRT__FCT_RACE_RESULTS results
       ON quali.RACE_KEY = results.RACE_KEY
      AND quali.DRIVER_KEY = results.DRIVER_KEY
INNER JOIN F1_MART.MRT__DIM_RACES races
       ON quali.RACE_KEY = races.RACE_KEY
INNER JOIN F1_MART.MRT__DIM_DRIVERS drivers
       ON quali.DRIVER_KEY = drivers.DRIVER_KEY
GROUP BY  1, 2;

-- reliability by constructor and season
SELECT SEASON, CONSTRUCTOR_ID, TOTAL_CAR_ENTRIES, TOTAL_RETIREMENTS,
       PCT_ENTRIES_RETIRED
FROM   F1_MART.MRT__AGG_CONSTRUCTOR_SEASON_SUMMARY
ORDER  BY SEASON, PCT_ENTRIES_RETIRED;
```

---

## 13. Extending the project

| Change | Where |
|---|---|
| New RAW table | add to `src_f1__raw.yml` → new `stg_f1__*` → new `slv_f1__*` → wire into mart |
| New business rule | silver, with a unit test |
| New metric | mart; in a fact if per-event, in an aggregate if per-season |
| New tie-break tier | `f1_countback.sql` only — both standings models inherit it |
| New SCD2-tracked attribute | add it to `check_cols` in `snap_f1__models.yml`, not a new snapshot |
| New entity needing SCD2 | new silver model first (one row per key), then a new `snapshot` block + `_history` mart model |
| Sprint races | upstream API/Glue first; then `slv_f1__results` needs a session-type column and the standings need to sum across both |
