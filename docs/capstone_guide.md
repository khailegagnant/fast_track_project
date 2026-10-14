# Capstone Guide — A Suggested Path Through It
 
This isn't a step-by-step recipe with exact commands — the point of the
capstone is that you make the implementation decisions yourself. What
this guide gives you instead: a sensible order to tackle things in, and
exactly which prior lecture/demo to go back to if you get stuck on a
particular phase.
 
If you're not sure where to start, start at Phase 0 and go in order. If a
phase is dragging on far longer than it should, that's a signal to
simplify your approach for that phase, not to push through.
 
---
 
## Phase 0 — Pick and sanity-check your data source
 
Pick one API from `capstone_project.md`'s shortlist, or your own pick
against its selection criteria. Before building anything:
 
- Actually hit the API once (`curl` or a Python `requests` call) and look
  at a real response. Confirm it's structured the way you expect, and
  that you can see at least one fact-like field (a measure, an event, a
  timestamp) and enough variety to derive 2+ dimensions from it.
- If it needs an API key, get it now — some free-tier signups take a few
  minutes to activate.
 
**If stuck:** this is exactly what `land_pokemon_batch` does in the Unit
22 & 23 demo (`pipeline_dag.py`) — a plain `requests.get()` call against a
public API, no auth. Use it as a reference for *how* to make the call, not
what to call.
 
## Phase 1 — Set up Snowflake
 
If you already have a Snowflake trial + service user + key pair from
following along with Unit 20 & 21 or Unit 22 & 23, **you're done — skip
to Phase 2.**
 
Otherwise: follow the Unit 20 & 21 demo README's Snowflake setup section
(free trial signup, warehouse/database/role, a dedicated `TYPE = SERVICE`
user with key-pair auth — never your personal login, never a password).
 
## Phase 2 — Land raw data
 
Write a small script that pulls data from your chosen API and writes it
to local files (JSON or CSV). Keep it simple — this doesn't need to be a
DAG or a class, just a script you can re-run.
 
Things worth thinking about here (you decide how far to take them):
- Do you need pagination to get a meaningful amount of data?
- Is there an obvious grain in the raw response (one row per event,
  reading, or record)?
 
**If stuck:** `land_pokemon_batch` in the Unit 22 & 23 demo is the
reference pattern — fetch, land raw, don't transform yet.
 
## Phase 3 — Staging models
 
Set up a dbt project (`dbt init` or hand-roll the folder structure — your
choice) with your Snowflake connection wired up via `profiles.yml` reading
from environment variables, same pattern as both demos. Build a staging
model or two that reads your raw landed data and does light
cleaning/typing — nothing star-schema-shaped yet.
 
**If stuck:** see `stg_pokemon.sql` in the Unit 22 & 23 demo, or the
staging models in the Unit 20 & 21 demo, for the shape of a staging layer.
 
## Phase 4 — Star schema marts
 
This is the core of the capstone. From your staging model(s), build:
- **At least one fact table**, with the grain stated explicitly (a
  comment at the top of the model is fine, same convention the demos
  use) — "one row per X."
- **At least two dimension tables**, each with a warehouse-generated
  surrogate key (an `md5()` hash of the natural key is enough — no
  package dependency required).
 
Decide your own grain, your own dimension boundaries, your own surrogate
key approach. This is the part of the exercise that's actually testing
dimensional modeling, so don't just copy the shape of `dim_pokemon`/
`fact_pokemon_stats` wholesale — think about what grain makes sense for
*your* data.
 
**If stuck:** Unit 20 & 21's subtopic A2 (dimensional modeling) and its
demo's `marts/` folder (`dim_customer`, `dim_product`, `fact_sales`) are
the reference for star schema mechanics — fact vs. dimension, grain,
surrogate keys. The Unit 22 & 23 demo's `dim_pokemon`/`dim_type`/
`fact_pokemon_stats` is a second, smaller example of the same pattern.
 
## Phase 5 — SCD Type 2 on a changing dimension
 
Pick one of your dimensions that could plausibly change over time (a
customer's address/segment, a drug's labeling status, a team's roster —
depends on your data). Add a dbt snapshot for it, and run your pipeline
twice with a simulated change in between (edit the underlying value, or
re-fetch with a tweak) so you end up with two retained versions.
 
**If stuck:** Unit 20 & 21's subtopic A3 covers SCD Type 1 vs. Type 2 and
the dbt snapshot mechanics (`dbt_valid_from`/`dbt_valid_to`). The Unit 22 &
23 LAB_GUIDE's Part 13 shows the general "run twice, prove it accumulated"
technique (there it's about incremental materialization rather than SCD2,
but the verification approach — run, change something, run again, check
both states are visible — is the same idea).
 
## Phase 6 — Data quality checks
 
Add dbt schema tests (`unique`, `not_null`, `accepted_values`,
`relationships`) to your staging and mart models — at least 3–5 total.
Think about which DQ dimension each test is actually checking
(completeness, validity, uniqueness) rather than adding tests at random.
 
Then go beyond bare pass/fail: implement a **fail/quarantine/warn**
pattern covering more than one DQ dimension — e.g. route rows that fail a
validity check to a `_quarantine` table while good rows continue, and use
`severity: warn` deliberately for lower-stakes checks instead of failing
the whole build. Add a simple **DQ reporting** output too — a model or
query that summarizes pass rates (doesn't need to be a dashboard).
 
Also add at least one basic Python test (`pytest`) somewhere in your fetch
or transform code — even something as simple as "does this function
return the expected number of fields" or "does the output file exist and
have the expected row count" satisfies the requirement.
 
**If stuck:** Unit 20 & 21's Part B (Data Quality Frameworks) covers the
four DQ dimensions, the dbt test config pattern (`severity: error` vs.
`severity: warn`), and the quarantine-pattern SQL sketch in detail.
 
## Phase 7 — Orchestrate it with Airflow
 
Wrap your fetch → stage → dbt build steps as tasks in an Airflow DAG,
instead of running them by hand. Either Floci (free, local — same as
Unit 22 & 23) or a plain local Apache Airflow install works; pick whichever
you're more comfortable setting up. At minimum: a task to fetch/land your
data, and a task that runs `dbt build` — chained with `>>` so dbt only
runs after landing succeeds.
 
**If stuck:** the Unit 22 & 23 demo's `pipeline_dag.py` and its
`LAB_GUIDE.md` are the direct reference — same shape (fetch task → dbt
build task), just against your own data instead of PokéAPI. Note its
`_dbt_working_dir` pattern (a per-run working directory, not a single
fixed path) if you run your DAG more than once — see the demo's docstring
for why that matters.
 
## Phase 8 — Write it up
 
In a README: how to run your project end to end (including triggering the
DAG), your grain decision and why you chose it, and 1–2 business-style
questions you can answer by querying your finished model (e.g. "a top-N by
some measure" or "a trend over time" — the exact question depends on your
data).
 
This is also when you grab the required screenshots — see
`capstone_project.md`'s "Evidence to include" section for the exact list.
Easiest to do this right after your last successful DAG run, while
everything's still fresh in Snowflake/Airflow rather than re-running things
later just to get a screenshot.
 
## Phase 9 — Optional stretch goals
 
Only attempt these if Phases 0–8 are solid and you have time left. Pick
whichever interests you — they're independent of each other:
 
- **AWS-flavored landing**: land your raw data in an S3 bucket and
  register it in a Glue Data Catalog, instead of local files — via Floci
  (free) or a real AWS account if you'd rather. See the Unit 22 & 23
  demo's `transform_pokemon` task.
- **A Spark step**: use PySpark locally (no cloud needed) to clean or
  dedupe your raw data before handing off to dbt. See Unit 15/16's local
  PySpark + Jupyter setup.
- **PII-style masking**: if your data has any field that resembles
  sensitive data (an email, a name, a patient identifier), mask or hash it
  in a staging model, and write a short reflection on what real PII
  handling would require in production. See Unit 24 for the concepts
  (classification, masking/tokenization) — you don't need Unit 24's AWS
  tooling (Macie, Lake Formation) for this, just the technique and the
  reasoning.
 
---
 
Check your work against `capstone_rubric.md` before calling it done.