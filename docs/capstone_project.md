# Capstone Project — DE FastTrack (GHC Q3 2026)
 
## What this is
 
The final deliverable for the DE FastTrack program. You'll build a small,
real, end-to-end analytics pipeline — from a live data source, orchestrated
by Airflow, through a proper dbt star schema with SCD Type 2 history and a
real data quality pattern, into a real Snowflake warehouse — using the
skills from the whole course. It's meant to be **achievable, not
exhausting** — if a step is dragging on far longer than it should,
simplify your approach for that step rather than grinding through it.
 
This is graded against a clear rubric (see `capstone_rubric.md`) with two
tiers: **Minimum** (what you need to pass) and **Exceeds Expectations**
(optional stretch, genuinely optional — not reaching it doesn't affect
whether you pass). Everything in Minimum is achievable with only what's
been taught hands-on in this course.
 
See `capstone_guide.md` for a suggested phase-by-phase walkthrough if
you're not sure where to start.
 
## The task
 
You're a data engineer. You've picked a live public data source (see
below). Your job:
 
1. **Land** raw data from that source somewhere reproducible (local files
   are fine).
2. **Model** it with dbt into a real star schema: one fact table at a
   clearly declared grain, plus at least two dimension tables.
3. **Materialize** that model against a real Snowflake account (free
   trial).
4. **Track history** on at least one plausibly-changing dimension using
   SCD Type 2 (a dbt snapshot), demonstrated across two runs with a
   simulated change.
5. **Add a real data quality pattern** — not just bare pass/fail tests:
   fail/quarantine/warn across more than one DQ dimension, plus a simple
   pass-rate summary — so a bad pipeline run doesn't silently produce bad
   data, and you can see at a glance how healthy a run was.
6. **Orchestrate it with Airflow** — the whole fetch → stage → dbt build
   sequence should run as a DAG, not a string of commands you run by hand.
7. **Write up** what you built: how to run it, why you modeled it the way
   you did, and 1–2 business-style questions answered by querying your
   finished model.
 
## Must-have vs. nice-to-have
 
| | Requirement |
|---|---|
| **Must have** | dbt, Snowflake |
| **Must have** | Airflow — your pipeline must be orchestrated as a DAG, not just a sequence of manual commands. Floci (free, local — same as Unit 22 & 23) or a plain local Apache Airflow install both satisfy this; either is free |
| **Must have** | At least locally deployable (your laptop, zero cost) |
| **Must have** | Some level of testing: dbt schema tests (validation) **and** at least one basic Python test (unit test) |
| **Must have** | The required parts above (dbt, Snowflake, Airflow, your data fetch) are achievable at zero cost — no paid cloud resources, no paid API tiers, no licensing costs |
| Nice to have | Spark (e.g. a cleaning/dedup step before dbt) |
| Nice to have | AWS — fully optional. Use **Floci** (free, local) if you want the practice without any cost, *or* a real AWS/GCP account if you'd rather and don't mind a small bill — your call, it doesn't affect grading either way |
 
## Picking your data source
 
Rather than everyone using the same fixed dataset, **you pick a live
public API** — this is closer to a real assignment ("go get data from
this system") than a pre-packaged CSV, and it's more interesting to build
against something you're curious about.
 
### Selection criteria (use these if proposing your own)
 
Your chosen API must be:
- **Free**, with no paid tier required — either no authentication at all,
  or a free API key you can get in a few minutes.
- **Structured** with enough variety to support at least one *fact-like*
  thing (something with a measurable event — a price, a score, a
  reported incident, a transaction) and at least two *dimension-like*
  things (reference/entity data — a company, a team, a drug, a location).
  Your source doesn't need to already be split into separate
  endpoints/tables for this — turning a flatter feed into a proper star
  schema is part of the exercise.
- **Reasonably stable and documented** — you shouldn't have to
  reverse-engineer an undocumented API to use it.
- **Modest in volume** — enough data to be meaningful, not so much that
  pagination or rate limits eat your whole time budget.
 
### Recommended shortlist
 
Pick one of these, or propose your own against the criteria above:
 
| API | Flavor | Why it works |
|---|---|---|
| [openFDA](https://open.fda.gov/apis/) | Healthcare | Free, no key required. Drug adverse-event and recall data — genuinely relational (drug, event, outcome, patient demographics), supports a real fact+dimension split. |
| A free-tier stock/market API (e.g. [Alpha Vantage](https://www.alphavantage.co/)) | Finance | Free API key. Naturally gives you a `fact_price` time series plus `dim_company`/`dim_date`. |
| [TheSportsDB](https://www.thesportsdb.com/api.php) | Sports | Free, no key required for the public tier. Teams, players, and match results with scores — a clean `fact_match` + `dim_team`/`dim_player` shape. |
| A municipal open-data feed (e.g. [NYC Open Data 311 requests](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9)) | Civic/ops | Free, no auth. Rich for fact/dimension modeling (request type, agency, borough, date). |
 
**Please don't use PokéAPI** for your primary source — it's the exact
dataset from the Unit 22 & 23 demo, so building against it again wouldn't
demonstrate you can do this with something new.
 
## Where this connects to the course
 
You're not starting from zero — every must-have skill here was taught
hands-on:
 
| Skill | Where it was taught |
|---|---|
| dbt models, sources, refs, materializations, tests | **Unit 20 & 21 demo** (`analytical-data-modeling-and-data-quality/demo/`) |
| Star schema design: fact/dimension tables, grain, surrogate keys | **Unit 20 & 21**, subtopic A2 |
| SCD Type 1/2 via dbt snapshots | **Unit 20 & 21**, subtopic A3 |
| Data quality dimensions, fail/quarantine/warn pattern | **Unit 20 & 21**, Part B |
| Snowflake free-trial setup, key-pair service-user auth | **Unit 20 & 21 demo** README |
| Fetching from a live public API in Python | **Unit 22 & 23 demo** (`pipeline_dag.py`'s PokéAPI fetch) |
| Airflow/MWAA orchestration (DAGs, task dependencies, running dbt from a task) | **Unit 22 & 23** |
| Floci (local AWS emulator, if you attempt the AWS nice-to-have) | **Unit 22 & 23 demo** |
| Basic Python testing (pytest, fixtures, assertions) | **Unit 1 & 2** |
 
## Deliverables checklist
 
- [ ] A dbt project that builds successfully against your own Snowflake
      free-trial account
- [ ] A staging layer + at least one mart with a fact table (grain stated
      explicitly, e.g. in a model comment or the README) and ≥2 dimension
      tables
- [ ] At least 3–5 dbt schema tests, all passing (or explicitly documented
      if you deliberately left one failing to demonstrate something)
- [ ] At least one basic Python test (pytest) covering some part of your
      fetch/transform logic
- [ ] SCD Type 2 on a plausibly-changing dimension, demonstrated across two
      runs with a simulated change
- [ ] A fail/quarantine/warn DQ pattern across more than one DQ dimension,
      plus a simple DQ pass-rate summary
- [ ] The pipeline runs as an Airflow DAG (Floci or a local Airflow
      install), not a manual sequence of commands
- [ ] A README covering: how to run it, your grain decision and why, and
      1–2 business questions answered via SQL against your finished model
- [ ] Confirmation everything required ran locally, at zero cost
 
Optional (see `capstone_rubric.md` for what each buys you):
- [ ] AWS-flavored landing via Floci (S3 + Glue Catalog), or a real AWS/GCP
      account if you'd rather
- [ ] A Spark cleaning/dedup step
- [ ] A PII-style masking/tokenization touch + a short reflection on real
      PII/security handling
 
## Evidence to include (screenshots)
 
You each have your own separate Snowflake trial account, so nobody grading
this can just glance at a shared warehouse to confirm it worked. Include
these screenshots in your README (inline images, or a `screenshots/`
folder linked from it) as direct evidence for the items that happen inside
Snowflake or Airflow:
 
- [ ] **Your tables in Snowflake** — a worksheet/UI view showing the
      finished mart tables and their row counts, right after a successful
      `dbt build`
- [ ] **Your business questions, answered** — the query + result for each
      of the 1–2 business questions from your README
- [ ] **SCD Type 2 in action** — a query result showing both retained
      versions of a changed row, with their `dbt_valid_from`/
      `dbt_valid_to` (or equivalent) values visible
- [ ] **Your DQ summary output** — the query result for your pass-rate/DQ
      reporting model
- [ ] **A successful Airflow DAG run** — the graph or tree view showing
      your tasks succeeded, not just the DAG file existing
 
Screenshots are the fast first-pass proof, not a replacement for working
code — since this whole project is meant to be runnable against anyone's
own free trial, a grader may still clone your repo and run it directly if
something needs a closer look.
 
## Credentials and cost
 
Same rules as every demo in this course: **never commit real credentials**
to your repo. Use a `.env`/`profiles.yml` pattern with a `.gitignore`, same
as the Unit 20 & 21 and Unit 22 & 23 demos. Snowflake auth should be
key-pair via a dedicated service user, not your personal login — see the
Unit 20 & 21 demo README for the exact setup steps if you need a refresher.
 
Everything *required* (dbt, Snowflake free trial, Airflow via Floci or a
local install, your chosen API) should cost you nothing. The one exception
is the AWS nice-to-have: if you'd rather practice against a real AWS/GCP
account than Floci, that's fine and won't affect grading — just know it's
optional, and any cost there is on you, not a requirement of the capstone.