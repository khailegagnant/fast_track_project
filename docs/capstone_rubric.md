# Capstone Rubric
 
Two tiers. **Minimum is what you need to pass** — it's fully achievable
with only what was taught hands-on in this course. Exceeds Expectations is
the only optional tier; not reaching it does not affect whether you pass.
 
Grading is checklist-based, not points-weighted — a submission passes once
**every** Minimum item is met, and reaches Exceeds Expectations once at
least one item in that tier is also met.
 
**On verification:** since every student has their own separate Snowflake
trial account, a grader can't always just glance at a shared warehouse to
confirm something worked. The required screenshots (see
`capstone_project.md`'s Evidence section) are the fast first-pass check for
anything that happens inside Snowflake or Airflow. They're not a
substitute for working code, though — because the whole project is meant
to be runnable against anyone's own free trial (same convention as the
Unit 20 & 21 and Unit 22 & 23 demos), a grader may still clone the repo and
run it directly against their own account if a screenshot looks off or a
submission needs a closer look.
 
## Minimum — must pass
 
| # | Criterion | How it's checked |
|---|---|---|
| M1 | dbt project builds successfully (`dbt build` or equivalent) against a real Snowflake free-trial account | Primarily the required screenshot (see `capstone_project.md`'s Evidence section) of Snowflake showing the built tables + row counts; the grader may also run it directly against their own trial account to confirm |
| M2 | Data was landed from a live public API meeting the selection criteria (or the approved recommended shortlist) in `capstone_project.md` | Check the source against the criteria; PokéAPI as a primary source does not meet this |
| M3 | A staging layer exists, feeding at least one mart | Read the dbt project's `models/` structure |
| M4 | The mart includes at least one fact table with an explicitly stated grain | Look for a stated grain (comment or README) — "one row per X" |
| M5 | The mart includes at least two dimension tables, each with a warehouse-generated surrogate key | Check for a surrogate key column (e.g. `md5()` of a natural key), not just the source's raw ID |
| M6 | At least 3–5 dbt schema tests exist and pass (or a deliberately-failing one is explicitly documented and explained) | `dbt test` output, or the project's test results |
| M7 | At least one basic Python (pytest) test exists, covering some part of the fetch/transform logic | Find and run the test file |
| M8 | The pipeline is orchestrated as an Airflow DAG — at least a fetch/land task and a `dbt build` task, chained in order | Floci or a local Airflow install both count; the required screenshot of the DAG run (graph/tree view showing success) is the primary evidence, not just the DAG file existing |
| M9 | A README exists covering: how to run the project (including triggering the DAG), the grain decision and rationale, and 1–2 business questions answered via SQL against the finished model | Read the README; the required screenshot of the business-question query results is the evidence those queries actually ran and returned something real |
| M10 | Everything required above runs locally, at zero cost — no paid API tier, no paid cloud resource (the optional AWS nice-to-have is exempt from this — see Exceeds Expectations) | Ask, or check for any billing-requiring service among the *required* parts |
| M11 | No real credentials are committed anywhere in the repo (Snowflake key, API keys) | Scan the repo; confirm `.env`/`profiles.yml`-style secrets are gitignored |
| M12 | SCD Type 2 implemented on a plausibly-changing dimension via a dbt snapshot | Demonstrated across at least two pipeline runs with a simulated change; the required screenshot is a Snowflake query showing both retained row versions with their `dbt_valid_from`/`dbt_valid_to` (or equivalent) |
| M13 | A fail/quarantine/warn DQ pattern implemented (not just bare pass/fail tests), covering more than one DQ dimension | E.g. a quarantine table for failing rows, `severity: warn` used deliberately for lower-stakes checks |
| M14 | A simple DQ reporting output — e.g. a pass-rate-by-table summary | Doesn't need a dashboard; the required screenshot of the summary's query output is enough evidence, no live dashboard needed |
 
## Exceeds Expectations — stretch tier
 
All of Minimum, plus at least one of the following (more than one is a
bonus, not a requirement):
 
| # | Criterion | Notes |
|---|---|---|
| E1 | AWS-flavored landing (S3 + Glue Catalog) instead of local files | Either Floci (free) or a real AWS account — your choice, cost is not a grading factor either way; must genuinely use the service, not just a comment saying it would |
| E2 | A Spark step used for cleaning/deduplication before dbt takes over | Local PySpark is fine — no cloud Spark required |
| E3 | A PII-style masking/tokenization touch on a sensitive-looking field, with a short written reflection on what real PII/security handling would require in production | The reflection matters as much as the masking itself — shows the thinking, not just the technique |
 
## What does *not* affect grading
 
- Which API you picked, as long as it meets the selection criteria —
  there's no "better" or "worse" choice among valid options.
- Whether you ran Airflow via Floci or a plain local install — either
  satisfies M8.
- If you attempt the AWS nice-to-have (E1), whether you used Floci or a
  real paid AWS/GCP account — your choice, no credit either way for which
  one.
- Code style/formatting preferences, beyond basic readability.
- Whether you attempted every stretch item — attempting zero, one, or all
  of them are all valid outcomes; partial stretch attempts are graded on
  what's actually working, not effort.