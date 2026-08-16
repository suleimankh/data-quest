# Rearc Data Quest — Process

## Architecture

I modeled the solution as three **Lakeflow Spark Declarative Pipelines**: one for each independent source system and one for the business-facing analytics layer.

```text
BLS                                  Data USA
 ↓                                      ↓
Programmatic Extraction             Programmatic Extraction
 ↓                                      ↓
Unity Catalog Volume                Unity Catalog Volume
 ↓                                      ↓
BLS Lakeflow Pipeline               Data USA Lakeflow Pipeline
 ↓                                      ↓
Bronze Streaming Tables             Bronze Streaming Table
 ↓                                      ↓
Silver Streaming Tables             Silver Streaming Table
          \                          /
           \                        /
            └── Databricks Workflow ──┐
                  ALL SUCCEEDED        │
                                      ↓
                              Analytics Pipeline
                                      ↓
                              Gold Materialized Views
                                      ↓
                           Validation / Consumption
                               /              \
                              ↓                ↓
                     AI/BI Dashboard      Genie Agent
```

The Unity Catalog organization is:

```text
rearc
├── datausa
│   ├── Volume: raw
│   ├── bronze_population
│   └── silver_population
│
└── bls
    ├── Volume: raw
    ├── bronze_bls_data
    ├── bronze_bls_series
    ├── bronze_bls_measure
    ├── bronze_bls_sector
    ├── silver_bls_data
    ├── silver_bls_series
    ├── silver_bls_measure
    └── silver_bls_sector

rearc_analytics
└── population_analytics
    ├── gold_population_stats
    ├── gold_best_year_by_series
    └── gold_series_population
```

I separated BLS and Data USA because they are independent source systems with different formats, metadata, ingestion behavior, data-quality contracts, and failure boundaries.

The Analytics pipeline is separated from source-specific processing because it owns the business-facing data products, including transformations that consume both source systems.

Within each pipeline, datasets and dependencies are declared using `pyspark.pipelines`. Lakeflow manages the table dependency graph, streaming state, execution, and materialization.

Across pipelines, a Databricks Workflow manages the higher-level dependency between the two source pipelines and the Analytics pipeline.

---

# Source Ingestion

## BLS

The BLS productivity directory is discovered programmatically rather than by hardcoding individual filenames.

This allows the extraction process to continue working if BLS adds or removes files from the directory.

BLS requires automated clients to provide contact information in the `User-Agent` header. Because this repository is public, the example uses a placeholder rather than exposing personal contact information:

```python
headers = {
    "User-Agent": "publicRepo@example.com"
}
```

The complete BLS directory is landed into:

```text
/Volumes/rearc/bls/raw/
```

I preserve the complete source directory as required by the Quest, even though only the datasets needed by the analytical model are promoted into Bronze tables.

## Data USA

The Data USA population API response is retrieved programmatically and landed into:

```text
/Volumes/rearc/datausa/raw/
```

I preserve the complete API response rather than extracting only the fields required for the analytical questions.

## Databricks Free Edition Network Limitation

While implementing source ingestion, I found that my Databricks Free Edition environment did not provide outbound DNS/network access to the required BLS and Data USA endpoints.

I isolated this by testing several external domains.

Allowed destinations such as `pypi.org` resolved successfully, while BLS, Data USA, and other external domains failed during DNS resolution.

This was different from the expected BLS `403 Forbidden` scenario described in the Quest.

The request was failing before reaching BLS, so changing the `User-Agent` could not resolve the problem.

After confirming the limitation with Rearc, I used the agreed approach:

1. Keep the programmatic extraction code in the repository.
2. Execute source retrieval outside the restricted Databricks environment.
3. Land the resulting files into Unity Catalog Volumes.
4. Treat those Volumes as the durable source boundary for the Lakeflow pipelines.

This keeps source acquisition programmatic and reproducible while decoupling downstream processing from external network availability.

---

# Raw Landing and Rerun Strategy

Repeated source extractions are written using timestamped filenames instead of overwriting existing files.

For example:

```text
population_20260814T120000.json
population_20260815T120000.json
population_20260816T120000.json
```

The same pattern is used for BLS extracts.

I chose this because it creates an append-oriented raw history that works naturally with Auto Loader.

It also provides:

- Source version history.
- Auditability.
- Replayability.
- File-level lineage.
- Easier troubleshooting.
- Reliable incremental file discovery.

The timestamp represents **extraction time**: when my ingestion process observed the source.

This is intentionally separate from the business date represented by the data and from any provider-side modification timestamp.

---

# Bronze Layer

Bronze is responsible for preserving **what arrived**.

Both source pipelines use Auto Loader to incrementally ingest files from their Unity Catalog Volumes.

Auto Loader tracks physical files that have already been processed, providing file-level idempotency during normal pipeline reruns.

## Data USA Bronze

A Data USA response is structured approximately as:

```json
{
  "annotations": {},
  "page": {},
  "columns": [],
  "data": []
}
```

I intentionally preserve this structure in Bronze rather than immediately flattening the population observations.

Therefore:

```text
population_20260814.json → 1 Bronze snapshot
population_20260815.json → 1 Bronze snapshot
population_20260816.json → 1 Bronze snapshot
```

Silver later converts each snapshot into individual business records:

```text
New Bronze row / source file
             ↓
        explode(data)
             ↓
      2013 population
      2014 population
      2015 population
             ...
      2024 population
```

This gives the layers a clear semantic boundary:

> **Bronze represents source snapshots. Silver represents individual business records.**

I also preserve Databricks `_metadata` in Bronze so each snapshot retains lineage back to the physical source file.

Because Data USA intentionally uses source field names such as `Nation ID`, I enabled Delta column mapping in Bronze rather than modifying the provider's raw representation. Those names are normalized later in Silver.

## BLS Bronze

BLS files are tab-separated and some source headers contain surrounding whitespace.

That whitespace has no business meaning and creates invalid Delta column names.

I therefore normalize only the BLS header whitespace during ingestion:

```python
def clean_column_names(df):
    return df.toDF(*[c.strip() for c in df.columns])
```

The underlying source files remain unchanged in the raw Volume.

The BLS Bronze layer exposes the datasets required downstream:

- Productivity observations.
- Series metadata.
- Measure lookup.
- Sector lookup.

The remaining BLS files remain preserved in the raw Volume.

---

# Schema Evolution

Data USA Bronze uses:

```python
.option("cloudFiles.inferColumnTypes", "true")
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
```

## `inferColumnTypes`

The source JSON contains meaningful primitive types such as years and population values.

Allowing Auto Loader to infer those types preserves more source semantics than initially representing everything as strings.

Silver still owns the final analytical contract.

## `addNewColumns`

External APIs evolve.

If Data USA adds another field, I want Bronze to preserve it rather than silently discard information because the original schema did not contain the new field.

The intended separation is:

```text
External Source
      ↓
Bronze
Flexible source representation
Additive schema evolution
      ↓
Silver
Controlled analytical contract
      ↓
Gold
Stable business outputs
```

This does not mean every schema change is automatically safe.

Missing required fields, renamed fields, or incompatible type changes should be detected and handled rather than automatically accepted.

Bronze is intentionally flexible; Silver is intentionally stricter.

---

# Silver Layer

Silver establishes **what can be trusted**.

The source-specific Silver pipelines transform raw provider representations into typed analytical records and apply data-quality rules.

## Data USA Silver

Silver:

- Explodes the nested `data` array.
- Selects required business fields.
- Normalizes provider field names.
- Applies analytical types.
- Enforces Lakeflow expectations.
- Deduplicates on the business key.

The resulting model is intentionally simple:

```text
nation_id     STRING
nation        STRING
year          INT
population    BIGINT
```

Expectations enforce required fields such as:

```text
nation_id IS NOT NULL
year IS NOT NULL
population IS NOT NULL
```

## BLS Silver

BLS Silver similarly normalizes the observation and reference datasets.

The observation business key is:

```text
series_id + year + period
```

Expectations enforce:

```text
series_id IS NOT NULL
year IS NOT NULL
period IS NOT NULL
value IS NOT NULL
```

Series, measure, and sector remain separate reference datasets so Gold can construct human-readable descriptions without carrying unnecessary source attributes.

---

# File-Level Idempotency vs. Business-Level Deduplication

Auto Loader and Silver deduplication solve different problems.

**Auto Loader asks:**

> Have I already processed this physical file?

**Silver asks:**

> Have I already processed this business record?

For example:

```text
population_20260814.json
        ↓
2013
2014
...
2024

population_20260815.json
        ↓
2013
2014
...
2024
```

These are different physical source snapshots, so Auto Loader should process both.

After `explode(data)`, however, Silver sees the same `nation_id + year` business keys again.

For the supplied assessment data, Silver uses:

```python
.dropDuplicates(["nation_id", "year"])
```

to prevent repeated observations from propagating downstream.

---

# Deduplication vs. Source Corrections

Simple deduplication is not equivalent to Change Data Capture.

For example:

```text
Snapshot 1
2023 → 334,914,895

Snapshot 2
2023 → 335,000,000
```

The second observation could represent a legitimate correction.

Both have the same business key, but `dropDuplicates` does not express the rule that the latest authoritative version should replace the earlier version.

I considered making Silver a materialized view over the complete Bronze history and using a window such as:

```text
PARTITION BY nation_id, year
ORDER BY extraction_timestamp DESC
```

with `row_number()` to retain the latest version.

That would provide deterministic latest-version resolution.

I decided not to introduce that additional complexity because the supplied dataset does not contain competing corrected versions that require CDC resolution.

For the Quest, streaming business-key deduplication preserves a simpler incremental architecture:

```text
Bronze Streaming
      ↓
Silver Streaming
      ↓
Gold Materialized Views
```

If corrections were part of the source contract, I would use explicit CDC/upsert semantics.

Likewise, for an asynchronous source such as Kafka, I would not assume arrival order represents business order. I would use an explicit event/source timestamp or sequence number to determine which version is authoritative.

---

# Gold / Analytics Layer

Gold represents **what the business needs**.

Gold is published under:

```text
rearc_analytics.population_analytics
```

and reads trusted Silver datasets using fully qualified Unity Catalog names:

```python
spark.read.table("rearc.datausa.silver_population")
spark.read.table("rearc.bls.silver_bls_data")
```

I use materialized views for Gold because the requested outputs involve aggregations, ranking, and cross-source joins rather than append-only processing.

## Question 1 — Population Statistics

`gold_population_stats`

Answers:

> What are the mean and standard deviation of annual US population across 2013–2018 inclusive?

The output contains:

```text
mean_population
stddev_population
```

## Question 2 — Best Year by BLS Series

`gold_best_year_by_series`

For every BLS series:

```text
Quarterly observations
        ↓
Group by series_id + year
        ↓
SUM(value)
        ↓
Rank annual totals descending
        ↓
Select best year
        ↓
Join BLS metadata
        ↓
Human-readable series label
```

The output contains:

```text
series_id
series_label
best_year
best_year_value
```

The human-readable label combines BLS sector and measure metadata so a consumer does not need to understand the raw BLS series code.

## Question 3 — BLS Metric + Population

`gold_series_population`

The BLS data is filtered to:

```text
series_id = PRS30006032
period    = Q01
```

and left-joined to annual US population by `year`.

The **left join is intentional** because the Quest asks for population *where available*. BLS observations therefore remain present even when Data USA does not contain population for that year.

---

# PySpark and Spark SQL

The Quest asks for each analytical question to be implemented in both PySpark and Spark SQL, with one selected as the primary version feeding Gold.

I chose **PySpark as the primary implementation** for the pipelines and Gold transformations.

Python is a first-class language in Databricks and integrates naturally with Lakeflow Spark Declarative Pipelines through `pyspark.pipelines`.

Coming from a strong software engineering and programming background, I prefer treating data pipelines as maintainable software rather than primarily as collections of SQL statements.

In my experience, the Python and Scala Spark communities tend to naturally bring established software-engineering practices into data engineering:

- Clear code structure.
- Reusable functions.
- Separation of concerns.
- Testability.
- Code review.
- Maintainability.

For a multi-stage transformation, I also find PySpark easier to read as a complete engineering story:

```text
Read trusted data
        ↓
Filter / Validate
        ↓
Transform
        ↓
Aggregate
        ↓
Join
        ↓
Apply business logic
        ↓
Publish result
```

This makes it easier for another engineer reviewing the repository to understand not only **what** an individual transformation does, but how the pieces fit together.

There is also a skills-demonstration reason for this choice.

In my experience, engineers who are comfortable solving Spark problems using the PySpark DataFrame API can generally express the relational portions of that logic in Spark SQL.

The reverse transition can sometimes require a larger step for engineers whose experience is primarily SQL-based, particularly when the solution involves reusable programmatic logic, dynamic transformations, testing, or broader application-engineering patterns.

For those reasons, **I strongly prefer PySpark as the primary implementation for this type of data engineering project**.

This is not a statement that PySpark is inherently better than Spark SQL.

Spark SQL is highly expressive for relational transformations, ad-hoc analysis, and environments where SQL provides the clearest representation of the business logic.

The appropriate tool should depend on the problem and the engineers maintaining the solution.

For this Quest, PySpark is the authoritative implementation feeding the pipeline tables.

Equivalent Spark SQL implementations are included under:

```text
alternative_spark_sql/
├── alternative Bronze.sql
├── alternative Silver.sql
└── alternative gold sql.sql
```

The SQL alternatives mirror the Bronze, Silver, and Gold patterns, including streaming-table definitions for Bronze/Silver and materialized analytical outputs for Gold.

I kept them separate from the primary PySpark implementation so the main execution path remains clear while still demonstrating equivalent Spark SQL capability.

In a production system, I would normally maintain one authoritative implementation of a business rule rather than duplicate Python and SQL implementations unless there were a specific reason to support both.

---

# Validation

I added explicit validation on top of the Gold layer rather than relying only on visual inspection of the output tables.

The validation is maintained under:

```text
gold_test/
└── test_gold.ipynb
```

## Question 1

Validation confirms:

- Exactly one output row.
- Mean population is present.
- Standard deviation is present.
- Results are valid numeric values.

## Question 2

Validation confirms:

- Results exist.
- Exactly one result exists per `series_id`.
- Required fields are populated.
- Human-readable series labels are present.
- `best_year_value` independently matches the maximum calculated annual value for each series.

The last test independently recalculates annual values from Silver and verifies that the value selected by Gold is actually the maximum for that `series_id`.

## Question 3

Validation confirms:

- `PRS30006032 / Q01` returns observations.
- Gold row count matches the corresponding Silver observations.
- Every expected `(year, value)` pair exists.
- Population is populated for years available in Data USA.
- BLS years without population remain present.

The validation completed successfully:

```text
✅ Question 1 passed
✅ Basic Question 2 tests passed
✅ Question 2 max-year validation passed
✅ Question 3 passed
✅ Population join validation passed

🎉 ALL GOLD TESTS PASSED
```

This provides an independent check of the Gold business logic rather than assuming that a successfully materialized table is necessarily correct.

---

# Pipeline Orchestration

The three Lakeflow pipelines are coordinated by a parent Databricks Workflow.

```text
             ┌── BLS Pipeline ────────┐
Start ───────┤                        ├── Analytics Pipeline
             └── Data USA Pipeline ───┘
                     ALL SUCCEEDED
```

BLS and Data USA are independent source pipelines, so they can execute independently and in parallel.

The Analytics pipeline runs only after **both source pipelines succeed**.

## Why `ALL SUCCEEDED`

The Gold tables do not all have identical dependencies:

```text
gold_population_stats
    → Data USA

gold_best_year_by_series
    → BLS

gold_series_population
    → BLS + Data USA
```

I chose `ALL SUCCEEDED` for the Quest because it provides a simple and consistent refresh boundary.

It prevents a cross-source Gold refresh where one source is current while another source failed to refresh.

The trade-off is that if one source fails, a source-specific Gold table that could technically refresh will also wait.

For example:

```text
Data USA ✅
BLS      ❌
     ↓
Analytics does not refresh
```

Even though `gold_population_stats` only requires Data USA.

For this assessment, I preferred the simpler and safer orchestration model.

If this became an availability or freshness problem in production, I would separate Gold processing according to its true dependencies:

```text
Data USA ─────────────→ Population Gold

BLS ──────────────────→ BLS Gold

Data USA ──┐
            ├─────────→ Cross-source Gold
BLS ───────┘
```

This would improve failure isolation and allow source-specific analytics to refresh independently.

The trade-off would be additional pipelines/tasks, dependency management, monitoring, and operational complexity. I would introduce that separation when the increased freshness or availability justified the cost.

---

# Bonus — Business Consumption

## AI/BI Dashboard

As part of the bonus work, I built an **AI/BI dashboard directly on top of the Gold layer**.

The goal was to give a non-technical stakeholder a useful analytical interface without requiring knowledge of Spark, SQL, the underlying source schemas, or BLS series codes.

The dashboard includes:

- Average US population for 2013–2018.
- Population standard deviation.
- BLS series and their best years.
- BLS metric values over time alongside US population.

The dashboard therefore provides both high-level KPIs and more detailed analytical exploration from the same governed Gold layer.

Dashboard screenshots are included under:

```text
screenshots/
└── Bonus/
    └── Gold Dashboard.png
```

## Genie Agent

I also added a **Genie Agent** over the analytical layer to provide a natural-language interface for non-technical consumers.

The dashboard provides curated visual exploration, while Genie allows users to ask follow-up questions conversationally without writing SQL.

Conceptually:

```text
              Governed Gold Layer
                     ↓
          ┌──────────┴──────────┐
          ↓                     ↓
   AI/BI Dashboard         Genie Agent
          \                     /
           \                   /
            Non-technical User
```

This allows the same governed analytical data products to support both predefined business views and self-service natural-language exploration.

Screenshots for the Genie implementation are maintained under:

```text
screenshots/
└── Bonus/
    └── Genie Agents/
```

---

# Bonus — Unity Catalog Access Control

I also added Unity Catalog access controls appropriate for a **read-only analytical consumer** of the Gold layer.

The architecture creates a natural governance boundary:

```text
rearc.datausa / rearc.bls
        ↓
Engineering / source processing


rearc_analytics.population_analytics
        ↓
Governed business-facing analytics
        ↓
Read-only analytical consumers
```

The intent is that an analyst can consume the Gold products without requiring access to raw Bronze or source-oriented Silver datasets.

For a larger production implementation, I would grant access through groups rather than individual identities and execute pipelines under service principals.

This makes the consumption boundary explicit and supports least-privilege access.

---

# Trade-offs and Production Considerations

## Data USA Incremental Extraction Limitation

I did not find a documented Data USA change cursor equivalent to:

```text
updated_since=<timestamp>
```

that returns everything added or modified since the previous ingestion.

This distinction matters because three different notions of time exist:

```text
Business time
    → Year represented by the observation

Ingestion time
    → When this pipeline retrieved the observation

Provider modification time
    → When Data USA last changed the observation
```

Fetching only the latest business year could therefore miss a later correction to an older year.

For a production implementation, I would consider:

```text
Frequent recent-period ingestion
              +
Periodic historical reconciliation
              ↓
Detect historical corrections
              ↓
CDC / upsert into Silver
```

If the provider exposed a reliable modification timestamp, change cursor, or CDC mechanism, I would prefer that over repeated historical reconciliation.

## Schema Drift

Bronze accepts additive source changes while Silver maintains the trusted analytical contract.

In production, I would monitor:

- Missing required fields.
- Incompatible type changes.
- Unexpected new fields.
- Null-rate changes.
- Schema-evolution events.

The objective is to allow Bronze to remain resilient to source evolution without silently breaking downstream business contracts.

## Data Volume and Performance

The assessment dataset is small, so I intentionally avoided premature optimization.

At larger scale, I would evaluate:

- File sizing and compaction.
- Clustering or partitioning where appropriate.
- Shuffle behavior.
- Join strategies.
- Incremental processing.
- Streaming-state growth.
- Delta optimization.
- Retention policies.

Optimization decisions should be driven by measured workload behavior rather than added automatically.

## Cost

For a production workload, I would evaluate:

- Pipeline execution frequency.
- Incremental versus full refresh behavior.
- Compute utilization.
- Raw snapshot retention.
- Storage growth.
- Gold freshness requirements.
- Business SLAs.

The objective would be to balance freshness, reliability, and cost rather than maximize refresh frequency by default.

## Security and Access Control

For production, I would continue the least-privilege pattern demonstrated in the bonus implementation.

Source-oriented schemas would remain primarily engineering-facing, while consumers receive governed access to analytical products.

Pipeline execution should use service principals rather than individual identities, and access should preferably be granted through groups.

## Monitoring and Observability

For a production implementation, I would monitor:

- Source arrival and freshness.
- Pipeline failures.
- Data-quality expectation results.
- Schema changes.
- Row-count anomalies.
- Duplicate/correction rates.
- Processing latency.
- Gold freshness.
- Workflow failures.
- Compute cost.

Alerts should surface meaningful failures and data-quality changes proactively rather than rely on downstream users to discover them.

## CI/CD and Deployment

For the assessment, the pipelines and supporting resources were configured directly in Databricks.

For production, I would version-control both code and infrastructure configuration and deploy through CI/CD.

**Databricks Asset Bundles (DABs)** would be a natural next step for consistently deploying pipelines, workflows, permissions, and related resources across environments such as:

```text
Development
     ↓
Test / QA
     ↓
Production
```

---

# Retrospective

The most time-consuming part of the Quest was not the transformation logic itself, but getting the development environment into a reliable state.

I was working from a new laptop, and Visual Studio Code was intermittently freezing, which added some friction while setting up and organizing the local development workflow.

I also had not used Databricks Free Edition in roughly two years, and the environment has changed since I last worked with it.

In particular, I initially expected to retrieve the BLS and Data USA sources directly from Databricks.

When those requests failed, I spent time determining whether the issue was related to the BLS `User-Agent` requirement, my request implementation, DNS, or the Databricks environment itself.

Testing the endpoints independently showed that the failures occurred during DNS resolution and affected multiple external domains, while allowed domains such as `pypi.org` remained reachable.

This confirmed that the issue was the Free Edition outbound-network restriction rather than the ingestion code or the expected BLS `403`.

After confirming the limitation with Rearc, I adjusted the architecture by separating source extraction from Databricks processing: the extraction logic remains programmatic and reproducible, while the resulting files are landed in Unity Catalog Volumes and those Volumes become the durable sources for the Lakeflow pipelines.

Although the environment troubleshooting took additional time, it reinforced an important engineering principle:

> **When a pipeline fails at an integration boundary, isolate the failure layer before changing application logic.**

In this case, distinguishing HTTP authorization, DNS/network connectivity, source extraction, and Spark processing prevented an infrastructure limitation from unnecessarily complicating the data pipeline design.

The Quest also reinforced the value of keeping the architecture proportional to the problem.

There were several places where I could have introduced additional complexity — latest-version CDC in Silver, more granular Gold pipelines, or more sophisticated orchestration — but I deliberately implemented the simpler design where it fully satisfied the current requirements and documented how I would evolve it if those constraints changed.

---

# Submission and Next Steps

I am submitting the completed Quest and the bonus enhancements implemented so far without making additional optional work a dependency for review.

The repository currently includes:

```text
Core
✓ Programmatic source ingestion
✓ Unity Catalog raw landing
✓ Auto Loader
✓ Bronze / Silver / Gold
✓ Lakeflow Spark Declarative Pipelines
✓ Data-quality expectations
✓ PySpark primary implementation
✓ Spark SQL alternatives
✓ Gold validation tests
✓ Pipeline orchestration

Bonus
✓ AI/BI Dashboard
✓ Genie Agent
✓ Read-only Unity Catalog access
```

I plan to continue improving the project where the additional work provides meaningful engineering value.

My next areas of exploration are:

- Packaging and deploying the pipelines, workflow, permissions, and supporting resources using **Databricks Asset Bundles (DABs)** rather than relying on manual workspace configuration.
- Evaluating additional production-readiness improvements where they provide meaningful value, particularly around observability, deployment automation, stronger data contracts, and operational resiliency.

I am intentionally treating these as incremental enhancements on top of the completed solution rather than making optional work a dependency for completing the assessment.

---

## Design Principle

The architecture ultimately follows a simple separation of responsibilities:

> **Bronze preserves what arrived. Silver establishes what can be trusted. Gold provides what the business needs.**

The result is an implementation that remains relatively simple for the scope of the Quest while still providing clear paths toward stronger CDC, orchestration, governance, observability, deployment automation, and self-service analytics in a production environment.
