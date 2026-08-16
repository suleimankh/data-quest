# Rearc Data Quest --- PROCESS.md

## Architecture

I modeled the solution as three Lakeflow Spark Declarative Pipelines
with source-oriented Bronze/Silver layers and a separate business-facing
analytics layer.

``` text
BLS                                  Data USA
 ↓                                      ↓
Programmatic extraction            Programmatic extraction
 ↓                                      ↓
/Volumes/rearc/bls/raw/             /Volumes/rearc/datausa/raw/
 ↓                                      ↓
BLS Lakeflow Pipeline               Data USA Lakeflow Pipeline
 ↓                                      ↓
Bronze streaming tables            bronze_population
 ↓                                      ↓
Silver streaming tables            silver_population
          \                          /
           \                        /
            └── Analytics Pipeline ┘
                       ↓
        rearc_analytics.population_analytics
                       ↓
             Gold materialized views
```

The final catalog organization is:

``` text
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

I separated Data USA and BLS because they are independent source systems
with different formats, metadata, ingestion behavior, and data-quality
contracts. Gold is separated from source-specific processing because it
represents business-facing analytics and includes cross-source joins.

The transformations themselves are declarative. I define the desired
datasets and dependencies using `pyspark.pipelines`, while Lakeflow
manages execution, dependency ordering, streaming state, and
materialization.

## Source Ingestion

### BLS

The BLS productivity directory is discovered programmatically rather
than by hardcoding individual filenames. The extraction therefore adapts
when BLS adds or removes files.

BLS requires automated clients to provide information that can be used
to contact the owner. Requests therefore include contact information in
the `User-Agent` header:

``` python
headers = {"User-Agent": "publicRepo@example.com"}
```

The full BLS directory is landed into `/Volumes/rearc/bls/raw/`. The
complete source directory is retained as required by the Quest, even
though only the files needed by the analytical model are promoted into
Bronze tables.

### Data USA

The population API response is retrieved programmatically and landed
into `/Volumes/rearc/datausa/raw/`.

I preserve the complete API response rather than extracting only the
fields required for Question 1.

### Databricks Free Edition Network Limitation

While implementing source extraction, I found that my Databricks Free
Edition environment restricted outbound DNS/network access to the
required BLS and Data USA domains.

I isolated this by testing multiple external hosts. Allowed destinations
such as `pypi.org` resolved successfully, while BLS, Data USA, and other
external domains failed during DNS resolution.

This was different from the expected BLS `403 Forbidden` case: the HTTP
request never reached BLS, so the correct `User-Agent` could not solve
the DNS restriction.

After confirming the issue with Rearc, I used the agreed workaround:

1.  Keep the complete programmatic extraction code in the repository.
2.  Execute source retrieval outside the restricted Databricks
    environment.
3.  Land the resulting files into the Unity Catalog Volumes.
4.  Treat those Volumes as the durable source boundary for the Lakeflow
    pipelines.

## Raw Landing and Rerun Strategy

I use timestamped filenames for repeated source extractions rather than
overwriting existing files, for example:

``` text
population_20260814T120000.json
population_20260815T120000.json
```

This creates an append-oriented raw landing pattern that works naturally
with Auto Loader and improves auditability, replayability, lineage,
troubleshooting, and incremental processing.

The extraction timestamp represents when my ingestion process observed
the source. It is distinct from the business date contained in the data
and from any provider-side modification timestamp.

## Bronze Layer

Bronze is responsible for preserving **what arrived**.

Both source pipelines use Auto Loader to incrementally ingest files from
their Unity Catalog Volumes into streaming tables. Auto Loader tracks
physical files already processed, providing file-level idempotency
during normal reruns.

### Data USA Bronze

A Data USA response is structured approximately as:

``` json
{
  "annotations": {},
  "page": {},
  "columns": [],
  "data": []
}
```

I intentionally preserve this structure in Bronze rather than
immediately flattening `data`.

``` text
population_20260814.json → 1 Bronze snapshot
population_20260815.json → 1 Bronze snapshot
```

Silver later transforms each snapshot:

``` text
New Bronze row / source file
             ↓
        explode(data)
             ↓
      2013 population
      2014 population
      2015 population
             ...
      2023 population
```

Bronze therefore represents source snapshots, while Silver represents
individual business records.

I preserve Databricks `_metadata` in Bronze for physical-file lineage.
Because Data USA intentionally uses field names such as `Nation ID`,
Delta column mapping is enabled for the raw table rather than changing
provider field names in Bronze. Those fields are normalized in Silver.

### BLS Bronze

BLS files are tab-separated and some source headers contain surrounding
whitespace. That whitespace is formatting noise and creates invalid
Delta column names.

I normalize only the BLS header whitespace before writing Bronze:

``` python
def clean_column_names(df):
    return df.toDF(*[c.strip() for c in df.columns])
```

The underlying raw files remain unchanged.

The BLS Bronze layer models the observation, series, measure, and sector
datasets needed downstream. Other BLS files remain in the raw Volume.

## Schema Evolution

Data USA Bronze uses:

``` python
.option("cloudFiles.inferColumnTypes", "true")
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
```

`inferColumnTypes` preserves meaningful primitive source types.
`addNewColumns` allows additive source evolution to be captured instead
of silently discarding newly introduced fields.

Bronze is intentionally flexible; Silver owns the stable analytical
contract. Missing required fields, renames, or incompatible type changes
should be surfaced at the trusted-data boundary.

## Silver Layer

Silver is responsible for establishing **what can be trusted**.

### Data USA Silver

Silver explodes the nested `data` array, normalizes provider field
names, casts analytical types, applies expectations, and deduplicates on
the business key.

``` text
nation_id     STRING
nation        STRING
year          INT
population    BIGINT
```

Expectations include non-null `nation_id`, `year`, and `population`.

### BLS Silver

BLS Silver normalizes the observation and reference datasets. The
observation business key is `series_id + year + period`.

Expectations enforce required fields including `series_id`, `year`,
`period`, and `value`.

Series, measure, and sector reference data are kept separately so Gold
can construct human-readable labels without carrying unnecessary
raw-source columns.

## File-Level Idempotency vs. Business-Level Deduplication

Auto Loader answers: **Have I already processed this physical file?**

Silver answers: **Have I already processed this business record?**

Two timestamped snapshots are valid different files, so Auto Loader
should process both. After `explode(data)`, however, Silver may see the
same `nation_id + year` keys again.

For the assessment, Silver uses:

``` python
.dropDuplicates(["nation_id", "year"])
```

to prevent repeated observations from propagating downstream.

## Deduplication vs. Corrections

Simple deduplication is not CDC.

If one snapshot contains `2023 → 334,914,895` and a later snapshot
contains `2023 → 335,000,000`, the latter may be a legitimate
correction. `dropDuplicates` does not express that latest-version rule.

I considered making Silver a materialized view over all Bronze history
and using a window ordered by source timestamp with `row_number()` to
retain the latest version. That would correctly model latest-version
resolution, but I considered it unnecessary complexity for this supplied
dataset.

For this assessment, source extraction and landing are controlled:
extractions are deliberate, uniquely timestamped, and landed in a
controlled sequence. Keeping Silver streaming preserves a simple and
efficient Bronze streaming → Silver streaming → Gold materialized-view
architecture.

For an asynchronous or uncontrolled source such as Kafka, I would not
rely on arrival order. I would preserve an explicit source/event
timestamp or sequence and use CDC/upsert semantics.

## Gold / Analytics Layer

Gold represents **what the business needs**.

Gold is isolated in `rearc_analytics.population_analytics` and reads
trusted Silver datasets using fully qualified Unity Catalog names:

``` python
spark.read.table("rearc.datausa.silver_population")
spark.read.table("rearc.bls.silver_bls_data")
```

Gold uses materialized views because the outputs involve aggregation,
ranking, and cross-source joins.

The Gold layer answers:

1.  Mean and standard deviation of annual US population from 2013--2018
    inclusive.
2.  Best year for every BLS `series_id`, based on the largest sum of
    quarterly values, with a human-readable label.
3.  `PRS30006032 / Q01` values by year joined with that year's US
    population where available.

## PySpark vs. Spark SQL

The Quest asks for each analytical question in both PySpark and Spark
SQL.

I selected **PySpark as the primary implementation feeding Gold**
because it fits naturally with the pipeline code and provides a clean
interface for DataFrame transformations, joins, aggregations, windows,
and reusable logic.

Each analytical question is also implemented independently in Spark SQL
as a documented alternative. The goal is to demonstrate fluency in both
APIs, not maintain two competing production implementations.

## Rerun and Resiliency

The solution protects reruns at multiple levels:

-   Unity Catalog Volumes provide a durable source boundary.
-   Timestamped source extracts avoid overwriting raw history.
-   Auto Loader tracks physical files already processed.
-   Silver applies business-key deduplication.

For a production source that publishes corrections, I would evolve
business-key deduplication into deterministic CDC/upsert behavior.

# Trade-offs and Production Considerations

## Schema Drift

Bronze tolerates additive schema evolution, while Silver should enforce
explicit contracts. In production I would monitor missing required
columns, incompatible type changes, unexpected fields, null-rate
changes, and schema-evolution events.

## Pipeline Orchestration

For the assessment, I kept the three Lakeflow pipelines independently runnable:

```text
Data USA Pipeline ─────┐
                       ├──→ Analytics Pipeline
BLS Pipeline ──────────┘
```

I did not add a parent Databricks Workflow/Job to orchestrate all three pipelines. The Data USA and BLS pipelines are independent and can run in parallel, while the Analytics pipeline should run only after both source pipelines complete successfully.

For the Quest, I kept this orchestration manual because another deployment/configuration layer was not necessary to demonstrate the requested Spark Declarative Pipeline functionality.

For production, I would add a parent Databricks Workflow with explicit dependencies:

```text
             ┌── Data USA Pipeline ──┐
Start ───────┤                       ├──→ Analytics Pipeline
             └── BLS Pipeline ───────┘
```

This would provide dependency-based execution, scheduling, retries, failure handling, centralized monitoring, alerting, and a single operational entry point. I would use dependency-based execution rather than fixed clock times so Analytics starts only after both upstream pipelines report success.

This creates two levels of orchestration:

```text
Within each pipeline:
Lakeflow manages table dependencies.

Across pipelines:
Databricks Workflow manages pipeline dependencies.
```

## Data USA Incremental Extraction Limitation

I did not find a documented Data USA change cursor equivalent to
`updated_since=<timestamp>` that returns everything added or modified
since the previous ingestion.

Business time (`Year`), ingestion time, and provider modification time
are different concepts. Fetching only the latest year could miss a
correction to an older year.

For production I would consider frequent recent-period ingestion plus
periodic historical reconciliation, followed by CDC/upsert into Silver.
If the provider exposed a reliable modification cursor, I would prefer
it.

## Data Volume and Performance

At larger scale I would evaluate file sizing/compaction, clustering or
partitioning where appropriate, shuffle behavior, join strategies,
incremental processing, Delta optimization, retention, and
streaming-state growth.

## Cost

For production I would evaluate pipeline frequency, incremental versus
full refresh behavior, compute utilization, raw snapshot retention,
storage growth, freshness requirements, and SLAs.

## Access Control

The catalog separation creates a governance boundary:

``` text
rearc.datausa / rearc.bls
Source engineering and controlled consumers

rearc_analytics.population_analytics
Business-facing analytical outputs
```

For production I would use least-privilege Unity Catalog grants and
service principals. Read-only analysts would normally receive Gold
access rather than raw-source access.

## Monitoring and Observability

I would monitor source arrival/freshness, pipeline failures, expectation
results, schema changes, row-count anomalies, duplicate/correction
rates, processing latency, Gold freshness, and compute cost.

## CI/CD

For production I would version-control pipeline configuration and deploy
through CI/CD. Databricks Asset Bundles would be a natural option for
consistent promotion across development, test, and production.

# Retrospective

The most time-consuming parts of the Quest were environmental and
platform-related rather than the analytical Spark logic.

## Local Development Environment

I was working from a new laptop, and Visual Studio Code intermittently
froze while I was setting up and organizing the local development
workflow.

## Databricks Free Edition Changes

I had not worked extensively with Databricks Free Edition for roughly
two years, and some platform behavior has changed since I last used it.

The first significant issue was outbound network access. I initially
expected the BLS `403` behavior described in the Quest, but requests
failed earlier during DNS resolution. Testing each integration boundary
separately identified the actual problem and prevented unnecessary
changes to correct request logic.

# Submission and Next Steps

I am submitting the completed core Quest without making optional
enhancements a dependency for review.

I plan to continue exploring bonus items after the core submission,
including:

-   A Genie space or dashboard over Gold for non-technical users.
-   Unity Catalog access controls for a read-only Gold consumer.
-   Databricks Asset Bundles for deployment.
-   Additional production-readiness improvements where they provide
    meaningful value.

## Design Principle

The architecture is guided by a simple separation of responsibilities:

> **Bronze preserves what arrived. Silver establishes what can be
> trusted. Gold provides what the business needs.**

This keeps the assessment implementation incremental and explainable
while leaving clear extension points for stronger CDC, schema contracts,
governance, monitoring, and deployment automation in a production
environment.
