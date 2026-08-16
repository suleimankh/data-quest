# Rearc Data Quest --- Databricks Edition

This repository contains my solution for the **Rearc Data Quest:
Databricks Edition**, built using Databricks Lakeflow Spark Declarative
Pipelines, Auto Loader, Delta Lake, and Unity Catalog.

## Quest Status

### Core Requirements

``` text
✓ Question 1 — Complete
✓ Question 2 — Complete
✓ Question 3 — Complete
```

The implementation includes:

``` text
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
```

## Bonus Status

``` text
✓ 1 — Self-service analytics on Gold
      ✓ AI/BI Dashboard
      ✓ Genie Agent

✓ 2 — Unity Catalog access controls
      ✓ Read-only analyst access to Gold

⏳ 3 — Databricks Asset Bundles
      Currently in progress

4 — Additional production-readiness improvements
    ✓ Databricks Workflow orchestration
    ✓ Automated Gold validation tests
    → Continuing to explore additional improvements where they add meaningful value
```

## Repository Structure

``` text
.
├── source_ingestion/
│   └── Source extraction and raw landing logic
│
├── datausa_pipeline/
│   └── Data USA Bronze and Silver pipeline
│
├── bls_pipeline/
│   └── BLS Bronze and Silver pipeline
│
├── analytics_pipeline/
│   └── Gold analytical pipeline
│
├── alternative_spark_sql/
│   └── Spark SQL alternatives to the PySpark implementation
│
├── gold_test/
│   └── Automated Gold validation
│
├── screenshots/
│   ├── Pipeline and catalog screenshots
│   └── Bonus dashboard, Genie, and access-control screenshots
│
├── PROCESS.md
└── README.md
```

## Pipeline Flow

``` text
             ┌── Data USA Pipeline ──┐
Source ──────┤                       ├──→ Analytics Pipeline → Gold
             └── BLS Pipeline ───────┘
                     ALL SUCCEEDED
```

The primary implementation uses **PySpark**, with equivalent **Spark SQL
alternatives** included separately.

## Validation

All three Gold questions are independently validated in:

``` text
gold_test/test_gold.ipynb
```

Current result:

``` text
✓ Question 1 passed
✓ Question 2 passed
✓ Question 3 passed
✓ Population join validation passed

ALL GOLD TESTS PASSED
```

## More Details

See **[PROCESS.md](PROCESS.md)** for the detailed engineering thought
process, including:

-   Architecture decisions
-   Bronze / Silver / Gold design
-   Auto Loader and schema evolution
-   Rerun and deduplication strategy
-   CDC and source-correction trade-offs
-   PySpark vs. Spark SQL
-   Pipeline orchestration
-   Data quality and validation
-   Production considerations
-   Retrospective
