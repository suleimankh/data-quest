Rearc Data Quest — Process

Architecture

I modeled the solution as a Lakeflow Spark Declarative Pipeline using a Bronze → Silver → Gold architecture.

External Sources
      ↓
Source Extraction
      ↓
Unity Catalog Volume
      ↓
Auto Loader
      ↓
Bronze Streaming Tables
      ↓
Silver Streaming Tables
      ↓
Gold Materialized Views

The main design goal was to separate source acquisition, raw ingestion, data quality, and business logic, while keeping the implementation proportional to the size and requirements of the assessment.

The pipeline is declarative rather than manually orchestrated. Bronze, Silver, and Gold datasets and their dependencies are declared using the Spark Pipelines API, while Lakeflow manages the dependency graph, execution order, streaming state, and materialization.

⸻

Source Ingestion

The Quest uses two external sources:

1. BLS productivity time-series data.
2. Data USA population API.

BLS

The BLS source directory is discovered programmatically rather than by hardcoding individual filenames.

BLS also requires automated clients to provide information that can be used to identify/contact the owner of the request. I therefore include my contact information in the User-Agent header:

headers = {
    "User-Agent": "suleimankhader@googlemail.com"
}

This follows the BLS data-access policy and addresses the 403 Forbidden behavior described in the Quest.

Databricks Free Edition Network Limitation

While implementing the source extraction in Databricks Free Edition, I encountered an environment-specific restriction.

Outbound DNS access to both the BLS and Data USA domains was unavailable. I verified the behavior against multiple external domains and confirmed that the requests were failing during DNS resolution, before an HTTP request could reach BLS.

This was therefore separate from the expected BLS 403 scenario. The correct User-Agent was implemented, but it could not affect a request that never reached the provider.

After discussing the limitation with Rearc, I used the agreed approach:

* Keep the programmatic extraction code in the repository.
* Execute the source retrieval outside the restricted Databricks environment.
* Land the resulting raw files into a Unity Catalog Volume.
* Treat the Volume as the durable source boundary for the Spark Declarative Pipeline.

This also decouples downstream pipeline execution from external network availability.

⸻

Raw Landing Strategy

Raw source extracts are written to:

/Volumes/rearc/data-quest/step1_folder/

Rather than overwriting an existing file when the source is retrieved again, the extraction process adds a timestamp to the filename.

For example:

population_20260814T120000.json
population_20260815T120000.json
population_20260816T120000.json

I chose this approach because it creates an append-oriented raw history and works naturally with Auto Loader.

Each extraction has a unique identity, previous source versions remain available, and Auto Loader can distinguish a new extraction from a file it has already processed.

It also improves:

* Auditability.
* Replayability.
* Source lineage.
* Troubleshooting.
* Incremental processing.

⸻

Bronze Layer

Bronze is responsible for preserving what arrived.

I use Auto Loader to incrementally ingest files from the Unity Catalog Volume into Bronze streaming tables.

Auto Loader tracks which physical files have already been processed, which makes normal pipeline reruns idempotent at the file level.

Preserving the Raw API Structure

For Data USA, I intentionally preserve the original API response rather than immediately flattening the population observations.

A Data USA response is structured approximately as:

{
  "annotations": {},
  "page": {},
  "columns": [],
  "data": []
}

The actual population observations are contained inside data.

Therefore, each timestamped source file effectively represents one Bronze snapshot:

population_20260814.json → 1 Bronze snapshot
population_20260815.json → 1 Bronze snapshot
population_20260816.json → 1 Bronze snapshot

The business records are extracted later in Silver:

New source file
      ↓
Auto Loader
      ↓
1 Bronze snapshot
      ↓
explode(data)
      ↓
2013 population
2014 population
2015 population
...
2023 population

This distinction is intentional:

Bronze represents source snapshots.

Silver represents individual business records.

I also preserve Databricks _metadata in Bronze so that each snapshot retains information about the physical source file, including filename and modification information.

⸻

Schema Evolution

Bronze uses Auto Loader with:

.option("cloudFiles.inferColumnTypes", "true")
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")

Why inferColumnTypes?

The JSON contains meaningful primitive types such as years and population values.

Allowing Auto Loader to infer these types preserves more of the source semantics than initially representing every field as a string.

Silver still owns the final analytical contract and explicitly normalizes the fields required downstream.

Why addNewColumns?

External APIs evolve.

If Data USA adds another field to its response, I do not want the new information to be silently discarded simply because the original Bronze schema did not contain it.

addNewColumns allows Auto Loader to evolve the Bronze schema to preserve additive source changes.

The design principle is:

External source
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

This does not mean every schema change is automatically considered safe.

For example, a renamed required field or an incompatible change to Population should be detected and handled rather than automatically accepted.

Bronze is intentionally flexible; Silver is intentionally stricter.

⸻

Silver Layer

Silver is responsible for producing trusted, normalized business records.

For the population dataset, Silver:

* Explodes the nested data array.
* Selects the required business fields.
* Normalizes source column names.
* Applies analytical data types.
* Enforces data-quality expectations.
* Deduplicates using the business key.

The resulting population schema is intentionally simple:

nation_id     STRING
nation        STRING
year          INT
population    BIGINT

Data Quality Expectations

The Quest specifically asks for expectations enforcing basic data quality.

I place these expectations in Silver rather than Bronze because the layers have different responsibilities.

Bronze asks:

What did the source provide?

Silver asks:

Is this record valid enough to trust analytically?

For example:

year IS NOT NULL
population IS NOT NULL

This prevents invalid records from propagating into Gold.

⸻

File-Level Idempotency vs. Business-Level Deduplication

There are two different forms of duplicate protection in this architecture.

Auto Loader answers:

Have I already processed this physical file?

Silver answers:

Have I already processed this business key?

For example:

population_20260814.json
        ↓
2013
2014
...
2023
population_20260815.json
        ↓
2013
2014
...
2023

These are two valid physical source snapshots, so Auto Loader should ingest both.

However, after explode(data), Silver sees repeated business keys:

nation_id + year

For the assessment, Silver therefore uses:

.dropDuplicates(["nation_id", "year"])

This prevents repeated observations from propagating into Gold.

⸻

Deduplication vs. Source Corrections

Streaming deduplication is deliberately simple, but it is important to recognize its limitation.

Suppose two snapshots contain:

Snapshot 1:
2023 → 334,914,895
Snapshot 2:
2023 → 335,000,000

The second value may represent a legitimate correction.

Both records have the same business key:

nation_id + year

dropDuplicates does not represent the business rule:

The latest authoritative version should replace the previous value.

Deduplication and Change Data Capture are therefore different problems.

Alternative Considered: Materialized Silver + Window Ranking

I considered making Silver a materialized view and reading the complete Bronze history using:

spark.read.table("bronze_population")

I could then use a window:

PARTITION BY nation_id, year
ORDER BY source_timestamp DESC

and row_number() to deterministically retain the newest version.

Conceptually:

All Bronze snapshots
       ↓
Partition by nation + year
       ↓
Order by source timestamp DESC
       ↓
row_number = 1
       ↓
Latest version

This would correctly handle historical corrections.

I decided not to introduce that additional complexity for this assessment because the source extraction and landing process is under our control.

Data is pulled deliberately, each extraction receives a unique timestamped filename, and files are landed into the Volume in a controlled sequence.

Silver therefore processes new Bronze snapshots incrementally in that same controlled environment.

For the supplied dataset, streaming business-key deduplication provides the required behavior while preserving a simple and efficient:

Bronze Streaming Table
        ↓
Silver Streaming Table
        ↓
Gold Materialized View

architecture.

This is a deliberate trade-off rather than an assumption that dropDuplicates provides CDC semantics.

What I Would Do for an Uncontrolled Source

I would make a different decision if the source were asynchronous or outside our control.

For example, with Kafka:

Event A: version 1
Event B: version 3
Event C: version 2  ← arrives late

processing order does not necessarily represent business order.

In that environment I would not depend on arrival order.

Instead, I would preserve an explicit event/source version or timestamp and use CDC/upsert semantics:

Bronze append stream
        ↓
Business key
nation_id + year
        ↓
Sequence by
source/event timestamp
        ↓
CDC / upsert
        ↓
Silver current state

That allows the authoritative version to be determined from source semantics rather than processing order.

⸻

Gold Layer

Gold is responsible for producing business-ready analytical outputs.

I use materialized views for Gold because the requested outputs involve aggregations, ranking, and joins over trusted Silver data rather than simple append-only event processing.

The Gold layer directly answers the three questions in the Quest:

1. Mean and standard deviation of annual US population from 2013–2018.
2. Best year for every BLS series_id, including a human-readable description.
3. Annual values for PRS30006032 / Q01 joined with population where available.

This keeps provider-specific structures and transformation details out of the final analytical interface.

⸻

PySpark vs. Spark SQL

The Quest asks for each analytical question to be implemented once in PySpark and once in Spark SQL.

I selected PySpark as the primary implementation feeding the Gold tables.

PySpark fits naturally with the rest of the pipeline implementation and provides a clean programmatic interface for:

* DataFrame transformations.
* Reusable logic.
* Window operations.
* Joins and aggregations.
* Integration with the Spark Pipelines API.

Each analytical question is also implemented independently in Spark SQL as a documented alternative.

The objective is to demonstrate equivalent fluency in both APIs, not to maintain two competing production implementations of the same business logic.

In a production system, I would normally select one authoritative implementation per transformation to avoid maintaining duplicated business logic.

⸻

Rerun and Resiliency Strategy

The architecture handles reruns at multiple levels.

1. Durable Landing

The Unity Catalog Volume separates external source acquisition from pipeline processing.

Once the source data has been landed, the pipeline can rerun without depending on BLS or Data USA being available.

2. Immutable/Timestamped Source Extracts

New extractions receive unique filenames rather than overwriting previous raw data:

population_20260814T120000.json
population_20260815T120000.json

This preserves raw history and provides clear source versions.

3. Auto Loader File Tracking

Auto Loader maintains state about files that have already been processed.

A normal pipeline restart therefore does not repeatedly ingest the same physical file.

4. Silver Business-Key Deduplication

Different source snapshots may contain the same business records.

Silver prevents repeated business keys from propagating to Gold.

For production data where existing values can legitimately be corrected, this would evolve from simple deduplication to explicit CDC/upsert semantics.

⸻

Trade-offs and Production Considerations

The Quest dataset is small, so I intentionally avoided adding infrastructure or complexity that would provide little practical value at this scale.

For a real client implementation, I would revisit the following areas.

Schema Drift

Bronze allows additive schema evolution using:

addNewColumns

This preserves newly introduced source fields, but it does not make all schema changes safe.

For production, I would maintain explicit Silver data contracts and monitor for:

* Missing required fields.
* Incompatible type changes.
* Unexpected new fields.
* Null-rate changes.
* Schema evolution events.

This allows Bronze to remain resilient to source evolution while keeping downstream contracts stable.

⸻

Data USA Incremental Extraction Limitation

The Data USA API provides time-oriented filtering capabilities, but it does not provide a documented change cursor such as:

updated_since=<timestamp>

that would allow the ingestion process to ask:

Give me every record that changed since my previous extraction.

This distinction matters because business time and source-change time are different concepts.

For example:

Business time:
Year = 2023
Ingestion time:
2026-08-15
Source modification time:
Not exposed as a documented incremental cursor

Retrieving only the latest business year could therefore miss a correction to an older year.

For the assessment, timestamped snapshots provide a simple and auditable solution.

For production, I would consider a hybrid strategy:

Frequent ingestion
Latest/recent source periods
        +
Periodic historical reconciliation
        ↓
Detect historical corrections
        ↓
CDC/upsert into Silver

If Data USA exposed a reliable modification timestamp or CDC mechanism in the future, I would prefer that mechanism over repeated historical reconciliation.

⸻

Data Volume

The current dataset is small enough that straightforward Spark transformations are appropriate.

At larger scale, I would evaluate:

* File sizing and compaction.
* Partitioning or clustering.
* Shuffle behavior.
* Join strategies.
* Incremental processing.
* Delta optimization.
* Retention policies.
* State-store growth.

Optimization decisions would be based on measured workload behavior rather than added prematurely.

⸻

Streaming State

Silver currently performs streaming business-key deduplication.

For this dataset, the amount of state is negligible.

For a long-running, high-volume pipeline, unbounded deduplication state could become expensive.

Depending on the source semantics, I would evaluate:

* Event/extraction timestamps.
* Watermarks.
* Bounded deduplication.
* CDC.
* Source sequence numbers.

The appropriate solution depends on whether the source is append-only, correction-based, or event-driven.

⸻

Cost

The assessment dataset is too small for aggressive compute optimization to provide meaningful value.

For production, I would evaluate:

* Pipeline execution frequency.
* Incremental versus full processing.
* Serverless/compute utilization.
* Storage growth from historical raw snapshots.
* Raw-data retention periods.
* Gold freshness requirements.
* Business SLAs.

The goal would be to balance data freshness against actual compute and storage cost.

⸻

Access Control

For production, I would use Unity Catalog to implement least-privilege access.

A typical model would be:

Raw / Bronze
Engineering and ingestion identities
Silver
Data engineering / controlled consumers
Gold
Read-only analysts and downstream applications

Production pipelines would run under service principals rather than individual user identities.

Where necessary, additional row-, column-, or object-level controls could be applied to sensitive datasets.

⸻

Monitoring and Observability

For a production implementation, I would monitor:

* Source arrival.
* Data freshness.
* Pipeline failures.
* Expectation results.
* Schema changes.
* Row-count anomalies.
* Duplicate/correction rates.
* Processing latency.
* Gold freshness.
* Compute cost.

Alerts would be configured for failures and meaningful data-quality changes so issues are detected proactively rather than by downstream consumers.

⸻

CI/CD and Deployment

For the assessment, the pipeline can be configured directly in Databricks.

For a production client implementation, I would version-control both pipeline code and configuration and deploy through CI/CD.

Databricks Asset Bundles would be a natural option for consistently deploying the solution across:

Development
     ↓
Test / QA
     ↓
Production

This would also allow pipeline configuration, permissions, and environment-specific parameters to be managed as code.

⸻

Design Principle

The central architectural principle throughout the solution is:

Bronze preserves what arrived. Silver establishes what can be trusted. Gold provides what the business needs.

The resulting architecture remains intentionally simple:

External Sources
       ↓
Timestamped Raw Files
       ↓
Unity Catalog Volume
       ↓
Auto Loader
       ↓
Bronze Streaming Tables
Raw source + lineage
       ↓
Silver Streaming Tables
Types + quality + business keys
       ↓
Gold Materialized Views
Business answers

For the assessment, this provides a simple, incremental, and explainable solution without introducing production-scale complexity where it is not required. At the same time, the architecture provides clear extension points for CDC, stronger schema contracts, access controls, monitoring, and CI/CD if the same pattern were deployed for a real client.
