# Rearc Data Quest: Databricks Edition

## Q. What is this quest?

It's a hands-on way to assess your data engineering skills on the same platform we
use for our clients every day: **Databricks**. It's also a representative sample
of the work we do at Rearc: pulling real, imperfect data from the outside world
and turning it into something trustworthy.

## Q. What skills should I have?

- Python proficiency
- Sourcing data programmatically from real-world APIs/websites, including
  handling access restrictions, rate limits, or bot-blocking gracefully
- SQL and Apache Spark, comfortable in **both** Spark SQL and the PySpark
  DataFrame API
- Databricks fundamentals: Delta Lake, Unity Catalog (or Hive Metastore), Volumes,
  Spark Declarative Pipelines
- Judgment around data quality: typing, nulls, deduplication, idempotency
- The habit of actually reading a data provider's own documentation instead of
  assuming a raw file is self-explanatory

You'll need access to a Databricks environment. [Free Edition](https://www.databricks.com/learn/free-edition)
works fine if you don't already have one.

## Q. What do I have to do?

**Step 1: Source it.** Pull the full contents of [this BLS productivity
time-series folder](https://download.bls.gov/pub/time.series/pr/), not just the
data file, and land everything as raw files in a Databricks **Volume**.
You'll likely hit a `403 Forbidden` at some point; see the Q&A below for how to
resolve this the way BLS actually wants you to.

Don't hardcode filenames or assume a one-time pull. If we re-run your ingestion
after the source has added, changed, or removed data, it should still work
correctly and shouldn't reprocess anything it's already ingested.

You'll also need [this population API](https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population)
([docs](https://datausa.io/about/api/)), saved as JSON in the same Volume.

**Step 2: Model it as a Spark Declarative Pipeline.** Build your Bronze, Silver,
and Gold tables as a Spark Declarative Pipeline, with expectations enforcing
basic data quality on the way in (expected schema, non-null keys). For each of
the three analytical questions below, implement the logic **once in Spark SQL
and once in PySpark**: pick whichever you prefer as the "primary" version
feeding the Gold table, and include the other as a documented alternative. We
want to see you're equally fluent in both, not that you can do it one way.

## Q. How do I know I've solved it?

Your Gold tables should correctly answer:

1. What are the mean and standard deviation of the annual US population across
   2013-2018 inclusive?
2. For every `series_id` in the BLS data, what's the **best year**, meaning the
   year with the largest sum of `value` across all its quarters? (e.g. if
   `PRS30006011` has values 1, 2 in 1995 and 3, 4 in 1996, its best year is 1996
   with a summed value of 7.) Include enough of a human-readable label for each
   series that someone unfamiliar with BLS series codes could understand at a
   glance what's actually being measured.
3. For `series_id = PRS30006032` and `period = Q01`, what was the `value` each
   year, joined with that year's `population` where available?

## Q. What do I have to submit?

1. **Your code**, as a link to a public (or shareable) git repository, including
   whatever you used to source and land the data.
2. **Your thought process (`PROCESS.md`)**, covering:
   - **Architecture**: why you modeled it this way (Bronze/Silver/Gold choices,
     why SQL vs. PySpark where you chose one as primary, how you handled
     re-running ingestion safely).
   - **Trade-offs**: what you'd handle differently for a real client, such as
     schema drift, data volume, cost, access control, or monitoring.
   - **Retrospective**: what was hardest to get right.
3. A screenshot of your tables/pipeline in Databricks, since we may not be able
   to access your live workspace when we review.

## Bonus

If you're having fun and don't want to stop, here are some directions to take
this further. Take as many as you like, we're genuinely curious how far you'll
run with it:

- Build a **Genie space** or a dashboard on top of your Gold layer that a
  non-technical stakeholder could actually use to answer questions themselves.
- Add Unity Catalog access controls appropriate for a read-only analyst
  consuming the Gold layer.
- Deploy it with **Databricks Asset Bundles** instead of clicking through the UI.
- Anything else you think would make this a better, more production-ready
  product: surprise us.

## Misc

**Q: How long should this take?**
Steps 1 and 2 are the core ask, and are very achievable in an afternoon. Bonus is
open-ended by design, so go as far as you want. If you're enjoying it, we'd rather
see an ambitious, polished result than a quick, minimal one. Treat this as a
chance to show us what you're capable of building, not just what's required.

**Q: How do I get around the 403 error fetching BLS data?**
*Hint 1:* BLS's data access policy is [here](https://www.bls.gov/bls/pss.htm).
*Hint 2:* The policy notes BLS "reserves the right to block robots that do not
contain information that can be used to contact the owner." How could you add
contact info to your programmatic requests?
*Hint 3:* A `User-Agent` header with your contact info resolves this and keeps
you compliant with BLS's policy.

**Q: Can I use AI to assist me?**
We (obviously) love AI, and you absolutely may use it as a reference tool.
However:
1. You must understand every line of code you submit; we'll ask you to explain
   your logic in the interview.
2. Please be open about usage. Tell us something like: "I used Claude to draft
   the pipeline scaffold, but had to fix the expectations logic myself."

**Q: Do I have to use specific tools?**
Beyond developing in Databricks and building your pipeline as a Spark Declarative
Pipeline, use whichever packages or approach you like, we're as interested in
what you choose and why as in the result.

**Q: Can I share this quest with others?**
No.
