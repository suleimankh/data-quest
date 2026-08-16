# Rearc Data Quest --- Databricks Edition

My solution for the Rearc Data Quest using **Databricks Lakeflow Spark
Declarative Pipelines, Auto Loader, Delta Lake, and Unity Catalog**.

## Repository Structure

``` text
.
├── source_ingestion/
│   └── Code for pulling BLS and Data USA data
│       and landing it into Unity Catalog Volumes
│
├── datausa_pipeline/
│   ├── bronze.py
│   └── silver.py
│
├── bls_pipeline/
│   ├── bronze.py
│   └── silver.py
│
├── analytics_pipeline/
│   ├── gold.py
│   └── SQL alternatives
│
├── screenshots/
│   └── Databricks pipeline, tables, and results
│
├── PROCESS.md
│   └── Architecture decisions, trade-offs,
│       resiliency, and retrospective
│
└── README.md
```

## Pipelines

### `datausa_pipeline/`

Data USA population processing:

``` text
Raw Volume → Bronze → Silver
```

### `bls_pipeline/`

BLS productivity data and metadata processing:

``` text
Raw Volume → Bronze → Silver
```

### `analytics_pipeline/`

Gold layer containing the answers to the three Quest questions:

``` text
Data USA Silver ──┐
                  ├──→ Gold
BLS Silver ───────┘
```

## More Details

See **[PROCESS.md](PROCESS.md)** for the architecture, design decisions,
trade-offs, rerun strategy, production considerations, and
retrospective.

Databricks pipeline and result screenshots are available in
**`screenshots/`**.
