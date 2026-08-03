# Databricks streaming lakehouse pipeline

An end-to-end streaming data pipeline built on Databricks, ingesting simulated e-commerce events through a Bronze → Silver → Gold lakehouse architecture, orchestrated as a scheduled Databricks Workflow.

![Architecture diagram](architecture.png)

## What it does

A Python event generator simulates e-commerce activity (page views, add-to-cart, and purchase events) written as JSON files. Databricks Autoloader picks these up incrementally and ingests them into a raw Bronze Delta table. A Silver layer cleans, validates, and deduplicates the event stream. A Gold layer aggregates purchase activity into 5-minute revenue windows using watermarked streaming aggregation. All three stages run as a single orchestrated Databricks Job with sequential task dependencies.

## Tech stack

- **Databricks** — compute and orchestration platform
- **Delta Lake** — storage format for all three layers (ACID transactions, schema enforcement)
- **Databricks Autoloader** — incremental, stateful file ingestion
- **PySpark Structured Streaming** — stream processing, windowed aggregation, watermarking
- **Databricks Workflows** — job scheduling and task-level orchestration

## Architecture

**Bronze (`bronze_streaming_events`)**
Raw ingestion layer. Autoloader watches a storage folder and incrementally loads new JSON event files into a Delta table, preserving the data exactly as received. Kept immutable and unfiltered so any downstream layer can be recomputed from scratch if transformation logic changes.

**Silver (`silver_events`)**
Cleaned, deduplicated event-level table. Reads Bronze as a stream, casts `event_time` to a proper timestamp, normalizes text fields, filters out null/invalid records, and drops duplicate events on `(user_id, event_type, product, event_time)`.

**Gold (`gold_revenue_by_window`)**
Business-facing aggregate table. Reads Silver as a stream, filters to purchase events, and computes total revenue and purchase counts in 5-minute tumbling windows, using a 10-minute watermark to bound state size and handle late-arriving events.

**Orchestration**
All three layers run as a single Databricks Job (`01_bronze_ingestion` → `02_silver_transform` → `03_gold_aggregation`) with sequential task dependencies. Each stream uses `trigger(availableNow=True)`, so every run processes all currently available data and then completes — making the pipeline schedulable (e.g. every 15 minutes) rather than requiring an always-on cluster.

## Key design decisions

- **Autoloader over manual file listing** — tracks ingested files internally, so re-runs only process new data instead of rescanning everything. Scales to high file volumes without extra bookkeeping code.
- **Bronze stays raw** — no filtering or cleaning at ingestion. This preserves an audit trail and lets Silver/Gold be recomputed from scratch if transformation logic changes, without needing to re-fetch source data.
- **Deduplication at Silver, not Bronze** — keeps the raw layer a faithful copy of what was received, while giving downstream consumers a trustworthy, deduplicated dataset to build on.
- **Watermarking at Gold** — bounds how long Spark holds state waiting for late-arriving events, preventing unbounded memory growth in a long-running streaming aggregation. The 10-minute watermark on 5-minute windows was chosen as a reasonable balance between timeliness and tolerance for late data; in a production system this would be tuned against actual event-delivery SLAs.
- **`trigger(availableNow=True)` over continuous streaming** — processes all available data per run and then stops, which is cheaper to operate (no always-on cluster) and easier to schedule and monitor as a Job. A true low-latency use case would swap this for a continuous or fixed `processingTime` trigger instead — the rest of the pipeline logic wouldn't need to change.

## Repository structure

```
databricks-streaming-pipeline/
├── README.md
├── architecture.png
├── notebooks/
│   ├── 00_event_generator.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transform.py
│   └── 03_gold_aggregation.py
├── job_config.json
└── docs/
    └── design_decisions.md
```

## Running it

1. Import the notebooks into a Databricks workspace.
2. Run `00_event_generator.py` to produce simulated event files (or point it at a real source).
3. Create a Databricks Job with three tasks — `01_bronze_ingestion` → `02_silver_transform` → `03_gold_aggregation` — each depending on the previous.
4. Run the job manually to verify, then set a schedule (e.g. every 15 minutes).
5. Query `gold_revenue_by_window` to see aggregated results.

## Possible extensions

- Swap the file-based generator for a real Kafka/Confluent event stream
- Add a Databricks SQL dashboard on top of the Gold table
- Add data quality expectations via Delta Live Tables
- Add CI (GitHub Actions) to lint/test the PySpark transformation logic
