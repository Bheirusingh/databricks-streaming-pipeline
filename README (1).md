# Databricks Streaming Pipeline Export

This folder contains a complete export of the **Databricks-Streaming-Pipeline** job and all its associated notebooks.

## Job Configuration

* **Job ID**: 109116121525043
* **Job Name**: Databricks-Streaming-Pipeline
* **Status**: Paused
* **Schedule**: Every 5 minutes (periodic trigger)
* **Performance Target**: Performance Optimized
* **Max Concurrent Runs**: 1

## Pipeline Architecture

This is a **Medallion Architecture** streaming pipeline with three layers:

### 1. Bronze Layer - Raw Data Ingestion
**File**: `01_Bronze_Ingestion.py`
**Task Key**: `01_bronze_ingestion`

Ingests raw e-commerce event data:
* Creates synthetic streaming events (page_view, add_to_cart, purchase)
* Writes to Delta table: `bronze_events`
* Performs basic data exploration and quality checks

### 2. Silver Layer - Data Transformation
**File**: `02_Silver_Transformation.py`
**Task Key**: `02_silver_transform`
**Dependencies**: `01_bronze_ingestion`

Cleans and enriches the data:
* Reads from Bronze layer using Auto Loader
* Applies data quality rules (null checks, type conversions)
* Normalizes fields (trim, uppercase event types)
* Writes to Delta table: `silver_events`

### 3. Gold Layer - Business Aggregations
**File**: `03_Gold_Aggregation.py`
**Task Key**: `03_gold_aggregation`
**Dependencies**: `02_silver_transform`

Creates business-ready aggregations:
* Reads from Silver layer as a stream
* Applies watermarking for late-arriving data
* Aggregates purchase events by 5-minute windows
* Calculates total revenue and purchase counts
* Writes to Delta table: `gold_revenue_by_window`

## Files in This Export

* `job-config.json` - Complete job configuration (can be used to recreate the job)
* `01_Bronze_Ingestion.py` - Bronze layer notebook code
* `02_Silver_Transformation.py` - Silver layer notebook code
* `03_Gold_Aggregation.py` - Gold layer notebook code
* `README.md` - This documentation file

## Data Tables Created

1. `bronze_events` - Raw events
2. `bronze_streaming_events` - Streaming raw events
3. `silver_events` - Cleaned and transformed events
4. `gold_revenue_by_window` - Aggregated revenue by time windows

## Checkpoint Locations

* Bronze: `/Volumes/workspace/default/demo_series1/checkpoints/bronze_events/`
* Silver: `/Volumes/workspace/default/demo_series1/checkpoints/silver_events/`
* Gold: `/Volumes/workspace/default/demo_series1/checkpoints/gold_revenue/`

## How to Use This Export

1. **To recreate the job**: Import `job-config.json` using the Databricks CLI or API
2. **To review the code**: Open the `.py` files in any text editor or IDE
3. **To import notebooks**: Upload the `.py` files to your Databricks workspace

## Export Details

* **Exported by**: aryarawlot14@gmail.com
* **Export Date**: 2026-08-03
* **Job Created**: 2026-08-02 23:09:39 UTC