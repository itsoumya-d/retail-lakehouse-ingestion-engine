# Retail Lakehouse Ingestion Engine

High-throughput retail POS transaction ingestion, CDC event normalization, and Medallion architecture (Bronze -> Silver -> Gold) transformation pipeline in Python.

## Features

- **Bronze Layer**: Raw event batching, schema validation, and poison message quarantine.
- **Silver Layer**: Idempotent deduplication, Change Data Capture (CDC) upsert, and ISO datetime standardization.
- **Gold Layer**: Dimensional aggregations, hourly sales velocity, inventory depletion metrics, and store KPIs.

## Running Tests

```bash
python3 -m unittest discover tests
# or
pytest
```
