import pytest
from retail_lakehouse.engine import RetailLakehousePipeline

@pytest.fixture
def pipeline():
    return RetailLakehousePipeline()

def test_valid_batch_ingestion_and_cdc_dedup(pipeline):
    records = [
        {"transaction_id": "TX-100", "store_id": "ST-AKL-01", "sku": "TOY-ROBOT-01", "quantity": 2, "unit_price": 29.99, "currency": "NZD", "event_type": "SALE"},
        {"transaction_id": "TX-101", "store_id": "ST-AKL-01", "sku": "TOY-BLOCK-02", "quantity": 1, "unit_price": 14.50, "currency": "NZD", "event_type": "SALE"},
        # Duplicate submission from terminal retry
        {"transaction_id": "TX-100", "store_id": "ST-AKL-01", "sku": "TOY-ROBOT-01", "quantity": 2, "unit_price": 29.99, "currency": "NZD", "event_type": "SALE"}
    ]
    accepted, quarantined = pipeline.ingest_bronze(records)
    assert accepted == 3
    assert quarantined == 0

    processed = pipeline.process_silver()
    assert processed == 3
    # In Silver layer, duplicates must be deduplicated by CDC key
    assert pipeline.get_silver_count() == 2

def test_quarantine_malformed_records(pipeline):
    bad_records = [
        {"transaction_id": "", "store_id": "ST-AKL-01", "sku": "SKU-01", "quantity": 1, "unit_price": 10.0},
        {"transaction_id": "TX-102", "store_id": "", "sku": "SKU-02", "quantity": 1, "unit_price": 10.0},
        {"transaction_id": "TX-103", "store_id": "ST-AKL-01", "sku": "", "quantity": 1, "unit_price": 10.0},
        {"transaction_id": "TX-104", "store_id": "ST-AKL-01", "sku": "SKU-04", "quantity": "invalid", "unit_price": 10.0},
        {"transaction_id": "TX-105", "store_id": "ST-AKL-01", "sku": "SKU-05", "quantity": 1, "unit_price": -5.0}
    ]
    accepted, quarantined = pipeline.ingest_bronze(bad_records)
    assert accepted == 0
    assert quarantined == 5
    assert pipeline.get_quarantine_count() == 5

def test_return_event_calculation(pipeline):
    records = [
        {"transaction_id": "TX-200", "store_id": "ST-WLG-01", "sku": "TOY-DRONE-01", "quantity": 1, "unit_price": 120.0, "currency": "NZD", "event_type": "SALE"},
        {"transaction_id": "TX-201", "store_id": "ST-WLG-01", "sku": "TOY-DRONE-01", "quantity": 1, "unit_price": 120.0, "currency": "NZD", "event_type": "RETURN"}
    ]
    pipeline.ingest_bronze(records)
    pipeline.process_silver()

    metrics = pipeline.get_gold_store_metrics()
    assert len(metrics) == 1
    # Net sales should balance to 0 (1 sale + 1 return)
    assert metrics[0]["net_sales_revenue"] == 0.0
    assert metrics[0]["total_units_sold"] == 0

def test_gold_metrics_aggregation(pipeline):
    records = [
        {"transaction_id": "TX-301", "store_id": "ST-AKL-01", "sku": "SKU-A", "quantity": 3, "unit_price": 10.0, "currency": "NZD"},
        {"transaction_id": "TX-302", "store_id": "ST-AKL-01", "sku": "SKU-B", "quantity": 1, "unit_price": 50.0, "currency": "NZD"},
        {"transaction_id": "TX-303", "store_id": "ST-CHC-01", "sku": "SKU-A", "quantity": 2, "unit_price": 10.0, "currency": "NZD"}
    ]
    pipeline.ingest_bronze(records)
    pipeline.process_silver()

    store_metrics = pipeline.get_gold_store_metrics()
    assert len(store_metrics) == 2
    # ST-AKL-01: 3*10 + 1*50 = 80.0
    akl = next(m for m in store_metrics if m["store_id"] == "ST-AKL-01")
    assert akl["net_sales_revenue"] == 80.0
    assert akl["total_units_sold"] == 4

    top_skus = pipeline.get_gold_top_skus(limit=2)
    assert len(top_skus) == 2
    # SKU-B revenue: 50.0; SKU-A revenue: (3+2)*10 = 50.0
    assert top_skus[0]["total_revenue"] == 50.0

def test_idempotent_reingestion(pipeline):
    records = [
        {"transaction_id": "TX-400", "store_id": "ST-AKL-01", "sku": "SKU-IDEMP", "quantity": 1, "unit_price": 25.0, "currency": "NZD"}
    ]
    pipeline.ingest_bronze(records)
    pipeline.process_silver()
    assert pipeline.get_silver_count() == 1

    # Re-run silver processing
    pipeline.process_silver()
    assert pipeline.get_silver_count() == 1

def test_multi_store_isolation(pipeline):
    records = [
        {"transaction_id": "TX-501", "store_id": "STORE-1", "sku": "SKU-X", "quantity": 5, "unit_price": 10.0, "currency": "NZD"},
        {"transaction_id": "TX-502", "store_id": "STORE-2", "sku": "SKU-Y", "quantity": 2, "unit_price": 20.0, "currency": "NZD"}
    ]
    pipeline.ingest_bronze(records)
    pipeline.process_silver()

    metrics = pipeline.get_gold_store_metrics()
    store_ids = {m["store_id"] for m in metrics}
    assert store_ids == {"STORE-1", "STORE-2"}

def test_large_batch_processing(pipeline):
    batch = [
        {"transaction_id": f"TX-BULK-{i}", "store_id": f"STORE-{i % 5}", "sku": f"SKU-{i % 10}", "quantity": 1, "unit_price": 15.0, "currency": "NZD"}
        for i in range(100)
    ]
    accepted, quarantined = pipeline.ingest_bronze(batch)
    assert accepted == 100
    assert quarantined == 0

    pipeline.process_silver()
    assert pipeline.get_silver_count() == 100
