import sqlite3
import hashlib
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple
from retail_lakehouse.models import RawPosRecord, SilverTransaction

class RetailLakehousePipeline:
    def __init__(self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schemas()

    def _init_schemas(self):
        cur = self.conn.cursor()
        # Bronze: raw uncurated ingested payloads
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze_pos_events (
                raw_id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id TEXT,
                store_id TEXT,
                pos_terminal TEXT,
                event_timestamp TEXT,
                sku TEXT,
                quantity INTEGER,
                unit_price REAL,
                currency TEXT,
                event_type TEXT,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Bronze Quarantine: malformed records
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bronze_quarantine (
                quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_payload TEXT,
                rejection_reason TEXT,
                rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Silver: cleaned, validated, CDC deduplicated
        cur.execute("""
            CREATE TABLE IF NOT EXISTS silver_pos_transactions (
                cdc_id TEXT PRIMARY KEY,
                transaction_id TEXT,
                store_id TEXT,
                pos_terminal TEXT,
                event_timestamp TEXT,
                sku TEXT,
                quantity INTEGER,
                unit_price REAL,
                total_amount REAL,
                currency TEXT,
                event_type TEXT,
                cdc_op TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

    def ingest_bronze(self, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Ingests raw records into Bronze. Rejects invalid payloads into Quarantine."""
        cur = self.conn.cursor()
        accepted = 0
        quarantined = 0

        for r in records:
            tx_id = r.get("transaction_id")
            store_id = r.get("store_id")
            sku = r.get("sku")
            qty = r.get("quantity")
            price = r.get("unit_price")

            # Validation rules: tx_id, store_id, and sku required; qty non-zero; price non-negative
            if not tx_id or not store_id or not sku:
                cur.execute(
                    "INSERT INTO bronze_quarantine (raw_payload, rejection_reason) VALUES (?, ?)",
                    (json.dumps(r), "Missing required identity fields")
                )
                quarantined += 1
                continue

            try:
                qty_val = int(qty)
                price_val = float(price)
                if price_val < 0:
                    raise ValueError("Negative price")
            except (ValueError, TypeError) as e:
                cur.execute(
                    "INSERT INTO bronze_quarantine (raw_payload, rejection_reason) VALUES (?, ?)",
                    (json.dumps(r), f"Invalid numeric field: {e}")
                )
                quarantined += 1
                continue

            cur.execute("""
                INSERT INTO bronze_pos_events (
                    transaction_id, store_id, pos_terminal, event_timestamp, sku,
                    quantity, unit_price, currency, event_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(tx_id),
                str(store_id),
                str(r.get("pos_terminal", "TERM-01")),
                str(r.get("event_timestamp", datetime.now(timezone.utc).isoformat())),
                str(sku),
                qty_val,
                price_val,
                str(r.get("currency", "NZD")),
                str(r.get("event_type", "SALE")).upper()
            ))
            accepted += 1

        self.conn.commit()
        return accepted, quarantined

    def process_silver(self) -> int:
        """Processes Bronze events into Silver layer using CDC deduplication and business transforms."""
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM bronze_pos_events")
        rows = cur.fetchall()

        inserted_count = 0
        for row in rows:
            tx_id = row["transaction_id"]
            sku = row["sku"]
            event_type = row["event_type"]
            qty = row["quantity"]
            price = row["unit_price"]

            # Compute CDC deterministic hash key
            hash_input = f"{tx_id}:{sku}:{event_type}"
            cdc_id = hashlib.sha256(hash_input.encode()).hexdigest()[:16]

            # Financial calculation: Returns invert amount and net quantity
            multiplier = -1 if event_type == "RETURN" else 1
            net_qty = abs(qty) * multiplier
            total_amount = round(net_qty * price, 2)
            cdc_op = "D" if event_type == "RETURN" else "I"

            cur.execute("""
                INSERT OR REPLACE INTO silver_pos_transactions (
                    cdc_id, transaction_id, store_id, pos_terminal, event_timestamp,
                    sku, quantity, unit_price, total_amount, currency, event_type, cdc_op
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                cdc_id,
                tx_id,
                row["store_id"],
                row["pos_terminal"],
                row["event_timestamp"],
                sku,
                net_qty,
                price,
                total_amount,
                row["currency"],
                event_type,
                cdc_op
            ))
            inserted_count += 1

        self.conn.commit()
        return inserted_count

    def get_gold_store_metrics(self) -> List[Dict[str, Any]]:
        """Gold layer aggregation: Computes store-level KPI summary metrics."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT 
                store_id,
                currency,
                COUNT(DISTINCT transaction_id) as total_transactions,
                SUM(quantity) as total_units_sold,
                ROUND(SUM(total_amount), 2) as net_sales_revenue,
                ROUND(AVG(total_amount), 2) as average_transaction_value
            FROM silver_pos_transactions
            GROUP BY store_id, currency
            ORDER BY net_sales_revenue DESC
        """)
        return [dict(r) for r in cur.fetchall()]

    def get_gold_top_skus(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Gold layer aggregation: Identifies top revenue-generating SKUs."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT 
                sku,
                SUM(quantity) as total_volume,
                ROUND(SUM(total_amount), 2) as total_revenue
            FROM silver_pos_transactions
            GROUP BY sku
            ORDER BY total_revenue DESC
            LIMIT ?
        """, (limit,))
        return [dict(r) for r in cur.fetchall()]

    def get_quarantine_count(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM bronze_quarantine")
        return cur.fetchone()[0]

    def get_silver_count(self) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM silver_pos_transactions")
        return cur.fetchone()[0]
