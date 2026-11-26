from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class RawPosRecord:
    transaction_id: str
    store_id: str
    pos_terminal: str
    event_timestamp: str
    sku: str
    quantity: int
    unit_price: float
    currency: str
    event_type: str = "SALE"  # SALE, RETURN, INVENTORY_ADJUSTMENT

@dataclass(frozen=True)
class SilverTransaction:
    cdc_id: str
    transaction_id: str
    store_id: str
    pos_terminal: str
    event_timestamp: str
    sku: str
    quantity: int
    unit_price: float
    total_amount: float
    currency: str
    event_type: str
    cdc_op: str
