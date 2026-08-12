from datetime import datetime

from pydantic import BaseModel


class MerchantSummary(BaseModel):
    merchant_id: str
    window_start: datetime
    total_amount_cents: int
    transaction_count: int

class Anomaly(BaseModel):
    merchant_id: str
    window_start: datetime
    amount_cents: int
    trailing_avg_cents: float
    ratio: float