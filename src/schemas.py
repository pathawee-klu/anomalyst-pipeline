from datetime import datetime, timezone
from typing import Literal
from pydantic import BaseModel, Field

class StockTick(BaseModel):
    symbol: str
    price: float = Field(..., gt=0, description="Current stock price")
    volume: float = Field(..., ge=0, description="Trade volume")
    timestamp: int = Field(..., description="Unix timestamp in milliseconds")

    def datetime_utc(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp / 1000.0, tz=timezone.utc)

class StockAnomalyEvent(BaseModel):
    symbol: str
    current_price: float
    previous_price: float
    percent_change: float
    anomaly_type: Literal["SPIKE", "DROP"]
    timestamp: int

    def datetime_utc(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp / 1000.0, tz=timezone.utc)
