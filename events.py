from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class MarketEvent:
    symbol: str
    period_end_date: date


@dataclass(frozen=True)
class BuyEvent:
    symbol: str
    period_end_date: date
    price: Optional[float]
    reason: str = ""