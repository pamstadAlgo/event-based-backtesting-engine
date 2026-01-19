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
    close_price: Optional[float]
    adj_close_price: Optional[float]
    intrinsic_value: Optional[float]
    bps: Optional[float] # book value per share
    rnoa: Optional[float]
    mos: Optional[float] #margin of safety
    nr_shares: Optional[float]
    reason: str = ""