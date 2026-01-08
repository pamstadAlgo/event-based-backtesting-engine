from __future__ import annotations

from abc import ABC, abstractmethod
from sqlalchemy.engine import Engine
from sqlalchemy import text

from backtester.events import MarketEvent, BuyEvent


class Strategy(ABC):
    """
    Blueprint for strategies.
    """

    def __init__(self, engine: Engine):
        self.engine = engine

    @abstractmethod
    def on_market(self, event: MarketEvent) -> BuyEvent | None:
        raise NotImplementedError