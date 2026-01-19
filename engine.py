import queue

from events import MarketEvent, BuyEvent
from data import PostgresDataHandler
from sink import CsvBuyWriter
from strategy import Strategy
from datetime import date

class BacktestEngine:
    def __init__(self, db_url: str, symbols: list[str], out_csv: str, strategy: Strategy, start_date: date):
        self.events = queue.Queue()

        self.data = PostgresDataHandler(db_url=db_url, symbols=symbols, start_date=start_date)
        self.writer = CsvBuyWriter(out_csv)
        self.strategy = strategy  # injected

    def run(self):
        for symbol, ped in self.data.stream():
            self.events.put(MarketEvent(symbol=symbol, period_end_date=ped))

            while not self.events.empty():
                ev = self.events.get()

                if isinstance(ev, MarketEvent):
                    buy = self.strategy.on_market(ev)
                    if buy is not None:
                        self.events.put(buy)

                elif isinstance(ev, BuyEvent):
                    self.writer.write(ev)