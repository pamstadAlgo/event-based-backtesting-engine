import csv
from pathlib import Path

from backtester.events import BuyEvent


class CsvBuyWriter:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(["symbol", "period_end_date", "price", "reason"])

    def write(self, buy_event: BuyEvent):
        with self.path.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                buy_event.symbol,
                buy_event.period_end_date.isoformat(),
                "" if buy_event.price is None else buy_event.price,
                buy_event.reason,
            ])