import csv
from pathlib import Path

from events import BuyEvent


class CsvBuyWriter:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(["symbol", "period_end_date", "close_price", "intrinsic_value", "bps", "rnoa", "MoS", "nrShares", "reason"])

    def write(self, buy_event: BuyEvent):
        with self.path.open("a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                buy_event.symbol,
                buy_event.period_end_date.isoformat(),
                "" if buy_event.close_price is None else buy_event.close_price,
                "" if buy_event.intrinsic_value is None else buy_event.intrinsic_value,
                "" if buy_event.bps is None else buy_event.bps,
                "" if buy_event.rnoa is None else buy_event.rnoa,
                "" if buy_event.mos is None else buy_event.mos,
                "" if buy_event.nr_shares is None else buy_event.nr_shares,
                buy_event.reason,
            ])