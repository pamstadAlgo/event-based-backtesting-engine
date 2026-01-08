from backtester.events import MarketEvent, BuyEvent
from sqlalchemy import text
from strategy import Strategy

class SimpleFundamentalStrategy(Strategy):
    def fetch_balance_sheet(self, symbol, period_end_date):
        sql = text("""
            SELECT *
            FROM balance_sheet
            WHERE symbol = :symbol AND period_end_date = :ped
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            return conn.execute(sql, {"symbol": symbol, "ped": period_end_date}).mappings().first()

    def fetch_income_statement(self, symbol, period_end_date):
        sql = text("""
            SELECT *
            FROM income_statement
            WHERE symbol = :symbol AND period_end_date = :ped
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            return conn.execute(sql, {"symbol": symbol, "ped": period_end_date}).mappings().first()

    def fetch_close_price(self, symbol, period_end_date):
        # Optional
        sql = text("""
            SELECT close
            FROM daily_close
            WHERE symbol = :symbol AND date = :d
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(sql, {"symbol": symbol, "d": period_end_date}).first()
            return None if row is None else row[0]

    def should_buy(self, bs, is_):
        # Placeholder logic – replace with your real buy rule
        if bs is None or is_ is None:
            return False, "missing statements"

        net_income = is_.get("net_income")
        total_equity = bs.get("total_equity")

        if net_income is None or total_equity is None:
            return False, "missing fields"

        if net_income > 0 and total_equity > 0:
            return True, "net_income>0 and equity>0"

        return False, "condition not met"

    def on_market(self, event: MarketEvent) -> BuyEvent | None:
        bs = self.fetch_balance_sheet(event.symbol, event.period_end_date)
        is_ = self.fetch_income_statement(event.symbol, event.period_end_date)

        buy, reason = self.should_buy(bs, is_)
        if not buy:
            return None

        price = self.fetch_close_price(event.symbol, event.period_end_date)
        return BuyEvent(
            symbol=event.symbol,
            period_end_date=event.period_end_date,
            price=price,
            reason=reason,
        )