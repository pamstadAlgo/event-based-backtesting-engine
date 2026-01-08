from sqlalchemy import create_engine, text


class PostgresDataHandler:
    """
    Responsibility: yield (symbol, period_end_date) in ascending order.
    """

    def __init__(self, db_url: str, symbols: list[str]):
        self.engine = create_engine(db_url, future=True)
        self.symbols = symbols

    def stream(self):
        sql = text("""
            SELECT symbol, period_end_date
            FROM financial_periods
            WHERE symbol = ANY(:symbols)
            ORDER BY period_end_date ASC, symbol ASC
        """)

        with self.engine.connect() as conn:
            result = conn.execute(sql, {"symbols": self.symbols})
            for row in result:
                yield row.symbol, row.period_end_date