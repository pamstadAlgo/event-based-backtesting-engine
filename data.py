from sqlalchemy import create_engine, text
from datetime import date

class PostgresDataHandler:
    """
    Responsibility: yield (symbol, period_end_date) in ascending order.
    """

    def __init__(self, db_url: str, symbols: list[str], start_date: date):
        self.engine = create_engine(db_url, future=True)
        self.symbols = symbols

    def stream(self):
        sql = text("""
            SELECT qfs_symbol_id, period_end_date
            FROM quickfs_dj_balancesheetquarter
            WHERE qfs_symbol_id = ANY(:symbols)
            AND period_end_date >= :start_date
            ORDER BY period_end_date ASC, qfs_symbol_id ASC
        """)

        with self.engine.connect() as conn:
            result = conn.execute(sql, {"symbols": self.symbols, "start_date" : self.start_date})
            for row in result:
                yield row.qfs_symbol_id, row.period_end_date