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
            SELECT qfs_symbol_id, period_end_date
            FROM quickfs_dj_balancesheetquarter
            WHERE qfs_symbol_id = ANY(:symbols)
            ORDER BY period_end_date ASC, qfs_symbol_id ASC
        """)

        with self.engine.connect() as conn:
            result = conn.execute(sql, {"symbols": self.symbols})
            for row in result:
                yield row.qfs_symbol_id, row.period_end_date