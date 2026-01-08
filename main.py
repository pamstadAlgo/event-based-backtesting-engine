from sqlalchemy import create_engine

from backtester.engine import BacktestEngine
from backtester.strategy import SimpleFundamentalStrategy


def main():
    db_url = "postgresql+psycopg2://user:password@localhost:5432/mydb"
    symbols = ["AAPL", "MSFT", "GOOG"]

    # One shared Engine for the strategy (simple and efficient)
    engine = create_engine(db_url, future=True)
    strategy = SimpleFundamentalStrategy(engine=engine)

    bt = BacktestEngine(
        db_url=db_url,
        symbols=symbols,
        out_csv="output/buys.csv",
        strategy=strategy,
    )
    bt.run()


if __name__ == "__main__":
    main()