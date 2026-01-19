from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from engine import BacktestEngine
from SimpleFundamentalStrategy import SimpleFundamentalStrategy
from PenmanTTMStrategy import PenmanTTMAsOfStrategy, PenmanConfig
from priceprovider import StooqPriceProvider, LocalStooqPriceProvider, EODHDPriceProvider
from store import ParquetRecordStore
from extract_tickers import extractTickers
from pathlib import Path
from datetime import date
load_dotenv()

def main():
    #get db connection variables
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ["DB_HOST"]
    port = os.environ["DB_PORT"]
    db = os.environ["POSTGRES_DB"]

    # URL-encode password to avoid issues with special characters
    #password_encoded = quote_plus(password)

    start_date = date(2000, 1, 1) # YYYY, MM, DD


    #db_url = "postgresql+psycopg2://user:password@localhost:5432/mydb"
    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    

    symbols = extractTickers()
    print('these are tickers: ', symbols[:20])
   # symbols = ["WLDN:US", "LEU:US", "NSSC:US", "IDR:US", "CELH:US", "INOD:US", "PVLA", "KTEL", "LUNA"]

    # One shared Engine for the strategy (simple and efficient)
    engine = create_engine(db_url, future=True)

    #initalize the price provider
    # price_provider = LocalStooqPriceProvider(root=Path("stooq_daily_data"))
    price_provider = EODHDPriceProvider(engine)

    #store will be used to store time series of equity valuations
    store = ParquetRecordStore(root_dir="data")

    #strategy = SimpleFundamentalStrategy(engine=engine)
    strategy = PenmanTTMAsOfStrategy(engine, PenmanConfig(), price_provider=price_provider, store=store)

    bt = BacktestEngine(
        db_url=db_url,
        symbols=symbols,
        out_csv="output/buys.csv",
        strategy=strategy,
        start_date=start_date
    )
    bt.run()


if __name__ == "__main__":
    main()