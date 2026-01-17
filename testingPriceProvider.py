from priceprovider import LocalStooqPriceProvider, EODHDPriceProvider
from pathlib import Path
from datetime import date
from sqlalchemy import create_engine
import os

user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["DB_HOST"]
port = os.environ["DB_PORT"]
db = os.environ["POSTGRES_DB"]

db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
engine = create_engine(db_url, future=True)

# provider = LocalStooqPriceProvider(root=Path("stooq_daily_data"))

provider = EODHDPriceProvider(engine)


asof_date, close = provider.last_close_in_month("WLDN:US", date(2012, 7, 1))
print(asof_date, close)