from priceprovider import LocalStooqPriceProvider
from pathlib import Path
from datetime import date

provider = LocalStooqPriceProvider(root=Path("stooq_daily_data"))


asof_date, close = provider.last_close_in_month("WLDN:US", date(2025, 7, 1))
print(asof_date, close)