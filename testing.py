from priceprovider import StooqPriceProvider
priceProvider = StooqPriceProvider()
from datetime import date

example_date = date(2006, 11, 1)


price_date, price = priceProvider.last_close_in_month('WLDN', example_date)

print(f"date: {price_date}, price: {price}")