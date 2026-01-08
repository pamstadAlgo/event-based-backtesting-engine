import pandas as pd
import pandas_datareader.data as web
import calendar
from datetime import date, datetime

class StooqPriceProvider:
    def __init__(self):
        self._cache: dict[str, pd.DataFrame] = {}

    def _to_stooq_candidates(self, symbol: str) -> list[str]:
        """
        Input examples:
          - "AAPL:US" -> ["AAPL", "AAPL.US"]
          - "MULT.DE" -> ["MULT.DE"]  (leave as-is)
          - "AAPL"    -> ["AAPL"]
          - "AAPL.US" -> ["AAPL.US"]
        """
        if ":" in symbol:
            base, suffix = symbol.split(":", 1)
            # 1) try base only, 2) then base.suffix (US -> AAPL.US)
            return [base, f"{base}.{suffix}"]
        return [symbol]

    def _fetch(self, stooq_symbol: str, start_date: date) -> pd.DataFrame:
        df = web.DataReader(stooq_symbol, "stooq", start=start_date, end=datetime.today()).sort_index()
        if not df.empty:
            # normalize index to plain date objects
            df.index = pd.to_datetime(df.index).date
        return df

    def _load_symbol(self, symbol: str, start_date: date) -> pd.DataFrame:
        #if data was already fetched for that symbol return fetched data
        if symbol in self._cache:
            return self._cache[symbol]
        
    
        #transform qfs symbol like AAPL:US to stooq candidates AAPL, AAPL.US
        candidates = self._to_stooq_candidates(symbol)

        for cand in candidates:
            try:
                df = self._fetch(cand, start_date=start_date)
                if not df.empty:
                    self._cache[symbol] = df
                    return df
            except Exception as e:
                last_exc = e
                continue

        # If all candidates failed/empty, cache empty df so we don't retry forever
        empty = pd.DataFrame()
        self._cache[symbol] = empty

        raise RuntimeError(f"no price data found for {symbol}, candidates tried: {candidates}")

        return self._cache[symbol]

        # if symbol not in self._cache:
        #     df = web.DataReader(symbol, "stooq", start=start_date, end=datetime.today()).sort_index()  # ascending
        #     # normalize index to dates (no time component)
        #     df.index = pd.to_datetime(df.index).date
        #     self._cache[symbol] = df
        # return self._cache[symbol]

    def last_close_in_month(self, symbol: str, month_start: date):
        """
        month_start is your DB date: 01-MM-YYYY.
        Returns (price_date, close) for last available trading day in that month.
        """
        df = self._load_symbol(symbol, start_date=month_start)

        year, month = month_start.year, month_start.month
        last_dom = calendar.monthrange(year, month)[1]
        month_end = date(year, month, last_dom)

        print('month_end: ', month_end)
        print('month_start: ', month_start)

        # slice using date-index
        m = df.loc[(df.index >= month_start) & (df.index <= month_end)]
        print('m: ', m)
        if m.empty:
            return None, None

        price_date = m.index[-1]
        close = float(m["Close"].iloc[-1])
        return price_date, close