import pandas as pd
import pandas_datareader.data as web
import calendar
from datetime import date, datetime
from pathlib import Path
from sqlalchemy import text
from helpers import EXCHANGE_MAPPING
import os
from io import StringIO
import requests

MISSING_SYMBOLS_FILE = Path("output/missing_price_symbols.txt")
MISSING_SYMBOLS_FILE.parent.mkdir(parents=True, exist_ok=True)


# @dataclass
# class LocalStooqConfig:
#     root: Path  # e.g. Path("stooq_daily_data")
#     missing_log: Path = Path("output/missing_price_symbols.txt")


class EODHDPriceProvider:
    """
    Gets daily price history from https://eodhd.com/
    """
    def __init__(self, engine):
        self.date_col_name = "Date"
        self.close_price_col_name = "Close"
        self.adj_close_price_col_name = "Adjusted_close"
        self.engine = engine
        self._cache: dict[str, pd.DataFrame] = {}     # symbol -> df

    def _log_missing_symbols(self, symbol, candidates):
        # ts = datetime.utcnow().isoformat(timespec="seconds")
        line = f"{symbol} | tried={candidates}\n"

        with MISSING_SYMBOLS_FILE.open("a") as f:
            f.write(line)

    def _exchange(self, qfs_symbol: str) -> str | None:
        """
        returns exchange for a given qfs symbol
        """
        query = text("""
            SELECT exchange
            from quickfs_dj_tradedcompanies
            where qfs_symbol = :symbol
            """)

        with self.engine.connect() as conn:
            exchange = conn.execute(query, {"symbol" : qfs_symbol}).scalar_one()
            return exchange
    
        return None

    def _transform_symbol(self, qfs_symbol: str) -> list[str] | None:
        #parse everything after : of qfs symbol
        if ":" in qfs_symbol:
            ticker, country_code = qfs_symbol.split(":")

            #get exchange of symbol
            exchange = self._exchange(qfs_symbol)

            #check if country_code$exchange exists in mapping dict
            try:
                eodhd_symbols = []

                for exchg in EXCHANGE_MAPPING[f'{country_code}${exchange}']:
                    eodhd_symbols.append(f"{ticker}.{exchg}")
                # #TO-DO: SYMBOLS SHOULD BE A LIST OF CANDIDATES; FOR EXAMPLE FOR LONDON WE HAVE AMBIGIOUS/MULTIPLE EXCHANGES
                # eodhd_symbol = f"{ticker}.{EXCHANGE_MAPPING[f'{country_code}${exchange}']}"
                return eodhd_symbols
            except Exception as e:
                print(f'{qfs_symbol} not able to transform for EODHD')
                return None

        return None
    
    def _load_symbol(self, symbol: str) -> pd.DataFrame:
        #check if historical price data has already been downloaded
        if symbol in self._cache:
            return self._cache[symbol]
        
        #if symbol not yet in cache, fetch from API endpoint
        #transform qfs symbol to eodhd symbol
        eodhd_symbols = self._transform_symbol(qfs_symbol=symbol)

        if eodhd_symbols is not None and len(eodhd_symbols) > 0:
            #try to fetch data from eodhd api
            for eodhd_symbol in eodhd_symbols:
                try:
                    url = f"https://eodhd.com/api/eod/{eodhd_symbol}?api_token={os.environ['EODHD_API_KEY']}&fmt=csv"
                    resp = requests.get(url)

                    #read csv file
                    df = pd.read_csv(StringIO(resp.text))

                    #create index based on date
                    df[self.date_col_name] =  pd.to_datetime(df[self.date_col_name].astype(str),format="%Y-%m-%d", errors="raise").dt.date
                    df = df.sort_values(self.date_col_name).set_index(self.date_col_name)

                    #store price data frame in cache
                    self._cache[symbol] = df
                    return df
                except Exception as e:
                    print(f"eodhd_symbol {eodhd_symbol} not available in price endpoint")

            #return empty data frame if no success
            self._log_missing_symbols(symbol=symbol, candidates=eodhd_symbols)

            df = pd.DataFrame()
            self._cache[symbol] = df
            return df
        else:
            self._log_missing_symbols(symbol=symbol, candidates=eodhd_symbols)

            #store empty dataframe
            df = pd.DataFrame()
            self._cache[symbol] = df
            print(f"empty dataframe for symbol: {symbol}, because not able to create eodhd ticker")
            return df

    def last_adj_close_in_month(self, symbol: str, month_start: date):
        df = self._load_symbol(symbol)

        #if data frame is empty return none for close price and date
        if df.empty:
            return None, None
        
        #extract date range
        year, month = month_start.year, month_start.month
        last_dom = calendar.monthrange(year, month)[1]
        month_end = date(year, month, last_dom)

        #filter historical close price data frame
        m = df.loc[(df.index >= month_start) & (df.index <= month_end)]
        if m.empty:
            return None, None

        price_date = m.index[-1]
        close = float(m[self.adj_close_price_col_name].iloc[-1])
        return price_date, close

    def last_close_in_month(self, symbol: str, month_start: date):
        #load historical price data of symbol
        df = self._load_symbol(symbol)

        #if data frame is empty return none for close price and date
        if df.empty:
            return None, None
        
        #extract date range
        year, month = month_start.year, month_start.month
        last_dom = calendar.monthrange(year, month)[1]
        month_end = date(year, month, last_dom)

        #filter historical close price data frame
        m = df.loc[(df.index >= month_start) & (df.index <= month_end)]
        if m.empty:
            return None, None

        price_date = m.index[-1]
        close = float(m[self.close_price_col_name].iloc[-1])
        return price_date, close


class LocalStooqPriceProvider:
    """
    Reads Stooq bulk TXT files from a nested folder structure like:

    stooq_daily_data/
      us/
        nasdaq/
          1/
            aapl.us.txt
          2/
            ...
        nyse/
          1/
            ...
      uk/
        ...
    """

    def __init__(self, root: Path):
        # self.cfg = cfg
        # self.cfg.missing_log.parent.mkdir(parents=True, exist_ok=True)
        self.root = Path(root)
        self.date_col_name = '<DATE>' #column 
        self.close_price_col_name = '<CLOSE>'

        if not self.root.exists():
            raise ValueError(f"Stooq root folder does not exist: {self.root}")
        
        self._cache: dict[str, pd.DataFrame] = {}     # symbol -> df
        self._file_cache: dict[str, Path] = {}        # symbol -> resolved file path

    # ---------- symbol normalization ----------
    def _parse_country(self, symbol: str) -> str | None:
        # Your format: WLDN:US -> "us"
        if ":" in symbol:
            _, c = symbol.split(":", 1)
            return c.strip().lower()
        return None

    def _candidates(self, symbol: str) -> list[str]:
        """
        Creates possible Stooq-style base names (without folder path).
        We’ll search for files like: <cand>.txt

        Examples:
          "WLDN:US" -> ["wldn.us", "wldn"]   (try stooq style first)
          "AAPL:US" -> ["aapl.us", "aapl"]
          "MULT.DE" -> ["mult.de"]
        """
        s = symbol.strip()

        # already stooq-like with dot suffix:
        if "." in s and ":" not in s:
            return [s.lower()]

        if ":" in s:
            base, country = s.split(":", 1)
            base = base.strip().lower()
            country = country.strip().lower()
            return [f"{base}.{country}", base]

        return [s.lower()]

    # ---------- filesystem lookup ----------
    def _country_roots_in_priority_order(self, country: str | None) -> list[Path]:
        """
        Prefer searching in the correct country folder first, then fallback to all.
        """
        if country:
            preferred = self.root / country
            if preferred.exists():
                # prefer this first
                others = [p for p in self.root.iterdir() if p.is_dir() and p.name != country]
                return [preferred] + others

        # no country or not found: search all country folders
        return [p for p in self.root.iterdir() if p.is_dir()]

    def _find_file(self, symbol: str) -> Path | None:
        """
        Recursively search within (country/exchange/bucket/...) for the .txt file.
        """
        if symbol in self._file_cache:
            return self._file_cache[symbol]

        country = self._parse_country(symbol)
        candidates = self._candidates(symbol)

        for croot in self._country_roots_in_priority_order(country):
            # recursive scan, but only for candidates, so it’s not too crazy
            for cand in candidates:
                # e.g. wldn.us.txt
                target = f"{cand}.txt"
                hits = list(croot.rglob(target))
                if hits:
                    self._file_cache[symbol] = hits[0]
                    return hits[0]

                # sometimes uppercase in filenames; rglob is case-sensitive on Linux/macOS
                # we can do a slightly broader search:
                # hits = list(croot.rglob(f"{cand}*.txt"))
                # if hits:
                #     # pick best match if exact exists
                #     exact = [h for h in hits if h.name.lower() == target]
                #     chosen = exact[0] if exact else hits[0]
                #     self._file_cache[symbol] = chosen
                #     return chosen

        return None

    def _log_missing_symbols(self, symbol, candidates):
        # ts = datetime.utcnow().isoformat(timespec="seconds")
        line = f"{symbol} | tried={candidates}\n"

        with MISSING_SYMBOLS_FILE.open("a") as f:
            f.write(line)
    # def _log_missing(self, symbol: str, candidates: list[str]) -> None:
    #     with self.cfg.missing_log.open("a") as f:
    #         f.write(f"{symbol} | tried={candidates}\n")

    # ---------- data loading ----------
    def _load_symbol(self, symbol: str) -> pd.DataFrame:
        if symbol in self._cache:
            return self._cache[symbol]

        candidates = self._candidates(symbol)
        p = self._find_file(symbol)

        if p is None:
            self._log_missing_symbols(symbol, candidates)
            self._cache[symbol] = pd.DataFrame()
            return self._cache[symbol]

        #wrappe in try except block because some files downloaded from stooq are empty
        try:
            df = pd.read_csv(p)
        except Exception as e:
            #assign empty dataframe
            df = pd.DataFrame()
            self._cache[symbol] = df
            print(f"Error when reading csv file for path:{p}, e: {e}")
            return df
        
        # Stooq files commonly use "Date" column
        if self.date_col_name not in df.columns:
            raise RuntimeError(f"Unexpected format in {p} (missing 'Date' column). Columns={list(df.columns)}")

        df[self.date_col_name] =  pd.to_datetime(df[self.date_col_name].astype(str),format="%Y%m%d", errors="raise").dt.date
        df = df.sort_values(self.date_col_name).set_index(self.date_col_name)

        # ensure Close exists
        if self.close_price_col_name not in df.columns:
            # sometimes lowercase in some dumps
            close_candidates = [c for c in df.columns if c.lower() == "close"]
            if close_candidates:
                df = df.rename(columns={close_candidates[0]: "Close"})
            else:
                raise RuntimeError(f"Unexpected format in {p} (missing 'Close'). Columns={list(df.columns)}")

        self._cache[symbol] = df
        return df

    # ---------- main API ----------
    def last_close_in_month(self, symbol: str, month_start: date):
        df = self._load_symbol(symbol)
        if df.empty:
            return None, None

        year, month = month_start.year, month_start.month
        last_dom = calendar.monthrange(year, month)[1]
        month_end = date(year, month, last_dom)

        m = df.loc[(df.index >= month_start) & (df.index <= month_end)]
        if m.empty:
            return None, None

        price_date = m.index[-1]
        close = float(m[self.close_price_col_name].iloc[-1])
        return price_date, close


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
    
    def log_missing_symbols(self, symbol, candidates):
        # ts = datetime.utcnow().isoformat(timespec="seconds")
        line = f"{symbol} | tried={candidates}\n"

        with MISSING_SYMBOLS_FILE.open("a") as f:
            f.write(line)


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

        # if symbol not in ["PCHM:US", "MUEL:US"]:
        self.log_missing_symbols(symbol, candidates)
            # raise RuntimeError(f"no price data found for {symbol}, candidates tried: {candidates}")

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

        # slice using date-index
        m = df.loc[(df.index >= month_start) & (df.index <= month_end)]
        if m.empty:
            return None, None

        price_date = m.index[-1]
        close = float(m["Close"].iloc[-1])
        return price_date, close