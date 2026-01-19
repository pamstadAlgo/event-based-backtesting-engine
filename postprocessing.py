import os
import time
import requests
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from priceprovider import EODHDPriceProvider
from dotenv import load_dotenv

load_dotenv()


INPUT_CSV = "output/buys.csv"
OUTPUT_CSV = "buys_postprocessed.csv"
ADJUSTED_CLOSE_PRICE_COL = ""

# EODHD endpoint template
EODHD_URL = "https://eodhd.com/api/eod/{symbol}?api_token={token}&fmt=csv"


def fetch_eod_history(eodhd_symbol: str, api_key: str, timeout=30, retries=3, backoff_sec=1.0) -> pd.DataFrame:
    """
    Fetch EOD history as CSV and return a DataFrame with at least columns:
    ['date', 'close'] (date is datetime64).
    """
    url = EODHD_URL.format(symbol=eodhd_symbol, token=api_key)

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()

            # EODHD returns CSV text
            df = pd.read_csv(StringIO(r.text))

            # Normalize column names
            df.columns = [c.strip().lower() for c in df.columns]

            if "date" not in df.columns or "adjusted_close" not in df.columns:
                raise ValueError(f"Unexpected columns for {eodhd_symbol}: {df.columns.tolist()}")

            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date", "adjusted_close"]).copy()
            df = df.sort_values("date")

            return df

        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff_sec * attempt)

    raise RuntimeError(f"Failed to fetch EOD history for {eodhd_symbol}: {last_err}")


def compute_metrics(price_df: pd.DataFrame, buy_date: pd.Timestamp, buy_price: float) -> dict:
    """
    price_df must contain columns ['date', 'adjusted_close'] sorted by date.
    We consider only rows with date strictly greater than buy_date.
    """
    after = price_df.loc[price_df["date"] > buy_date].copy()

    # No price data after buy date
    if after.empty:
        return {
            "max_close_after_buy": None,
            "max_close_date": None,
            "max_return": None,
            "days_to_max": None,
            "double_date": None,
            "days_to_double": None,
        }

    # Max close after buy
    idx = after["adjusted_close"].idxmax()
    max_close = float(after.loc[idx, "adjusted_close"])
    max_date = after.loc[idx, "date"]

    max_return = (max_close - buy_price) / buy_price
    days_to_max = int((max_date - buy_date).days)

    # Doubling: first date where close >= 2 * buy_price
    target = 2.0 * buy_price
    doubled = after.loc[after["adjusted_close"] >= target].copy()
    if doubled.empty:
        double_date = None
        days_to_double = None
    else:
        double_date = doubled.iloc[0]["date"]
        days_to_double = int((double_date - buy_date).days)

    return {
        "max_close_after_buy": max_close,
        "max_close_date": max_date.date().isoformat() if pd.notna(max_date) else None,
        "max_return": max_return,
        "days_to_max": days_to_max,
        "double_date": double_date.date().isoformat() if double_date is not None else None,
        "days_to_double": days_to_double,
    }


def main():
    api_key = os.environ['EODHD_API_KEY']
    if not api_key:
        raise RuntimeError("Missing environment variable EODHD_API_KEY")

    df = pd.read_csv(INPUT_CSV)
    df["period_end_date"] = pd.to_datetime(df["period_end_date"], errors="coerce")
    df = df.dropna(subset=["symbol", "period_end_date", "adj_close_price"]).copy()

    # Keep only earliest entry per symbol
    df = df.sort_values(["symbol", "period_end_date"])
    df_earliest = df.groupby("symbol", as_index=False).first()

    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ["DB_HOST"]
    port = os.environ["DB_PORT"]
    db = os.environ["POSTGRES_DB"]

    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

    #create db engine to fetch exchange of symbol
    engine = create_engine(db_url, future=True)

    #initialize EODHD class to fetch prices
    price_provider = EODHDPriceProvider(engine)

    results = []
    for _, row in df_earliest.iterrows():
        symbol = str(row["symbol"])
        buy_date = pd.Timestamp(row["period_end_date"])
        buy_price = float(row["adj_close_price"])

        #eodhd_symbol = to_eodhd_symbol(symbol)
        eodhd_symbols = price_provider._transform_symbol(symbol)

        try:
            for eodhd_symbol in eodhd_symbols:
                try:
                    price_df = fetch_eod_history(eodhd_symbol, api_key)
                    metrics = compute_metrics(price_df, buy_date, buy_price)
                    status = "ok"
                    error = None
                    break
                except Exception as e:
                    print(f'eodhd_symbol not available: {eodhd_symbol}')
                
        except Exception as e:
            metrics = {
                "max_close_after_buy": None,
                "max_close_date": None,
                "max_return": None,
                "days_to_max": None,
                "double_date": None,
                "days_to_double": None,
            }
            status = "error"
            error = str(e)

        out_row = row.to_dict()
        out_row["eodhd_symbol"] = eodhd_symbol
        out_row.update(metrics)
        out_row["fetch_status"] = status
        out_row["fetch_error"] = error
        results.append(out_row)

        # Be nice to the API (basic rate-limiting)
        # time.sleep(0.2)

    out_df = pd.DataFrame(results)

    # Optional: ensure nice column ordering (keeps original columns first)
    original_cols = list(df.columns)
    extra_cols = [c for c in out_df.columns if c not in original_cols]
    out_df = out_df[original_cols + extra_cols]

    out_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote {len(out_df)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
