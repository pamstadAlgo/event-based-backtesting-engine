import os
import requests
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from helpers import parse_ticker, SUFFIX_TO_EXCHANGES
import time



def extractTickers():
    # ===============================
    # 0. Define which columns you want to export; by default we export original ticket (as in MCC sheet, db_ticker, qfs_symbol, company_name, exchange, last close price, equity value penman, margin of safety penman)
    # ===============================
    OPTIONAL_COLUMNS = {
        "Popular": "Popular",
        "REV YoY": "REV YoY",
        "MKT CAP": "MKT CAP",
        "OPmargin": "OPmargin",
        "STOCK PRICE": "STOCK PRICE"
    }

    #normalize keys (lower case and strip white spaces)
    # normalize keys in-place
    OPTIONAL_COLUMNS = {k.lower().strip(): v for k, v in OPTIONAL_COLUMNS.items()}

    # ===============================
    # 0. Load environment (dev)
    # ===============================

    # expects DB_* vars in .env or .env.dev
    load_dotenv()

    # ===============================
    # 1. Download Google Sheet
    # ===============================

    SHEET_ID = "1nj8BFXnedgQ2hugQL4YQ5QSwBhIiztBS358cu7zUWM4"
    EXPORT_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=xlsx"

    INPUT_EXCEL = "google_sheet.xlsx"

    response = requests.get(EXPORT_URL)
    response.raise_for_status()

    with open(INPUT_EXCEL, "wb") as f:
        f.write(response.content)

    print("✔ Google Sheet downloaded")

    # ===============================
    # 2. Load all sheets & extract tickers
    # ===============================

    xls = pd.ExcelFile(INPUT_EXCEL)

    raw_tickers: set[str] = set()
    ticker_metadata: dict[str, dict] = {} #store additional info like revenue growth, market cap etc.

    #iterate through different sheets of microcap club
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)

        df.columns = df.columns.str.lower().str.strip()

        if "ticker" not in df.columns:
            print(f"⚠ Sheet '{sheet_name}' skipped (no ticker column)")
            continue

        tickers = (
            df["ticker"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
        )

        #insert all tickers into set
        raw_tickers.update(tickers)

        # determine which optional columns exist on this sheet
        existing_optional_cols = [
            col for col in OPTIONAL_COLUMNS.keys()
            if col in df.columns
        ]

        if not existing_optional_cols:
            continue


        # keep only ticker + optional columns, take first non-null per ticker
        subset = (
            df[["ticker"] + existing_optional_cols]
            .groupby("ticker", as_index=False)
            .first()
        )

        # merge into metadata dict (first value wins across sheets)
        for _, row in subset.iterrows():
            ticker_metadata.setdefault(row["ticker"], {})

            for col_key in existing_optional_cols:
                #display_name = OPTIONAL_COLUMNS[col_key]
                if col_key not in ticker_metadata[row["ticker"]]:
                    ticker_metadata[row["ticker"]][col_key] = row[col_key]

    print(f"✔ Collected {len(raw_tickers)} unique tickers")

    # ===============================
    # 3. Ticker normalization
    # ===============================

    # Build ticker variants and metadata
    ticker_meta = {}
    for raw in raw_tickers:
        symbol, suffix = parse_ticker(raw)
        ticker_meta[raw] = {"symbol": symbol, "suffix": suffix, "variants": {raw, symbol}}

    #all_variants = sorted({v for meta in ticker_meta.values() for v in meta["variants"]})
    all_variants = sorted({v for meta in ticker_meta.values() for v in meta["variants"]})

    print(f"✔ Collected {len(raw_tickers)} unique tickers")
    print(f"✔ Querying {len(all_variants)} ticker variants")

    """
    we transform ticker_variant.values [["AAPL.US", "AAPL"],["BMW.DE", "BMW"],["MSFT"]] to a set of all variants present
    """

    # ===============================
    # 4. Database connection (HOST → Docker)
    # ===============================

    conn = psycopg2.connect(
        host="localhost",
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    cur = conn.cursor()

    query = """
    SELECT ticker, qfs_symbol, name, exchange, industry
    FROM quickfs_dj_tradedcompanies
    WHERE ticker = ANY(%s)
    OR qfs_symbol = ANY(%s);
    """

    cur.execute(query, (all_variants, all_variants))
    rows = cur.fetchall()
    db_df = pd.DataFrame( rows, columns=["db_ticker", "db_qfs_symbol", "name", "exchange", "industry"] )

    db_df["db_ticker"] = db_df["db_ticker"].str.upper()

    print(f"✔ Found {len(db_df)} matching DB rows")

    # ===============================
    # 5. Resolve matches per original ticker
    # ===============================
    found_rows = []
    missing_rows = []
    iterator = 0

    qfs_symbols = []

    for original_ticker, meta in ticker_meta.items():
        #extract symbol and suffix; symbol: ZEN, suffix: V (ZEN.V)
        symbol = meta["symbol"]
        suffix = meta["suffix"]
        variants = meta["variants"]

        #get candidates. We can have multiple entries with the same ticker, like SPN --> then we need to match based on suffix/exchange
        candidates = db_df[
            db_df["db_ticker"].isin(variants) | db_df["db_qfs_symbol"].isin(variants)
        ]

        if candidates.empty:
            missing_rows.append({
                "original_ticker": original_ticker,
                "tried_variants": ", ".join(variants),
            })
            continue

        #exchnage based resolution
        if suffix and suffix in SUFFIX_TO_EXCHANGES:
            expected_exchanges = SUFFIX_TO_EXCHANGES[suffix]
            exchange_matches = candidates[candidates["exchange"].isin(expected_exchanges)]

            if len(exchange_matches) == 1:
                match = exchange_matches.iloc[0]
            else:
                match = candidates.iloc[0]  # fallback
                print('ambibgious match for: ', match["db_qfs_symbol"])
        #case when ticker has not suffix
        else:
            # 2️⃣ Unique ticker fallback
            if len(candidates) == 1:
                match = candidates.iloc[0]
            else:
                expected_exchanges = SUFFIX_TO_EXCHANGES[""]
                exchange_matches = candidates[candidates["exchange"].isin(expected_exchanges)]
                #no suffix can be NYSE, NASDAQ, OTC
                if len(exchange_matches) == 1:
                    match = exchange_matches.iloc[0]
                else:
                    match = candidates.iloc[0]  # fallback
                    print('ambibgious match for: ', match["db_qfs_symbol"])

        # match = matches.iloc[0]
        qfs_symbols.append(match["db_qfs_symbol"])
        #qfs_symbol = match["db_qfs_symbol"]
    
    return qfs_symbols