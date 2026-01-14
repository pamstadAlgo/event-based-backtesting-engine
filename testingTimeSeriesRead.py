import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

SYMBOL = "XPEL:US"


def read_symbol_safe(root="data", dataset="valuations_penman_ttm", symbol="WLDN:US"):
    path = Path(root) / dataset / f"symbol={symbol}"
    files = sorted(path.glob("*.parquet"))

    if not files:
        raise FileNotFoundError(f"No parquet files found in {path}")

    dfs = []
    for f in files:
        df_part = pd.read_parquet(f)
        dfs.append(df_part)

    df = pd.concat(dfs, ignore_index=True)

    # optional: ensure proper sorting + add symbol back
    if "asof_date" in df.columns:
        df = df.sort_values("asof_date")
    df["symbol"] = symbol
    return df

df = read_symbol_safe(symbol=SYMBOL)
print(df)


# Make sure asof_date is datetime
df["asof_date"] = pd.to_datetime(df["asof_date"])
df = df.sort_values("asof_date")

plt.figure(figsize=(10, 5))

plt.plot(df["asof_date"], df["equity_val_per_share"], label="Equity Value / Share")
plt.plot(df["asof_date"], df["close"], label="Close Price")

plt.xlabel("Date")
plt.ylabel("Price")
plt.title(f"{SYMBOL} – Penman Value vs Market Price")
plt.legend()
plt.grid(True)

plt.tight_layout()
plt.show()