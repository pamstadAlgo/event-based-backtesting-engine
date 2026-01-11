
import pandas as pd
import pandas_datareader.data as web
from sqlalchemy import text
from dataclasses import dataclass
from datetime import date
from priceprovider import StooqPriceProvider
from events import MarketEvent, BuyEvent
from strategy import Strategy
from typing import List
from store import ParquetRecordStore

# -----------------------------
# Config
# -----------------------------
@dataclass
class PenmanConfig:
    wacc: float = 0.10
    tax_rate: float = 0.30
    margin_of_safety: float = 0.50  # buy if value >= price*(1+MOS)
    min_price: float = 0.01

# -----------------------------
# Strategy: Penman TTM as-of date
# -----------------------------
class PenmanTTMAsOfStrategy(Strategy):
    """
    Computes Penman-style equity value per share using TTM data *as-of* a derived trading date.
    - event.period_end_date is a month bucket (01-MM-YYYY)
    - we convert it to the last available trading day in that month (via Stooq)
    - all fundamentals queries are anchored to <= asof_date (no look-ahead)
    """

    def __init__(self, engine, cfg: PenmanConfig, price_provider: StooqPriceProvider, store: ParquetRecordStore):
        super().__init__(engine)
        self.cfg = cfg
        self.prices = price_provider
        self.store = store

    def equity_val_penman_ttm_asof(self, symbol: str, asof_date: date):
        """
        Mirrors your original function but makes it "as-of": every query has period_end_date <= asof_date.
        Returns a dict-like row or None.
        """
        sql = text("""
        WITH params AS (
            SELECT
                CAST(:symbol AS text)  AS qfs_symbol,
                CAST(:tax_rate AS float) AS tax_rate,
                CAST(:wacc AS float)     AS wacc,
                CAST(:asof AS date)      AS asof_date
        ),

        -- TTM EBIT from last 4 quarters <= asof_date
        calc_vals AS (
            SELECT SUM(operating_income) AS sustainable_ebit
            FROM (
                SELECT operating_income
                FROM quickfs_dj_incomestatementquarter, params
                WHERE qfs_symbol_id = params.qfs_symbol
                  AND period_end_date <= params.asof_date
                ORDER BY period_end_date DESC
                LIMIT 4
            ) t
            HAVING COUNT(*) = 4
        ),

        -- Rank balance sheet quarters as-of date (latest first)
        bs_ranked AS (
            SELECT
                net_operating_assets,
                total_equity,
                period_end_date,
                ROW_NUMBER() OVER (ORDER BY period_end_date DESC) AS rn
            FROM quickfs_dj_balancesheetquarter, params
            WHERE qfs_symbol_id = params.qfs_symbol
              AND period_end_date <= params.asof_date
        ),

        -- avg_noa from rn in (4, 8), b0 from rn=1 (as-of)
        noa_b0 AS (
            SELECT
                AVG(CASE WHEN rn IN (4, 8) THEN net_operating_assets END) AS avg_noa,
                MAX(CASE WHEN rn = 1 THEN total_equity END) AS b0
            FROM bs_ranked
        ),

        -- Shares diluted from latest IS quarter <= asof_date
        shares AS (
            SELECT shares_diluted
            FROM quickfs_dj_incomestatementquarter, params
            WHERE qfs_symbol_id = params.qfs_symbol
              AND period_end_date <= params.asof_date
            ORDER BY period_end_date DESC
            LIMIT 1
        ),

        equity_calc AS (
            SELECT
                b0
                + ((sustainable_ebit * (1 - tax_rate) - wacc * avg_noa) / (1 + wacc))
                + ((sustainable_ebit * (1 - tax_rate) - wacc * avg_noa) / ((1 + wacc) * wacc))
                AS equity_val_total,

                sustainable_ebit * (1 - tax_rate) - wacc * avg_noa AS residual_earnings,
                sustainable_ebit * (1 - tax_rate) AS net_operating_profit,
                avg_noa,
                b0
            FROM calc_vals, noa_b0, params
        ),

        rnoa AS (
            SELECT CASE WHEN avg_noa > 0 THEN net_operating_profit / avg_noa ELSE NULL END AS rnoa
            FROM equity_calc
        )

        SELECT
            CASE
                WHEN (SELECT shares_diluted FROM shares) > 0
                THEN ec.equity_val_total / (SELECT shares_diluted FROM shares)
                ELSE NULL
            END AS equity_val_per_share,

            ec.equity_val_total,
            (SELECT shares_diluted FROM shares) AS shares_diluted,
            ec.residual_earnings,
            r.rnoa,
            ec.avg_noa,
            ec.b0
        FROM equity_calc ec
        CROSS JOIN rnoa r;
        """)

        with self.engine.connect() as conn:
            row = conn.execute(sql, {
                "symbol": symbol,
                "tax_rate": self.cfg.tax_rate,
                "wacc": self.cfg.wacc,
                "asof": asof_date,
            }).mappings().first()

        return row  # dict-like mapping or None

    def has_valid_last4_quarters(self, symbol: str, asof: date) -> bool:
        """
        Function that checks if the last four entries are actually four quarters apart. For some companies, mainly on OTC, they are not required to file quarterly, so the last four entries in quarterly tables can be spaced 4 years apart and not 12 months
        """
        last_4_dates_sql = text("""
        SELECT period_end_date
        FROM quickfs_dj_incomestatementquarter
        WHERE qfs_symbol_id = :symbol
        AND period_end_date <= :asof
        ORDER BY period_end_date DESC
        LIMIT 4;
        """)

        with self.engine.connect() as conn:
            dates: List[date] = conn.execute(last_4_dates_sql, {"symbol": symbol, "asof": asof}).scalars().all()

        #if there are less than four data points available, skip
        if len(dates) != 4:
            return False
    
        #check that newst and oldest date are at max 1 year apart
        newest = dates[0]
        oldest = dates[-1]

        return (newest.year - oldest.year) <= 1

    def on_market(self, event: MarketEvent) -> BuyEvent | None:
        # event.period_end_date is 01-MM-YYYY (month bucket)
        asof_date, close = self.prices.last_close_in_month(event.symbol, event.period_end_date)
        if close is None or close < self.cfg.min_price:
            return None
        
        #check if last four data points are valid to compute the penman equity val
        if not self.has_valid_last4_quarters(event.symbol, asof_date):
            return None

        # Compute Penman valuation anchored to asof_date (no look-ahead)
        res = self.equity_val_penman_ttm_asof(event.symbol, asof_date)
        if not res:
            return None

        value = res.get("equity_val_per_share")
        if value is None or value <= 0:
            return None
        
        #store result of penman computation as a time series
        self.store.append(
            dataset="valuations_penman_ttm",
            record={
                "symbol": event.symbol,
                "asof_date": asof_date.isoformat(),
                "period_bucket": event.period_end_date.isoformat(),
                "close": close,
                "equity_val_per_share": float(value),
                "equity_val_total": res.get("equity_val_total"),
                "shares_diluted": res.get("shares_diluted"),
                "residual_earnings": res.get("residual_earnings"),
                "rnoa": res.get("rnoa"),
                "wacc": float(self.cfg.wacc),
                "tax_rate": float(self.cfg.tax_rate),
            },
            partition_cols=["symbol"],
        )

        threshold = close * (1.0 + self.cfg.margin_of_safety)
        if value >= threshold:
            reason = (
                f"Penman TTM as-of {asof_date}: value={value:.2f} "
                f">= close={close:.2f} * (1+MOS {self.cfg.margin_of_safety:.0%})"
            )
            return BuyEvent(
                symbol=event.symbol,
                period_end_date=asof_date,  # log the real tradable date
                price=close,
                reason=reason,
            )

        return None