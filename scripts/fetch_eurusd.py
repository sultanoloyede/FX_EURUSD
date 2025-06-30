"""
fetch_and_merge_eurusd.py  –  v2
--------------------------------
• Pull EUR/USD 1-hour and 1-day candles from Twelve Data
• Forward-fill daily OHLCV onto the hourly index
• Override daily-close so it always equals the current hour’s close
• Save to CSV  (eurusd_1h_plus_1d.csv)

Prereqs:
    pip install twelvedata pandas python-dotenv
    export TD_API_KEY="YOUR_TWELVE_DATA_KEY"
"""
from dotenv import load_dotenv
import os, sys
import pandas as pd
from twelvedata import TDClient


# ── helpers ────────────────────────────────────────────────────── #
def fetch_candles(symbol: str, interval: str, td: TDClient) -> pd.DataFrame:
    df = td.time_series(
        symbol=symbol,
        interval=interval,
        outputsize=5000,
        order="ASC"
    ).as_pandas()

    df.index = pd.to_datetime(df.index, utc=True)
    df.sort_index(inplace=True)
    df.rename_axis("timestamp", inplace=True)
    return df


def merge_daily_into_hourly(hr_df: pd.DataFrame,
                            day_df: pd.DataFrame) -> pd.DataFrame:
    day_to_hour = day_df.reindex(hr_df.index, method="ffill").add_prefix("D_")
    merged = pd.concat([hr_df, day_to_hour], axis=1)

    # —— Override the daily close with the current 1-hour close —— #
    merged["D_close"] = merged["close"]        # <-- one-liner fix
    return merged


# ── main ───────────────────────────────────────────────────────── #
def main():
    load_dotenv()

    api_key = os.getenv("TD_API_KEY")
    if not api_key:
        sys.exit("❌ Set TD_API_KEY environment variable first.")

    td = TDClient(apikey=api_key)

    eurusd_1h = fetch_candles("EUR/USD", "1h",   td)
    eurusd_1d = fetch_candles("EUR/USD", "1day", td)

    combined = merge_daily_into_hourly(eurusd_1h, eurusd_1d)
    combined.to_csv("eurusd_1h_plus_1d.csv")

    print("✅ Saved   eurusd_1h_plus_1d.csv")
    print(combined.tail(3))


if __name__ == "__main__":
    main()
