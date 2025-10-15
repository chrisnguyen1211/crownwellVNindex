import os
import argparse
import pandas as pd
from helpers import fetch_all_tickers, fetch_prices_vnd
from web_scraper import VietnamStockDataScraper


def main():
    parser = argparse.ArgumentParser(description="Quick scan and export stock data with price_vnd")
    parser.add_argument("--output", help="Output CSV path", default=os.path.expanduser("~/Downloads/quick_scan.csv"))
    parser.add_argument("--add", help="Add one missing ticker (optional)")
    args = parser.parse_args()

    # Allow adding one ticker through env for helpers
    if args.add:
        os.environ["ADD_SYMBOL"] = args.add.strip().upper()

    out_path = args.output
    tickers_df = fetch_all_tickers()
    symbols = sorted(tickers_df["symbol"].unique().tolist())

    scraper = VietnamStockDataScraper()
    df = scraper.scrape_multiple_stocks(symbols)

    # Keep a focused set of columns similar to UI export
    cols = [
        "symbol",
        "price_vnd",
        "pe_ratio",
        "pb_ratio",
        "roe",
        "roa",
        "market_cap",
        "free_float",
        "foreign_ownership",
        "management_ownership",
        "outstanding_shares",
        "avg_trading_value",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA

    # Fetch prices and fill price_vnd
    prices = fetch_prices_vnd(symbols)
    df["price_vnd"] = df["symbol"].map(lambda s: f"{prices.get(s, None):.2f}" if isinstance(prices.get(s), (int, float)) else pd.NA)

    df = df[cols]
    df.to_csv(out_path, index=False)
    print(f"Wrote {len(df)} rows to {out_path}")


if __name__ == "__main__":
    main()





