#!/usr/bin/env python3
"""
Update CSV with latest VND prices for Vietnamese stocks.

Features:
- Reads an input CSV containing a `symbol` column
- Fetches latest price (VND) via VNDirect APIs (robust with fallbacks)
- Adds/updates a `price_vnd` column
- Optionally add a missing ticker to reach the desired count

Usage examples:
  python3 update_price_vnd.py --input "/Users/nguyenhuycuong/Downloads/2025-10-01T05-52_export.csv"
  python3 update_price_vnd.py --input "/path/to/file.csv" --output "/path/to/output.csv" --add VBD

Notes:
- Requires internet access.
- No external dependencies beyond Python stdlib and `requests`.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

try:
    import requests
except Exception as exc:  # pragma: no cover
    print("This script requires the 'requests' package. Install via: pip install requests", file=sys.stderr)
    raise


VNDS_STOCK_PRICES_V4 = "https://finfo-api.vndirect.com.vn/v4/stock_prices/"
VNDS_STOCK_PRICES_QUERY = "q=code:{symbol}&sort=date:desc&size=1"
VNDS_SNAPSHOT = "https://prices.vndirect.com.vn/priceservice/snapshot"


@dataclass
class FetchResult:
    symbol: str
    price_vnd: Optional[float]
    source: str
    error: Optional[str] = None


def _http_get_json(url: str, timeout: float = 6.0) -> Optional[dict]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (compatible; price-updater/1.0)",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception:
        return None


def fetch_price_v4(symbol: str) -> Optional[float]:
    """Fetch latest close price via finfo v4 endpoint (reliable)."""
    url = f"{VNDS_STOCK_PRICES_V4}?{VNDS_STOCK_PRICES_QUERY.format(symbol=symbol)}"
    data = _http_get_json(url)
    if not data:
        return None
    try:
        items = data.get("data") or []
        if not items:
            return None
        item = items[0]
        # Prefer 'close' then 'adClose' then 'matchPrice' depending on availability
        for key in ("close", "adClose", "matchPrice", "last" ):
            if key in item and item[key] is not None:
                return float(item[key])
        return None
    except Exception:
        return None


def fetch_prices_snapshot_batch(symbols: List[str]) -> Dict[str, Optional[float]]:
    """Try VNDirect snapshot batch endpoint. Returns mapping symbol->price or None."""
    if not symbols:
        return {}
    joined = ",".join(symbols)
    url = f"{VNDS_SNAPSHOT}?symbols={joined}"
    data = _http_get_json(url)
    prices: Dict[str, Optional[float]] = {s: None for s in symbols}
    if not data:
        return prices
    try:
        # Response is often a list of objects with fields like 'symbol', 'lastPrice' or 'matchedPrice'
        if isinstance(data, list):
            for obj in data:
                sym = obj.get("symbol") or obj.get("code")
                if not sym:
                    continue
                raw = obj.get("lastPrice") or obj.get("matchedPrice") or obj.get("last")
                try:
                    prices[sym] = float(raw) if raw is not None else None
                except Exception:
                    prices[sym] = None
        elif isinstance(data, dict):
            # some variants return dict with 'data'
            arr = data.get("data") or []
            for obj in arr:
                sym = obj.get("symbol") or obj.get("code")
                raw = obj.get("lastPrice") or obj.get("matchedPrice") or obj.get("last")
                try:
                    prices[sym] = float(raw) if raw is not None else None
                except Exception:
                    prices[sym] = None
    except Exception:
        pass
    return prices


def fetch_price_with_fallback(symbol: str) -> FetchResult:
    # 1) Try finfo v4 (close price)
    price = fetch_price_v4(symbol)
    if price is not None:
        return FetchResult(symbol=symbol, price_vnd=price, source="vndirect_v4")

    # 2) Try snapshot batch (single)
    prices = fetch_prices_snapshot_batch([symbol])
    price2 = prices.get(symbol)
    if price2 is not None:
        return FetchResult(symbol=symbol, price_vnd=price2, source="snapshot")

    return FetchResult(symbol=symbol, price_vnd=None, source="none", error="not_found")


def read_csv_rows(path: str) -> List[dict]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_csv_rows(path: str, rows: List[dict], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def ensure_price_column(fieldnames: List[str]) -> List[str]:
    if "price_vnd" not in fieldnames:
        return fieldnames + ["price_vnd"]
    return fieldnames


def add_missing_symbol_if_needed(rows: List[dict], add_symbol: Optional[str]) -> List[dict]:
    if not add_symbol:
        return rows
    symbols_existing = {r.get("symbol", "").strip().upper() for r in rows}
    symbol_to_add = add_symbol.strip().upper()
    if symbol_to_add in symbols_existing:
        return rows
    # Create an empty row with required fields
    base: dict = {k: "" for k in rows[0].keys()} if rows else {"symbol": symbol_to_add}
    base["symbol"] = symbol_to_add
    return rows + [base]


def process(input_path: str, output_path: Optional[str], add_symbol: Optional[str]) -> str:
    rows = read_csv_rows(input_path)
    if not rows:
        raise SystemExit("Input CSV has no rows.")

    # Add missing ticker if requested
    rows = add_missing_symbol_if_needed(rows, add_symbol)

    # Build fieldnames and ensure price column
    fieldnames = list(rows[0].keys())
    fieldnames = ensure_price_column(fieldnames)

    # Collect symbols in order
    symbols: List[str] = []
    for r in rows:
        sym = (r.get("symbol") or "").strip().upper()
        if sym:
            symbols.append(sym)

    # Fetch prices; use finfo v4 first for each symbol, with brief pacing to avoid rate limits
    symbol_to_price: Dict[str, Optional[float]] = {}
    for idx, sym in enumerate(symbols):
        res = fetch_price_with_fallback(sym)
        symbol_to_price[sym] = res.price_vnd
        # Light sleep to be gentle with API
        time.sleep(0.15)

    # Fill rows
    for r in rows:
        sym = (r.get("symbol") or "").strip().upper()
        price = symbol_to_price.get(sym)
        r["price_vnd"] = f"{price:.2f}" if isinstance(price, (int, float)) else ""

    out_path = output_path or input_path
    write_csv_rows(out_path, rows, fieldnames)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Add latest price_vnd to stock CSV using VNDirect APIs")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--output", required=False, help="Path to output CSV file (optional, defaults to overwrite input)")
    parser.add_argument("--add", required=False, help="Add a missing ticker symbol (e.g., VBD) if not present")
    args = parser.parse_args()

    out = process(args.input, args.output, args.add)
    print(f"Updated CSV written to: {out}")


if __name__ == "__main__":
    main()


