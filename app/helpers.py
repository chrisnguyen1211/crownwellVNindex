from typing import List, Dict, Optional
import os
import time
import pandas as pd
from vnstock import Finance, Listing
import requests
from pathlib import Path
from bs4 import BeautifulSoup


def fetch_all_tickers(exchanges: List[str] = None) -> pd.DataFrame:
    """Fetch all VN tickers across exchanges with robust fallbacks.
    Order:
      1) StockAnalysis HOSE/HNX + HNX UPCOM web pages (scraped)
      2) VNDirect v4 stocks API (exchange + symbol)
      3) vnstock Listing API
      4) Cache
    """
    # A) Prefer persisted file if present
    try:
        file_df = _ticker_file_load()
        if isinstance(file_df, pd.DataFrame) and not file_df.empty:
            if exchanges:
                file_df = file_df[file_df['exchange'].isin([e.upper() for e in exchanges])]
            if not file_df.empty:
                return file_df[["symbol","exchange"]]
    except Exception:
        pass
    # 0) Try user-provided listing pages first
    try:
        frames: List[pd.DataFrame] = []
        hose = _scrape_stockanalysis_list(
            url="https://stockanalysis.com/list/ho-chi-minh-stock-exchange/",
            exchange_code="HOSE",
        )
        if not hose.empty:
            frames.append(hose)
        hnx = _scrape_stockanalysis_list(
            url="https://stockanalysis.com/list/hanoi-stock-exchange/",
            exchange_code="HNX",
        )
        if not hnx.empty:
            frames.append(hnx)
        upcom = _scrape_hnx_upcom_list(
            url="https://hnx.vn/en-gb/cophieu-etfs/chung-khoan-uc.html",
            exchange_code="UPCOM",
        )
        if not upcom.empty:
            frames.append(upcom)
        if frames:
            df0 = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["symbol"]) 
            if exchanges:
                df0 = df0[df0['exchange'].isin([e.upper() for e in exchanges])]
            _cache_save_tickers(df0[["symbol","exchange"]])
            _ticker_file_save(df0[["symbol","exchange"]], source="web")
            return df0[["symbol","exchange"]]
    except Exception:
        _log_listing_err("Exception during web-scraped listings")
    # 1) VNDirect v4: try full list first, then per floor if needed (with retries)
    try:
        # Attempt single-shot (some deployments allow large size)
        rows = []
        full_url = "https://finfo-api.vndirect.com.vn/v4/stocks?size=5000"
        for attempt in range(3):
            js_all = _http_get_json(full_url, timeout=12.0)
            data_all = js_all.get('data') if isinstance(js_all, dict) else None
            if isinstance(data_all, list) and data_all:
                for item in data_all:
                    code = (item.get('code') or item.get('symbol') or "").strip().upper()
                    ex = (item.get('floor') or item.get('exchange') or "").strip().upper()
                    if not code:
                        continue
                    rows.append({"symbol": code, "exchange": ex})
                break
            else:
                _log_listing_err(f"VNDirect full listing attempt {attempt+1} returned 0 items")
                time.sleep(0.7 * (attempt + 1))
        # If still small, query per floor
        if len(rows) < 100:
            floors = ["HOSE", "HNX", "UPCOM"]
            for floor in floors:
                url = f"https://finfo-api.vndirect.com.vn/v4/stocks?q=floor:{floor}%20AND%20type:STOCK&size=2000"
                got = False
                for attempt in range(3):
                    js = _http_get_json(url, timeout=12.0)
                    data = js.get('data') if isinstance(js, dict) else None
                    if isinstance(data, list) and data:
                        for item in data:
                            code = (item.get('code') or item.get('symbol') or "").strip().upper()
                            ex = (item.get('floor') or item.get('exchange') or floor).strip().upper()
                            if not code:
                                continue
                            rows.append({"symbol": code, "exchange": ex})
                        got = True
                        break
                    else:
                        _log_listing_err(f"VNDirect floor {floor} attempt {attempt+1} returned 0 items")
                        time.sleep(0.7 * (attempt + 1))
                if not got:
                    _log_listing_err(f"VNDirect floor {floor} failed after retries")
        if rows:
            df = pd.DataFrame(rows).drop_duplicates(subset=['symbol'])
            if exchanges:
                df = df[df['exchange'].isin([e.upper() for e in exchanges])]
            df = df[['symbol', 'exchange']]
            _cache_save_tickers(df)
            _ticker_file_save(df, source="vndirect")
            return df
    except Exception:
        _log_listing_err("Exception during VNDirect listing fetch")

    # 2) vnstock Listing API
    try:
        from vnstock import Listing as _Listing
        lst = _Listing()
        df = lst.all_symbols()
        if isinstance(df, pd.DataFrame) and not df.empty and 'symbol' in df.columns:
            df['symbol'] = df['symbol'].astype(str).str.upper()
            if 'exchange' in df.columns:
                df['exchange'] = df['exchange'].astype(str).str.upper()
            df = df.dropna(subset=['symbol']).drop_duplicates(subset=['symbol'])
            if exchanges and 'exchange' in df.columns:
                df = df[df['exchange'].isin([e.upper() for e in exchanges])]
            keep = ['symbol'] + ([c for c in ['exchange', 'organ_name'] if c in df.columns])
            out = df[keep]
            _cache_save_tickers(out)
            _ticker_file_save(out, source="vnstock")
            return out
    except Exception:
        _log_listing_err("Exception during vnstock Listing fetch")

    # Fallback to last cached full list if available
    cached = _cache_load_tickers()
    if isinstance(cached, pd.DataFrame) and not cached.empty:
        cached["_from_cache"] = True
        _ticker_file_save(cached[["symbol","exchange"]], source="cache")
        return cached
    # No cache available
    return pd.DataFrame(columns=["symbol","exchange"])


# ---------------- Cache helpers for tickers ----------------
def _cache_path() -> Path:
    base = Path(__file__).resolve().parent
    return base / "symbols_cache.csv"


def _cache_save_tickers(df: pd.DataFrame) -> None:
    try:
        path = _cache_path()
        to_save = df.copy()
        cols = [c for c in ["symbol", "exchange"] if c in to_save.columns]
        if cols:
            to_save[cols].drop_duplicates(subset=["symbol"]).to_csv(path, index=False)
    except Exception:
        pass


def _cache_load_tickers() -> pd.DataFrame:
    try:
        path = _cache_path()
        if path.exists():
            df = pd.read_csv(path)
            if 'symbol' in df.columns:
                df['symbol'] = df['symbol'].astype(str).str.upper()
            if 'exchange' in df.columns:
                df['exchange'] = df['exchange'].astype(str).str.upper()
            return df
    except Exception:
        pass
    return pd.DataFrame()


# ---------------- Ticker file (persisted between runs) ----------------
def _ticker_file_path() -> Path:
    base = Path(__file__).resolve().parent
    return base / "tickers.csv"


def _ticker_file_save(df: pd.DataFrame, source: str = "") -> None:
    try:
        path = _ticker_file_path()
        out = df.copy()
        out = out[["symbol","exchange"]].drop_duplicates(subset=["symbol"]).sort_values(["exchange","symbol"])  # group by exchange visually
        out.to_csv(path, index=False)
    except Exception:
        pass


def _ticker_file_load() -> pd.DataFrame:
    try:
        path = _ticker_file_path()
        if path.exists():
            df = pd.read_csv(path)
            if 'symbol' in df.columns:
                df['symbol'] = df['symbol'].astype(str).str.upper()
            if 'exchange' in df.columns:
                df['exchange'] = df['exchange'].astype(str).str.upper()
            return df[["symbol","exchange"]]
    except Exception:
        pass
    return pd.DataFrame()


# --------------- Diagnostics for listing fetch ---------------
_LAST_LISTING_ERRORS: List[str] = []


def _log_listing_err(message: str) -> None:
    try:
        if len(_LAST_LISTING_ERRORS) > 30:
            _LAST_LISTING_ERRORS[:] = _LAST_LISTING_ERRORS[-30:]
        _LAST_LISTING_ERRORS.append(message)
    except Exception:
        pass


def get_last_listing_errors() -> List[str]:
    return list(_LAST_LISTING_ERRORS)


def fetch_income_statement(symbol: str) -> pd.DataFrame:
    try:
        df = Finance(symbol=symbol, source='TCBS').income_statement(period="year")
        df = df.reset_index().rename(columns={"period": "year"})
        df["symbol"] = symbol
        return df
    except Exception:
        return pd.DataFrame()


def fetch_ratios(symbol: str) -> pd.DataFrame:
    try:
        df = Finance(symbol=symbol, source='TCBS').ratio(period="year")
        df = df.reset_index().rename(columns={"period": "year"})
        df["symbol"] = symbol
        return df
    except Exception:
        return pd.DataFrame()


def fetch_cash_flow(symbol: str) -> pd.DataFrame:
    try:
        df = Finance(symbol=symbol, source='TCBS').cash_flow(period="year")
        df = df.reset_index().rename(columns={"period": "year"})
        df["symbol"] = symbol
        return df
    except Exception:
        return pd.DataFrame()


def extract_additional_metrics(ratios_df: pd.DataFrame, cash_flow_df: pd.DataFrame) -> dict:
    """Extract additional metrics from vnstock API data"""
    metrics = {}
    
    if not ratios_df.empty:
        latest = ratios_df.sort_values(["year"]).tail(1)
        
        # EV/EBITDA
        if "value_before_ebitda" in latest.columns and len(latest):
            metrics["ev_ebitda"] = latest["value_before_ebitda"].iloc[0]
        
        # Gross Margin
        if "gross_profit_margin" in latest.columns and len(latest):
            metrics["gross_margin"] = latest["gross_profit_margin"].iloc[0]
        
        # Operating Margin
        if "operating_profit_margin" in latest.columns and len(latest):
            metrics["operating_margin"] = latest["operating_profit_margin"].iloc[0]
        
        # Debt Ratios
        if "debt_on_equity" in latest.columns and len(latest):
            metrics["debt_to_equity"] = latest["debt_on_equity"].iloc[0]
        
        if "debt_on_asset" in latest.columns and len(latest):
            metrics["debt_to_asset"] = latest["debt_on_asset"].iloc[0]
        
        # Liquidity Ratios
        if "current_payment" in latest.columns and len(latest):
            metrics["current_ratio"] = latest["current_payment"].iloc[0]
        
        if "quick_payment" in latest.columns and len(latest):
            metrics["quick_ratio"] = latest["quick_payment"].iloc[0]
        
        # EPS and Book Value
        if "earning_per_share" in latest.columns and len(latest):
            metrics["eps"] = latest["earning_per_share"].iloc[0]
        
        if "book_value_per_share" in latest.columns and len(latest):
            metrics["book_value_per_share"] = latest["book_value_per_share"].iloc[0]
        
        # Dividend yield
        if "dividend" in latest.columns and len(latest):
            metrics["dividend_yield"] = latest["dividend"].iloc[0]
    
    if not cash_flow_df.empty:
        latest_cf = cash_flow_df.sort_values(["year"]).tail(1)
        
        # Free Cash Flow
        if "free_cash_flow" in latest_cf.columns and len(latest_cf):
            metrics["free_cash_flow"] = latest_cf["free_cash_flow"].iloc[0]
        
        # Operating Cash Flow
        if "from_sale" in latest_cf.columns and len(latest_cf):
            metrics["operating_cash_flow"] = latest_cf["from_sale"].iloc[0]
    
    return metrics


def compute_cagr(series: pd.Series, years: int = 3) -> float:
    s = series.dropna().sort_index()
    if len(s) < years + 1:
        return float("nan")
    start = s.iloc[-(years + 1)]
    end = s.iloc[-1]
    if start <= 0:
        return float("nan")
    return (end / start) ** (1 / years) - 1


def fetch_balance_sheet(symbol: str) -> pd.DataFrame:
    try:
        df = Finance(symbol=symbol, source='TCBS').balance_sheet(period="year")
        df = df.reset_index().rename(columns={"period": "year"})
        df["symbol"] = symbol
        return df
    except Exception:
        return pd.DataFrame()


def compute_roe_roa_from_statements(income_df: pd.DataFrame, bs_df: pd.DataFrame) -> tuple[float, float]:
    if income_df.empty or bs_df.empty:
        return float("nan"), float("nan")
    inc = income_df.copy()
    bs = bs_df.copy()
    # normalize
    inc.columns = [c.lower() for c in inc.columns]
    bs.columns = [c.lower() for c in bs.columns]
    # pick columns
    year_col = "year" if "year" in inc.columns else ("period" if "period" in inc.columns else None)
    if year_col is None or year_col not in bs.columns:
        return float("nan"), float("nan")
    net_income_col = "post_tax_profit" if "post_tax_profit" in inc.columns else ("share_holder_income" if "share_holder_income" in inc.columns else None)
    equity_col = "equity" if "equity" in bs.columns else ("owner_equity" if "owner_equity" in bs.columns else None)
    assets_col = "total_assets" if "total_assets" in bs.columns else ("assets" if "assets" in bs.columns else None)
    if not all([net_income_col, equity_col, assets_col]):
        return float("nan"), float("nan")
    # join latest two years to compute average equity/assets
    bs_sorted = bs.sort_values(by=year_col)
    # use last two periods average; if only one, use that
    last = bs_sorted.tail(2)
    avg_equity = last[equity_col].mean()
    avg_assets = last[assets_col].mean()
    latest_year = inc[year_col].max()
    latest_income = inc.loc[inc[year_col] == latest_year, net_income_col].values
    if len(latest_income) == 0:
        latest_income = [float("nan")]
    ni = float(latest_income[0])
    roe = ni / avg_equity if avg_equity and avg_equity != 0 else float("nan")
    roa = ni / avg_assets if avg_assets and avg_assets != 0 else float("nan")
    return roe, roa



# ================= VNDirect price helpers =================
_VNDS_STOCK_PRICES_V4 = "https://finfo-api.vndirect.com.vn/v4/stock_prices/"
_VNDS_STOCK_PRICES_QUERY = "q=code:{symbol}&sort=date:desc&size=1"
_VNDS_SNAPSHOT = "https://prices.vndirect.com.vn/priceservice/snapshot"
_TCBS_BARS = "https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars"


def _normalize_price_vnd(value: Optional[float]) -> Optional[float]:
    """Normalize price to VND. Some VNDirect endpoints return prices in thousand VND.
    Heuristic: if 0 < price < 1000 → treat as thousand-VND and multiply by 1000.
    """
    if not isinstance(value, (int, float)):
        return None
    price = float(value)
    if price <= 0:
        return None
    if price < 1000:
        return price * 1000.0
    return price


def _http_get_json(url: str, timeout: float = 6.0) -> Optional[dict]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (compatible; crownwell-scan/1.0)",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception:
        return None


def _http_get_text(url: str, timeout: float = 8.0) -> Optional[str]:
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (compatible; crownwell-scan/1.0)",
    }
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code == 200 and isinstance(r.text, str):
            return r.text
        return None
    except Exception:
        return None


def _scrape_stockanalysis_list(url: str, exchange_code: str) -> pd.DataFrame:
    """Scrape symbol list from StockAnalysis exchange pages.
    Example: https://stockanalysis.com/list/ho-chi-minh-stock-exchange/
    """
    html = _http_get_text(url)
    if not html:
        _log_listing_err(f"StockAnalysis fetch failed: {exchange_code}")
        return pd.DataFrame()
    try:
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        if not table:
            _log_listing_err(f"StockAnalysis table not found: {exchange_code}")
            return pd.DataFrame()
        symbols: List[str] = []
        for a in table.find_all('a'):
            txt = (a.get_text() or '').strip().upper()
            # crude filter: ticker links tend to be short uppercase strings
            if 1 <= len(txt) <= 6 and txt.isalnum():
                symbols.append(txt)
        symbols = sorted(set(symbols))
        if not symbols:
            _log_listing_err(f"StockAnalysis no symbols parsed: {exchange_code}")
            return pd.DataFrame()
        return pd.DataFrame({"symbol": symbols, "exchange": [exchange_code]*len(symbols)})
    except Exception:
        _log_listing_err(f"StockAnalysis parse error: {exchange_code}")
        return pd.DataFrame()


def _scrape_hnx_upcom_list(url: str, exchange_code: str) -> pd.DataFrame:
    """Scrape UPCOM symbol list from HNX page (English UPCOM list).
    Example: https://hnx.vn/en-gb/cophieu-etfs/chung-khoan-uc.html
    """
    html = _http_get_text(url)
    if not html:
        _log_listing_err("HNX UPCOM fetch failed")
        return pd.DataFrame()
    try:
        soup = BeautifulSoup(html, 'html.parser')
        symbols: List[str] = []
        # UPCOM page has tables; collect uppercase short codes from cells/links
        for tag in soup.find_all(['a','td']):
            txt = (tag.get_text() or '').strip().upper()
            if 1 <= len(txt) <= 6 and txt.isalnum():
                symbols.append(txt)
        symbols = sorted(set(symbols))
        if not symbols:
            _log_listing_err("HNX UPCOM no symbols parsed")
            return pd.DataFrame()
        return pd.DataFrame({"symbol": symbols, "exchange": [exchange_code]*len(symbols)})
    except Exception:
        _log_listing_err("HNX UPCOM parse error")
        return pd.DataFrame()


def fetch_price_vnd(symbol: str) -> Optional[float]:
    """Fetch latest price in VND for a single symbol using VNDirect with fallback."""
    # Try v4 close
    url = f"{_VNDS_STOCK_PRICES_V4}?{_VNDS_STOCK_PRICES_QUERY.format(symbol=symbol)}"
    data = _http_get_json(url)
    if data and isinstance(data, dict):
        items = data.get("data") or []
        if items:
            item = items[0]
            for key in ("close", "adClose", "matchPrice", "last"):
                val = item.get(key)
                if val is not None:
                    try:
                        price = _normalize_price_vnd(float(val))
                        if price and price >= 1000:
                            return price
                        # Fall through to other sources when suspicious or None
                        break
                    except Exception:
                        pass
    # Fallback snapshot
    url2 = f"{_VNDS_SNAPSHOT}?symbols={symbol}"
    snap = _http_get_json(url2)
    if isinstance(snap, list):
        for obj in snap:
            if (obj.get("symbol") or obj.get("code")) == symbol:
                for k in ("lastPrice", "matchedPrice", "last"):
                    v = obj.get(k)
                    if v is not None:
                        try:
                            price = _normalize_price_vnd(float(v))
                            if price and price >= 1000:
                                return price
                            break
                        except Exception:
                            pass
    elif isinstance(snap, dict):
        arr = snap.get("data") or []
        for obj in arr:
            if (obj.get("symbol") or obj.get("code")) == symbol:
                for k in ("lastPrice", "matchedPrice", "last"):
                    v = obj.get(k)
                    if v is not None:
                        try:
                            price = _normalize_price_vnd(float(v))
                            if price and price >= 1000:
                                return price
                            break
                        except Exception:
                            pass
    # Final fallback: TCBS latest close
    try:
        import time as _t
        now = int(_t.time())
        start = now - 60*60*24*14
        url3 = f"{_TCBS_BARS}?ticker={symbol}&type=stock&resolution=1&from={start}&to={now}"
        r = requests.get(url3, timeout=8)
        if r.ok:
            js = r.json()
            data = js.get('data') if isinstance(js, dict) else None
            if data and isinstance(data, list) and len(data) > 0:
                last = data[-1]
                cp = last.get('close') or last.get('c')
                if isinstance(cp, (int, float)):
                    norm = _normalize_price_vnd(float(cp))
                    if norm and norm >= 1000:
                        return norm
    except Exception:
        pass
    return None


def fetch_prices_vnd(symbols: List[str]) -> Dict[str, Optional[float]]:
    """Batch fetch prices; uses per-symbol fallback internally if needed."""
    if not symbols:
        return {}
    result: Dict[str, Optional[float]] = {s: None for s in symbols}
    # First try snapshot batch
    joined = ",".join(symbols)
    snap = _http_get_json(f"{_VNDS_SNAPSHOT}?symbols={joined}")
    if isinstance(snap, list):
        for obj in snap:
            sym = (obj.get("symbol") or obj.get("code") or "").upper()
            if not sym:
                continue
            raw = obj.get("lastPrice") or obj.get("matchedPrice") or obj.get("last")
            try:
                result[sym] = _normalize_price_vnd(float(raw)) if raw is not None else None
            except Exception:
                result[sym] = None
    elif isinstance(snap, dict):
        arr = snap.get("data") or []
        for obj in arr:
            sym = (obj.get("symbol") or obj.get("code") or "").upper()
            raw = obj.get("lastPrice") or obj.get("matchedPrice") or obj.get("last")
            try:
                result[sym] = _normalize_price_vnd(float(raw)) if raw is not None else None
            except Exception:
                result[sym] = None
    # Fill missing using v4 one-by-one
    for s in symbols:
        val = result.get(s)
        if not isinstance(val, (int, float)) or val < 1000:
            result[s] = fetch_price_vnd(s)
            time.sleep(0.12)
    return result

