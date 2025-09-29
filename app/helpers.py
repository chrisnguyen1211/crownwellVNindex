from typing import List
import pandas as pd
from vnstock import Finance, Listing


def fetch_all_tickers(exchanges: List[str] = None) -> pd.DataFrame:
    # VN30 stocks only for faster processing
    vn30_stocks = [
        "ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG",
        "MBB", "MSN", "MWG", "PLX", "POW", "SAB", "SSI", "STB", "TCB", "TPB",
        "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE", "VSH", "VTO"
    ]
    return pd.DataFrame({"symbol": vn30_stocks, "exchange": ["HOSE"]*len(vn30_stocks)})


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


