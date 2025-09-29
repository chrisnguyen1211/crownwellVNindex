import os
import pandas as pd
import numpy as np
import streamlit as st
from typing import Dict, List
from datetime import datetime, timezone, timedelta

from helpers import (
    fetch_all_tickers,
    fetch_income_statement,
    fetch_ratios,
    fetch_balance_sheet,
    compute_cagr,
    compute_roe_roa_from_statements,
)
from web_scraper import VietnamStockDataScraper


st.set_page_config(page_title="VN Stock Screener", layout="wide")

st.title("Crownwell VNIndex Screener: Quantitative data")

# Sidebar controls mapped from your criteria table (defaults can be adjusted)
st.sidebar.header("Criteria")
criteria: Dict[str, float] = {}
criteria["min_revenue_cagr_3y"] = st.sidebar.number_input(
    "Min Revenue CAGR (3Y)", min_value=0.0, max_value=1.0, value=0.12, step=0.01
)
criteria["min_profit_cagr_3y"] = st.sidebar.number_input(
    "Min Profit CAGR (3Y)", min_value=0.0, max_value=1.0, value=0.15, step=0.01
)
criteria["min_roe"] = st.sidebar.number_input(
    "Min ROE", min_value=0.0, max_value=1.0, value=0.15, step=0.01
)
criteria["min_roa"] = st.sidebar.number_input(
    "Min ROA", min_value=0.0, max_value=1.0, value=0.05, step=0.01
)
criteria["max_pb"] = st.sidebar.number_input(
    "Max P/B", min_value=0.0, max_value=100.0, value=2.0, step=0.1
)
criteria["max_pe"] = st.sidebar.number_input(
    "Max P/E (optional, 0 disables)", min_value=0.0, max_value=200.0, value=0.0, step=0.5
)

# Additional criteria from your table
st.sidebar.subheader("Additional Criteria")
criteria["max_peg"] = st.sidebar.number_input(
    "Max PEG (optional, 0 disables)", min_value=0.0, max_value=10.0, value=1.5, step=0.1
)
criteria["max_ev_ebitda"] = st.sidebar.number_input(
    "Max EV/EBITDA (optional, 0 disables)", min_value=0.0, max_value=50.0, value=10.0, step=0.5
)
criteria["min_gross_margin"] = st.sidebar.number_input(
    "Min Gross Profit Margin %", min_value=0.0, max_value=100.0, value=0.0, step=1.0
)
criteria["min_free_float"] = st.sidebar.number_input(
    "Min Free Float %", min_value=0.0, max_value=100.0, value=40.0, step=1.0
)
criteria["min_market_cap_billion"] = st.sidebar.number_input(
    "Min Market Cap (billion VND)", min_value=0.0, max_value=100000.0, value=0.0, step=100.0
)
criteria["min_foreign_ownership"] = st.sidebar.number_input(
    "Min Foreign Ownership %", min_value=0.0, max_value=100.0, value=0.0, step=1.0
)
criteria["max_management_ownership"] = st.sidebar.number_input(
    "Max Management Ownership %", min_value=0.0, max_value=100.0, value=100.0, step=1.0
)
criteria["min_avg_trading_value_billion"] = st.sidebar.number_input(
    "Min Avg Trading Value (billion VND/day)", min_value=0.0, max_value=1000.0, value=1.0, step=1.0
)

side_by_side = st.sidebar.checkbox(
    "Show per-criterion tables side-by-side", value=True
)

scan = st.button("Scan now")

# Auto-run once on first load; subsequent presses refresh
if 'has_scanned' not in st.session_state:
    st.session_state['has_scanned'] = True
    scan = True

# Initialize CafeF scraper (single session)
_scraper = VietnamStockDataScraper()

# Clear cached metrics when Scan is pressed
if scan:
    try:
        st.cache_data.clear()
    except Exception:
        pass


@st.cache_data(ttl=3600, show_spinner=False)
def calculate_metrics(symbols: List[str]) -> pd.DataFrame:
    rows = []
    
    for sym in symbols:
        try:
            inc = fetch_income_statement(sym)
            rat = fetch_ratios(sym)
            if inc.empty and rat.empty:
                continue

            # Income metrics
            rev_series = inc.set_index("year")["revenue"] if "revenue" in inc.columns else pd.Series(dtype=float)
            prof_series = (
                inc.set_index("year")["post_tax_profit"]
                if "post_tax_profit" in inc.columns
                else pd.Series(dtype=float)
            )

            rev_cagr = compute_cagr(rev_series)
            prof_cagr = compute_cagr(prof_series)

            if np.isnan(rev_cagr) and "year_revenue_growth" in inc.columns:
                rev_cagr = inc["year_revenue_growth"].tail(3).mean()
            if np.isnan(prof_cagr) and "year_share_holder_income_growth" in inc.columns:
                prof_cagr = inc["year_share_holder_income_growth"].tail(3).mean()

            # Ratios (latest available year)
            latest = rat.sort_values(["year"]).tail(1) if not rat.empty else pd.DataFrame()
            pe = latest["price_to_earning"].iloc[0] if "price_to_earning" in latest.columns and len(latest) else np.nan
            pb = latest["price_to_book"].iloc[0] if "price_to_book" in latest.columns and len(latest) else np.nan
            roe_val = latest["roe"].iloc[0] if "roe" in latest.columns and len(latest) else np.nan
            roa_val = latest["roa"].iloc[0] if "roa" in latest.columns and len(latest) else np.nan
            
            # Use scraped P/E and P/B if available
            scraped_pe = scraped.get('pe_ratio', np.nan)
            scraped_pb = scraped.get('pb_ratio', np.nan)
            if pd.notna(scraped_pe) and scraped_pe > 0:
                pe = scraped_pe
            if pd.notna(scraped_pb) and scraped_pb > 0:
                pb = scraped_pb

            # If ROE/ROA missing, compute from statements
            if pd.isna(roe_val) or pd.isna(roa_val):
                bs = fetch_balance_sheet(sym)
                roe_c, roa_c = compute_roe_roa_from_statements(inc, bs)
                if pd.isna(roe_val):
                    roe_val = roe_c
                if pd.isna(roa_val):
                    roa_val = roa_c

            # Normalize ROE/ROA to fraction if in percents
            if pd.notna(roe_val) and roe_val > 1:
                roe_val = roe_val / 100.0
            if pd.notna(roa_val) and roa_val > 1:
                roa_val = roa_val / 100.0

            # Additional metrics from ratios
            peg = np.nan
            ev_ebitda = np.nan
            gross_margin = np.nan
            
            if not latest.empty:
                # PEG calculation
                if pd.notna(pe) and pd.notna(prof_cagr) and prof_cagr > 0:
                    peg = pe / (prof_cagr * 100)  # Convert CAGR to percentage
                
                # EV/EBITDA
                if "value_before_ebitda" in latest.columns:
                    ev_ebitda = latest["value_before_ebitda"].iloc[0]
                
                # Gross profit margin
                if "gross_profit_margin" in latest.columns:
                    gross_margin = latest["gross_profit_margin"].iloc[0]

            # Calculate additional metrics from available data
            # Market cap estimation using P/E and earnings
            market_cap = np.nan
            price_per_share = np.nan
            
            if not latest.empty and 'price_to_earning' in latest.columns and 'earning_per_share' in latest.columns:
                pe_ratio = latest['price_to_earning'].iloc[0]
                eps = latest['earning_per_share'].iloc[0]
                if pd.notna(pe_ratio) and pd.notna(eps) and pe_ratio > 0 and eps > 0:
                    price_per_share = pe_ratio * eps
                    # Estimate shares from revenue (more realistic)
                    if pd.notna(rev_series.iloc[-1]) and rev_series.iloc[-1] > 0:
                        # Estimate shares based on revenue per share
                        # Revenue is already in billions VND from vnstock API
                        revenue = rev_series.iloc[-1]
                        estimated_shares = revenue / (eps * 0.1)
                        market_cap = (price_per_share * estimated_shares) / 1_000_000_000  # Convert to billion VND
            
            # If market cap still NaN, try alternative method
            if pd.isna(market_cap) and not latest.empty and 'book_value_per_share' in latest.columns and 'price_to_book' in latest.columns:
                book_value = latest['book_value_per_share'].iloc[0]
                pb_ratio = latest['price_to_book'].iloc[0]
                if pd.notna(book_value) and pd.notna(pb_ratio) and pb_ratio > 0:
                    price_per_share = book_value * pb_ratio
                    # Estimate shares from revenue
                    if pd.notna(rev_series.iloc[-1]) and rev_series.iloc[-1] > 0:
                        estimated_shares = rev_series.iloc[-1] / (price_per_share * 0.1)
                        market_cap = (price_per_share * estimated_shares) / 1_000_000_000
            
            # If still no market cap, use simple estimation
            if pd.isna(market_cap) and pd.notna(rev_series.iloc[-1]) and rev_series.iloc[-1] > 0:
                # Simple estimation: market cap = revenue * 2-5x
                revenue_multiple = np.random.uniform(2.0, 5.0)
                # Revenue is already in billions VND from vnstock API
                market_cap = rev_series.iloc[-1] * revenue_multiple
            
            # Initialize valuation vars before any use
            market_val = np.nan
            current_price = np.nan
            shares_outstanding = np.nan
            scraped = {}

            # Prefer real data from CafeF/Vietstock scraper
            try:
                scraped = _scraper.get_stock_overview(sym)
            except Exception:
                scraped = {}

            free_float = scraped.get('free_float', np.nan)
            foreign_ownership = scraped.get('foreign_ownership', np.nan)
            management_ownership = scraped.get('management_ownership', np.nan)
            # Avg trading value (billion VND/day): prefer CafeF, else compute from TCBS 20D
            avg_trading_value = scraped.get('avg_trading_value', np.nan)
            if pd.isna(avg_trading_value):
                try:
                    import requests, time
                    now = int(time.time())
                    start = now - 60*60*24*40
                    url = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars?ticker={sym}&type=stock&resolution=1&from={start}&to={now}"
                    r = requests.get(url, timeout=10)
                    if r.ok:
                        js = r.json()
                        data = js.get('data') if isinstance(js, dict) else None
                        if data and len(data) >= 5:
                            # compute average of last up to 20 days
                            tail = data[-20:]
                            vals = []
                            for bar in tail:
                                c = bar.get('close') or bar.get('c')
                                v = bar.get('volume') or bar.get('v')
                                if isinstance(c, (int, float)) and isinstance(v, (int, float)) and c > 0 and v > 0:
                                    vals.append((c * v) / 1_000_000_000)
                            if vals:
                                avg_trading_value = float(np.mean(vals))
                except Exception:
                    pass
            # Ensure est_val is defined before any checks
            est_val = np.nan

            market_val_source = 'unknown'
            # Prefer CafeF market cap if available; do NOT override when present
            if scraped:
                mc = scraped.get('market_cap')
                # Accept only if within plausible bounds (1B - 10,000,000B) and not placeholder
                if pd.notna(mc) and 1 <= mc <= 10_000_000 and mc != 1000:
                    market_val = mc
                    market_val_source = 'cafef'

            # If CafeF missing, fetch latest price from TCBS and compute price * shares
            if pd.isna(market_val):
                try:
                    import requests, time
                    now = int(time.time())
                    start = now - 60*60*24*14
                    url = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars?ticker={sym}&type=stock&resolution=1&from={start}&to={now}"
                    r = requests.get(url, timeout=10)
                    if r.ok:
                        js = r.json()
                        data = js.get('data') if isinstance(js, dict) else None
                        if data and isinstance(data, list) and len(data) > 0:
                            last = data[-1]
                            cp = last.get('close') or last.get('c')
                            if isinstance(cp, (int, float)) and cp > 0:
                                current_price = cp
                except Exception:
                    pass

            # Use outstanding shares from scraper if available
            if (pd.isna(shares_outstanding) or shares_outstanding <= 0) and scraped:
                os = scraped.get('outstanding_shares')
                if pd.notna(os) and os > 0:
                    shares_outstanding = os

            # Compute shares outstanding using equity and BVPS when available, else revenue/EPS heuristic
            try:
                shares_from_equity = np.nan
                if 'book_value_per_share' in latest.columns and not latest['book_value_per_share'].isna().all():
                    bvps = latest['book_value_per_share'].iloc[0]
                    # Fetch latest equity from balance sheet
                    bs = fetch_balance_sheet(sym)
                    if not bs.empty and 'equity' in bs.columns:
                        eq = bs.sort_values(['year']).tail(1)['equity'].iloc[0]
                        # equity likely in billion VND; convert to VND then divide by BVPS (VND/share)
                        if pd.notna(eq) and pd.notna(bvps) and bvps > 0:
                            shares_from_equity = (eq * 1_000_000_000) / bvps
                if pd.notna(shares_from_equity) and shares_from_equity > 0:
                    shares_outstanding = shares_from_equity
            except Exception:
                pass

            # Try outstanding shares from scraper if available
            if (pd.isna(shares_outstanding) or shares_outstanding <= 0) and scraped:
                os = scraped.get('outstanding_shares')
                if pd.notna(os) and os > 0:
                    shares_outstanding = os

            if (pd.isna(shares_outstanding) or shares_outstanding <= 0) and not latest.empty and 'earning_per_share' in latest.columns and pd.notna(rev_series.iloc[-1]):
                eps = latest['earning_per_share'].iloc[0]
                if pd.notna(eps) and eps > 0:
                    # Heuristic: approximate shares from revenue and an assumed revenue/share ratio (~1e3 VND/share)
                    shares_outstanding = (rev_series.iloc[-1] * 1_000_000_000) / max(eps, 1)

            # Fallback to price * shares if CafeF missing and we have price and shares
            if pd.isna(market_val) and pd.notna(current_price) and pd.notna(shares_outstanding) and current_price > 0 and shares_outstanding > 0:
                market_val = (current_price * shares_outstanding) / 1_000_000_000
                market_val_source = 'price_x_shares'

            # Sanitize percentages - ensure they're in [0,1] range
            for _col in ['free_float', 'foreign_ownership', 'management_ownership']:
                _val = locals().get(_col)
                if pd.notna(_val):
                    if _val > 1:
                        locals()[_col] = 1.0
                    elif _val < 0:
                        locals()[_col] = 0.0

            # Calculate Est Val via simple DCF (fallback to EPS approach if cash flow missing)
            if pd.isna(est_val):
                try:
                    from vnstock import Finance as _Fin
                    cflow = _Fin(symbol=sym, source='TCBS').cash_flow(period='year')
                    if isinstance(cflow, pd.DataFrame) and not cflow.empty:
                        cf = cflow.reset_index().rename(columns={'period':'year'})
                        cf.columns = [str(c).lower() for c in cf.columns]
                        ocf_col = next((c for c in cf.columns if c in ['cash_from_operation','cashflow_from_operating_activities','operating_cash_flow','cash_flows_from_operating_activities','net_cash_from_operating_activities']), None)
                        capex_col = next((c for c in cf.columns if c in ['capex','purchase_of_fixed_assets','net_capex','investment_in_fixed_assets','purchases_of_property_plant_and_equipment']), None)
                        ocf0 = float(pd.to_numeric(cf[ocf_col], errors='coerce').dropna().tail(1).values[0]) if ocf_col and ocf_col in cf.columns and pd.to_numeric(cf[ocf_col], errors='coerce').dropna().size>0 else np.nan
                        capex0 = float(pd.to_numeric(cf[capex_col], errors='coerce').dropna().tail(1).values[0]) if capex_col and capex_col in cf.columns and pd.to_numeric(cf[capex_col], errors='coerce').dropna().size>0 else np.nan
                        if pd.isna(ocf0) and pd.notna(prof_series).any():
                            ocf0 = float(prof_series.dropna().tail(1).values[0])
                        if pd.isna(capex0) and pd.notna(rev_series).any():
                            capex0 = float(rev_series.dropna().tail(1).values[0]) * 0.08
                        if pd.notna(ocf0) and pd.notna(capex0):
                            fcf0 = ocf0 - capex0
                            g_candidates = [x for x in [prof_cagr, rev_cagr] if pd.notna(x) and x > 0]
                            g = min(max(np.mean(g_candidates) if g_candidates else 0.08, 0.02), 0.20)
                            r = 0.12
                            gt = 0.03
                            horizon = 5
                            pv = 0.0
                            fcf_t = fcf0
                            for t in range(1, horizon + 1):
                                fcf_t = fcf_t * (1 + g)
                                pv += fcf_t / ((1 + r) ** t)
                            tv = (fcf_t * (1 + gt)) / (r - gt)
                            pv += tv / ((1 + r) ** horizon)
                            est_val = pv
                except Exception:
                    pass
            if pd.isna(est_val) and not latest.empty and 'earning_per_share' in latest.columns and pd.notna(shares_outstanding):
                eps = latest['earning_per_share'].iloc[0]
                if pd.notna(eps) and eps > 0 and pd.notna(prof_cagr):
                    eps_next = eps * (1 + max(prof_cagr, 0))
                    est_val = (eps_next * shares_outstanding) / 1_000_000_000

            rows.append(
                dict(
                    symbol=sym,
                    revenue_cagr_3y=rev_cagr,
                    profit_cagr_3y=prof_cagr,
                    pe=pe,
                    pb=pb,
                    roe=roe_val,
                    roa=roa_val,
                    peg=peg,
                    ev_ebitda=ev_ebitda,
                    gross_margin=gross_margin,
                    free_float=free_float,
                    foreign_ownership=foreign_ownership,
                    management_ownership=management_ownership,
                    avg_trading_value=avg_trading_value,
                    est_val=est_val,
                    market_val=market_val,
                    market_val_source=market_val_source,
                )
            )
        except Exception as e:
            st.warning(f"Error processing {sym}: {e}")
            continue
    return pd.DataFrame(rows)


def apply_criteria(df: pd.DataFrame, crit: Dict[str, float]) -> pd.DataFrame:
    if df.empty:
        return df
    
    # Basic criteria
    cond = (
        (df["revenue_cagr_3y"].fillna(-1) >= crit["min_revenue_cagr_3y"]) &
        (df["profit_cagr_3y"].fillna(-1) >= crit["min_profit_cagr_3y"]) &
        (df["roe"].fillna(-1) >= crit["min_roe"]) &
        (df["roa"].fillna(-1) >= crit["min_roa"]) &
        (df["pb"].fillna(10**9) <= crit["max_pb"]) 
    )
    
    # Optional criteria
    if crit["max_pe"] > 0:
        cond &= (df["pe"].fillna(10**9) <= crit["max_pe"])
    if crit["max_peg"] > 0:
        cond &= (df["peg"].fillna(10**9) <= crit["max_peg"])
    if crit["max_ev_ebitda"] > 0:
        cond &= (df["ev_ebitda"].fillna(10**9) <= crit["max_ev_ebitda"])
    if crit["min_gross_margin"] > 0:
        cond &= (df["gross_margin"].fillna(-1) >= crit["min_gross_margin"])
    
    # Web scraped criteria
    if crit["min_free_float"] > 0:
        cond &= (df["free_float"].fillna(-1) >= crit["min_free_float"] / 100.0)
    if crit["min_market_cap_billion"] > 0:
        cond &= (df["market_cap"].fillna(-1) >= crit["min_market_cap_billion"])
    if crit["min_foreign_ownership"] > 0:
        cond &= (df["foreign_ownership"].fillna(-1) >= crit["min_foreign_ownership"] / 100.0)
    if crit["max_management_ownership"] < 100:
        cond &= (df["management_ownership"].fillna(101) <= crit["max_management_ownership"] / 100.0)
    if crit["min_avg_trading_value_billion"] > 0:
        cond &= (df["avg_trading_value"].fillna(-1) >= crit["min_avg_trading_value_billion"])
    
    return df[cond].sort_values(by=["profit_cagr_3y","roe"], ascending=False)


if scan:
    with st.spinner("Fetching tickers..."):
        all_tickers = fetch_all_tickers()
        symbols = sorted(all_tickers["symbol"].unique().tolist())

    with st.spinner(f"Downloading data for {len(symbols)} tickers..."):
        metrics = calculate_metrics(symbols)

    # Ensure required columns exist to avoid KeyError on empty/partial data
    required_cols = [
        "symbol",
        "revenue_cagr_3y",
        "profit_cagr_3y",
        "roe",
        "roa",
        "pe",
        "pb",
        "peg",
        "ev_ebitda",
        "gross_margin",
        "free_float",
        "foreign_ownership",
        "management_ownership",
        "avg_trading_value",
        "est_val",
        "market_val",
        "market_val_source",
    ]
    for col in required_cols:
        if col not in metrics.columns:
            metrics[col] = np.nan

    # Last scan time (GMT+7)
    ts = datetime.now(timezone(timedelta(hours=7)))
    st.caption(f"Last scan: {ts.strftime('%Y-%m-%d %H:%M:%S')} GMT+7")

    st.subheader("Raw metrics")
    if not metrics.empty:
        # Format metrics with units for better readability
        display_metrics = metrics.copy()
        
        # Format percentages (multiply by 100 and add %)
        percentage_cols = ['revenue_cagr_3y', 'profit_cagr_3y', 'roe', 'roa', 'free_float', 'foreign_ownership', 'management_ownership']
        for col in percentage_cols:
            if col in display_metrics.columns:
                display_metrics[col] = display_metrics[col].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
        
        # Format trading value (billion VND)
        if 'avg_trading_value' in display_metrics.columns:
            display_metrics['avg_trading_value'] = display_metrics['avg_trading_value'].apply(lambda x: f"{x:.1f}B VND/day" if pd.notna(x) and x > 0 else "N/A")
        
        # Format ratios (add units)
        if 'pe' in display_metrics.columns:
            display_metrics['pe'] = display_metrics['pe'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
        
        if 'pb' in display_metrics.columns:
            display_metrics['pb'] = display_metrics['pb'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
        
        if 'peg' in display_metrics.columns:
            display_metrics['peg'] = display_metrics['peg'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
        
        if 'ev_ebitda' in display_metrics.columns:
            display_metrics['ev_ebitda'] = display_metrics['ev_ebitda'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
        
        if 'gross_margin' in display_metrics.columns:
            display_metrics['gross_margin'] = display_metrics['gross_margin'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
        
        # Format valuation columns
        if 'est_val' in display_metrics.columns:
            display_metrics['est_val'] = display_metrics['est_val'].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")
        
        if 'market_val' in display_metrics.columns:
            display_metrics['market_val'] = display_metrics['market_val'].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")
        if 'market_val_source' in display_metrics.columns:
            display_metrics['market_val_source'] = display_metrics['market_val_source'].fillna('')
        
        raw_column_config = {
            'revenue_cagr_3y': st.column_config.TextColumn(
                'revenue_cagr_3y', help='CAGR doanh thu 3 năm: tính từ báo cáo kết quả kinh doanh.'
            ),
            'profit_cagr_3y': st.column_config.TextColumn(
                'profit_cagr_3y', help='CAGR lợi nhuận sau thuế 3 năm: tính từ báo cáo kết quả kinh doanh.'
            ),
            'peg': st.column_config.TextColumn(
                'peg', help='PEG = P/E ÷ Profit CAGR.'
            ),
            'est_val': st.column_config.TextColumn(
                'est_val', help='Est Val (DCF 5Y): FCF=OCF−Capex; r=12%, g_terminal=3%; fallback EPS_next×Shares.'
            ),
            'market_val': st.column_config.TextColumn(
                'market_val', help='Market Val: CafeF "Vốn hóa thị trường (tỷ đồng)"; fallback Price×Shares.'
            ),
        }
        st.dataframe(display_metrics, column_config=raw_column_config)
    else:
        st.dataframe(pd.DataFrame({"note":["No data fetched. Try again or adjust universe."]}))

    # Per-criterion tables
    st.subheader("Per-criterion breakdown")
    if side_by_side:
        c1, c2, c3, c4 = st.columns(4)
    else:
        c1, c2, c3, c4 = st.container(), st.container(), st.container(), st.container()

    with c1:
        st.markdown("**Growth**")
        if metrics.empty:
            st.dataframe(pd.DataFrame())
        else:
            t_growth = metrics[(metrics["revenue_cagr_3y"].fillna(-1) >= criteria["min_revenue_cagr_3y"]) & (metrics["profit_cagr_3y"].fillna(-1) >= criteria["min_profit_cagr_3y"])][["symbol","revenue_cagr_3y","profit_cagr_3y"]].copy()
            if not t_growth.empty:
                t_growth["revenue_cagr_3y"] = t_growth["revenue_cagr_3y"].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
                t_growth["profit_cagr_3y"] = t_growth["profit_cagr_3y"].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
                t_growth.columns = ["Symbol", "Revenue CAGR (3Y)", "Profit CAGR (3Y)"]
            st.dataframe(t_growth)
    
    with c2:
        st.markdown("**Profitability**")
        if metrics.empty:
            st.dataframe(pd.DataFrame())
        else:
            t_prof = metrics[(metrics["roe"].fillna(-1) >= criteria["min_roe"]) & (metrics["roa"].fillna(-1) >= criteria["min_roa"])][["symbol","roe","roa"]].copy()
            if not t_prof.empty:
                t_prof["roe"] = t_prof["roe"].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
                t_prof["roa"] = t_prof["roa"].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
                t_prof.columns = ["Symbol", "ROE", "ROA"]
            st.dataframe(t_prof)
    
    with c3:
        st.markdown("**Valuation**")
        if metrics.empty:
            st.dataframe(pd.DataFrame())
        else:
            t_val = metrics[(metrics["pb"].fillna(10**9) <= criteria["max_pb"])][["symbol","pe","pb","peg"]].copy()
            if criteria["max_pe"] > 0:
                t_val = t_val[t_val["pe"].fillna(10**9) <= criteria["max_pe"]]
            if criteria["max_peg"] > 0:
                t_val = t_val[t_val["peg"].fillna(10**9) <= criteria["max_peg"]]
            if not t_val.empty:
                t_val["pe"] = t_val["pe"].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
                t_val["pb"] = t_val["pb"].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
                t_val["peg"] = t_val["peg"].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
                t_val.columns = ["Symbol", "P/E", "P/B", "PEG"]
            st.dataframe(t_val)
    
    with c4:
        st.markdown("**Additional**")
        if metrics.empty:
            st.dataframe(pd.DataFrame())
        else:
            t_add = metrics[["symbol","ev_ebitda","gross_margin","free_float","est_val","market_val","market_val_source"]].copy()
            if criteria["max_ev_ebitda"] > 0:
                t_add = t_add[t_add["ev_ebitda"].fillna(10**9) <= criteria["max_ev_ebitda"]]
            if criteria["min_gross_margin"] > 0:
                t_add = t_add[t_add["gross_margin"].fillna(-1) >= criteria["min_gross_margin"]]
            if criteria["min_free_float"] > 0:
                t_add = t_add[t_add["free_float"].fillna(-1) >= criteria["min_free_float"] / 100.0]
            if not t_add.empty:
                t_add["ev_ebitda"] = t_add["ev_ebitda"].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
                t_add["gross_margin"] = t_add["gross_margin"].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
                t_add["free_float"] = t_add["free_float"].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
                t_add["est_val"] = t_add["est_val"].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")
                t_add["market_val"] = t_add["market_val"].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")
                t_add.columns = ["Symbol", "EV/EBITDA", "Gross Margin", "Free Float", "Est Val", "Market Val", "Market Val Source"]
            add_column_config = {
                'Est Val': st.column_config.TextColumn(
                    'Est Val', help='Est Val (DCF 5Y): FCF=OCF−Capex; r=12%, g_terminal=3%; fallback EPS_next×Shares.'
                ),
                'Market Val': st.column_config.TextColumn(
                    'Market Val', help='CafeF "Vốn hóa thị trường (tỷ đồng)"; fallback Price×Shares.'
                ),
            }
            st.dataframe(t_add, column_config=add_column_config)

    # Final pass
    st.subheader("Final pass list")
    passed = apply_criteria(metrics, criteria) if not metrics.empty else pd.DataFrame()
    
    if not passed.empty:
        # Format final pass list with units
        display_passed = passed.copy()
        
        # Format percentages
        percentage_cols = ['revenue_cagr_3y', 'profit_cagr_3y', 'roe', 'roa', 'free_float', 'foreign_ownership', 'management_ownership']
        for col in percentage_cols:
            if col in display_passed.columns:
                display_passed[col] = display_passed[col].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
        
        # Format ratios
        ratio_cols = ['pe', 'pb', 'peg', 'ev_ebitda']
        for col in ratio_cols:
            if col in display_passed.columns:
                display_passed[col] = display_passed[col].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
        
        # Format trading value
        if 'avg_trading_value' in display_passed.columns:
            display_passed['avg_trading_value'] = display_passed['avg_trading_value'].apply(lambda x: f"{x:.1f}B VND/day" if pd.notna(x) else "N/A")
        
        if 'gross_margin' in display_passed.columns:
            display_passed['gross_margin'] = display_passed['gross_margin'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
        
        # Format valuation columns
        if 'est_val' in display_passed.columns:
            display_passed['est_val'] = display_passed['est_val'].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")
        
        if 'market_val' in display_passed.columns:
            display_passed['market_val'] = display_passed['market_val'].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")
        
        # Rename columns for better display
        column_mapping = {
            'symbol': 'Symbol',
            'revenue_cagr_3y': 'Revenue CAGR (3Y)',
            'profit_cagr_3y': 'Profit CAGR (3Y)',
            'pe': 'P/E',
            'pb': 'P/B',
            'roe': 'ROE',
            'roa': 'ROA',
            'peg': 'PEG',
            'ev_ebitda': 'EV/EBITDA',
            'gross_margin': 'Gross Margin',
            'free_float': 'Free Float',
            'foreign_ownership': 'Foreign Ownership',
            'management_ownership': 'Management Ownership',
            'avg_trading_value': 'Avg Trading Value',
            'est_val': 'Est Val',
            'market_val': 'Market Val',
            'market_val_source': 'Market Val Source'
        }
        
        display_passed = display_passed.rename(columns=column_mapping)
        fp_column_config = {
            'Est Val': st.column_config.TextColumn(
                'Est Val', help='Est Val (DCF 5Y): FCF=OCF−Capex; r=12%, g_terminal=3%; fallback EPS_next×Shares.'
            ),
            'Market Val': st.column_config.TextColumn(
                'Market Val', help='CafeF "Vốn hóa thị trường (tỷ đồng)"; fallback Price×Shares.'
            ),
            'PEG': st.column_config.TextColumn(
                'PEG', help='PEG = P/E ÷ Profit CAGR.'
            ),
            'Revenue CAGR (3Y)': st.column_config.TextColumn(
                'Revenue CAGR (3Y)', help='CAGR doanh thu 3 năm: tính từ báo cáo kết quả kinh doanh.'
            ),
            'Profit CAGR (3Y)': st.column_config.TextColumn(
                'Profit CAGR (3Y)', help='CAGR lợi nhuận sau thuế 3 năm: tính từ báo cáo kết quả kinh doanh.'
            ),
        }
        st.dataframe(display_passed, column_config=fp_column_config)
    else:
        st.dataframe(passed)

    st.download_button(
        label="Download CSV",
        data=passed.to_csv(index=False),
        file_name="vn_screener_pass.csv",
        mime="text/csv",
    )

    st.success(f"Completed. {len(passed)} symbols matched.")


