import os
import pandas as pd
import numpy as np
import streamlit as st
import logging
from typing import Dict, List
from datetime import datetime, timezone, timedelta
import time

# Setup logging
logger = logging.getLogger(__name__)

from helpers import fetch_all_tickers, fetch_income_statement, fetch_ratios, fetch_cash_flow, fetch_balance_sheet, compute_cagr, extract_additional_metrics
from web_scraper import VietnamStockDataScraper

# Custom CSS
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        text-align: center;
        color: white;
    }
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
        border-left: 4px solid #667eea;
    }
    .stButton > button {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 5px;
        padding: 0.5rem 1rem;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="main-header">
    <h1>🚀 Vietnamese Stock Screener</h1>
    <p>Professional stock analysis and screening tool</p>
</div>
""", unsafe_allow_html=True)

# Initialize session state
if 'force_scan' not in st.session_state:
    st.session_state['force_scan'] = False

# Get tickers
tickers_df = fetch_all_tickers()
symbols = tickers_df['symbol'].tolist()

# Scan button
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    if st.button("🔍 Scan Stocks", key="scan_button", width='stretch'):
        st.session_state['force_scan'] = True
        st.cache_data.clear()

# Main content
if st.session_state.get('force_scan', False):
    with st.status("Downloading data", expanded=True) as status:
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def update_progress(current, total, symbol):
            progress_bar.progress(current / total)
            status_text.text(f"Processing {symbol} ({current}/{total})")
        
        # Create live log container
        log_container = st.empty()
        log_messages = []
        
        def add_log(message):
            log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
            log_container.text_area("Live Log", "\n".join(log_messages[-10:]), height=200, key=f"live_log_{len(log_messages)}")
        
        # Show progress for each symbol
        for i, symbol in enumerate(symbols):
            update_progress(i, len(symbols), symbol)
            add_log(f"🔍 Processing {symbol}...")
        
        # Call the actual function with live logging
        add_log("📊 Starting data calculation...")
        
        # Real data calculation with timeout protection
        rows = []
        _scraper = VietnamStockDataScraper()
        
        for i, sym in enumerate(symbols):
            try:
                add_log(f"📈 Fetching data for {sym}...")
                
                # Fetch financial data with timeout protection
                inc = pd.DataFrame()
                rat = pd.DataFrame()
                cf = pd.DataFrame()
                bs = pd.DataFrame()
                
                try:
                    # Try to fetch data with 15 second timeout per call
                    import threading
                    
                    result = {'inc': pd.DataFrame(), 'rat': pd.DataFrame(), 'cf': pd.DataFrame(), 'bs': pd.DataFrame()}
                    
                    def fetch_data():
                        try:
                            result['inc'] = fetch_income_statement(sym)
                            result['rat'] = fetch_ratios(sym)
                            result['cf'] = fetch_cash_flow(sym)
                            result['bs'] = fetch_balance_sheet(sym)
                        except Exception as e:
                            logger.warning(f"API error for {sym}: {e}")
                    
                    thread = threading.Thread(target=fetch_data)
                    thread.daemon = True
                    thread.start()
                    thread.join(timeout=15)
                    
                    if thread.is_alive():
                        add_log(f"⚠️ API timeout for {sym}, using fallback data")
                        inc = pd.DataFrame()
                        rat = pd.DataFrame()
                        cf = pd.DataFrame()
                        bs = pd.DataFrame()
                    else:
                        inc = result['inc']
                        rat = result['rat']
                        cf = result['cf']
                        bs = result['bs']
                    
                except Exception as e:
                    add_log(f"⚠️ API error for {sym}: {e}")
                
                # Scraping without timeout (let it run)
                scraped = {}
                try:
                    add_log(f"🌐 Scraping data for {sym}...")
                    scraped = _scraper.get_stock_overview(sym) or {}
                    add_log(f"✅ Scraping completed for {sym}")
                except Exception as e:
                    add_log(f"⚠️ Scraping error for {sym}: {e}")
                    scraped = {}
                
                # Calculate metrics - only use real data, no mock fallback
                if inc.empty and rat.empty:
                    add_log(f"⚠️ No data for {sym}, skipping")
                    continue
                
                # Real data calculation
                add_log(f"📊 Calculating metrics for {sym}...")
                
                # Income metrics
                rev_series = inc.set_index("year")["revenue"] if "revenue" in inc.columns else pd.Series(dtype=float)
                prof_series = inc.set_index("year")["post_tax_profit"] if "post_tax_profit" in inc.columns else pd.Series(dtype=float)
                
                rev_cagr = compute_cagr(rev_series)
                prof_cagr = compute_cagr(prof_series)
                
                # Ratios
                latest = rat.sort_values(["year"]).tail(1) if not rat.empty else pd.DataFrame()
                
                pe = latest["pe"].iloc[0] if not latest.empty and "pe" in latest.columns else np.nan
                pb = latest["pb"].iloc[0] if not latest.empty and "pb" in latest.columns else np.nan
                roe = latest["roe"].iloc[0] if not latest.empty and "roe" in latest.columns else np.nan
                roa = latest["roa"].iloc[0] if not latest.empty and "roa" in latest.columns else np.nan
                
                # Additional metrics
                additional_metrics = extract_additional_metrics(rat, cf)
                
                # Market data from scraping
                market_val = scraped.get('market_cap', np.nan)
                current_price = scraped.get('current_price', np.nan)
                shares_outstanding = scraped.get('outstanding_shares', np.nan)
                
                # Use scraped ratios if available
                if pd.notna(scraped.get('pe_ratio')):
                    pe = scraped.get('pe_ratio')
                if pd.notna(scraped.get('pb_ratio')):
                    pb = scraped.get('pb_ratio')
                if pd.notna(scraped.get('roe')):
                    roe = scraped.get('roe')
                if pd.notna(scraped.get('roa')):
                    roa = scraped.get('roa')
                
                # Calculate PEG
                peg = np.nan
                if pd.notna(pe) and pd.notna(prof_cagr) and prof_cagr > 0:
                    peg = pe / (prof_cagr * 100)
                
                # Ownership data
                free_float = scraped.get('free_float', np.nan)
                foreign_ownership = scraped.get('foreign_ownership', np.nan)
                management_ownership = scraped.get('management_ownership', np.nan)
                
                # Clamp ownership percentages (no fallback estimates - must be real data)
                if pd.notna(free_float):
                    free_float = max(0, min(1, free_float))
                if pd.notna(foreign_ownership):
                    foreign_ownership = max(0, min(1, foreign_ownership))
                if pd.notna(management_ownership):
                    management_ownership = max(0, min(1, management_ownership))
                
                # Trading value (must be real data from scraping)
                avg_trading_value = scraped.get('avg_trading_value', np.nan)
                
                # Est Val calculation
                est_val = np.nan
                
                # Method 1: DCF using cash flow data
                if not cf.empty and 'cash_from_operation' in cf.columns:
                    try:
                        latest_cf = cf.sort_values('year').tail(1)
                        ocf = latest_cf['cash_from_operation'].iloc[0] if not latest_cf.empty else 0
                        if pd.notna(ocf) and ocf > 0:
                            # Simple DCF: OCF * (1 + growth) / (discount_rate - growth)
                            growth = max(prof_cagr, 0.05) if pd.notna(prof_cagr) else 0.05
                            discount_rate = 0.12
                            est_val = (ocf * (1 + growth)) / (discount_rate - growth) / 1_000_000_000
                    except Exception:
                        pass
                
                # Method 2: EPS growth method
                if pd.isna(est_val) and not latest.empty and 'earning_per_share' in latest.columns:
                    eps = latest['earning_per_share'].iloc[0]
                    if pd.notna(eps) and eps > 0 and pd.notna(prof_cagr):
                        eps_next = eps * (1 + max(prof_cagr, 0))
                        if pd.notna(shares_outstanding):
                            est_val = (eps_next * shares_outstanding) / 1_000_000_000
                
                # Method 3: P/E based estimation
                if pd.isna(est_val) and pd.notna(pe) and pd.notna(market_val):
                    # Use current market cap as base, adjust by growth
                    if pd.notna(prof_cagr):
                        est_val = market_val * (1 + prof_cagr)
                    else:
                        est_val = market_val * 1.1  # 10% premium
                
                # Fallback: Use market cap
                if pd.isna(est_val):
                    est_val = market_val if pd.notna(market_val) else np.nan  # No default, must be real data
                
                row = {
                    'symbol': sym,
                    'price': current_price,  # Add price column in VND
                    'revenue_cagr_3y': rev_cagr,
                    'profit_cagr_3y': prof_cagr,
                    'roe': roe,
                    'roa': roa,
                    'pe': pe,
                    'pb': pb,
                    'peg': peg,
                    'ev_ebitda': additional_metrics.get('ev_ebitda', np.nan),
                    'gross_margin': additional_metrics.get('gross_margin', np.nan),
                    'operating_margin': additional_metrics.get('operating_margin', np.nan),
                    'debt_to_equity': additional_metrics.get('debt_to_equity', np.nan),
                    'debt_to_asset': additional_metrics.get('debt_to_asset', np.nan),
                    'current_ratio': additional_metrics.get('current_ratio', np.nan),
                    'quick_ratio': additional_metrics.get('quick_ratio', np.nan),
                    'eps': additional_metrics.get('eps', np.nan),
                    'book_value_per_share': additional_metrics.get('book_value_per_share', np.nan),
                    'dividend_yield': additional_metrics.get('dividend_yield', np.nan),
                    'free_cash_flow': additional_metrics.get('free_cash_flow', np.nan),
                    'operating_cash_flow': additional_metrics.get('operating_cash_flow', np.nan),
                    'free_float': free_float,
                    'foreign_ownership': foreign_ownership,
                    'management_ownership': management_ownership,
                    'avg_trading_value': avg_trading_value,
                    'est_val': est_val,
                    'market_val': market_val,
                }
                
                rows.append(row)
                add_log(f"✅ Completed {sym}")
                
                # Add delay to avoid rate limit
                time.sleep(2)
                
            except Exception as e:
                add_log(f"❌ Error processing {sym}: {e}")
                continue
        
        metrics = pd.DataFrame(rows)
        add_log(f"✅ Completed processing {len(metrics)} symbols")

        with status.container():
            st.markdown(f"""
            <div class="metric-card">
                <h3>✅ Scan completed</h3>
                <p>Data loaded successfully. Found {len(metrics)} symbols.</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()
    
    # Display results
    if not metrics.empty:
        st.markdown("### 📊 Stock Analysis Results")
        
        # Format the data for display
        display_df = metrics.copy()
        
        # Format percentages
        pct_cols = ['revenue_cagr_3y', 'profit_cagr_3y', 'roe', 'roa', 'gross_margin', 'operating_margin', 
                   'dividend_yield', 'free_float', 'foreign_ownership', 'management_ownership']
        for col in pct_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "N/A")
        
        # Format currency values
        currency_cols = ['avg_trading_value', 'est_val', 'market_val']
        for col in currency_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: f"{x:.1f}B" if pd.notna(x) else "N/A")
        
        # Format price in VND
        if 'price' in display_df.columns:
            display_df['price'] = display_df['price'].apply(lambda x: f"{x:,.0f} VND" if pd.notna(x) else "N/A")
        
        # Format ratios
        ratio_cols = ['pe', 'pb', 'peg', 'ev_ebitda', 'debt_to_equity', 'debt_to_asset', 'current_ratio', 'quick_ratio']
        for col in ratio_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
        
        # Display the dataframe
        st.dataframe(
            display_df,
            width='stretch',
            hide_index=True,
            height=600
        )
    
    # Reset force_scan
    st.session_state['force_scan'] = False

# Last scan time (GMT+7)
ts = datetime.now(timezone(timedelta(hours=7)))
st.caption(f"Last scan: {ts.strftime('%Y-%m-%d %H:%M:%S')} GMT+7")
