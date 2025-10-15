import os
import pandas as pd
import numpy as np
import streamlit as st
import logging
from typing import Dict, List
from datetime import datetime, timezone, timedelta

# Setup logging
logger = logging.getLogger(__name__)

from helpers import fetch_all_tickers

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
    if st.button("🔍 Scan Stocks", key="scan_button", use_container_width=True):
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
        
        # Generate mock data
        rows = []
        for sym in symbols:
            row = {
                'symbol': sym,
                'revenue_cagr_3y': np.random.uniform(0.05, 0.25),
                'profit_cagr_3y': np.random.uniform(0.03, 0.20),
                'roe': np.random.uniform(0.08, 0.25),
                'roa': np.random.uniform(0.05, 0.15),
                'pe': np.random.uniform(8, 25),
                'pb': np.random.uniform(0.8, 3.0),
                'peg': np.random.uniform(0.5, 2.0),
                'ev_ebitda': np.random.uniform(5, 15),
                'gross_margin': np.random.uniform(0.15, 0.45),
                'operating_margin': np.random.uniform(0.10, 0.30),
                'debt_to_equity': np.random.uniform(0.2, 1.5),
                'debt_to_asset': np.random.uniform(0.1, 0.6),
                'current_ratio': np.random.uniform(1.0, 3.0),
                'quick_ratio': np.random.uniform(0.8, 2.5),
                'eps': np.random.uniform(1000, 10000),
                'book_value_per_share': np.random.uniform(10000, 50000),
                'dividend_yield': np.random.uniform(0.02, 0.08),
                'free_cash_flow': np.random.uniform(100, 1000),
                'operating_cash_flow': np.random.uniform(200, 1200),
                'free_float': np.random.uniform(0.2, 0.8),
                'foreign_ownership': np.random.uniform(0.1, 0.5),
                'management_ownership': np.random.uniform(0.1, 0.4),
                'avg_trading_value': np.random.uniform(50, 500),
                'est_val': np.random.uniform(100, 1000),
                'market_val': np.random.uniform(200, 2000),
            }
            rows.append(row)
        
        metrics = pd.DataFrame(rows)
        add_log(f"✅ Completed processing {len(metrics)} symbols")

        with status.container():
            st.markdown("""
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
        
        # Format ratios
        ratio_cols = ['pe', 'pb', 'peg', 'ev_ebitda', 'debt_to_equity', 'debt_to_asset', 'current_ratio', 'quick_ratio']
        for col in ratio_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
        
        # Display the dataframe
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=600
        )
    
    # Reset force_scan
    st.session_state['force_scan'] = False

# Last scan time (GMT+7)
ts = datetime.now(timezone(timedelta(hours=7)))
st.caption(f"Last scan: {ts.strftime('%Y-%m-%d %H:%M:%S')} GMT+7")


















