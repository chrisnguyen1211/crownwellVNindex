import os
import pandas as pd
import numpy as np
import streamlit as st
import logging
from typing import Dict, List, Set
from datetime import datetime, timezone, timedelta

# Setup logging
logger = logging.getLogger(__name__)

from helpers import (
    fetch_all_tickers,
    fetch_income_statement,
    fetch_ratios,
    fetch_balance_sheet,
    fetch_cash_flow,
    compute_cagr,
    compute_roe_roa_from_statements,
    extract_additional_metrics,
    get_last_listing_errors,
)
from helpers import fetch_prices_vnd
from web_scraper import VietnamStockDataScraper
from supabase_helper import supabase_storage


st.set_page_config(
    page_title="Crownwell VNIndex Screener",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS to match TitanLabs style
st.markdown("""
<style>
    /* Main container styling */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }
    
    /* Header styling */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem 0;
        margin: -2rem -1rem 2rem -1rem;
        border-radius: 0 0 20px 20px;
        text-align: center;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
    }
    
    .main-header h1 {
        color: white;
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0;
        text-shadow: 0 2px 4px rgba(0,0,0,0.3);
    }
    
    .main-header p {
        color: rgba(255,255,255,0.9);
        font-size: 1.1rem;
        margin: 0.5rem 0 0 0;
    }
    
    /* Card styling */
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        border: 1px solid #e1e5e9;
        margin-bottom: 1rem;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 20px rgba(0,0,0,0.12);
    }
    
    .metric-card h3 {
        color: #2c3e50;
        font-size: 1.2rem;
        font-weight: 600;
        margin: 0 0 1rem 0;
        border-bottom: 2px solid #3498db;
        padding-bottom: 0.5rem;
    }
    
    /* Button styling */
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        font-size: 1rem;
        transition: all 0.3s ease;
        box-shadow: 0 2px 10px rgba(102, 126, 234, 0.3);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 20px rgba(102, 126, 234, 0.4);
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background: linear-gradient(180deg, #f8f9fa 0%, #e9ecef 100%);
    }
    
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #f8f9fa 0%, #e9ecef 100%);
    }
    
    /* Data table styling */
    .dataframe {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 10px rgba(0,0,0,0.08);
    }
    
    /* Status indicators */
    .status-pass {
        background: #d4edda;
        color: #155724;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
    }
    
    .status-fail {
        background: #f8d7da;
        color: #721c24;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
    }
    
    /* Raw log readability: disable hover color flip */
    .stTextArea textarea {
        color: #212529 !important;
        background: #ffffff !important;
    }
    .stTextArea textarea:hover, .stTextArea textarea:focus {
        color: #212529 !important;
        background: #ffffff !important;
    }
    
    /* Loading spinner */
    .loading-container {
        text-align: center;
        padding: 3rem;
    }
    
    /* Footer */
    .footer {
        text-align: center;
        padding: 2rem 0;
        margin-top: 3rem;
        border-top: 1px solid #e1e5e9;
        color: #6c757d;
    }
</style>
""", unsafe_allow_html=True)

# Header section with TitanLabs-style design
st.markdown("""
<div class="main-header">
    <h1>📊 Crownwell VNIndex Screener</h1>
    <p>Advanced Quantitative Stock Analysis & Screening Platform</p>
</div>
""", unsafe_allow_html=True)

# Metrics explanation (expander only)
with st.expander("📖 Chú thích các chỉ số tài chính", expanded=False):
    st.markdown("""
        ### 📊 Nhóm cơ bản (có trong bảng)
        - **symbol**: Mã cổ phiếu
        - **price_vnd**: Giá cổ phiếu hiện tại (VND)
        - **eps**: Lợi nhuận trên mỗi cổ phiếu; đầu vào cho P/E/định giá
        - **eps_norm**: EPS chuẩn hóa để so sánh giữa các kỳ
        - **revenue_cagr_3y**: Tăng trưởng doanh thu bình quân 3 năm
        - **profit_cagr_3y**: Tăng trưởng lợi nhuận bình quân 3 năm

        ### 📈 Định giá (có trong bảng)
        - **pe**: Price/Earnings — mức định giá theo lợi nhuận
        - **pb**: Price/Book — mức định giá theo vốn chủ sở hữu
        - **peg**: PE chia tăng trưởng lợi nhuận — ~1 hợp lý
        - **ev_ebitda**: EV/EBITDA — so sánh ngang doanh nghiệp

        ### 💰 Hiệu quả (có trong bảng)
        - **roe**: LN ròng/Vốn chủ sở hữu
        - **roa**: LN ròng/Tổng tài sản
        - **gross_margin**: (Doanh thu - Giá vốn)/Doanh thu
        - **operating_margin**: LN hoạt động/Doanh thu

        ### 🏦 Cấu trúc tài chính & thanh khoản (có trong bảng)
        - **debt_to_equity**: Tổng nợ/Vốn chủ sở hữu
        - **debt_to_asset**: Tổng nợ/Tổng tài sản
        - **current_ratio**: Tài sản ngắn hạn/Nợ ngắn hạn
        - **quick_ratio**: (Tài sản ngắn hạn - Hàng tồn kho)/Nợ ngắn hạn

        ### 🏛️ Sở hữu (có trong bảng)
        - **free_float**: Tỷ lệ cổ phiếu tự do chuyển nhượng
        - **foreign_ownership**: Tỷ lệ sở hữu nước ngoài

        ### 🏦 Ngân hàng (có trong bảng)
        - **npl_ratio**: Tỷ lệ nợ xấu = Nợ xấu/Tổng dư nợ
        - **llr**: Bao phủ nợ xấu = Dự phòng/Nợ xấu

        ### 📊 Giao dịch & định giá (có trong bảng)
        - **avg_trading_value**: Giá trị giao dịch trung bình (tỷ VND/ngày)
        - **est_val**: Giá trị ước tính từ mô hình
        - **market_val**: Vốn hóa thị trường (tỷ VND)

        ### ➕ Có thể xuất hiện khi có dữ liệu
        - **book_value_per_share**: Giá trị sổ sách/CP
        - **dividend_yield**: Tỷ suất cổ tức
        - **free_cash_flow**: Dòng tiền tự do
        - **operating_cash_flow**: Dòng tiền từ HĐKD
        """)

# Sidebar controls with TitanLabs-style design
st.sidebar.markdown("""
<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 1rem; border-radius: 8px; margin-bottom: 1rem;">
    <h3 style="color: white; margin: 0; text-align: center;">🎯 Screening Criteria</h3>
</div>
""", unsafe_allow_html=True)
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
criteria["min_avg_trading_value_billion"] = st.sidebar.number_input(
    "Min Avg Trading Value (billion VND/day)", min_value=0.0, max_value=1000.0, value=1.0, step=1.0
)

# Additional metrics criteria
st.sidebar.markdown("### 📊 Additional Metrics")
criteria["max_ev_ebitda"] = st.sidebar.number_input(
    "Max EV/EBITDA", min_value=0.0, value=15.0, step=0.5
)
criteria["min_gross_margin"] = st.sidebar.number_input(
    "Min Gross Margin (%)", min_value=0.0, max_value=100.0, value=20.0, step=1.0
) / 100.0
criteria["min_operating_margin"] = st.sidebar.number_input(
    "Min Operating Margin (%)", min_value=0.0, max_value=100.0, value=10.0, step=1.0
) / 100.0
criteria["max_debt_to_equity"] = st.sidebar.number_input(
    "Max Debt-to-Equity", min_value=0.0, value=1.0, step=0.1
)
criteria["min_current_ratio"] = st.sidebar.number_input(
    "Min Current Ratio", min_value=0.0, value=1.0, step=0.1
)
criteria["min_quick_ratio"] = st.sidebar.number_input(
    "Min Quick Ratio", min_value=0.0, value=0.5, step=0.1
)
criteria["min_dividend_yield"] = st.sidebar.number_input(
    "Min Dividend Yield (%)", min_value=0.0, max_value=100.0, value=2.0, step=0.1
) / 100.0

side_by_side = st.sidebar.checkbox(
    "Show per-criterion tables side-by-side", value=True
)

# Scan button with TitanLabs-style design
st.markdown("""
<div style="text-align: center; margin: 2rem 0;">
""", unsafe_allow_html=True)

scan_clicked = st.button("🚀 Scan All VN Stocks", key="scan_button")

st.markdown("</div>", unsafe_allow_html=True)

# Do not auto-scan on first load; use cached results if available
if 'last_scan_metrics' not in st.session_state:
    st.session_state['last_scan_metrics'] = pd.DataFrame()
if 'last_scan_symbols' not in st.session_state:
    st.session_state['last_scan_symbols'] = []

# If clicked, request a force scan and rerun immediately to refresh UI
if scan_clicked:
    try:
        st.cache_data.clear()
    except Exception:
        pass
    st.session_state['force_scan'] = True
    st.rerun()

# Only scan when Scan button explicitly requested it
scan = bool(st.session_state.get('force_scan', False))

# Debug: Show current state
st.info(f"🔍 Debug: scan = {scan}, force_scan = {st.session_state.get('force_scan', False)}")

# Initialize CafeF scraper (single session)
_scraper = VietnamStockDataScraper()
 
# Helper: render Market Boards given a metrics DataFrame
def _render_market_boards(title_note: str, df: pd.DataFrame, ex_map_in: Dict[str, str] = None, vn30_in: Set[str] = None):
    try:
        st.markdown("""
        <div class="metric-card">
            <h3>📊 Market Boards</h3>
        </div>
        """, unsafe_allow_html=True)
        if title_note:
            st.caption(title_note)
        # Align columns and formatting with Streaming Results
        display_df = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
        try:
            preferred_cols = [
                'symbol','company_name','price_vnd','eps','eps_norm','revenue_cagr_3y','profit_cagr_3y','pe','pb','peg','roe','roa',
                'ev_ebitda','gross_margin','operating_margin','debt_to_equity','debt_to_asset',
                'current_ratio','quick_ratio','free_float','foreign_ownership',
                'npl_ratio','llr','avg_trading_value','est_val','market_val'
            ]
            existing_cols = [c for c in preferred_cols if c in display_df.columns]
            remaining_cols = [c for c in display_df.columns if c not in existing_cols]
            display_df = display_df[existing_cols + remaining_cols]

            percentage_cols = ['revenue_cagr_3y', 'profit_cagr_3y', 'roe', 'roa', 'free_float', 'foreign_ownership', 'npl_ratio', 'llr']
            for col in percentage_cols:
                if col in display_df.columns:
                    display_df[col] = display_df[col].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")

            if 'price_vnd' in display_df.columns:
                display_df['price_vnd'] = display_df['price_vnd'].apply(lambda x: f"{x:,.0f} VND" if pd.notna(x) and x > 0 else "N/A")
            
            # Clean company names - remove JavaScript code, VietstockFinance, and ticker symbols
            if 'company_name' in display_df.columns:
                def clean_company_name(x):
                    if pd.isna(x) or any(js_word in str(x).lower() for js_word in ['$', 'function', 'document', 'ready', 'click', 'hide']):
                        return "N/A"
                    name = str(x).strip()
                    # Remove "VietstockFinance" and similar suffixes
                    name = name.replace(' - VietstockFinance', '').replace(' - Vietstock', '').replace(' | VietstockFinance', '').replace(' | Vietstock', '')
                    # Remove ticker symbols at the end (e.g., " - AAM", " | AAM")
                    import re
                    name = re.sub(r'\s*[-|]\s*[A-Z]{2,4}$', '', name)
                    return name
                display_df['company_name'] = display_df['company_name'].apply(clean_company_name)
            if 'eps' in display_df.columns:
                display_df['eps'] = display_df['eps'].apply(lambda x: f"{float(x):,.0f}" if pd.notna(x) else "N/A")
            if 'eps_norm' in display_df.columns:
                display_df['eps_norm'] = display_df['eps_norm'].apply(lambda x: f"{float(x):,.0f}" if pd.notna(x) else "N/A")
            if 'avg_trading_value' in display_df.columns:
                display_df['avg_trading_value'] = display_df['avg_trading_value'].apply(lambda x: f"{x:.1f}B VND/day" if pd.notna(x) and x > 0 else "N/A")
            if 'pe' in display_df.columns:
                display_df['pe'] = display_df['pe'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
            if 'pb' in display_df.columns:
                display_df['pb'] = display_df['pb'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
            if 'peg' in display_df.columns:
                display_df['peg'] = display_df['peg'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
            if 'ev_ebitda' in display_df.columns:
                display_df['ev_ebitda'] = display_df['ev_ebitda'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
            if 'gross_margin' in display_df.columns:
                display_df['gross_margin'] = display_df['gross_margin'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
            if 'operating_margin' in display_df.columns:
                display_df['operating_margin'] = display_df['operating_margin'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
            if 'debt_to_equity' in display_df.columns:
                display_df['debt_to_equity'] = display_df['debt_to_equity'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            if 'debt_to_asset' in display_df.columns:
                display_df['debt_to_asset'] = display_df['debt_to_asset'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            if 'current_ratio' in display_df.columns:
                display_df['current_ratio'] = display_df['current_ratio'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            if 'quick_ratio' in display_df.columns:
                display_df['quick_ratio'] = display_df['quick_ratio'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
            if 'book_value_per_share' in display_df.columns:
                display_df['book_value_per_share'] = display_df['book_value_per_share'].apply(lambda x: f"{float(x):.0f}" if pd.notna(x) and str(x).replace('.','').replace('-','').isdigit() else "N/A")
            if 'dividend_yield' in display_df.columns:
                display_df['dividend_yield'] = display_df['dividend_yield'].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
            if 'free_cash_flow' in display_df.columns:
                display_df['free_cash_flow'] = display_df['free_cash_flow'].apply(lambda x: f"{x:.1f}B" if pd.notna(x) else "N/A")
            if 'operating_cash_flow' in display_df.columns:
                display_df['operating_cash_flow'] = display_df['operating_cash_flow'].apply(lambda x: f"{x:.1f}B" if pd.notna(x) else "N/A")
            if 'est_val' in display_df.columns:
                display_df['est_val'] = display_df['est_val'].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")
            if 'market_val' in display_df.columns:
                display_df['market_val'] = display_df['market_val'].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")
        except Exception:
            pass
        ex_map_local = ex_map_in or st.session_state.get('symbol_to_exchange', {})
        if not ex_map_local:
            try:
                at = fetch_all_tickers()
                if isinstance(at, pd.DataFrame) and 'symbol' in at.columns and 'exchange' in at.columns:
                    ex_map_local = {str(r['symbol']).upper(): str(r['exchange']).upper() for _, r in at.iterrows()}
            except Exception:
                ex_map_local = {}
        vn30_local = vn30_in or set(st.session_state.get('vn30_set', [])) or {"ACB","BCM","BID","BVH","CTG","FPT","GAS","GVR","HDB","HPG",
                        "MBB","MSN","MWG","PLX","POW","SAB","SSI","STB","TCB","TPB",
                        "VCB","VHM","VIB","VIC","VJC","VNM","VPB","VRE","VSH"}
        try:
            df_seg = display_df.copy()
            if 'exchange' not in df_seg.columns:
                df_seg['exchange'] = df_seg['symbol'].map(lambda s: ex_map_local.get(s, ''))
        except Exception:
            df_seg = pd.DataFrame(columns=['symbol','exchange'])

        def _render_segment(title: str, _df: pd.DataFrame):
            st.markdown(f"**{title}**")
            if _df.empty:
                st.dataframe(pd.DataFrame())
                return
            try:
                st.dataframe(_df)
            except Exception:
                st.dataframe(_df)

        try:
            # Filter VN30 from original df before formatting, then apply same formatting
            vn30_raw = df[df['symbol'].isin(vn30_local)] if not df.empty else pd.DataFrame()
            if not vn30_raw.empty:
                # Apply same formatting to VN30 subset
                vn30_display = vn30_raw.copy()
                try:
                    existing_cols = [c for c in preferred_cols if c in vn30_display.columns]
                    remaining_cols = [c for c in vn30_display.columns if c not in existing_cols]
                    vn30_display = vn30_display[existing_cols + remaining_cols]
                    
                    for col in percentage_cols:
                        if col in vn30_display.columns:
                            vn30_display[col] = vn30_display[col].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
                    
                    if 'price_vnd' in vn30_display.columns:
                        vn30_display['price_vnd'] = vn30_display['price_vnd'].apply(lambda x: f"{x:,.0f} VND" if pd.notna(x) and x > 0 else "N/A")
                    if 'eps' in vn30_display.columns:
                        vn30_display['eps'] = vn30_display['eps'].apply(lambda x: f"{float(x):,.0f}" if pd.notna(x) else "N/A")
                    if 'eps_norm' in vn30_display.columns:
                        vn30_display['eps_norm'] = vn30_display['eps_norm'].apply(lambda x: f"{float(x):,.0f}" if pd.notna(x) else "N/A")
                    if 'avg_trading_value' in vn30_display.columns:
                        vn30_display['avg_trading_value'] = vn30_display['avg_trading_value'].apply(lambda x: f"{x:.1f}B VND/day" if pd.notna(x) and x > 0 else "N/A")
                    if 'pe' in vn30_display.columns:
                        vn30_display['pe'] = vn30_display['pe'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
                    if 'pb' in vn30_display.columns:
                        vn30_display['pb'] = vn30_display['pb'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
                    if 'peg' in vn30_display.columns:
                        vn30_display['peg'] = vn30_display['peg'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
                    if 'ev_ebitda' in vn30_display.columns:
                        vn30_display['ev_ebitda'] = vn30_display['ev_ebitda'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
                    if 'gross_margin' in vn30_display.columns:
                        vn30_display['gross_margin'] = vn30_display['gross_margin'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
                    if 'operating_margin' in vn30_display.columns:
                        vn30_display['operating_margin'] = vn30_display['operating_margin'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
                    if 'debt_to_equity' in vn30_display.columns:
                        vn30_display['debt_to_equity'] = vn30_display['debt_to_equity'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
                    if 'debt_to_asset' in vn30_display.columns:
                        vn30_display['debt_to_asset'] = vn30_display['debt_to_asset'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
                    if 'current_ratio' in vn30_display.columns:
                        vn30_display['current_ratio'] = vn30_display['current_ratio'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
                    if 'quick_ratio' in vn30_display.columns:
                        vn30_display['quick_ratio'] = vn30_display['quick_ratio'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
                    if 'book_value_per_share' in vn30_display.columns:
                        vn30_display['book_value_per_share'] = vn30_display['book_value_per_share'].apply(lambda x: f"{float(x):.0f}" if pd.notna(x) and str(x).replace('.','').replace('-','').isdigit() else "N/A")
                    if 'dividend_yield' in vn30_display.columns:
                        vn30_display['dividend_yield'] = vn30_display['dividend_yield'].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
                    if 'free_cash_flow' in vn30_display.columns:
                        vn30_display['free_cash_flow'] = vn30_display['free_cash_flow'].apply(lambda x: f"{x:.1f}B" if pd.notna(x) else "N/A")
                    if 'operating_cash_flow' in vn30_display.columns:
                        vn30_display['operating_cash_flow'] = vn30_display['operating_cash_flow'].apply(lambda x: f"{x:.1f}B" if pd.notna(x) else "N/A")
                    if 'est_val' in vn30_display.columns:
                        vn30_display['est_val'] = vn30_display['est_val'].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")
                    if 'market_val' in vn30_display.columns:
                        vn30_display['market_val'] = vn30_display['market_val'].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")
                except Exception:
                    pass
            else:
                vn30_display = pd.DataFrame()
        except Exception:
            vn30_display = pd.DataFrame()
        _render_segment("VN30", vn30_display)

        for ex_name, ex_title in [("HOSE", "HOSE"), ("HNX", "HNX"), ("UPCOM", "UPCOM")]:
            try:
                syms = [s for s in df_seg.loc[df_seg['exchange'] == ex_name, 'symbol'].tolist()]
                seg_df = display_df[display_df['symbol'].isin(syms)] if syms else pd.DataFrame()
            except Exception:
                seg_df = pd.DataFrame()
            _render_segment(ex_title, seg_df)
    except Exception:
        pass

# Latest Scan (Supabase) section only
try:
    supabase_data_latest = supabase_storage.load_all_exchanges_data()
    if supabase_data_latest:
        all_dfs = []
        for exchange, df_ex in supabase_data_latest.items():
            if isinstance(df_ex, pd.DataFrame) and not df_ex.empty:
                df_copy = df_ex.copy()
                if 'exchange' not in df_copy.columns:
                    df_copy['exchange'] = exchange
                all_dfs.append(df_copy)
        if all_dfs:
            latest_df = pd.concat(all_dfs, ignore_index=True)
            # Attach live prices if missing
            try:
                if 'symbol' in latest_df.columns and ('price_vnd' not in latest_df.columns or latest_df['price_vnd'].isna().all()):
                    syms_latest = [str(s) for s in latest_df['symbol'].dropna().astype(str).unique().tolist()]
                    if syms_latest:
                        price_map_latest = fetch_prices_vnd(syms_latest)
                        latest_df['price_vnd'] = latest_df['symbol'].map(lambda s: price_map_latest.get(s))
            except Exception:
                pass
            _render_market_boards("Latest scan from Supabase.", latest_df)
        else:
            st.dataframe(pd.DataFrame({"note":["Supabase has no data."]}))
    else:
        st.dataframe(pd.DataFrame({"note":["Failed to load data from Supabase."]}))
except Exception as _e:
    st.dataframe(pd.DataFrame({"note":["Error loading Supabase data."], "error":[str(_e)]}))

# Clear cached metrics when Scan is pressed
if scan:
    try:
        st.cache_data.clear()
        st.toast("Refreshing metrics...", icon="⏳")
    except Exception:
        pass


@st.cache_data(ttl=3600, show_spinner=False)
def calculate_metrics(symbols: List[str]) -> pd.DataFrame:
    rows = []
    
    for sym in symbols:
        try:
            # Fetch real data from APIs
            inc = fetch_income_statement(sym)
            rat = fetch_ratios(sym)
            cf = fetch_cash_flow(sym)
            bs = fetch_balance_sheet(sym)
            
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

            # If ROE/ROA missing, compute from statements
            if pd.isna(roe_val) or pd.isna(roa_val):
                bs = fetch_balance_sheet(sym)
                roe_c, roa_c = compute_roe_roa_from_statements(inc, bs)
                if pd.isna(roe_val):
                    roe_val = roe_c
                if pd.isna(roa_val):
                    roa_val = roa_c

            # Extract additional metrics from vnstock API
            additional_metrics = extract_additional_metrics(rat, cf)
            
            # EV/EBITDA
            ev_ebitda = additional_metrics.get("ev_ebitda", np.nan)
            
            # Gross Margin
            gross_margin = additional_metrics.get("gross_margin", np.nan)
            
            # Operating Margin
            operating_margin = additional_metrics.get("operating_margin", np.nan)
            
            # Debt Ratios
            debt_to_equity = additional_metrics.get("debt_to_equity", np.nan)
            debt_to_asset = additional_metrics.get("debt_to_asset", np.nan)
            
            # Liquidity Ratios
            current_ratio = additional_metrics.get("current_ratio", np.nan)
            quick_ratio = additional_metrics.get("quick_ratio", np.nan)
            
            # EPS and Book Value
            eps = additional_metrics.get("eps", np.nan)
            book_value_per_share = additional_metrics.get("book_value_per_share", np.nan)
            
            # Dividend Yield
            dividend_yield = additional_metrics.get("dividend_yield", np.nan)
            
            # Free Cash Flow
            free_cash_flow = additional_metrics.get("free_cash_flow", np.nan)
            operating_cash_flow = additional_metrics.get("operating_cash_flow", np.nan)

            # Normalize ROE/ROA to fraction if in percents
            if pd.notna(roe_val) and roe_val > 1:
                roe_val = roe_val / 100.0
            if pd.notna(roa_val) and roa_val > 1:
                roa_val = roa_val / 100.0

            # Additional metrics from ratios
            peg = np.nan
            if not latest.empty:
                # PEG calculation
                if pd.notna(pe) and pd.notna(prof_cagr) and prof_cagr > 0:
                    peg = pe / prof_cagr  # prof_cagr is already a fraction (0.15 for 15%)

            # Bank-specific fallbacks: use appropriate ratio metrics for banks
            try:
                bank_symbols = {"ACB","BID","CTG","VCB","TCB","TPB","MBB","STB","VIB","VPB","HDB"}
                if sym in bank_symbols and not latest.empty:
                    # Map interest margins to our gross/operating margin slots (as %)
                    if pd.isna(gross_margin) and "interest_margin" in latest.columns:
                        im = latest["interest_margin"].iloc[0]
                        if pd.notna(im):
                            gross_margin = im
                    if pd.isna(operating_margin):
                        col = "pre_provision_on_toi" if "pre_provision_on_toi" in latest.columns else ("post_tax_on_toi" if "post_tax_on_toi" in latest.columns else None)
                        if col:
                            om = latest[col].iloc[0]
                            if pd.notna(om):
                                operating_margin = om
                    # Liquidity proxy
                    if pd.isna(current_ratio) and "liquidity_on_liability" in latest.columns:
                        lr = latest["liquidity_on_liability"].iloc[0]
                        if pd.notna(lr):
                            current_ratio = lr
                    if pd.isna(quick_ratio) and pd.notna(current_ratio):
                        quick_ratio = current_ratio
            except Exception:
                pass

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

            # Enable scraping for additional data
            try:
                scraped = _scraper.get_stock_overview(sym)
            except Exception as e:
                st.warning(f"Scraping failed for {sym}: {e}")
                scraped = {}
            
            # Add delay to avoid TCBS rate limit
            import time
            time.sleep(3)  # Increased delay to avoid rate limit

            # Use scraped P/E and P/B if available
            scraped_pe = scraped.get('pe_ratio', np.nan)
            scraped_pb = scraped.get('pb_ratio', np.nan)
            scraped_roe = scraped.get('roe', np.nan)
            scraped_roa = scraped.get('roa', np.nan)
            scraped_eps = scraped.get('eps', np.nan)
            scraped_npl = scraped.get('npl_ratio', np.nan)
            scraped_llr = scraped.get('llr', np.nan)
            scraped_div_yield = scraped.get('dividend_yield', np.nan)
            scraped_company_name = scraped.get('company_name', np.nan)
            
            # Fallback: get company name from vnstock API if scraper failed
            if pd.isna(scraped_company_name) or any(js_word in str(scraped_company_name).lower() for js_word in ['$', 'function', 'document', 'ready']):
                try:
                    from vnstock import Listing as _Listing
                    lst = _Listing()
                    df_names = lst.all_symbols()
                    if isinstance(df_names, pd.DataFrame) and not df_names.empty and 'symbol' in df_names.columns and 'organ_name' in df_names.columns:
                        name_row = df_names[df_names['symbol'].str.upper() == sym.upper()]
                        if not name_row.empty:
                            api_company_name = name_row['organ_name'].iloc[0]
                            if pd.notna(api_company_name) and len(str(api_company_name).strip()) > 5:
                                scraped_company_name = str(api_company_name).strip()
                except Exception:
                    pass
            
            if pd.notna(scraped_pe) and scraped_pe > 0:
                pe = scraped_pe
            if pd.notna(scraped_pb) and scraped_pb > 0:
                pb = scraped_pb
            if pd.notna(scraped_roe) and scraped_roe > 0:
                roe_val = scraped_roe
            if pd.notna(scraped_roa) and scraped_roa > 0:
                roa_val = scraped_roa

            if pd.notna(scraped_eps) and scraped_eps > 0:
                eps = scraped_eps

            free_float = scraped.get('free_float', np.nan)
            foreign_ownership = scraped.get('foreign_ownership', np.nan)
            # management_ownership removed per request
            npl_ratio = scraped_npl if pd.notna(scraped_npl) else np.nan
            llr = scraped_llr if pd.notna(scraped_llr) else np.nan
            if pd.notna(scraped_div_yield):
                dividend_yield = scraped_div_yield
            # Avg trading value calculation: KLGD (shares) * price = trading value in VND
            # Unit conversion: HOSE: 10 shares, HNX/UPCOM: 100 shares
            klgd_shares = scraped.get('klgd_shares', np.nan)
            if pd.notna(klgd_shares) and pd.notna(current_price) and current_price > 0:
                # Assume HOSE (most VN30 stocks are on HOSE) - multiply by 10 for unit conversion
                klgd_actual_shares = klgd_shares * 10  # HOSE unit conversion
                avg_trading_value = (klgd_actual_shares * current_price) / 1_000_000_000  # Convert to billion VND
            else:
                # Fallback: use existing TCBS API method
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

            # Prefer CafeF market cap if available; do NOT override when present
            if scraped:
                mc = scraped.get('market_cap')
                # Accept only if within plausible bounds (1B - 10,000,000B) and not placeholder
                if pd.notna(mc) and 1 <= mc <= 10_000_000 and mc != 1000:
                    market_val = mc

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
                eps_latest = latest['earning_per_share'].iloc[0]
                if pd.isna(eps) and pd.notna(eps_latest) and eps_latest > 0:
                    eps = eps_latest
                if pd.notna(eps_latest) and eps_latest > 0:
                    # Heuristic: approximate shares from revenue and an assumed revenue/share ratio (~1e3 VND/share)
                    shares_outstanding = (rev_series.iloc[-1] * 1_000_000_000) / max(eps_latest, 1)

            # Fallback to price * shares if CafeF missing and we have price and shares
            if pd.isna(market_val) and pd.notna(current_price) and pd.notna(shares_outstanding) and current_price > 0 and shares_outstanding > 0:
                market_val = (current_price * shares_outstanding) / 1_000_000_000

            # Normalize EPS units if value appears 100x too large (e.g., 330,471 → 3,304.71)
            try:
                if pd.notna(eps):
                    eps_val = float(eps)
                    if eps_val > 20000:
                        eps = eps_val / 100.0
            except Exception:
                pass

            # Sanitize percentages - ensure they're in [0,1] range
            if pd.notna(free_float):
                free_float = 1.0 if free_float > 1 else (0.0 if free_float < 0 else free_float)
            if pd.notna(foreign_ownership):
                foreign_ownership = 1.0 if foreign_ownership > 1 else (0.0 if foreign_ownership < 0 else foreign_ownership)

            # Fallback computations for missing ratios from statements
            try:
                # Gross/Operating margin as percentage values if missing
                if (pd.isna(gross_margin)) and not inc.empty and set(["gross_profit","revenue"]).issubset(set(inc.columns)):
                    latest_year = inc.sort_values(["year"]).tail(1)
                    gp = float(latest_year["gross_profit"].iloc[0]) if not latest_year["gross_profit"].isna().all() else np.nan
                    rv = float(latest_year["revenue"].iloc[0]) if not latest_year["revenue"].isna().all() else np.nan
                    if pd.notna(gp) and pd.notna(rv) and rv > 0:
                        gross_margin = (gp / rv) * 100.0
                if (pd.isna(operating_margin)) and not inc.empty and set(["operation_profit","revenue"]).issubset(set(inc.columns)):
                    latest_year = inc.sort_values(["year"]).tail(1)
                    op = float(latest_year["operation_profit"].iloc[0]) if not latest_year["operation_profit"].isna().all() else np.nan
                    rv = float(latest_year["revenue"].iloc[0]) if not latest_year["revenue"].isna().all() else np.nan
                    if pd.notna(op) and pd.notna(rv) and rv > 0:
                        operating_margin = (op / rv) * 100.0

                # Debt & liquidity ratios if missing
                if not bs.empty:
                    bs_latest = bs.sort_values(["year"]).tail(1)
                    debt_total = None
                    if "short_debt" in bs_latest.columns and "long_debt" in bs_latest.columns:
                        sd = float(bs_latest["short_debt"].iloc[0]) if not bs_latest["short_debt"].isna().all() else np.nan
                        ld = float(bs_latest["long_debt"].iloc[0]) if not bs_latest["long_debt"].isna().all() else np.nan
                        debt_total = (sd if pd.notna(sd) else 0) + (ld if pd.notna(ld) else 0)
                    if (pd.isna(debt_to_equity)) and ("debt" in bs_latest.columns or debt_total is not None) and "equity" in bs_latest.columns:
                        eq = float(bs_latest["equity"].iloc[0]) if not bs_latest["equity"].isna().all() else np.nan
                        db = float(bs_latest["debt"].iloc[0]) if "debt" in bs_latest.columns and not bs_latest["debt"].isna().all() else np.nan
                        total_debt = debt_total if debt_total is not None else db
                        if pd.notna(total_debt) and pd.notna(eq) and eq != 0:
                            debt_to_equity = total_debt / eq
                    if (pd.isna(debt_to_asset)) and ("debt" in bs_latest.columns or debt_total is not None) and ("asset" in bs_latest.columns or "total_assets" in bs_latest.columns):
                        at = float(bs_latest["asset"].iloc[0]) if "asset" in bs_latest.columns and not bs_latest["asset"].isna().all() else (float(bs_latest["total_assets"].iloc[0]) if "total_assets" in bs_latest.columns and not bs_latest["total_assets"].isna().all() else np.nan)
                        db = float(bs_latest["debt"].iloc[0]) if "debt" in bs_latest.columns and not bs_latest["debt"].isna().all() else np.nan
                        total_debt = debt_total if debt_total is not None else db
                        if pd.notna(total_debt) and pd.notna(at) and at != 0:
                            debt_to_asset = total_debt / at
                    if pd.isna(current_ratio) and set(["short_asset","short_debt"]).issubset(set(bs_latest.columns)):
                        sa = float(bs_latest["short_asset"].iloc[0]) if not bs_latest["short_asset"].isna().all() else np.nan
                        sd = float(bs_latest["short_debt"].iloc[0]) if not bs_latest["short_debt"].isna().all() else np.nan
                        if pd.notna(sa) and pd.notna(sd) and sd != 0:
                            current_ratio = sa / sd
                    if pd.isna(quick_ratio):
                        cash = float(bs_latest["cash"].iloc[0]) if "cash" in bs_latest.columns and not bs_latest["cash"].isna().all() else np.nan
                        si = float(bs_latest["short_invest"].iloc[0]) if "short_invest" in bs_latest.columns and not bs_latest["short_invest"].isna().all() else 0.0
                        sr = float(bs_latest["short_receivable"].iloc[0]) if "short_receivable" in bs_latest.columns and not bs_latest["short_receivable"].isna().all() else 0.0
                        sd = float(bs_latest["short_debt"].iloc[0]) if "short_debt" in bs_latest.columns and not bs_latest["short_debt"].isna().all() else np.nan
                        quick_assets = (cash if pd.notna(cash) else 0.0) + (si if pd.notna(si) else 0.0) + (sr if pd.notna(sr) else 0.0)
                        if pd.notna(quick_assets) and pd.notna(sd) and sd != 0:
                            quick_ratio = quick_assets / sd
            except Exception:
                pass

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
            # Final fallback: if still NaN, use market cap when available
            if pd.isna(est_val) and pd.notna(market_val):
                est_val = market_val

            rows.append(
                dict(
                    symbol=sym,
                    company_name=scraped_company_name if pd.notna(scraped_company_name) else np.nan,
                    revenue_cagr_3y=rev_cagr,
                    profit_cagr_3y=prof_cagr,
                    pe=pe,
                    pb=pb,
                    roe=roe_val,
                    roa=roa_val,
                    peg=peg,
                    ev_ebitda=ev_ebitda,
                    gross_margin=gross_margin,
                    operating_margin=operating_margin,
                    debt_to_equity=debt_to_equity,
                    debt_to_asset=debt_to_asset,
                    current_ratio=current_ratio,
                    quick_ratio=quick_ratio,
                    eps=eps,
                    book_value_per_share=book_value_per_share,
                    dividend_yield=dividend_yield,
                    free_cash_flow=free_cash_flow,
                    operating_cash_flow=operating_cash_flow,
                    free_float=free_float,
                    foreign_ownership=foreign_ownership,
                    # management_ownership removed
                    npl_ratio=npl_ratio if 'npl_ratio' in locals() else np.nan,
                    llr=llr if 'llr' in locals() else np.nan,
                    avg_trading_value=avg_trading_value,
                    est_val=est_val,
                    market_val=market_val,
                )
            )
        except Exception as e:
            st.warning(f"Error processing {sym}: {e}")
            continue
    return pd.DataFrame(rows)


def calculate_metrics_for_symbol(symbol: str, on_log=None) -> Dict:
    """Compute metrics for a single symbol by reusing cached bulk function.
    on_log: optional callable(msg: str) to emit live logs.
    """
    try:
        if callable(on_log):
            on_log(f"Start processing {symbol}")
        df = calculate_metrics([symbol])
        if isinstance(df, pd.DataFrame) and not df.empty:
            row = df.iloc[0].to_dict()
            if callable(on_log):
                on_log(f"Finished {symbol}")
            return row
        if callable(on_log):
            on_log(f"No data for {symbol}")
        return {}
    except Exception as _ex:
        if callable(on_log):
            on_log(f"Error processing {symbol}: {_ex}")
        return {}


def calculate_metrics_streaming(
    symbols: List[str],
    on_progress,
    on_log,
    on_row=None,
) -> pd.DataFrame:
    """Process symbols sequentially and emit live logs and progress updates.

    If provided, on_row(row: Dict) will be called immediately after each symbol
    is processed to enable streaming UI updates.
    """
    rows: List[Dict] = []
    total = len(symbols)
    for idx, sym in enumerate(symbols, start=1):
        on_progress(idx, total, sym)
        row = calculate_metrics_for_symbol(sym, on_log=on_log)
        if row:
            rows.append(row)
            if callable(on_row):
                try:
                    on_row(row)
                except Exception:
                    # ignore streaming UI errors to keep scanning
                    pass
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
    # management_ownership filter removed
    if crit["min_avg_trading_value_billion"] > 0:
        cond &= (df["avg_trading_value"].fillna(-1) >= crit["min_avg_trading_value_billion"])
    
    # Additional metrics criteria
    if crit["max_ev_ebitda"] > 0:
        cond &= (df["ev_ebitda"].fillna(999) <= crit["max_ev_ebitda"])
    
    if crit["min_gross_margin"] > 0:
        cond &= (df["gross_margin"].fillna(-1) >= crit["min_gross_margin"])
    
    if crit["min_operating_margin"] > 0:
        cond &= (df["operating_margin"].fillna(-1) >= crit["min_operating_margin"])
    
    if crit["max_debt_to_equity"] > 0:
        cond &= (df["debt_to_equity"].fillna(999) <= crit["max_debt_to_equity"])
    
    if crit["min_current_ratio"] > 0:
        cond &= (df["current_ratio"].fillna(-1) >= crit["min_current_ratio"])
    
    if crit["min_quick_ratio"] > 0:
        cond &= (df["quick_ratio"].fillna(-1) >= crit["min_quick_ratio"])
    
    if crit["min_dividend_yield"] > 0:
        cond &= (df["dividend_yield"].fillna(-1) >= crit["min_dividend_yield"])
    
    return df[cond].sort_values(by=["profit_cagr_3y","roe"], ascending=False)


if scan:
    try:
        status = st.empty()
        with status.container():
            st.markdown("""
            <div class="metric-card">
                <h3>🚀 Scan in progress</h3>
                <p>Fetching all Vietnam tickers…</p>
            </div>
            """, unsafe_allow_html=True)
        all_tickers = fetch_all_tickers()
        symbols = sorted(all_tickers["symbol"].unique().tolist())
        src = "live"
        if "_from_cache" in all_tickers.columns:
            src = "cache"
        if not symbols:
            errs = []
            try:
                errs = get_last_listing_errors()
            except Exception:
                errs = []
            msg = "Không lấy được danh sách toàn thị trường. VNDIRECT/vnstock có thể đang lỗi."
            if errs:
                msg += "\n\nChi tiết: " + " | ".join(errs[-5:])
            st.error(msg)
            st.stop()
        if "_from_cache" in all_tickers.columns:
            st.info("Đang dùng danh sách mã từ cache (lần scan trước) do nguồn live tạm thời lỗi.")
        try:
            st.write(f"Universe size: {len(symbols)} tickers")
        except Exception:
            pass
        # Build symbol→exchange map and VN30 set for segmentation
        ex_map = {}
        try:
            if isinstance(all_tickers, pd.DataFrame) and 'exchange' in all_tickers.columns:
                ex_map = {str(r['symbol']).upper(): str(r['exchange']).upper() for _, r in all_tickers.iterrows()}
        except Exception:
            ex_map = {}
        vn30_set = {"ACB","BCM","BID","BVH","CTG","FPT","GAS","GVR","HDB","HPG",
                    "MBB","MSN","MWG","PLX","POW","SAB","SSI","STB","TCB","TPB",
                    "VCB","VHM","VIB","VIC","VJC","VNM","VPB","VRE","VSH"}

        with status.container():
            st.markdown(f"""
            <div class="metric-card">
                <h3>📥 Downloading data</h3>
                <p>Downloading data for {len(symbols)} tickers…</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Live progress + live log (no prefilled data)
        progress_bar = st.progress(0)
        status_text = st.empty()
        log_container = st.empty()

        # Streaming table placeholders for four segments
        st.markdown("""
        <div class="metric-card">
            <h3>📊 Streaming Results</h3>
        </div>
        """, unsafe_allow_html=True)
        seg_vn30_ph = st.empty()
        seg_hose_ph = st.empty()
        seg_hnx_ph = st.empty()
        seg_upcom_ph = st.empty()

        # Accumulators for streaming rows (use dict to avoid nonlocal issues)
        seg = {
            'vn30': pd.DataFrame(),
            'HOSE': pd.DataFrame(),
            'HNX': pd.DataFrame(),
            'UPCOM': pd.DataFrame(),
        }
        # Buffers for incremental Supabase writes
        batch_size = 25
        pending: Dict[str, List[Dict]] = {'HOSE': [], 'HNX': [], 'UPCOM': [], 'VN30': []}

        def flush_pending():
            try:
                total = 0
                for ex_key, rows in pending.items():
                    if not rows:
                        continue
                    df_flush = pd.DataFrame(rows)
                    df_flush['exchange'] = ex_key
                    supabase_storage.save_stocks_data(df_flush, ex_key)
                    total += len(rows)
                    pending[ex_key].clear()
                if total > 0:
                    add_log(f"💾 Flushed {total} rows to Supabase")
            except Exception:
                add_log("❌ Error flushing to Supabase (will retry next batch)")

        if 'live_logs' not in st.session_state:
            st.session_state['live_logs'] = []

        def add_log(message: str):
            st.session_state['live_logs'].append(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
            # Keep last 200 lines max
            if len(st.session_state['live_logs']) > 200:
                st.session_state['live_logs'] = st.session_state['live_logs'][-200:]
            # Update the placeholder with latest tail (no key to avoid conflicts)
            log_container.text_area(
                label="Live Log",
                value="\n".join(st.session_state['live_logs'][-20:]),
                height=220,
            )
        
        def update_progress(current, total, symbol):
            progress_bar.progress(min(max(current / total, 0.0), 1.0))
            status_text.text(f"Processing {symbol} ({current}/{total})")
            # Also write to live log so user sees where we are
            try:
                add_log(f"Scanning {current}/{total}: {symbol}")
            except Exception:
                pass

        # Now that add_log is defined, record universe source
        try:
            add_log(f"Universe source: {src}, symbols: {len(symbols)}")
        except Exception:
            pass

        # Resume from Supabase: skip symbols that already exist there
        try:
            sup_data = supabase_storage.load_all_exchanges_data()
            existing_syms: Set[str] = set()
            for ex_df in (sup_data or {}).values():
                try:
                    # ex_df is a DataFrame
                    if hasattr(ex_df, 'symbol') or ('symbol' in ex_df.columns):
                        existing_syms.update(str(s).upper() for s in list(ex_df['symbol'].dropna().astype(str)))
                except Exception:
                    pass
            if existing_syms:
                before = len(symbols)
                symbols = [s for s in symbols if s.upper() not in existing_syms]
                add_log(f"⏩ Resume mode: skipping {before - len(symbols)} existing symbols, remaining {len(symbols)}")
        except Exception:
            add_log("⚠️ Resume check failed; scanning all symbols")

        add_log("📊 Starting data calculation…")

        # Streaming callback: append row and refresh segment tables
        def on_row_stream(row: Dict):
            try:
                r = pd.DataFrame([row])
                sym = str(row.get('symbol', '')).upper()
                ex = str(ex_map.get(sym, row.get('exchange', ''))).upper()

                # Attach live price for this symbol
                try:
                    price_map_single = fetch_prices_vnd([sym])
                    if sym in price_map_single and pd.notna(price_map_single[sym]):
                        r['price_vnd'] = price_map_single[sym]
                except Exception:
                    pass

                # Format the row data for display
                r_formatted = r.copy()
                
                # Clean company names
                if 'company_name' in r_formatted.columns:
                    def clean_company_name(x):
                        if pd.isna(x) or any(js_word in str(x).lower() for js_word in ['$', 'function', 'document', 'ready', 'click', 'hide']):
                            return "N/A"
                        name = str(x).strip()
                        name = name.replace(' - VietstockFinance', '').replace(' - Vietstock', '').replace(' | VietstockFinance', '').replace(' | Vietstock', '')
                        import re
                        name = re.sub(r'\s*[-|]\s*[A-Z]{2,4}$', '', name)
                        return name
                    r_formatted['company_name'] = r_formatted['company_name'].apply(clean_company_name)
                
                # Format percentages
                percentage_cols = ['revenue_cagr_3y', 'profit_cagr_3y', 'roe', 'roa', 'free_float', 'foreign_ownership', 'npl_ratio', 'llr']
                for col in percentage_cols:
                    if col in r_formatted.columns:
                        r_formatted[col] = r_formatted[col].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
                
                # Format price
                if 'price_vnd' in r_formatted.columns:
                    r_formatted['price_vnd'] = r_formatted['price_vnd'].apply(lambda x: f"{x:,.0f} VND" if pd.notna(x) and x > 0 else "N/A")
                
                # Format EPS
                if 'eps' in r_formatted.columns:
                    r_formatted['eps'] = r_formatted['eps'].apply(lambda x: f"{float(x):,.0f}" if pd.notna(x) else "N/A")
                if 'eps_norm' in r_formatted.columns:
                    r_formatted['eps_norm'] = r_formatted['eps_norm'].apply(lambda x: f"{float(x):,.0f}" if pd.notna(x) else "N/A")
                
                # Format ratios
                if 'pe' in r_formatted.columns:
                    r_formatted['pe'] = r_formatted['pe'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
                if 'pb' in r_formatted.columns:
                    r_formatted['pb'] = r_formatted['pb'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
                if 'peg' in r_formatted.columns:
                    r_formatted['peg'] = r_formatted['peg'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
                if 'ev_ebitda' in r_formatted.columns:
                    r_formatted['ev_ebitda'] = r_formatted['ev_ebitda'].apply(lambda x: f"{x:.1f}x" if pd.notna(x) else "N/A")
                
                # Format margins
                if 'gross_margin' in r_formatted.columns:
                    r_formatted['gross_margin'] = r_formatted['gross_margin'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
                if 'operating_margin' in r_formatted.columns:
                    r_formatted['operating_margin'] = r_formatted['operating_margin'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
                if 'dividend_yield' in r_formatted.columns:
                    r_formatted['dividend_yield'] = r_formatted['dividend_yield'].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
                
                # Format trading value
                if 'avg_trading_value' in r_formatted.columns:
                    r_formatted['avg_trading_value'] = r_formatted['avg_trading_value'].apply(lambda x: f"{x:.1f}B VND/day" if pd.notna(x) and x > 0 else "N/A")
                
                # Format valuation
                if 'est_val' in r_formatted.columns:
                    r_formatted['est_val'] = r_formatted['est_val'].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")
                if 'market_val' in r_formatted.columns:
                    r_formatted['market_val'] = r_formatted['market_val'].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")

                # VN30 membership: show also in VN30 segment
                if sym in vn30_set:
                    seg['vn30'] = pd.concat([seg['vn30'], r_formatted], ignore_index=True)
                    seg_vn30_ph.dataframe(seg['vn30'])
                    rec = dict(row); rec['exchange'] = 'VN30'; pending['VN30'].append(rec)

                if ex == 'HOSE':
                    seg['HOSE'] = pd.concat([seg['HOSE'], r_formatted], ignore_index=True)
                    seg_hose_ph.dataframe(seg['HOSE'])
                    rec = dict(row); rec['exchange'] = 'HOSE'; pending['HOSE'].append(rec)
                elif ex == 'HNX':
                    seg['HNX'] = pd.concat([seg['HNX'], r_formatted], ignore_index=True)
                    seg_hnx_ph.dataframe(seg['HNX'])
                    rec = dict(row); rec['exchange'] = 'HNX'; pending['HNX'].append(rec)
                elif ex == 'UPCOM':
                    seg['UPCOM'] = pd.concat([seg['UPCOM'], r_formatted], ignore_index=True)
                    seg_upcom_ph.dataframe(seg['UPCOM'])
                    rec = dict(row); rec['exchange'] = 'UPCOM'; pending['UPCOM'].append(rec)

                # Flush in batches
                total_pending = sum(len(v) for v in pending.values())
                if total_pending >= batch_size:
                    flush_pending()
            except Exception:
                pass

        metrics = calculate_metrics_streaming(
            symbols,
            on_progress=update_progress,
            on_log=add_log,
            on_row=on_row_stream,
        )
        # Cache results in session so page refresh won't re-scan
        st.session_state['last_scan_symbols'] = symbols
        st.session_state['last_scan_metrics'] = metrics.copy() if isinstance(metrics, pd.DataFrame) else pd.DataFrame()
        st.session_state['symbol_to_exchange'] = ex_map
        st.session_state['vn30_set'] = list(vn30_set)
        # Attach live prices (VND)
        try:
            price_map = fetch_prices_vnd(symbols)
            if isinstance(metrics, pd.DataFrame) and not metrics.empty:
                metrics["price_vnd"] = metrics["symbol"].map(lambda s: price_map.get(s))
                add_log("💰 Attached price_vnd for symbols")
        except Exception as _e:
            add_log("⚠️ Failed to attach price_vnd")
        add_log(f"✅ Completed processing {len(metrics)} symbols")
        # Final flush
        try:
            flush_pending()
        except Exception:
            pass

        with status.container():
            st.markdown("""
            <div class="metric-card">
                <h3>✅ Scan completed</h3>
                <p>Data loaded successfully. Rows were streamed live during scan.</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Clear progress indicators and streaming results
        progress_bar.empty()
        status_text.empty()
        log_container.empty()
        
        # Clear streaming result placeholders
        seg_vn30_ph.empty()
        seg_hose_ph.empty()
        seg_hnx_ph.empty()
        seg_upcom_ph.empty()
        
        # Clear live logs
        if 'live_logs' in st.session_state:
            st.session_state['live_logs'] = []
        
        # Clear streaming results from session state
        if 'streaming_results' in st.session_state:
            st.session_state['streaming_results'] = {}
        
        # Set scan status to completed
        st.session_state['scan_status'] = 'completed'
    except Exception as e:
        st.error(f"Scan failed: {e}")
        metrics = pd.DataFrame()  # Initialize empty DataFrame on error
    finally:
        # Always reset to avoid auto-scan on refresh
        try:
            st.session_state['force_scan'] = False
        except Exception:
            pass

    # Debug: Check metrics status
    st.info(f"🔍 Debug: metrics.empty = {metrics.empty}, shape = {metrics.shape if not metrics.empty else 'N/A'}")

    # Ensure required columns exist to avoid KeyError on empty/partial data
    if not metrics.empty:
        required_cols = [
            "symbol",
            "company_name",
            "price_vnd",
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
            "avg_trading_value",
            "est_val",
            "market_val",
        ]
        for col in required_cols:
            if col not in metrics.columns:
                metrics[col] = np.nan

        # Build data quality helpers (EPS normalization and liquidity flags)
        def _normalize_eps(v):
            try:
                v = float(v)
            except Exception:
                return np.nan
            if v <= 0:
                return v
            return v * 1000.0 if v < 1000 else v

        try:
            if 'eps' in metrics.columns:
                metrics['eps_norm'] = metrics['eps'].apply(_normalize_eps)
                # flags removed per request
        except Exception:
            pass

        for col in ["current_ratio", "quick_ratio"]:
            if col not in metrics.columns:
                metrics[col] = np.nan
        try:
            # ratio flags removed
            pass
        except Exception:
            pass

    # Last scan time (GMT+7)
    ts = datetime.now(timezone(timedelta(hours=7)))
    st.caption(f"Last scan: {ts.strftime('%Y-%m-%d %H:%M:%S')} GMT+7")
    
    # Save data to Supabase
    try:
        add_log("💾 Saving data to Supabase...")
        
        # Prepare data by exchange
        data_by_exchange = {}
        for exchange in ['HOSE', 'HNX', 'UPCOM']:
            exchange_symbols = [s for s in symbols if ex_map.get(s, '').upper() == exchange]
            if exchange_symbols:
                exchange_df = metrics[metrics['symbol'].isin(exchange_symbols)].copy()
                if not exchange_df.empty:
                    data_by_exchange[exchange] = exchange_df
        
        # Save to Supabase
        if data_by_exchange:
            success = supabase_storage.save_all_exchanges_data(data_by_exchange)
            if success:
                add_log("✅ Data saved to Supabase successfully")
                add_log("🎉 Scan completed! Live logs and streaming results will be cleared.")
                
                # Clear live logs and streaming results after successful scan
                if 'live_logs' in st.session_state:
                    st.session_state['live_logs'] = []
                if 'streaming_results' in st.session_state:
                    st.session_state['streaming_results'] = {}
                if 'scan_status' in st.session_state:
                    st.session_state['scan_status'] = 'completed'
                
                # Ensure we do NOT auto-scan on rerun; then refresh UI
                st.session_state['force_scan'] = False
                st.rerun()
            else:
                add_log("❌ Failed to save data to Supabase")
        else:
            add_log("⚠️ No data to save to Supabase")
            
    except Exception as e:
        add_log(f"❌ Error saving to Supabase: {str(e)}")
    
    # Reset force_scan after successful scan to prevent loops
    st.session_state['force_scan'] = False

    # Display results with TitanLabs-style cards
    st.markdown("""
    <div class="metric-card">
        <h3>📈 Raw Metrics Overview</h3>
    </div>
    """, unsafe_allow_html=True)

    # Top toolbar: quick filters and view options
    tt1, tt2, tt3, tt4 = st.columns([2,1,1,1])
    with tt1:
        quick_search = st.text_input("Filter by symbol (comma-separated)", placeholder="e.g. FPT, VNM, VCB")
    with tt2:
        rows_per_page = st.number_input("Rows per page", min_value=10, max_value=200, value=30, step=10)
    with tt3:
        page = st.number_input("Page", min_value=1, value=1, step=1)
    with tt4:
        compact = st.checkbox("Compact mode", value=True)
    if not metrics.empty:
        # Format metrics with units for better readability
        display_metrics = metrics.copy()

        # Apply quick filter by symbol list
        if quick_search:
            try:
                wanted = [s.strip().upper() for s in quick_search.split(',') if s.strip()]
                if wanted:
                    display_metrics = display_metrics[display_metrics['symbol'].isin(wanted)]
            except Exception:
                pass

        # Sorting controls (low→high / high→low) applied on raw numeric values
        st.markdown("### Sort options")
        sort_cols_candidates = [
            c for c in display_metrics.columns
            if c not in ['symbol'] and str(display_metrics[c].dtype) != 'object'
        ]
        sc1, sc2 = st.columns([2,1])
        with sc1:
            sort_col = st.selectbox(
                "Select metric to sort",
                options=sorted(sort_cols_candidates),
                index=sorted(sort_cols_candidates).index('pe') if 'pe' in sort_cols_candidates else 0,
            ) if sort_cols_candidates else (None)
        with sc2:
            sort_order = st.radio("Order", options=["Low → High", "High → Low"], index=0, horizontal=True)

        try:
            if sort_col:
                ascending = True if sort_order == "Low → High" else False
                display_metrics = display_metrics.sort_values(by=[sort_col], ascending=ascending, na_position='last')
        except Exception:
            pass

        # Stable column order for readability
        preferred_cols = [
            'symbol','company_name','price_vnd','eps','eps_norm','revenue_cagr_3y','profit_cagr_3y','pe','pb','peg','roe','roa',
            'ev_ebitda','gross_margin','operating_margin','debt_to_equity','debt_to_asset',
            'current_ratio','quick_ratio','free_float','foreign_ownership',
            'npl_ratio','llr','avg_trading_value','est_val','market_val'
        ]
        existing_cols = [c for c in preferred_cols if c in display_metrics.columns]
        remaining_cols = [c for c in display_metrics.columns if c not in existing_cols]
        display_metrics = display_metrics[existing_cols + remaining_cols]
        
        # Format percentages (multiply by 100 and add %)
        percentage_cols = ['revenue_cagr_3y', 'profit_cagr_3y', 'roe', 'roa', 'free_float', 'foreign_ownership', 'npl_ratio', 'llr']
        for col in percentage_cols:
            if col in display_metrics.columns:
                display_metrics[col] = display_metrics[col].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
        
        # Format price (VND)
        if 'price_vnd' in display_metrics.columns:
            display_metrics['price_vnd'] = display_metrics['price_vnd'].apply(lambda x: f"{x:,.0f} VND" if pd.notna(x) and x > 0 else "N/A")
        
        # Clean company names - remove JavaScript code, VietstockFinance, and ticker symbols
        if 'company_name' in display_metrics.columns:
            def clean_company_name(x):
                if pd.isna(x) or any(js_word in str(x).lower() for js_word in ['$', 'function', 'document', 'ready', 'click', 'hide']):
                    return "N/A"
                name = str(x).strip()
                # Remove "VietstockFinance" and similar suffixes
                name = name.replace(' - VietstockFinance', '').replace(' - Vietstock', '').replace(' | VietstockFinance', '').replace(' | Vietstock', '')
                # Remove ticker symbols at the end (e.g., " - AAM", " | AAM")
                import re
                name = re.sub(r'\s*[-|]\s*[A-Z]{2,4}$', '', name)
                return name
            display_metrics['company_name'] = display_metrics['company_name'].apply(clean_company_name)

        # Format eps/eps_norm
        if 'eps' in display_metrics.columns:
            display_metrics['eps'] = display_metrics['eps'].apply(lambda x: f"{float(x):,.0f}" if pd.notna(x) else "N/A")
        if 'eps_norm' in display_metrics.columns:
            display_metrics['eps_norm'] = display_metrics['eps_norm'].apply(lambda x: f"{float(x):,.0f}" if pd.notna(x) else "N/A")

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
        
        if 'operating_margin' in display_metrics.columns:
            display_metrics['operating_margin'] = display_metrics['operating_margin'].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A")
        
        if 'debt_to_equity' in display_metrics.columns:
            display_metrics['debt_to_equity'] = display_metrics['debt_to_equity'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
        
        if 'debt_to_asset' in display_metrics.columns:
            display_metrics['debt_to_asset'] = display_metrics['debt_to_asset'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
        
        if 'current_ratio' in display_metrics.columns:
            display_metrics['current_ratio'] = display_metrics['current_ratio'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
        
        if 'quick_ratio' in display_metrics.columns:
            display_metrics['quick_ratio'] = display_metrics['quick_ratio'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
        
        # EPS already formatted above; avoid re-formatting to prevent N/A overrides
        
        if 'book_value_per_share' in display_metrics.columns:
            display_metrics['book_value_per_share'] = display_metrics['book_value_per_share'].apply(lambda x: f"{float(x):.0f}" if pd.notna(x) and str(x).replace('.','').replace('-','').isdigit() else "N/A")
        
        if 'dividend_yield' in display_metrics.columns:
            display_metrics['dividend_yield'] = display_metrics['dividend_yield'].apply(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "N/A")
        
        if 'free_cash_flow' in display_metrics.columns:
            display_metrics['free_cash_flow'] = display_metrics['free_cash_flow'].apply(lambda x: f"{x:.1f}B" if pd.notna(x) else "N/A")
        
        if 'operating_cash_flow' in display_metrics.columns:
            display_metrics['operating_cash_flow'] = display_metrics['operating_cash_flow'].apply(lambda x: f"{x:.1f}B" if pd.notna(x) else "N/A")
        
        # Format valuation columns
        if 'est_val' in display_metrics.columns:
            display_metrics['est_val'] = display_metrics['est_val'].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")
        
        if 'market_val' in display_metrics.columns:
            display_metrics['market_val'] = display_metrics['market_val'].apply(lambda x: f"{x:.1f}B VND" if pd.notna(x) and x > 0 else "N/A")
        
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
        # Pagination
        try:
            total_rows = len(display_metrics)
            start = max((page-1)*rows_per_page, 0)
            end = min(start + rows_per_page, total_rows)
            page_df = display_metrics.iloc[start:end]
        except Exception:
            page_df = display_metrics

        # Compact mode CSS
        if compact:
            st.markdown("""
            <style>
            .stDataFrame {font-size: 12px}
            .stDataFrame table td, .stDataFrame table th {padding: 4px 8px}
            </style>
            """, unsafe_allow_html=True)

        # Highlight price_vnd column for emphasis
        def _highlight_price(row: pd.Series) -> List[str]:
            styles = [''] * len(row)
            try:
                col_idx = list(page_df.columns).index('price_vnd') if 'price_vnd' in page_df.columns else -1
                if col_idx >= 0:
                    styles[col_idx] = 'font-weight:700; color:#1f6feb;'
            except Exception:
                pass
            return styles

        try:
            st.dataframe(
                page_df.style.apply(_highlight_price, axis=1),
                column_config=raw_column_config,
                use_container_width=True,
                hide_index=True,
            )
        except Exception:
            st.dataframe(page_df, column_config=raw_column_config, use_container_width=True, hide_index=True)
        st.caption(f"Showing {len(page_df)}/{len(display_metrics)} (Total: {len(metrics)})")

        # Segmented tables: VN30, HOSE, HNX, UPCOM
        ex_map = st.session_state.get('symbol_to_exchange', {})
        if not ex_map:
            try:
                at = fetch_all_tickers()
                if isinstance(at, pd.DataFrame) and 'symbol' in at.columns and 'exchange' in at.columns:
                    ex_map = {str(r['symbol']).upper(): str(r['exchange']).upper() for _, r in at.iterrows()}
            except Exception:
                ex_map = {}
        vn30_set = set(st.session_state.get('vn30_set', [])) or {"ACB","BCM","BID","BVH","CTG","FPT","GAS","GVR","HDB","HPG",
                    "MBB","MSN","MWG","PLX","POW","SAB","SSI","STB","TCB","TPB",
                    "VCB","VHM","VIB","VIC","VJC","VNM","VPB","VRE","VSH"}
        try:
            metrics_seg = metrics.copy()
            metrics_seg['exchange'] = metrics_seg['symbol'].map(lambda s: ex_map.get(s, ''))
        except Exception:
            metrics_seg = metrics.copy()
            if 'exchange' not in metrics_seg.columns:
                metrics_seg['exchange'] = ''

        def render_segment(title: str, df: pd.DataFrame):
            st.markdown(f"**{title}**")
            if df.empty:
                st.dataframe(pd.DataFrame())
                return
            try:
                st.dataframe(df)
            except Exception:
                st.dataframe(df)

        st.markdown("""
        <div class="metric-card">
            <h3>📚 Segmented Views</h3>
        </div>
        """, unsafe_allow_html=True)

        # VN30
        try:
            vn30_df = display_metrics[display_metrics['symbol'].isin(vn30_set)] if not display_metrics.empty else display_metrics
        except Exception:
            vn30_df = pd.DataFrame()
        render_segment("VN30", vn30_df)

        # HOSE / HNX / UPCOM based on original metrics exchange mapping
        for ex_name, ex_title in [("HOSE", "HOSE"), ("HNX", "HNX"), ("UPCOM", "UPCOM")]:
            try:
                syms = [s for s in metrics_seg.loc[metrics_seg['exchange'] == ex_name, 'symbol'].tolist()]
                seg_df = display_metrics[display_metrics['symbol'].isin(syms)] if syms else pd.DataFrame()
            except Exception:
                seg_df = pd.DataFrame()
            render_segment(ex_title, seg_df)
    else:
        # If no fresh metrics, try to load from Supabase first, then fallback to cache
        metrics = pd.DataFrame()
        
        # Try to load from Supabase
        try:
            st.info("📥 Loading data from Supabase...")
            supabase_data = supabase_storage.load_all_exchanges_data()
            
            if supabase_data:
                # Combine all exchange data
                all_data = []
                for exchange, df in supabase_data.items():
                    if not df.empty:
                        df['exchange'] = exchange
                        all_data.append(df)
                
                if all_data:
                    metrics = pd.concat(all_data, ignore_index=True)
                    st.success(f"✅ Loaded {len(metrics)} records from Supabase")
                    st.info(f"🔍 Debug: metrics shape = {metrics.shape}, columns = {list(metrics.columns)}")
                    
                    # Update session state with Supabase data
                    st.session_state['last_scan_metrics'] = metrics.copy()
                    # Ensure exchange map exists for rendering
                    st.session_state['symbol_to_exchange'] = dict(zip(metrics['symbol'], metrics['exchange']))
                    
                    # Reset force_scan to prevent auto-scanning
                    st.session_state['force_scan'] = False

                    # Render segmented tables from Supabase data (VN30/HOSE/HNX/UPCOM)
                    st.markdown("""
                    <div class="metric-card">
                        <h3>📊 Market Boards</h3>
                    </div>
                    """, unsafe_allow_html=True)

                    ex_map = st.session_state.get('symbol_to_exchange', {})
                    # Static VN30 set to support cold loads
                    vn30_set = set(st.session_state.get('vn30_set', [])) or {"ACB","BCM","BID","BVH","CTG","FPT","GAS","GVR","HDB","HPG",
                                "MBB","MSN","MWG","PLX","POW","SAB","SSI","STB","TCB","TPB",
                                "VCB","VHM","VIB","VIC","VJC","VNM","VPB","VRE","VSH"}

                    def render_segment(title: str, df: pd.DataFrame):
                        st.markdown(f"**{title}**")
                        if df.empty:
                            st.dataframe(pd.DataFrame())
                        else:
                            st.dataframe(df)

                    display_metrics = metrics.copy()
                    try:
                        metrics_seg = display_metrics.copy()
                        # ensure exchange column exists
                        if 'exchange' not in metrics_seg.columns:
                            metrics_seg['exchange'] = metrics_seg['symbol'].map(lambda s: ex_map.get(s, ''))
                    except Exception:
                        metrics_seg = pd.DataFrame(columns=['symbol','exchange'])

                    # VN30
                    try:
                        vn30_df = display_metrics[display_metrics['symbol'].isin(vn30_set)] if not display_metrics.empty else display_metrics
                    except Exception:
                        vn30_df = pd.DataFrame()
                    render_segment("VN30", vn30_df)

                    # HOSE / HNX / UPCOM
                    for ex_name, ex_title in [("HOSE", "HOSE"), ("HNX", "HNX"), ("UPCOM", "UPCOM")]:
                        try:
                            syms = [s for s in metrics_seg.loc[metrics_seg['exchange'] == ex_name, 'symbol'].tolist()]
                            seg_df = display_metrics[display_metrics['symbol'].isin(syms)] if syms else pd.DataFrame()
                        except Exception:
                            seg_df = pd.DataFrame()
                        render_segment(ex_title, seg_df)
                else:
                    st.warning("⚠️ No data found in Supabase")
            else:
                st.warning("⚠️ Failed to load data from Supabase")
                
        except Exception as e:
            st.warning(f"⚠️ Error loading from Supabase: {str(e)}")
            import traceback
            st.error(f"Full error: {traceback.format_exc()}")
        
        # Fallback to cached metrics if Supabase failed
        if metrics.empty:
            cached = st.session_state.get('last_scan_metrics', pd.DataFrame())
            if isinstance(cached, pd.DataFrame) and not cached.empty:
                metrics = cached.copy()
                st.info("📋 Using cached data from previous scan")

                # Render segmented tables using cached data
                st.markdown("""
                <div class="metric-card">
                    <h3>📊 Market Boards</h3>
                </div>
                """, unsafe_allow_html=True)

                ex_map = st.session_state.get('symbol_to_exchange', {})
                if not ex_map:
                    try:
                        at = fetch_all_tickers()
                        if isinstance(at, pd.DataFrame) and 'symbol' in at.columns and 'exchange' in at.columns:
                            ex_map = {str(r['symbol']).upper(): str(r['exchange']).upper() for _, r in at.iterrows()}
                    except Exception:
                        ex_map = {}
                vn30_set = set(st.session_state.get('vn30_set', [])) or {"ACB","BCM","BID","BVH","CTG","FPT","GAS","GVR","HDB","HPG",
                            "MBB","MSN","MWG","PLX","POW","SAB","SSI","STB","TCB","TPB",
                            "VCB","VHM","VIB","VIC","VJC","VNM","VPB","VRE","VSH"}

                def render_segment(title: str, df: pd.DataFrame):
                    st.markdown(f"**{title}**")
                    if df.empty:
                        st.dataframe(pd.DataFrame())
                    else:
                        st.dataframe(df)

                display_metrics = metrics.copy()
                try:
                    metrics_seg = display_metrics.copy()
                    if 'exchange' not in metrics_seg.columns:
                        metrics_seg['exchange'] = metrics_seg['symbol'].map(lambda s: ex_map.get(s, ''))
                except Exception:
                    metrics_seg = pd.DataFrame(columns=['symbol','exchange'])

                # VN30
                try:
                    vn30_df = display_metrics[display_metrics['symbol'].isin(vn30_set)] if not display_metrics.empty else display_metrics
                except Exception:
                    vn30_df = pd.DataFrame()
                render_segment("VN30", vn30_df)

                # HOSE / HNX / UPCOM
                for ex_name, ex_title in [("HOSE", "HOSE"), ("HNX", "HNX"), ("UPCOM", "UPCOM")]:
                    try:
                        syms = [s for s in metrics_seg.loc[metrics_seg['exchange'] == ex_name, 'symbol'].tolist()]
                        seg_df = display_metrics[display_metrics['symbol'].isin(syms)] if syms else pd.DataFrame()
                    except Exception:
                        seg_df = pd.DataFrame()
                    render_segment(ex_title, seg_df)
            else:
                st.dataframe(pd.DataFrame({"note":["No data available. Click Scan to run a new scan."]}))

    # Per-criterion tables
    st.markdown("""
    <div class="metric-card">
        <h3>🎯 Per-Criterion Analysis</h3>
    </div>
    """, unsafe_allow_html=True)
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
            t_add = metrics[["symbol","ev_ebitda","gross_margin","free_float","est_val","market_val"]].copy()
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
                t_add.columns = ["Symbol", "EV/EBITDA", "Gross Margin", "Free Float", "Est Val", "Market Val"]
            add_column_config = {
                'Est Val': st.column_config.TextColumn(
                    'Est Val', help='Est Val (DCF 5Y): FCF=OCF−Capex; r=12%, g_terminal=3%; fallback EPS_next×Shares.'
                ),
                'Market Val': st.column_config.TextColumn(
                    'Market Val', help='CafeF "Vốn hóa thị trường (tỷ đồng)"; fallback Price×Shares.'
                ),
            }
            st.dataframe(t_add, column_config=add_column_config)

    # (Removed) Data Quality & Definitions block per request

    # Final pass
    st.markdown("""
    <div class="metric-card">
        <h3>🏆 Final Pass List</h3>
    </div>
    """, unsafe_allow_html=True)
    passed = apply_criteria(metrics, criteria) if not metrics.empty else pd.DataFrame()
    
    if not passed.empty:
        # Format final pass list with units
        display_passed = passed.copy()
        
        # Format percentages
        percentage_cols = ['revenue_cagr_3y', 'profit_cagr_3y', 'roe', 'roa', 'free_float', 'foreign_ownership']
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
            # removed management_ownership mapping
            'avg_trading_value': 'Avg Trading Value',
            'est_val': 'Est Val',
            'market_val': 'Market Val',
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
        label="📥 Download CSV",
        data=passed.to_csv(index=False),
        file_name="vn_screener_pass.csv",
        mime="text/csv",
    )
    
    # Footer with TitanLabs-style design
    st.markdown("""
    <div class="footer">
        <p>📊 Crownwell VNIndex Screener | Powered by Advanced Quantitative Analysis</p>
        <p>Data sourced from Vietstock Finance, CafeF, and TCBS APIs</p>
    </div>
    """, unsafe_allow_html=True)

    st.success(f"Completed. {len(passed)} symbols matched.")


