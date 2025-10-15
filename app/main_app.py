import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
import logging

# Import existing modules
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
    fetch_prices_vnd
)
from web_scraper import VietnamStockDataScraper
from supabase_helper import supabase_storage
from macro_dashboard_with_charts import create_macro_dashboard

# Setup logging
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Crownwell Investment Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    /* Main container styling */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1400px;
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
    
    /* Navigation styling */
    .nav-container {
        background: white;
        padding: 1rem;
        border-radius: 12px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        margin-bottom: 2rem;
    }
    
    .nav-button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.75rem 1.5rem;
        margin: 0.25rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .nav-button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    
    .nav-button.active {
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
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
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    
    /* Status indicators */
    .status-good {
        color: #27ae60;
        font-weight: bold;
    }
    
    .status-warning {
        color: #f39c12;
        font-weight: bold;
    }
    
    .status-danger {
        color: #e74c3c;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

def create_navigation():
    """Create navigation between different pages"""
    st.markdown("""
    <div class="nav-container">
        <div style="text-align: center;">
            <h2 style="margin: 0 0 1rem 0; color: #2c3e50;">Crownwell Investment Platform</h2>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Navigation buttons
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("📊 VN Stock Screener", key="nav_screener", use_container_width=True):
            st.session_state.current_page = "screener"
            st.rerun()
    
    with col2:
        if st.button("📈 Macro Data Dashboard", key="nav_macro", use_container_width=True):
            st.session_state.current_page = "macro"
            st.rerun()
    
    with col3:
        if st.button("📋 Portfolio Analysis", key="nav_portfolio", use_container_width=True):
            st.session_state.current_page = "portfolio"
            st.rerun()
    
    with col4:
        if st.button("⚙️ Settings", key="nav_settings", use_container_width=True):
            st.session_state.current_page = "settings"
            st.rerun()

def create_vn_stock_screener():
    """Create the VN Stock Screener page (existing functionality)"""
    
    st.markdown("""
    <div class="main-header">
        <h1>📊 VN Stock Screener</h1>
        <p>Advanced Vietnamese Stock Analysis & Screening</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize session state
    if 'scraper' not in st.session_state:
        st.session_state.scraper = VietnamStockDataScraper()
    
    if 'scan_results' not in st.session_state:
        st.session_state.scan_results = None
    
    if 'scan_progress' not in st.session_state:
        st.session_state.scan_progress = 0
    
    # Sidebar for screening criteria
    st.sidebar.markdown("### 🔍 Screening Criteria")
    
    # Basic criteria
    criteria = {}
    
    st.sidebar.markdown("#### Financial Performance")
    criteria["min_revenue_cagr_3y"] = st.sidebar.number_input(
        "Min Revenue CAGR 3Y (%)", min_value=0.0, max_value=100.0, value=10.0, step=1.0
    )
    criteria["min_profit_cagr_3y"] = st.sidebar.number_input(
        "Min Profit CAGR 3Y (%)", min_value=0.0, max_value=100.0, value=15.0, step=1.0
    )
    criteria["min_roe"] = st.sidebar.number_input(
        "Min ROE (%)", min_value=0.0, max_value=100.0, value=15.0, step=1.0
    )
    criteria["min_roa"] = st.sidebar.number_input(
        "Min ROA (%)", min_value=0.0, max_value=100.0, value=8.0, step=1.0
    )
    criteria["max_pb"] = st.sidebar.number_input(
        "Max P/B Ratio", min_value=0.0, max_value=20.0, value=3.0, step=0.1
    )
    criteria["max_pe"] = st.sidebar.number_input(
        "Max P/E Ratio", min_value=0.0, max_value=100.0, value=20.0, step=1.0
    )
    
    st.sidebar.markdown("### 📊 Additional Criteria")
    criteria["max_peg"] = st.sidebar.number_input(
        "Max PEG Ratio", min_value=0.0, max_value=10.0, value=2.0, step=0.1
    )
    criteria["max_ev_ebitda"] = st.sidebar.number_input(
        "Max EV/EBITDA", min_value=0.0, max_value=50.0, value=15.0, step=1.0
    )
    criteria["min_gross_margin"] = st.sidebar.number_input(
        "Min Gross Margin (%)", min_value=0.0, max_value=100.0, value=20.0, step=1.0
    )
    criteria["min_free_float"] = st.sidebar.number_input(
        "Min Free Float (%)", min_value=0.0, max_value=100.0, value=15.0, step=1.0
    )
    criteria["min_market_cap_billion"] = st.sidebar.number_input(
        "Min Market Cap (Billion VND)", min_value=0.0, max_value=1000.0, value=1.0, step=0.1
    )
    criteria["min_foreign_ownership"] = st.sidebar.number_input(
        "Min Foreign Ownership (%)", min_value=0.0, max_value=100.0, value=5.0, step=1.0
    )
    criteria["min_avg_trading_value_billion"] = st.sidebar.number_input(
        "Min Avg Trading Value (Billion VND)", min_value=0.0, max_value=100.0, value=1.0, step=0.1
    )
    
    st.sidebar.markdown("### 📊 Additional Metrics")
    criteria["max_ev_ebitda"] = st.sidebar.number_input(
        "Max EV/EBITDA", min_value=0.0, max_value=50.0, value=15.0, step=1.0
    )
    criteria["min_gross_margin"] = st.sidebar.number_input(
        "Min Gross Margin (%)", min_value=0.0, max_value=100.0, value=20.0, step=1.0
    )
    criteria["min_operating_margin"] = st.sidebar.number_input(
        "Min Operating Margin (%)", min_value=0.0, max_value=100.0, value=10.0, step=1.0
    )
    criteria["max_debt_to_equity"] = st.sidebar.number_input(
        "Max Debt-to-Equity", min_value=0.0, max_value=10.0, value=1.0, step=0.1
    )
    criteria["min_current_ratio"] = st.sidebar.number_input(
        "Min Current Ratio", min_value=0.0, max_value=10.0, value=1.2, step=0.1
    )
    criteria["min_quick_ratio"] = st.sidebar.number_input(
        "Min Quick Ratio", min_value=0.0, max_value=10.0, value=1.0, step=0.1
    )
    criteria["min_dividend_yield"] = st.sidebar.number_input(
        "Min Dividend Yield (%)", min_value=0.0, max_value=20.0, value=2.0, step=0.1
    )
    
    # Additional options
    side_by_side = st.sidebar.checkbox(
        "Show side-by-side comparison", value=False,
        help="Display metrics in side-by-side format"
    )
    
    # Exchange selection
    st.sidebar.markdown("### 🏢 Exchange Selection")
    exchanges = st.sidebar.multiselect(
        "Select Exchanges",
        ["HOSE", "HNX", "UPCOM"],
        default=["HOSE", "HNX"],
        help="Choose which exchanges to scan"
    )
    
    # Main content area
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.markdown("### 🚀 Quick Stock Scan")
        
        if st.button("🔍 Start Comprehensive Scan", type="primary", use_container_width=True):
            with st.spinner("Scanning Vietnamese stocks..."):
                try:
                    # Get all tickers
                    tickers_df = fetch_all_tickers(exchanges)
                    symbols = tickers_df['symbol'].tolist()
                    
                    if not symbols:
                        st.error("No symbols found. Please check your exchange selection.")
                        return
                    
                    st.info(f"Found {len(symbols)} symbols to analyze")
                    
                    # Initialize progress
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # Scan stocks
                    results = []
                    total_symbols = len(symbols)
                    
                    for i, symbol in enumerate(symbols):
                        try:
                            status_text.text(f"Analyzing {symbol} ({i+1}/{total_symbols})")
                            
                            # Get stock data
                            stock_data = st.session_state.scraper.get_stock_overview(symbol)
                            
                            # Get financial statements
                            income_df = fetch_income_statement(symbol)
                            ratios_df = fetch_ratios(symbol)
                            balance_df = fetch_balance_sheet(symbol)
                            cash_flow_df = fetch_cash_flow(symbol)
                            
                            # Calculate metrics
                            metrics = extract_additional_metrics(ratios_df, cash_flow_df)
                            
                            # Calculate ROE and ROA
                            roe, roa = compute_roe_roa_from_statements(income_df, balance_df)
                            
                            # Calculate CAGR
                            if not income_df.empty:
                                revenue_cagr = compute_cagr(income_df['revenue'], 3)
                                profit_cagr = compute_cagr(income_df['post_tax_profit'], 3)
                            else:
                                revenue_cagr = float('nan')
                                profit_cagr = float('nan')
                            
                            # Get current price
                            price = fetch_prices_vnd([symbol]).get(symbol)
                            
                            # Create result record
                            result = {
                                'symbol': symbol,
                                'company_name': stock_data.get('company_name', ''),
                                'current_price': price,
                                'market_cap': stock_data.get('market_cap', 0),
                                'free_float': stock_data.get('free_float', 0),
                                'foreign_ownership': stock_data.get('foreign_ownership', 0),
                                'avg_trading_value': stock_data.get('avg_trading_value', 0),
                                'pe_ratio': stock_data.get('pe_ratio', 0),
                                'pb_ratio': stock_data.get('pb_ratio', 0),
                                'roe': roe,
                                'roa': roa,
                                'revenue_cagr_3y': revenue_cagr,
                                'profit_cagr_3y': profit_cagr,
                                **metrics
                            }
                            
                            results.append(result)
                            
                        except Exception as e:
                            logger.warning(f"Error analyzing {symbol}: {e}")
                            continue
                        
                        # Update progress
                        progress = (i + 1) / total_symbols
                        progress_bar.progress(progress)
                    
                    # Store results
                    st.session_state.scan_results = pd.DataFrame(results)
                    st.session_state.scan_progress = 100
                    
                    status_text.text("Analysis complete!")
                    st.success(f"Successfully analyzed {len(results)} stocks")
                    
                except Exception as e:
                    st.error(f"Error during scan: {e}")
                    logger.error(f"Scan error: {e}")
    
    with col2:
        st.markdown("### 📊 Scan Status")
        
        if st.session_state.scan_results is not None:
            st.success(f"✅ Scan Complete")
            st.metric("Stocks Analyzed", len(st.session_state.scan_results))
            
            # Show summary statistics
            df = st.session_state.scan_results
            
            if not df.empty:
                st.markdown("#### Summary Statistics")
                
                # Market cap distribution
                market_cap_stats = df['market_cap'].describe()
                st.metric("Avg Market Cap", f"{market_cap_stats['mean']:.1f}B VND")
                
                # ROE distribution
                roe_stats = df['roe'].describe()
                st.metric("Avg ROE", f"{roe_stats['mean']:.1f}%")
                
                # P/E distribution
                pe_stats = df['pe_ratio'].describe()
                st.metric("Avg P/E", f"{pe_stats['mean']:.1f}")
        else:
            st.info("No scan results available")
    
    # Display results
    if st.session_state.scan_results is not None and not st.session_state.scan_results.empty:
        st.markdown("### 📋 Scan Results")
        
        df = st.session_state.scan_results.copy()
        
        # Apply filters
        filtered_df = df.copy()
        
        for key, value in criteria.items():
            if key in df.columns and not pd.isna(value):
                if key.startswith('min_'):
                    filtered_df = filtered_df[filtered_df[key] >= value]
                elif key.startswith('max_'):
                    filtered_df = filtered_df[filtered_df[key] <= value]
        
        st.info(f"Showing {len(filtered_df)} stocks matching criteria (out of {len(df)} total)")
        
        if not filtered_df.empty:
            # Display options
            col1, col2, col3 = st.columns(3)
            
            with col1:
                rows_per_page = st.number_input("Rows per page", min_value=10, max_value=200, value=30, step=10)
            
            with col2:
                page = st.number_input("Page", min_value=1, value=1, step=1)
            
            with col3:
                sort_col = st.selectbox(
                    "Sort by",
                    ['market_cap', 'roe', 'pe_ratio', 'pb_ratio', 'revenue_cagr_3y', 'profit_cagr_3y'],
                    index=0
                )
            
            # Sort data
            if sort_col in filtered_df.columns:
                filtered_df = filtered_df.sort_values(sort_col, ascending=False)
            
            # Pagination
            total_rows = len(filtered_df)
            max_page = (total_rows - 1) // rows_per_page + 1
            
            if page > max_page:
                page = max_page
            
            start = max((page-1)*rows_per_page, 0)
            end = min(start + rows_per_page, total_rows)
            page_df = filtered_df.iloc[start:end]
            
            # Display data
            if side_by_side:
                # Side-by-side format
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### Financial Metrics")
                    financial_cols = ['symbol', 'company_name', 'current_price', 'market_cap', 'pe_ratio', 'pb_ratio', 'roe', 'roa']
                    financial_df = page_df[financial_cols]
                    st.dataframe(financial_df, use_container_width=True)
                
                with col2:
                    st.markdown("#### Growth & Ownership")
                    growth_cols = ['symbol', 'revenue_cagr_3y', 'profit_cagr_3y', 'free_float', 'foreign_ownership', 'avg_trading_value']
                    growth_df = page_df[growth_cols]
                    st.dataframe(growth_df, use_container_width=True)
            else:
                # Standard format
                st.dataframe(page_df, use_container_width=True)
            
            st.caption(f"Showing {len(page_df)}/{len(filtered_df)} stocks (Page {page}/{max_page})")
        else:
            st.warning("No stocks match the selected criteria")

def create_portfolio_analysis():
    """Create Portfolio Analysis page"""
    st.markdown("""
    <div class="main-header">
        <h1>📋 Portfolio Analysis</h1>
        <p>Advanced Portfolio Management & Analysis Tools</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.info("🚧 Portfolio Analysis feature is under development. Coming soon!")
    
    # Placeholder content
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <h3>📊 Portfolio Overview</h3>
            <p>Track your portfolio performance, allocation, and risk metrics.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
            <h3>📈 Performance Analysis</h3>
            <p>Analyze returns, volatility, and risk-adjusted performance.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-card">
            <h3>⚖️ Risk Management</h3>
            <p>Monitor risk exposure, diversification, and correlation analysis.</p>
        </div>
        """, unsafe_allow_html=True)

def create_settings():
    """Create Settings page"""
    st.markdown("""
    <div class="main-header">
        <h1>⚙️ Settings</h1>
        <p>Configure your investment platform preferences</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.info("🔧 Settings panel is under development. Coming soon!")
    
    # Placeholder content
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="metric-card">
            <h3>🔑 API Configuration</h3>
            <p>Configure API keys for FRED, Alpha Vantage, and other data sources.</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card">
            <h3>📊 Display Preferences</h3>
            <p>Customize charts, tables, and dashboard layouts.</p>
        </div>
        """, unsafe_allow_html=True)

def main():
    """Main application function"""
    
    # Initialize session state
    if 'current_page' not in st.session_state:
        st.session_state.current_page = "screener"
    
    # Create navigation
    create_navigation()
    
    # Route to appropriate page
    if st.session_state.current_page == "screener":
        create_vn_stock_screener()
    elif st.session_state.current_page == "macro":
        create_macro_dashboard()
    elif st.session_state.current_page == "portfolio":
        create_portfolio_analysis()
    elif st.session_state.current_page == "settings":
        create_settings()
    else:
        create_vn_stock_screener()  # Default to screener

if __name__ == "__main__":
    main()
