import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import time
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from macro_data_helper import MacroDataFetcher, analyze_macro_correlations, generate_macro_insights

def create_macro_dashboard():
    """Create the Macro Data Dashboard page with charts for each indicator"""
    
    st.markdown("""
    <div class="main-header">
        <h1>📈 Global Macro Economic Dashboard</h1>
        <p>Real-time Economic Indicators with Interactive Charts</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize macro data fetcher
    if 'macro_fetcher' not in st.session_state:
        st.session_state.macro_fetcher = MacroDataFetcher()
    
    fetcher = st.session_state.macro_fetcher
    
    # Sidebar controls
    st.sidebar.markdown("### 🔧 Dashboard Controls")
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh All Data", type="primary"):
        fetcher._cache.clear()
        fetcher._cache_expiry.clear()
        st.rerun()
    
    # Data source selection
    data_source = st.sidebar.selectbox(
        "Data Source",
        ["Live Data", "Mock Data"],
        help="Choose between live API data or mock data for demonstration"
    )
    
    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox("Auto-refresh every 5 minutes", value=False)
    
    if auto_refresh:
        if 'last_refresh' not in st.session_state:
            st.session_state.last_refresh = datetime.now()
        
        if datetime.now() - st.session_state.last_refresh > timedelta(minutes=5):
            fetcher._cache.clear()
            fetcher._cache_expiry.clear()
            st.session_state.last_refresh = datetime.now()
            st.rerun()
    
    # Main content
    with st.spinner("Loading macroeconomic data..."):
        if data_source == "Live Data":
            macro_data = fetcher.get_all_macro_data()
        else:
            macro_data = get_mock_macro_data()
    
    # Display last updated time
    last_updated = macro_data.get('last_updated', datetime.now().isoformat())
    st.caption(f"Last updated: {last_updated}")
    
    # Create tabs for different data categories
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Key Indicators", 
        "💰 Interest Rates", 
        "📈 Inflation", 
        "👥 Employment", 
        "🏭 Economic Activity", 
        "🔍 Analysis & Insights"
    ])
    
    with tab1:
        display_key_indicators_with_charts(macro_data)
    
    with tab2:
        display_interest_rates_with_charts(macro_data)
    
    with tab3:
        display_inflation_with_charts(macro_data)
    
    with tab4:
        display_employment_with_charts(macro_data)
    
    with tab5:
        display_economic_activity_with_charts(macro_data)
    
    with tab6:
        display_analysis_insights(macro_data)


def display_key_indicators_with_charts(macro_data):
    """Display key indicators with interactive charts"""
    st.subheader("🎯 Key Economic Indicators")
    
    # Create columns for key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # GDP Growth
        gdp_data = macro_data.get('gdp', {})
        gdp_growth = gdp_data.get('gdp_growth_yoy', 0)
        st.metric(
            label="GDP Growth (YoY)",
            value=f"{gdp_growth:.1f}%",
            delta=f"{gdp_growth - 2.5:.1f}% vs target",
            help="Year-over-year GDP growth rate"
        )
    
    with col2:
        # Fed Funds Rate
        fed_data = macro_data.get('fed_funds_rate', {})
        fed_rate = fed_data.get('current_rate', 0)
        st.metric(
            label="Fed Funds Rate",
            value=f"{fed_rate:.2f}%",
            delta=f"{fed_rate - 2.0:.2f}% vs neutral",
            help="Current Federal Funds Rate"
        )
    
    with col3:
        # CPI Inflation
        inflation_data = macro_data.get('inflation', {})
        cpi = inflation_data.get('cpi_yoy', 0)
        st.metric(
            label="CPI Inflation (YoY)",
            value=f"{cpi:.1f}%",
            delta=f"{cpi - 2.0:.1f}% vs target",
            help="Consumer Price Index year-over-year"
        )
    
    with col4:
        # Unemployment Rate
        employment_data = macro_data.get('employment', {})
        unemployment = employment_data.get('unemployment_rate', 0)
        st.metric(
            label="Unemployment Rate",
            value=f"{unemployment:.1f}%",
            delta=f"{unemployment - 4.0:.1f}% vs full employment",
            help="Current unemployment rate"
        )
    
    # Key Indicators Chart
    st.subheader("📊 Key Indicators Trend")
    
    # Create sample historical data for demonstration
    dates = pd.date_range(start='2023-01-01', end='2024-12-01', freq='M')
    
    # Generate realistic trend data
    np.random.seed(42)
    gdp_trend = 2.5 + np.cumsum(np.random.normal(0, 0.2, len(dates)))
    fed_trend = 2.0 + np.cumsum(np.random.normal(0, 0.1, len(dates)))
    cpi_trend = 2.0 + np.cumsum(np.random.normal(0, 0.15, len(dates)))
    unemployment_trend = 4.0 + np.cumsum(np.random.normal(0, 0.1, len(dates)))
    
    # Create subplot
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('GDP Growth (YoY)', 'Fed Funds Rate', 'CPI Inflation (YoY)', 'Unemployment Rate'),
        vertical_spacing=0.1
    )
    
    # GDP Growth
    fig.add_trace(
        go.Scatter(x=dates, y=gdp_trend, name='GDP Growth', line=dict(color='blue')),
        row=1, col=1
    )
    fig.add_hline(y=2.5, line_dash="dash", line_color="green", row=1, col=1)
    
    # Fed Funds Rate
    fig.add_trace(
        go.Scatter(x=dates, y=fed_trend, name='Fed Rate', line=dict(color='red')),
        row=1, col=2
    )
    fig.add_hline(y=2.0, line_dash="dash", line_color="orange", row=1, col=2)
    
    # CPI Inflation
    fig.add_trace(
        go.Scatter(x=dates, y=cpi_trend, name='CPI', line=dict(color='purple')),
        row=2, col=1
    )
    fig.add_hline(y=2.0, line_dash="dash", line_color="green", row=2, col=1)
    
    # Unemployment Rate
    fig.add_trace(
        go.Scatter(x=dates, y=unemployment_trend, name='Unemployment', line=dict(color='orange')),
        row=2, col=2
    )
    fig.add_hline(y=4.0, line_dash="dash", line_color="red", row=2, col=2)
    
    fig.update_layout(height=600, showlegend=False, title_text="Key Economic Indicators Over Time")
    st.plotly_chart(fig, use_container_width=True)


def display_interest_rates_with_charts(macro_data):
    """Display interest rates with yield curve charts"""
    st.subheader("💰 Interest Rates & Yield Curve")
    
    # Fed Funds Rate
    fed_data = macro_data.get('fed_funds_rate', {})
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### Federal Funds Rate")
        
        # Current rate
        current_rate = fed_data.get('current_rate', 0)
        st.metric("Current Rate", f"{current_rate:.2f}%")
        
        # Fed Rate Chart
        dates = pd.date_range(start='2023-01-01', end='2024-12-01', freq='M')
        np.random.seed(42)
        fed_trend = 2.0 + np.cumsum(np.random.normal(0, 0.1, len(dates)))
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates, 
            y=fed_trend,
            mode='lines+markers',
            name='Fed Funds Rate',
            line=dict(color='red', width=3),
            marker=dict(size=6)
        ))
        
        fig.add_hline(y=2.0, line_dash="dash", line_color="green", 
                     annotation_text="Neutral Rate (2%)")
        
        fig.update_layout(
            title="Federal Funds Rate Over Time",
            xaxis_title="Date",
            yaxis_title="Rate (%)",
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### Fed Rate Analysis")
        
        if current_rate > 5.0:
            st.warning("⚠️ High interest rate environment")
            st.markdown("- **Impact**: Higher borrowing costs")
            st.markdown("- **Sectors**: Beneficial for banks, challenging for growth stocks")
        elif current_rate < 2.0:
            st.success("✅ Low interest rate environment")
            st.markdown("- **Impact**: Lower borrowing costs")
            st.markdown("- **Sectors**: Beneficial for growth stocks, challenging for banks")
        else:
            st.info("ℹ️ Moderate interest rate environment")
            st.markdown("- **Impact**: Balanced economic conditions")
    
    # Treasury Yield Curve
    st.subheader("📈 Treasury Yield Curve")
    
    treasury_data = macro_data.get('treasury_yields', {})
    
    if treasury_data:
        # Create yield curve data
        maturities = ['3M', '6M', '1Y', '2Y', '5Y', '10Y', '30Y']
        yields = [
            treasury_data.get('3_month', 0),
            treasury_data.get('6_month', 0),
            treasury_data.get('1_year', 0),
            treasury_data.get('2_year', 0),
            treasury_data.get('5_year', 0),
            treasury_data.get('10_year', 0),
            treasury_data.get('30_year', 0)
        ]
        
        # Filter out None values
        valid_data = [(m, y) for m, y in zip(maturities, yields) if y is not None]
        if valid_data:
            maturities_clean, yields_clean = zip(*valid_data)
            
            # Create yield curve chart
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=maturities_clean, 
                y=yields_clean,
                mode='lines+markers',
                name='Yield Curve',
                line=dict(color='blue', width=3),
                marker=dict(size=8)
            ))
            
            fig.update_layout(
                title="US Treasury Yield Curve",
                xaxis_title="Maturity",
                yaxis_title="Yield (%)",
                height=400,
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Yield curve analysis
            if len(yields_clean) >= 2:
                short_yield = yields_clean[0]  # 3M
                long_yield = yields_clean[-1]  # 30Y
                spread = long_yield - short_yield
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Short-term (3M)", f"{short_yield:.2f}%")
                
                with col2:
                    st.metric("Long-term (30Y)", f"{long_yield:.2f}%")
                
                with col3:
                    st.metric("Spread", f"{spread:.2f}%", 
                             delta="Inverted" if spread < 0 else "Normal")
                
                if spread < 0:
                    st.warning("⚠️ **Inverted Yield Curve** - Often signals economic recession risk")
                elif spread < 1.0:
                    st.info("ℹ️ **Flat Yield Curve** - Economic uncertainty")
                else:
                    st.success("✅ **Normal Yield Curve** - Healthy economic conditions")


def display_inflation_with_charts(macro_data):
    """Display inflation data with charts"""
    st.subheader("📈 Inflation Indicators")
    
    inflation_data = macro_data.get('inflation', {})
    
    # CPI vs Core CPI Chart
    st.markdown("### Consumer Price Index (CPI)")
    
    dates = pd.date_range(start='2023-01-01', end='2024-12-01', freq='M')
    np.random.seed(42)
    cpi_trend = 2.0 + np.cumsum(np.random.normal(0, 0.15, len(dates)))
    core_cpi_trend = 2.2 + np.cumsum(np.random.normal(0, 0.12, len(dates)))
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=cpi_trend, 
        name='CPI', line=dict(color='blue', width=3)
    ))
    fig.add_trace(go.Scatter(
        x=dates, y=core_cpi_trend, 
        name='Core CPI', line=dict(color='red', width=3)
    ))
    
    fig.add_hline(y=2.0, line_dash="dash", line_color="green", 
                 annotation_text="Fed Target (2%)")
    
    fig.update_layout(
        title="CPI vs Core CPI Over Time",
        xaxis_title="Date",
        yaxis_title="Inflation Rate (%)",
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # PCE vs Core PCE Chart
    st.markdown("### Personal Consumption Expenditures (PCE)")
    
    pce_trend = 1.8 + np.cumsum(np.random.normal(0, 0.12, len(dates)))
    core_pce_trend = 2.0 + np.cumsum(np.random.normal(0, 0.10, len(dates)))
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=pce_trend, 
        name='PCE', line=dict(color='green', width=3)
    ))
    fig.add_trace(go.Scatter(
        x=dates, y=core_pce_trend, 
        name='Core PCE', line=dict(color='purple', width=3)
    ))
    
    fig.add_hline(y=2.0, line_dash="dash", line_color="green", 
                 annotation_text="Fed Target (2%)")
    
    fig.update_layout(
        title="PCE vs Core PCE Over Time",
        xaxis_title="Date",
        yaxis_title="Inflation Rate (%)",
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Current inflation metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        cpi_yoy = inflation_data.get('cpi_yoy', 0)
        st.metric("CPI YoY", f"{cpi_yoy:.1f}%")
    
    with col2:
        core_cpi_yoy = inflation_data.get('core_cpi_yoy', 0)
        st.metric("Core CPI YoY", f"{core_cpi_yoy:.1f}%")
    
    with col3:
        pce_yoy = inflation_data.get('pce_yoy', 0)
        st.metric("PCE YoY", f"{pce_yoy:.1f}%")
    
    with col4:
        core_pce_yoy = inflation_data.get('core_pce_yoy', 0)
        st.metric("Core PCE YoY", f"{core_pce_yoy:.1f}%")


def display_employment_with_charts(macro_data):
    """Display employment data with charts"""
    st.subheader("👥 Employment Indicators")
    
    employment_data = macro_data.get('employment', {})
    
    # Nonfarm Payrolls Chart
    st.markdown("### Nonfarm Payrolls")
    
    dates = pd.date_range(start='2023-01-01', end='2024-12-01', freq='M')
    np.random.seed(42)
    nfp_trend = 150000 + np.cumsum(np.random.normal(0, 50000, len(dates)))
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=nfp_trend, 
        name='Nonfarm Payrolls', line=dict(color='blue', width=3)
    ))
    
    fig.add_hline(y=150000, line_dash="dash", line_color="green", 
                 annotation_text="Strong Job Growth (150K)")
    
    fig.update_layout(
        title="Nonfarm Payrolls Over Time",
        xaxis_title="Date",
        yaxis_title="Jobs Added",
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Unemployment Rate Chart
    st.markdown("### Unemployment Rate")
    
    unemployment_trend = 4.0 + np.cumsum(np.random.normal(0, 0.1, len(dates)))
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=unemployment_trend, 
        name='Unemployment Rate', line=dict(color='red', width=3)
    ))
    
    fig.add_hline(y=4.0, line_dash="dash", line_color="green", 
                 annotation_text="Full Employment (4%)")
    
    fig.update_layout(
        title="Unemployment Rate Over Time",
        xaxis_title="Date",
        yaxis_title="Unemployment Rate (%)",
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Current employment metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        nfp = employment_data.get('nonfarm_payrolls', 0)
        st.metric("Nonfarm Payrolls", f"{nfp:,}")
    
    with col2:
        unemployment = employment_data.get('unemployment_rate', 0)
        st.metric("Unemployment Rate", f"{unemployment:.1f}%")
    
    with col3:
        participation = employment_data.get('labor_force_participation', 0)
        st.metric("Labor Force Participation", f"{participation:.1f}%")
    
    with col4:
        avg_earnings = employment_data.get('average_hourly_earnings', 0)
        st.metric("Avg Hourly Earnings", f"{avg_earnings:.1f}%")


def display_economic_activity_with_charts(macro_data):
    """Display economic activity indicators with charts"""
    st.subheader("🏭 Economic Activity Indicators")
    
    # PMI Data
    pmi_data = macro_data.get('pmi', {})
    
    st.markdown("### Purchasing Managers Index (PMI)")
    
    dates = pd.date_range(start='2023-01-01', end='2024-12-01', freq='M')
    np.random.seed(42)
    manufacturing_pmi = 50 + np.cumsum(np.random.normal(0, 2, len(dates)))
    services_pmi = 52 + np.cumsum(np.random.normal(0, 1.5, len(dates)))
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=manufacturing_pmi, 
        name='Manufacturing PMI', line=dict(color='blue', width=3)
    ))
    fig.add_trace(go.Scatter(
        x=dates, y=services_pmi, 
        name='Services PMI', line=dict(color='green', width=3)
    ))
    
    fig.add_hline(y=50, line_dash="dash", line_color="red", 
                 annotation_text="Expansion/Contraction Line")
    
    fig.update_layout(
        title="PMI by Sector Over Time",
        xaxis_title="Date",
        yaxis_title="PMI",
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Retail Sales Chart
    st.markdown("### Retail Sales")
    
    retail_trend = 0.5 + np.cumsum(np.random.normal(0, 0.3, len(dates)))
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=retail_trend, 
        name='Retail Sales MoM', line=dict(color='purple', width=3)
    ))
    
    fig.add_hline(y=0, line_dash="dash", line_color="red", 
                 annotation_text="Zero Growth")
    
    fig.update_layout(
        title="Retail Sales Month-over-Month",
        xaxis_title="Date",
        yaxis_title="Growth Rate (%)",
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Current activity metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        manufacturing_pmi = pmi_data.get('manufacturing_pmi', 50)
        st.metric("Manufacturing PMI", f"{manufacturing_pmi:.1f}")
    
    with col2:
        services_pmi = pmi_data.get('services_pmi', 50)
        st.metric("Services PMI", f"{services_pmi:.1f}")
    
    with col3:
        retail_sales = macro_data.get('retail_sales', {}).get('retail_sales_mom', 0)
        st.metric("Retail Sales MoM", f"{retail_sales:.1f}%")
    
    with col4:
        ip_growth = macro_data.get('industrial_production', {}).get('industrial_production_mom', 0)
        st.metric("Industrial Production MoM", f"{ip_growth:.1f}%")


def display_analysis_insights(macro_data):
    """Display analysis and investment insights"""
    st.subheader("🔍 Macroeconomic Analysis & Investment Insights")
    
    # Generate correlations and insights
    correlations = analyze_macro_correlations(macro_data)
    insights = generate_macro_insights(macro_data)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Key Relationships")
        
        # Display correlation analysis
        if correlations.get('analysis'):
            for analysis in correlations['analysis']:
                st.markdown(analysis)
        
        # Yield curve analysis
        yield_curve = correlations.get('treasury_yield_curve')
        if yield_curve is not None:
            st.markdown(f"**Yield Curve Spread (10Y-2Y):** {yield_curve:.2f}%")
            if yield_curve < 0:
                st.warning("⚠️ Inverted yield curve - recession risk")
            elif yield_curve < 1.0:
                st.info("ℹ️ Flat yield curve - economic uncertainty")
            else:
                st.success("✅ Normal yield curve - healthy economy")
    
    with col2:
        st.markdown("### 🎯 Market Outlook")
        
        outlook = insights.get('market_outlook', 'neutral')
        risk_level = insights.get('risk_level', 'medium')
        
        # Market outlook indicator
        if outlook == 'bullish':
            st.success("🚀 **Bullish Market Outlook**")
            st.markdown("- Positive economic indicators")
            st.markdown("- Consider increasing equity allocation")
        elif outlook == 'bearish':
            st.warning("📉 **Bearish Market Outlook**")
            st.markdown("- Negative economic indicators")
            st.markdown("- Consider reducing risk exposure")
        else:
            st.info("📊 **Neutral Market Outlook**")
            st.markdown("- Mixed economic signals")
            st.markdown("- Maintain balanced allocation")
        
        # Risk level
        st.markdown(f"**Risk Level:** {risk_level.title()}")
        
        if risk_level == 'high':
            st.warning("⚠️ High risk environment")
        elif risk_level == 'low':
            st.success("✅ Low risk environment")
        else:
            st.info("ℹ️ Medium risk environment")
    
    # Sector recommendations
    st.subheader("🏢 Sector Recommendations")
    
    sector_recommendations = insights.get('sector_recommendations', [])
    if sector_recommendations:
        for recommendation in sector_recommendations:
            st.markdown(f"• {recommendation}")
    else:
        st.info("No specific sector recommendations at this time")
    
    # Investment recommendations
    st.subheader("💡 Investment Recommendations")
    
    recommendations = insights.get('recommendations', [])
    if recommendations:
        for recommendation in recommendations:
            st.markdown(f"• {recommendation}")
    else:
        st.info("No specific investment recommendations at this time")


def get_mock_macro_data():
    """Generate mock macroeconomic data for demonstration"""
    return {
        'fed_funds_rate': {
            'current_rate': 5.25,
            'forecast_next_meeting': 5.25,
            'forecast_3_months': 5.0,
            'forecast_6_months': 4.75,
            'last_updated': datetime.now().isoformat(),
            'source': 'CME FedWatch'
        },
        'treasury_yields': {
            '3_month': 5.25,
            '6_month': 5.30,
            '1_year': 5.15,
            '2_year': 4.85,
            '5_year': 4.45,
            '10_year': 4.25,
            '30_year': 4.35,
            'last_updated': datetime.now().isoformat(),
            'source': 'Treasury.gov'
        },
        'inflation': {
            'cpi_yoy': 3.2,
            'cpi_mom': 0.3,
            'core_cpi_yoy': 3.8,
            'core_cpi_mom': 0.4,
            'pce_yoy': 2.8,
            'pce_mom': 0.2,
            'core_pce_yoy': 3.2,
            'core_pce_mom': 0.3,
            'last_updated': datetime.now().isoformat(),
            'source': 'BLS/BEA'
        },
        'employment': {
            'nonfarm_payrolls': 150000,
            'unemployment_rate': 3.8,
            'labor_force_participation': 62.8,
            'average_hourly_earnings': 0.3,
            'last_updated': datetime.now().isoformat(),
            'source': 'BLS'
        },
        'gdp': {
            'gdp_growth_qoq': 2.1,
            'gdp_growth_yoy': 2.8,
            'gdp_level': 28000,
            'last_updated': datetime.now().isoformat(),
            'source': 'BEA'
        },
        'pmi': {
            'manufacturing_pmi': 49.5,
            'services_pmi': 52.1,
            'composite_pmi': 51.2,
            'last_updated': datetime.now().isoformat(),
            'source': 'ISM'
        },
        'retail_sales': {
            'retail_sales_mom': 0.7,
            'retail_sales_yoy': 3.2,
            'retail_sales_ex_auto_mom': 0.5,
            'last_updated': datetime.now().isoformat(),
            'source': 'Census Bureau'
        },
        'industrial_production': {
            'industrial_production_mom': 0.4,
            'industrial_production_yoy': 1.8,
            'capacity_utilization': 78.5,
            'last_updated': datetime.now().isoformat(),
            'source': 'Federal Reserve'
        },
        'consumer_confidence': {
            'consumer_confidence_index': 102.3,
            'present_situation_index': 143.1,
            'expectations_index': 75.2,
            'last_updated': datetime.now().isoformat(),
            'source': 'Conference Board'
        },
        'housing': {
            'housing_starts': 1400000,
            'housing_starts_mom': 5.2,
            'building_permits': 1450000,
            'building_permits_mom': 2.1,
            'last_updated': datetime.now().isoformat(),
            'source': 'Census Bureau'
        },
        'ppi': {
            'ppi_yoy': 2.1,
            'ppi_mom': 0.2,
            'core_ppi_yoy': 2.8,
            'core_ppi_mom': 0.3,
            'last_updated': datetime.now().isoformat(),
            'source': 'BLS'
        },
        'last_updated': datetime.now().isoformat()
    }


# Example usage
if __name__ == "__main__":
    create_macro_dashboard()
