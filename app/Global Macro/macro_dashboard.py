import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import time

from macro_data_helper import MacroDataFetcher, analyze_macro_correlations, generate_macro_insights

def create_macro_dashboard():
    """Create the Macro Data Dashboard page"""
    
    st.markdown("""
    <div class="main-header">
        <h1>📈 Macro Economic Data Dashboard</h1>
        <p>Global Macroeconomic Indicators & Analysis</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Initialize macro data fetcher
    if 'macro_fetcher' not in st.session_state:
        st.session_state.macro_fetcher = MacroDataFetcher()
    
    fetcher = st.session_state.macro_fetcher
    
    # Sidebar controls
    st.sidebar.markdown("### 🔧 Macro Data Controls")
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh All Data", type="primary"):
        # Clear cache and refresh
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
        # Auto-refresh logic
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
            # Use mock data for demonstration
            macro_data = get_mock_macro_data()
    
    # Display last updated time
    last_updated = macro_data.get('last_updated', datetime.now().isoformat())
    st.caption(f"Last updated: {last_updated}")
    
    # Create tabs for different data categories
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Key Indicators", 
        "💰 Interest Rates", 
        "📈 Inflation & Employment", 
        "🏭 Economic Activity", 
        "🔍 Analysis & Insights"
    ])
    
    with tab1:
        display_key_indicators(macro_data)
    
    with tab2:
        display_interest_rates(macro_data)
    
    with tab3:
        display_inflation_employment(macro_data)
    
    with tab4:
        display_economic_activity(macro_data)
    
    with tab5:
        display_analysis_insights(macro_data)


def display_key_indicators(macro_data):
    """Display key macroeconomic indicators"""
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
    
    # Additional key metrics
    st.subheader("📋 Additional Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Manufacturing PMI
        pmi_data = macro_data.get('pmi', {})
        manufacturing_pmi = pmi_data.get('manufacturing_pmi', 50)
        pmi_color = "green" if manufacturing_pmi > 50 else "red"
        st.markdown(f"""
        <div style="text-align: center; padding: 1rem; border-radius: 8px; background-color: {pmi_color}20;">
            <h3 style="color: {pmi_color}; margin: 0;">Manufacturing PMI</h3>
            <h2 style="color: {pmi_color}; margin: 0;">{manufacturing_pmi:.1f}</h2>
            <p style="margin: 0; font-size: 0.9rem;">{"Expansion" if manufacturing_pmi > 50 else "Contraction"}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        # Consumer Confidence
        confidence_data = macro_data.get('consumer_confidence', {})
        confidence = confidence_data.get('consumer_confidence_index', 100)
        confidence_color = "green" if confidence > 100 else "orange" if confidence > 80 else "red"
        st.markdown(f"""
        <div style="text-align: center; padding: 1rem; border-radius: 8px; background-color: {confidence_color}20;">
            <h3 style="color: {confidence_color}; margin: 0;">Consumer Confidence</h3>
            <h2 style="color: {confidence_color}; margin: 0;">{confidence:.1f}</h2>
            <p style="margin: 0; font-size: 0.9rem;">{"Strong" if confidence > 100 else "Moderate" if confidence > 80 else "Weak"}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        # Retail Sales
        retail_data = macro_data.get('retail_sales', {})
        retail_sales = retail_data.get('retail_sales_mom', 0)
        retail_color = "green" if retail_sales > 0.5 else "orange" if retail_sales > 0 else "red"
        st.markdown(f"""
        <div style="text-align: center; padding: 1rem; border-radius: 8px; background-color: {retail_color}20;">
            <h3 style="color: {retail_color}; margin: 0;">Retail Sales (MoM)</h3>
            <h2 style="color: {retail_color}; margin: 0;">{retail_sales:.1f}%</h2>
            <p style="margin: 0; font-size: 0.9rem;">{"Strong" if retail_sales > 0.5 else "Moderate" if retail_sales > 0 else "Weak"}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        # Housing Starts
        housing_data = macro_data.get('housing', {})
        housing_starts = housing_data.get('housing_starts', 0)
        housing_change = housing_data.get('housing_starts_mom', 0)
        housing_color = "green" if housing_change > 0 else "red"
        st.markdown(f"""
        <div style="text-align: center; padding: 1rem; border-radius: 8px; background-color: {housing_color}20;">
            <h3 style="color: {housing_color}; margin: 0;">Housing Starts</h3>
            <h2 style="color: {housing_color}; margin: 0;">{housing_starts/1000:.0f}K</h2>
            <p style="margin: 0; font-size: 0.9rem;">{housing_change:+.1f}% MoM</p>
        </div>
        """, unsafe_allow_html=True)


def display_interest_rates(macro_data):
    """Display interest rates and yield curve"""
    st.subheader("💰 Interest Rates & Yield Curve")
    
    # Fed Funds Rate
    fed_data = macro_data.get('fed_funds_rate', {})
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### Federal Funds Rate")
        
        # Current rate
        current_rate = fed_data.get('current_rate', 0)
        st.metric("Current Rate", f"{current_rate:.2f}%")
        
        # Forecasts
        forecast_data = {
            "Next Meeting": fed_data.get('forecast_next_meeting', current_rate),
            "3 Months": fed_data.get('forecast_3_months', current_rate),
            "6 Months": fed_data.get('forecast_6_months', current_rate)
        }
        
        forecast_df = pd.DataFrame(list(forecast_data.items()), columns=['Period', 'Rate'])
        forecast_df['Rate'] = forecast_df['Rate'].astype(float)
        
        fig = px.bar(forecast_df, x='Period', y='Rate', 
                    title="Fed Funds Rate Forecast",
                    color='Rate',
                    color_continuous_scale='RdYlBu_r')
        fig.update_layout(height=400)
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


def display_inflation_employment(macro_data):
    """Display inflation and employment data"""
    st.subheader("📈 Inflation & Employment")
    
    # Inflation data
    inflation_data = macro_data.get('inflation', {})
    employment_data = macro_data.get('employment', {})
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Inflation Indicators")
        
        # CPI vs Core CPI
        cpi_yoy = inflation_data.get('cpi_yoy', 0)
        core_cpi_yoy = inflation_data.get('core_cpi_yoy', 0)
        pce_yoy = inflation_data.get('pce_yoy', 0)
        core_pce_yoy = inflation_data.get('core_pce_yoy', 0)
        
        inflation_df = pd.DataFrame({
            'Indicator': ['CPI', 'Core CPI', 'PCE', 'Core PCE'],
            'YoY Rate': [cpi_yoy, core_cpi_yoy, pce_yoy, core_pce_yoy]
        })
        
        fig = px.bar(inflation_df, x='Indicator', y='YoY Rate',
                    title="Inflation Rates (Year-over-Year)",
                    color='YoY Rate',
                    color_continuous_scale='Reds')
        fig.add_hline(y=2.0, line_dash="dash", line_color="green", 
                     annotation_text="Fed Target (2%)")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Inflation analysis
        if cpi_yoy > 3.0:
            st.warning("⚠️ **High Inflation** - Above Fed's comfort zone")
            st.markdown("- Fed likely to maintain or increase rates")
            st.markdown("- Pressure on consumer spending")
        elif cpi_yoy < 2.0:
            st.success("✅ **Low Inflation** - Below Fed's target")
            st.markdown("- Fed may consider rate cuts")
            st.markdown("- Supportive for economic growth")
        else:
            st.info("ℹ️ **Moderate Inflation** - Near Fed's target")
    
    with col2:
        st.markdown("### 👥 Employment Indicators")
        
        # Employment metrics
        nfp = employment_data.get('nonfarm_payrolls', 0)
        unemployment = employment_data.get('unemployment_rate', 0)
        participation = employment_data.get('labor_force_participation', 0)
        avg_earnings = employment_data.get('average_hourly_earnings', 0)
        
        # Create employment dashboard
        col2_1, col2_2 = st.columns(2)
        
        with col2_1:
            st.metric("Nonfarm Payrolls", f"{nfp:,}", delta=f"{nfp-150000:,}")
            st.metric("Unemployment Rate", f"{unemployment:.1f}%")
        
        with col2_2:
            st.metric("Labor Force Participation", f"{participation:.1f}%")
            st.metric("Avg Hourly Earnings", f"{avg_earnings:.1f}%")
        
        # Employment trend chart
        employment_df = pd.DataFrame({
            'Metric': ['NFP (K)', 'Unemployment (%)', 'Participation (%)', 'Earnings (%)'],
            'Value': [nfp/1000, unemployment, participation, avg_earnings],
            'Target': [150, 4.0, 63.0, 0.3]
        })
        
        fig = go.Figure()
        fig.add_trace(go.Bar(name='Current', x=employment_df['Metric'], y=employment_df['Value']))
        fig.add_trace(go.Bar(name='Target/Benchmark', x=employment_df['Metric'], y=employment_df['Target']))
        
        fig.update_layout(
            title="Employment Indicators",
            height=400,
            barmode='group'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Employment analysis
        if unemployment < 4.0 and nfp > 100000:
            st.success("✅ **Strong Labor Market**")
            st.markdown("- Low unemployment, strong job creation")
            st.markdown("- Supportive for consumer spending")
        elif unemployment > 5.0 or nfp < 50000:
            st.warning("⚠️ **Weak Labor Market**")
            st.markdown("- High unemployment or weak job creation")
            st.markdown("- Pressure on consumer confidence")


def display_economic_activity(macro_data):
    """Display economic activity indicators"""
    st.subheader("🏭 Economic Activity Indicators")
    
    # PMI data
    pmi_data = macro_data.get('pmi', {})
    retail_data = macro_data.get('retail_sales', {})
    industrial_data = macro_data.get('industrial_production', {})
    housing_data = macro_data.get('housing', {})
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🏭 Purchasing Managers Index (PMI)")
        
        manufacturing_pmi = pmi_data.get('manufacturing_pmi', 50)
        services_pmi = pmi_data.get('services_pmi', 50)
        composite_pmi = pmi_data.get('composite_pmi', 50)
        
        pmi_df = pd.DataFrame({
            'Sector': ['Manufacturing', 'Services', 'Composite'],
            'PMI': [manufacturing_pmi, services_pmi, composite_pmi]
        })
        
        fig = px.bar(pmi_df, x='Sector', y='PMI',
                    title="PMI by Sector",
                    color='PMI',
                    color_continuous_scale='RdYlGn')
        fig.add_hline(y=50, line_dash="dash", line_color="black", 
                     annotation_text="Expansion/Contraction Line")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # PMI analysis
        if manufacturing_pmi > 50 and services_pmi > 50:
            st.success("✅ **Broad Economic Expansion**")
            st.markdown("- Both manufacturing and services expanding")
        elif manufacturing_pmi < 50 and services_pmi < 50:
            st.warning("⚠️ **Broad Economic Contraction**")
            st.markdown("- Both manufacturing and services contracting")
        else:
            st.info("ℹ️ **Mixed Economic Signals**")
            st.markdown("- Divergence between sectors")
    
    with col2:
        st.markdown("### 🛒 Retail Sales & Industrial Production")
        
        retail_sales_mom = retail_data.get('retail_sales_mom', 0)
        retail_sales_yoy = retail_data.get('retail_sales_yoy', 0)
        ip_mom = industrial_data.get('industrial_production_mom', 0)
        ip_yoy = industrial_data.get('industrial_production_yoy', 0)
        capacity_util = industrial_data.get('capacity_utilization', 0)
        
        # Create activity indicators chart
        activity_df = pd.DataFrame({
            'Indicator': ['Retail Sales (MoM)', 'Retail Sales (YoY)', 'IP (MoM)', 'IP (YoY)', 'Capacity Util'],
            'Value': [retail_sales_mom, retail_sales_yoy, ip_mom, ip_yoy, capacity_util]
        })
        
        fig = px.bar(activity_df, x='Indicator', y='Value',
                    title="Economic Activity Indicators",
                    color='Value',
                    color_continuous_scale='Blues')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Activity analysis
        if retail_sales_mom > 0.5 and ip_mom > 0.3:
            st.success("✅ **Strong Economic Activity**")
            st.markdown("- Strong retail sales and industrial production")
        elif retail_sales_mom < 0 and ip_mom < 0:
            st.warning("⚠️ **Weak Economic Activity**")
            st.markdown("- Declining retail sales and industrial production")
    
    # Housing data
    st.subheader("🏠 Housing Market")
    
    housing_starts = housing_data.get('housing_starts', 0)
    housing_starts_mom = housing_data.get('housing_starts_mom', 0)
    building_permits = housing_data.get('building_permits', 0)
    building_permits_mom = housing_data.get('building_permits_mom', 0)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Housing Starts", f"{housing_starts/1000:.0f}K", delta=f"{housing_starts_mom:+.1f}%")
    
    with col2:
        st.metric("Building Permits", f"{building_permits/1000:.0f}K", delta=f"{building_permits_mom:+.1f}%")
    
    with col3:
        # Housing trend analysis
        if housing_starts_mom > 5.0:
            st.success("🚀 Strong housing growth")
        elif housing_starts_mom < -5.0:
            st.warning("📉 Weak housing market")
        else:
            st.info("📊 Stable housing market")
    
    with col4:
        # Housing vs economic activity
        if housing_starts > 1400000:
            st.success("✅ Above historical average")
        else:
            st.warning("⚠️ Below historical average")


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
    
    # Key drivers
    st.subheader("🔑 Key Market Drivers")
    
    key_drivers = insights.get('key_drivers', [])
    if key_drivers:
        for driver in key_drivers:
            st.markdown(f"• {driver}")
    else:
        st.info("No specific key drivers identified")
    
    # Economic calendar
    st.subheader("📅 Upcoming Economic Events")
    
    # Mock economic calendar
    events = [
        {"Date": "Next Week", "Event": "FOMC Meeting", "Impact": "High"},
        {"Date": "Next Week", "Event": "Nonfarm Payrolls", "Impact": "High"},
        {"Date": "Next Month", "Event": "CPI Release", "Impact": "High"},
        {"Date": "Next Month", "Event": "GDP Release", "Impact": "Medium"},
        {"Date": "Next Month", "Event": "PMI Release", "Impact": "Medium"}
    ]
    
    events_df = pd.DataFrame(events)
    st.dataframe(events_df, use_container_width=True)


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
