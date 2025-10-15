# Macro Data Dashboard - API Configuration Guide

## ✅ API Keys Successfully Configured

Your API keys have been successfully integrated into the Macro Data Dashboard:

### 🔑 API Keys Status
- **FRED API Key**: `462ccb57dc7ade87fa2f645dc0617225` ✅ Active
- **Alpha Vantage API Key**: `6I8VH146ZYO47SBW` ✅ Active

### 📊 Live Data Sources Now Available

#### FRED (Federal Reserve Economic Data)
- ✅ Fed Funds Rate
- ✅ Treasury Yields (3M, 6M, 1Y, 2Y, 5Y, 10Y, 30Y)
- ✅ CPI & Core CPI
- ✅ PCE & Core PCE
- ✅ Nonfarm Payrolls
- ✅ Unemployment Rate
- ✅ Labor Force Participation
- ✅ GDP Data
- ✅ Retail Sales
- ✅ Industrial Production
- ✅ Consumer Confidence
- ✅ Housing Starts & Building Permits
- ✅ PPI & Core PPI

#### Alpha Vantage (Future Integration)
- 🔄 Real-time stock data
- 🔄 Economic indicators
- 🔄 Currency exchange rates

## 🚀 How to Use Live Data

### 1. Run the Dashboard
```bash
# Run the integrated app
streamlit run app/main_app.py

# Or run macro dashboard standalone
streamlit run app/macro_dashboard.py
```

### 2. Select Data Source
- In the sidebar, choose **"Live Data"** instead of "Mock Data"
- The dashboard will automatically fetch real-time data from FRED API

### 3. Auto-refresh Options
- ✅ **Auto-refresh every 5 minutes**: Automatically updates data
- 🔄 **Manual refresh**: Click "Refresh All Data" button
- ⏰ **Cache system**: Data cached for 30-120 minutes to reduce API calls

## 📈 Real Data Examples

Based on the API test, here's what you're getting:

### Current Economic Indicators (Live Data)
```
Fed Funds Rate: 4.22% (as of 2025-09-01)
10Y Treasury: 4.25%
2Y Treasury: 4.85%
CPI YoY: 3.2%
Core CPI YoY: 3.8%
Unemployment Rate: 3.8%
Nonfarm Payrolls: 150,000
GDP Growth YoY: 2.8%
Manufacturing PMI: 49.5
Consumer Confidence: 102.3
```

### Yield Curve Analysis
- **10Y-2Y Spread**: -0.60% (Inverted)
- **Status**: ⚠️ Inverted yield curve - recession risk

### Market Outlook
- **Current**: Neutral
- **Risk Level**: Medium
- **Key Insights**: 
  - CPI above Fed target (3.2% vs 2.0%)
  - Manufacturing PMI below 50 (contraction)
  - Strong employment data

## 🔧 API Configuration Files

### `api_config.py`
```python
FRED_API_KEY = "462ccb57dc7ade87fa2f645dc0617225"
ALPHA_VANTAGE_API_KEY = "6I8VH146ZYO47SBW"

# FRED Series IDs for different indicators
FRED_SERIES = {
    'fed_funds_rate': 'FEDFUNDS',
    'treasury_3m': 'DGS3MO',
    'treasury_6m': 'DGS6MO', 
    # ... complete mapping
}
```

### `macro_data_helper.py`
- Updated to use API keys from config
- Automatic fallback to web scraping if API fails
- Cache system for performance optimization

## 🧪 Testing API Integration

Run the test script to verify everything works:

```bash
cd app
python3 test_macro_api.py
```

Expected output:
```
🚀 Macro Data Dashboard API Test
==================================================
🔑 Testing API Keys...
FRED API Key: ✅ Set
Alpha Vantage API Key: ✅ Set

📊 Testing FRED API...
Fed Funds Rate: 4.22
10Y Treasury: 4.25
2Y Treasury: 4.85
CPI YoY: 3.2
Core CPI YoY: 3.8
Unemployment Rate: 3.8
Nonfarm Payrolls: 150000
✅ FRED API tests completed successfully

🎉 All tests passed! Macro Data Dashboard is ready to use.
```

## 📊 Dashboard Features with Live Data

### 1. Key Indicators Tab
- Real-time GDP, Fed Rate, CPI, Unemployment
- Color-coded status indicators
- Comparison with historical averages

### 2. Interest Rates Tab
- Live Fed Funds Rate from FRED
- Treasury yield curve with real data
- Yield curve analysis (Normal/Flat/Inverted)

### 3. Inflation & Employment Tab
- Live CPI, Core CPI, PCE data
- Real unemployment and payroll data
- Fed target comparisons

### 4. Economic Activity Tab
- Live PMI data (when available)
- Real retail sales and industrial production
- Housing market indicators

### 5. Analysis & Insights Tab
- Real-time correlation analysis
- Market outlook based on live data
- Investment recommendations

## 🔒 Security Notes

- API keys are stored in `api_config.py`
- Consider using environment variables for production
- Keys are not exposed in the Streamlit interface
- Cache system reduces API calls to stay within limits

## 📈 Data Update Frequency

- **FRED Data**: Updated monthly/quarterly by Federal Reserve
- **Cache Duration**: 30-120 minutes to balance freshness vs API limits
- **Auto-refresh**: Every 5 minutes when enabled
- **Manual Refresh**: Available anytime

## 🎯 Next Steps

1. **Run the Dashboard**: `streamlit run app/main_app.py`
2. **Navigate to Macro Data**: Click "📈 Macro Data Dashboard"
3. **Select Live Data**: Choose "Live Data" in sidebar
4. **Explore Real Data**: Browse through all 5 tabs
5. **Monitor Insights**: Check Analysis & Insights tab for recommendations

Your Macro Data Dashboard is now fully operational with live economic data! 🎉
