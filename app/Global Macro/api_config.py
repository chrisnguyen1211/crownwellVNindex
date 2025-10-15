# API Configuration for Macro Data Dashboard
# Store API keys securely

FRED_API_KEY = "462ccb57dc7ade87fa2f645dc0617225"
ALPHA_VANTAGE_API_KEY = "6I8VH146ZYO47SBW"

# Additional API endpoints
FRED_BASE_URL = "https://api.stlouisfed.org/fred"
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# FRED Series IDs for different indicators
FRED_SERIES = {
    'fed_funds_rate': 'FEDFUNDS',
    'treasury_3m': 'DGS3MO',
    'treasury_6m': 'DGS6MO', 
    'treasury_1y': 'DGS1',
    'treasury_2y': 'DGS2',
    'treasury_5y': 'DGS5',
    'treasury_10y': 'DGS10',
    'treasury_30y': 'DGS30',
    'cpi': 'CPIAUCSL',
    'core_cpi': 'CPILFESL',
    'pce': 'PCEPI',
    'core_pce': 'PCEPILFE',
    'nonfarm_payrolls': 'PAYEMS',
    'unemployment_rate': 'UNRATE',
    'labor_force_participation': 'CIVPART',
    'gdp': 'GDP',
    'gdp_growth': 'GDPC1',
    'retail_sales': 'RSAFS',
    'retail_sales_ex_auto': 'RSAFSNA',
    'industrial_production': 'INDPRO',
    'capacity_utilization': 'TCU',
    'consumer_confidence': 'UMCSENT',
    'present_situation': 'UMCSENT1',
    'expectations': 'UMCSENT2',
    'housing_starts': 'HOUST',
    'building_permits': 'PERMIT',
    'ppi': 'PPIACO',
    'core_ppi': 'PPIFGS'
}
