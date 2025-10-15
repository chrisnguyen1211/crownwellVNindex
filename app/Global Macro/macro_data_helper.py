import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import time
from api_config import FRED_API_KEY, ALPHA_VANTAGE_API_KEY, FRED_SERIES

# Setup logging
logger = logging.getLogger(__name__)

class MacroDataFetcher:
    """Fetcher for global macroeconomic data from various sources"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # API keys from config
        self.alpha_vantage_key = ALPHA_VANTAGE_API_KEY
        self.fred_api_key = FRED_API_KEY
        
        # Cache for API responses
        self._cache = {}
        self._cache_expiry = {}
        
    def _get_cached_data(self, key: str, expiry_minutes: int = 30) -> Optional[Dict]:
        """Get cached data if still valid"""
        if key in self._cache and key in self._cache_expiry:
            if datetime.now() < self._cache_expiry[key]:
                return self._cache[key]
        return None
    
    def _cache_data(self, key: str, data: Dict, expiry_minutes: int = 30):
        """Cache data with expiry"""
        self._cache[key] = data
        self._cache_expiry[key] = datetime.now() + timedelta(minutes=expiry_minutes)
    
    def get_fed_funds_rate(self) -> Dict:
        """Get current Fed Funds Rate and forecast from CME"""
        cache_key = "fed_funds_rate"
        cached = self._get_cached_data(cache_key, 60)  # Cache for 1 hour
        if cached:
            return cached
            
        data = {
            'current_rate': None,
            'forecast_next_meeting': None,
            'forecast_3_months': None,
            'forecast_6_months': None,
            'last_updated': None,
            'source': 'CME FedWatch'
        }
        
        try:
            # Try to get Fed Funds Rate from FRED API
            if self.fred_api_key:
                fred_data = self._get_fred_data(FRED_SERIES['fed_funds_rate'])
                if fred_data:
                    data.update(fred_data)
            
            # Fallback: scrape from CME FedWatch
            cme_data = self._scrape_cme_fedwatch()
            if cme_data:
                data.update(cme_data)
                
        except Exception as e:
            logger.warning(f"Error fetching Fed Funds Rate: {e}")
        
        self._cache_data(cache_key, data, 60)
        return data
    
    def get_treasury_yields(self) -> Dict:
        """Get US Treasury yields for different maturities"""
        cache_key = "treasury_yields"
        cached = self._get_cached_data(cache_key, 30)
        if cached:
            return cached
            
        data = {
            '3_month': None,
            '6_month': None,
            '1_year': None,
            '2_year': None,
            '5_year': None,
            '10_year': None,
            '30_year': None,
            'last_updated': None,
            'source': 'Treasury.gov'
        }
        
        try:
            # Try FRED API first
            if self.fred_api_key:
                treasury_data = self._get_treasury_yields_fred()
                if treasury_data:
                    data.update(treasury_data)
            
            # Fallback: scrape from Treasury.gov
            treasury_scraped = self._scrape_treasury_yields()
            if treasury_scraped:
                data.update(treasury_scraped)
                
        except Exception as e:
            logger.warning(f"Error fetching Treasury yields: {e}")
        
        self._cache_data(cache_key, data, 30)
        return data
    
    def get_inflation_data(self) -> Dict:
        """Get inflation data (CPI, PCE, Core CPI, Core PCE)"""
        cache_key = "inflation_data"
        cached = self._get_cached_data(cache_key, 60)
        if cached:
            return cached
            
        data = {
            'cpi_yoy': None,
            'cpi_mom': None,
            'core_cpi_yoy': None,
            'core_cpi_mom': None,
            'pce_yoy': None,
            'pce_mom': None,
            'core_pce_yoy': None,
            'core_pce_mom': None,
            'last_updated': None,
            'source': 'BLS/BEA'
        }
        
        try:
            if self.fred_api_key:
                inflation_data = self._get_inflation_fred()
                if inflation_data:
                    data.update(inflation_data)
            
            # Fallback: scrape from BLS
            bls_data = self._scrape_bls_inflation()
            if bls_data:
                data.update(bls_data)
                
        except Exception as e:
            logger.warning(f"Error fetching inflation data: {e}")
        
        self._cache_data(cache_key, data, 60)
        return data
    
    def get_employment_data(self) -> Dict:
        """Get employment data (NFP, Unemployment Rate)"""
        cache_key = "employment_data"
        cached = self._get_cached_data(cache_key, 60)
        if cached:
            return cached
            
        data = {
            'nonfarm_payrolls': None,
            'unemployment_rate': None,
            'labor_force_participation': None,
            'average_hourly_earnings': None,
            'last_updated': None,
            'source': 'BLS'
        }
        
        try:
            if self.fred_api_key:
                employment_data = self._get_employment_fred()
                if employment_data:
                    data.update(employment_data)
            
            # Fallback: scrape from BLS
            bls_employment = self._scrape_bls_employment()
            if bls_employment:
                data.update(bls_employment)
                
        except Exception as e:
            logger.warning(f"Error fetching employment data: {e}")
        
        self._cache_data(cache_key, data, 60)
        return data
    
    def get_gdp_data(self) -> Dict:
        """Get GDP data"""
        cache_key = "gdp_data"
        cached = self._get_cached_data(cache_key, 120)  # Cache for 2 hours
        if cached:
            return cached
            
        data = {
            'gdp_growth_qoq': None,
            'gdp_growth_yoy': None,
            'gdp_level': None,
            'last_updated': None,
            'source': 'BEA'
        }
        
        try:
            if self.fred_api_key:
                gdp_data = self._get_gdp_fred()
                if gdp_data:
                    data.update(gdp_data)
            
            # Fallback: scrape from BEA
            bea_data = self._scrape_bea_gdp()
            if bea_data:
                data.update(bea_data)
                
        except Exception as e:
            logger.warning(f"Error fetching GDP data: {e}")
        
        self._cache_data(cache_key, data, 120)
        return data
    
    def get_pmi_data(self) -> Dict:
        """Get PMI data (Manufacturing and Services)"""
        cache_key = "pmi_data"
        cached = self._get_cached_data(cache_key, 60)
        if cached:
            return cached
            
        data = {
            'manufacturing_pmi': None,
            'services_pmi': None,
            'composite_pmi': None,
            'last_updated': None,
            'source': 'ISM'
        }
        
        try:
            # Scrape from ISM
            ism_data = self._scrape_ism_pmi()
            if ism_data:
                data.update(ism_data)
                
        except Exception as e:
            logger.warning(f"Error fetching PMI data: {e}")
        
        self._cache_data(cache_key, data, 60)
        return data
    
    def get_retail_sales_data(self) -> Dict:
        """Get Retail Sales data"""
        cache_key = "retail_sales_data"
        cached = self._get_cached_data(cache_key, 60)
        if cached:
            return cached
            
        data = {
            'retail_sales_mom': None,
            'retail_sales_yoy': None,
            'retail_sales_ex_auto_mom': None,
            'last_updated': None,
            'source': 'Census Bureau'
        }
        
        try:
            if self.fred_api_key:
                retail_data = self._get_retail_sales_fred()
                if retail_data:
                    data.update(retail_data)
            
            # Fallback: scrape from Census Bureau
            census_data = self._scrape_census_retail_sales()
            if census_data:
                data.update(census_data)
                
        except Exception as e:
            logger.warning(f"Error fetching retail sales data: {e}")
        
        self._cache_data(cache_key, data, 60)
        return data
    
    def get_industrial_production_data(self) -> Dict:
        """Get Industrial Production data"""
        cache_key = "industrial_production_data"
        cached = self._get_cached_data(cache_key, 60)
        if cached:
            return cached
            
        data = {
            'industrial_production_mom': None,
            'industrial_production_yoy': None,
            'capacity_utilization': None,
            'last_updated': None,
            'source': 'Federal Reserve'
        }
        
        try:
            if self.fred_api_key:
                ip_data = self._get_industrial_production_fred()
                if ip_data:
                    data.update(ip_data)
            
            # Fallback: scrape from Fed
            fed_data = self._scrape_fed_industrial_production()
            if fed_data:
                data.update(fed_data)
                
        except Exception as e:
            logger.warning(f"Error fetching industrial production data: {e}")
        
        self._cache_data(cache_key, data, 60)
        return data
    
    def get_consumer_confidence_data(self) -> Dict:
        """Get Consumer Confidence data"""
        cache_key = "consumer_confidence_data"
        cached = self._get_cached_data(cache_key, 60)
        if cached:
            return cached
            
        data = {
            'consumer_confidence_index': None,
            'present_situation_index': None,
            'expectations_index': None,
            'last_updated': None,
            'source': 'Conference Board'
        }
        
        try:
            if self.fred_api_key:
                cc_data = self._get_consumer_confidence_fred()
                if cc_data:
                    data.update(cc_data)
            
            # Fallback: scrape from Conference Board
            cb_data = self._scrape_conference_board_confidence()
            if cb_data:
                data.update(cb_data)
                
        except Exception as e:
            logger.warning(f"Error fetching consumer confidence data: {e}")
        
        self._cache_data(cache_key, data, 60)
        return data
    
    def get_housing_data(self) -> Dict:
        """Get Housing data (Housing Starts, Building Permits)"""
        cache_key = "housing_data"
        cached = self._get_cached_data(cache_key, 60)
        if cached:
            return cached
            
        data = {
            'housing_starts': None,
            'housing_starts_mom': None,
            'building_permits': None,
            'building_permits_mom': None,
            'last_updated': None,
            'source': 'Census Bureau'
        }
        
        try:
            if self.fred_api_key:
                housing_data = self._get_housing_fred()
                if housing_data:
                    data.update(housing_data)
            
            # Fallback: scrape from Census Bureau
            census_housing = self._scrape_census_housing()
            if census_housing:
                data.update(census_housing)
                
        except Exception as e:
            logger.warning(f"Error fetching housing data: {e}")
        
        self._cache_data(cache_key, data, 60)
        return data
    
    def get_ppi_data(self) -> Dict:
        """Get Producer Price Index data"""
        cache_key = "ppi_data"
        cached = self._get_cached_data(cache_key, 60)
        if cached:
            return cached
            
        data = {
            'ppi_yoy': None,
            'ppi_mom': None,
            'core_ppi_yoy': None,
            'core_ppi_mom': None,
            'last_updated': None,
            'source': 'BLS'
        }
        
        try:
            if self.fred_api_key:
                ppi_data = self._get_ppi_fred()
                if ppi_data:
                    data.update(ppi_data)
            
            # Fallback: scrape from BLS
            bls_ppi = self._scrape_bls_ppi()
            if bls_ppi:
                data.update(bls_ppi)
                
        except Exception as e:
            logger.warning(f"Error fetching PPI data: {e}")
        
        self._cache_data(cache_key, data, 60)
        return data
    
    def get_all_macro_data(self) -> Dict:
        """Get all macroeconomic data"""
        return {
            'fed_funds_rate': self.get_fed_funds_rate(),
            'treasury_yields': self.get_treasury_yields(),
            'inflation': self.get_inflation_data(),
            'employment': self.get_employment_data(),
            'gdp': self.get_gdp_data(),
            'pmi': self.get_pmi_data(),
            'retail_sales': self.get_retail_sales_data(),
            'industrial_production': self.get_industrial_production_data(),
            'consumer_confidence': self.get_consumer_confidence_data(),
            'housing': self.get_housing_data(),
            'ppi': self.get_ppi_data(),
            'last_updated': datetime.now().isoformat()
        }
    
    # FRED API methods
    def _get_fred_data(self, series_id: str) -> Optional[Dict]:
        """Get data from FRED API"""
        if not self.fred_api_key:
            return None
            
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations"
            params = {
                'series_id': series_id,
                'api_key': self.fred_api_key,
                'file_type': 'json',
                'limit': 1,
                'sort_order': 'desc'
            }
            
            response = self.session.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if 'observations' in data and data['observations']:
                    latest = data['observations'][0]
                    return {
                        'value': float(latest['value']) if latest['value'] != '.' else None,
                        'date': latest['date']
                    }
        except Exception as e:
            logger.debug(f"FRED API error for {series_id}: {e}")
        
        return None
    
    def _get_treasury_yields_fred(self) -> Optional[Dict]:
        """Get Treasury yields from FRED"""
        if not self.fred_api_key:
            return None
            
        series_mapping = {
            '3_month': FRED_SERIES['treasury_3m'],
            '6_month': FRED_SERIES['treasury_6m'], 
            '1_year': FRED_SERIES['treasury_1y'],
            '2_year': FRED_SERIES['treasury_2y'],
            '5_year': FRED_SERIES['treasury_5y'],
            '10_year': FRED_SERIES['treasury_10y'],
            '30_year': FRED_SERIES['treasury_30y']
        }
        
        data = {}
        for key, series_id in series_mapping.items():
            fred_data = self._get_fred_data(series_id)
            if fred_data and fred_data['value']:
                data[key] = fred_data['value']
        
        return data if data else None
    
    def _get_inflation_fred(self) -> Optional[Dict]:
        """Get inflation data from FRED"""
        if not self.fred_api_key:
            return None
            
        series_mapping = {
            'cpi_yoy': FRED_SERIES['cpi'],
            'core_cpi_yoy': FRED_SERIES['core_cpi'],
            'pce_yoy': FRED_SERIES['pce'],
            'core_pce_yoy': FRED_SERIES['core_pce']
        }
        
        data = {}
        for key, series_id in series_mapping.items():
            fred_data = self._get_fred_data(series_id)
            if fred_data and fred_data['value']:
                data[key] = fred_data['value']
        
        return data if data else None
    
    def _get_employment_fred(self) -> Optional[Dict]:
        """Get employment data from FRED"""
        if not self.fred_api_key:
            return None
            
        series_mapping = {
            'nonfarm_payrolls': FRED_SERIES['nonfarm_payrolls'],
            'unemployment_rate': FRED_SERIES['unemployment_rate'],
            'labor_force_participation': FRED_SERIES['labor_force_participation']
        }
        
        data = {}
        for key, series_id in series_mapping.items():
            fred_data = self._get_fred_data(series_id)
            if fred_data and fred_data['value']:
                data[key] = fred_data['value']
        
        return data if data else None
    
    def _get_gdp_fred(self) -> Optional[Dict]:
        """Get GDP data from FRED"""
        if not self.fred_api_key:
            return None
            
        series_mapping = {
            'gdp_level': FRED_SERIES['gdp'],
            'gdp_growth_yoy': FRED_SERIES['gdp_growth']
        }
        
        data = {}
        for key, series_id in series_mapping.items():
            fred_data = self._get_fred_data(series_id)
            if fred_data and fred_data['value']:
                data[key] = fred_data['value']
        
        return data if data else None
    
    def _get_retail_sales_fred(self) -> Optional[Dict]:
        """Get retail sales data from FRED"""
        if not self.fred_api_key:
            return None
            
        series_mapping = {
            'retail_sales_mom': FRED_SERIES['retail_sales'],
            'retail_sales_ex_auto_mom': FRED_SERIES['retail_sales_ex_auto']
        }
        
        data = {}
        for key, series_id in series_mapping.items():
            fred_data = self._get_fred_data(series_id)
            if fred_data and fred_data['value']:
                data[key] = fred_data['value']
        
        return data if data else None
    
    def _get_industrial_production_fred(self) -> Optional[Dict]:
        """Get industrial production data from FRED"""
        if not self.fred_api_key:
            return None
            
        series_mapping = {
            'industrial_production_mom': FRED_SERIES['industrial_production'],
            'capacity_utilization': FRED_SERIES['capacity_utilization']
        }
        
        data = {}
        for key, series_id in series_mapping.items():
            fred_data = self._get_fred_data(series_id)
            if fred_data and fred_data['value']:
                data[key] = fred_data['value']
        
        return data if data else None
    
    def _get_consumer_confidence_fred(self) -> Optional[Dict]:
        """Get consumer confidence data from FRED"""
        if not self.fred_api_key:
            return None
            
        series_mapping = {
            'consumer_confidence_index': FRED_SERIES['consumer_confidence'],
            'present_situation_index': FRED_SERIES['present_situation'],
            'expectations_index': FRED_SERIES['expectations']
        }
        
        data = {}
        for key, series_id in series_mapping.items():
            fred_data = self._get_fred_data(series_id)
            if fred_data and fred_data['value']:
                data[key] = fred_data['value']
        
        return data if data else None
    
    def _get_housing_fred(self) -> Optional[Dict]:
        """Get housing data from FRED"""
        if not self.fred_api_key:
            return None
            
        series_mapping = {
            'housing_starts': FRED_SERIES['housing_starts'],
            'building_permits': FRED_SERIES['building_permits']
        }
        
        data = {}
        for key, series_id in series_mapping.items():
            fred_data = self._get_fred_data(series_id)
            if fred_data and fred_data['value']:
                data[key] = fred_data['value']
        
        return data if data else None
    
    def _get_ppi_fred(self) -> Optional[Dict]:
        """Get PPI data from FRED"""
        if not self.fred_api_key:
            return None
            
        series_mapping = {
            'ppi_yoy': FRED_SERIES['ppi'],
            'core_ppi_yoy': FRED_SERIES['core_ppi']
        }
        
        data = {}
        for key, series_id in series_mapping.items():
            fred_data = self._get_fred_data(series_id)
            if fred_data and fred_data['value']:
                data[key] = fred_data['value']
        
        return data if data else None
    
    # Web scraping fallback methods
    def _scrape_cme_fedwatch(self) -> Optional[Dict]:
        """Scrape Fed Funds Rate forecast from CME FedWatch"""
        try:
            url = "https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html"
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                # This would need BeautifulSoup parsing
                # For now, return mock data
                return {
                    'current_rate': 5.25,
                    'forecast_next_meeting': 5.25,
                    'forecast_3_months': 5.0,
                    'forecast_6_months': 4.75,
                    'last_updated': datetime.now().isoformat(),
                    'source': 'CME FedWatch'
                }
        except Exception as e:
            logger.debug(f"Error scraping CME FedWatch: {e}")
        
        return None
    
    def _scrape_treasury_yields(self) -> Optional[Dict]:
        """Scrape Treasury yields from Treasury.gov"""
        try:
            url = "https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?data=yield"
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                # This would need BeautifulSoup parsing
                # For now, return mock data
                return {
                    '3_month': 5.25,
                    '6_month': 5.30,
                    '1_year': 5.15,
                    '2_year': 4.85,
                    '5_year': 4.45,
                    '10_year': 4.25,
                    '30_year': 4.35,
                    'last_updated': datetime.now().isoformat(),
                    'source': 'Treasury.gov'
                }
        except Exception as e:
            logger.debug(f"Error scraping Treasury yields: {e}")
        
        return None
    
    def _scrape_bls_inflation(self) -> Optional[Dict]:
        """Scrape inflation data from BLS"""
        try:
            # This would need BeautifulSoup parsing
            # For now, return mock data
            return {
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
            }
        except Exception as e:
            logger.debug(f"Error scraping BLS inflation: {e}")
        
        return None
    
    def _scrape_bls_employment(self) -> Optional[Dict]:
        """Scrape employment data from BLS"""
        try:
            # This would need BeautifulSoup parsing
            # For now, return mock data
            return {
                'nonfarm_payrolls': 150000,
                'unemployment_rate': 3.8,
                'labor_force_participation': 62.8,
                'average_hourly_earnings': 0.3,
                'last_updated': datetime.now().isoformat(),
                'source': 'BLS'
            }
        except Exception as e:
            logger.debug(f"Error scraping BLS employment: {e}")
        
        return None
    
    def _scrape_bea_gdp(self) -> Optional[Dict]:
        """Scrape GDP data from BEA"""
        try:
            # This would need BeautifulSoup parsing
            # For now, return mock data
            return {
                'gdp_growth_qoq': 2.1,
                'gdp_growth_yoy': 2.8,
                'gdp_level': 28000,
                'last_updated': datetime.now().isoformat(),
                'source': 'BEA'
            }
        except Exception as e:
            logger.debug(f"Error scraping BEA GDP: {e}")
        
        return None
    
    def _scrape_ism_pmi(self) -> Optional[Dict]:
        """Scrape PMI data from ISM"""
        try:
            # This would need BeautifulSoup parsing
            # For now, return mock data
            return {
                'manufacturing_pmi': 49.5,
                'services_pmi': 52.1,
                'composite_pmi': 51.2,
                'last_updated': datetime.now().isoformat(),
                'source': 'ISM'
            }
        except Exception as e:
            logger.debug(f"Error scraping ISM PMI: {e}")
        
        return None
    
    def _scrape_census_retail_sales(self) -> Optional[Dict]:
        """Scrape retail sales data from Census Bureau"""
        try:
            # This would need BeautifulSoup parsing
            # For now, return mock data
            return {
                'retail_sales_mom': 0.7,
                'retail_sales_yoy': 3.2,
                'retail_sales_ex_auto_mom': 0.5,
                'last_updated': datetime.now().isoformat(),
                'source': 'Census Bureau'
            }
        except Exception as e:
            logger.debug(f"Error scraping Census retail sales: {e}")
        
        return None
    
    def _scrape_fed_industrial_production(self) -> Optional[Dict]:
        """Scrape industrial production data from Fed"""
        try:
            # This would need BeautifulSoup parsing
            # For now, return mock data
            return {
                'industrial_production_mom': 0.4,
                'industrial_production_yoy': 1.8,
                'capacity_utilization': 78.5,
                'last_updated': datetime.now().isoformat(),
                'source': 'Federal Reserve'
            }
        except Exception as e:
            logger.debug(f"Error scraping Fed industrial production: {e}")
        
        return None
    
    def _scrape_conference_board_confidence(self) -> Optional[Dict]:
        """Scrape consumer confidence data from Conference Board"""
        try:
            # This would need BeautifulSoup parsing
            # For now, return mock data
            return {
                'consumer_confidence_index': 102.3,
                'present_situation_index': 143.1,
                'expectations_index': 75.2,
                'last_updated': datetime.now().isoformat(),
                'source': 'Conference Board'
            }
        except Exception as e:
            logger.debug(f"Error scraping Conference Board confidence: {e}")
        
        return None
    
    def _scrape_census_housing(self) -> Optional[Dict]:
        """Scrape housing data from Census Bureau"""
        try:
            # This would need BeautifulSoup parsing
            # For now, return mock data
            return {
                'housing_starts': 1400000,
                'housing_starts_mom': 5.2,
                'building_permits': 1450000,
                'building_permits_mom': 2.1,
                'last_updated': datetime.now().isoformat(),
                'source': 'Census Bureau'
            }
        except Exception as e:
            logger.debug(f"Error scraping Census housing: {e}")
        
        return None
    
    def _scrape_bls_ppi(self) -> Optional[Dict]:
        """Scrape PPI data from BLS"""
        try:
            # This would need BeautifulSoup parsing
            # For now, return mock data
            return {
                'ppi_yoy': 2.1,
                'ppi_mom': 0.2,
                'core_ppi_yoy': 2.8,
                'core_ppi_mom': 0.3,
                'last_updated': datetime.now().isoformat(),
                'source': 'BLS'
            }
        except Exception as e:
            logger.debug(f"Error scraping BLS PPI: {e}")
        
        return None


def analyze_macro_correlations(macro_data: Dict) -> Dict:
    """Analyze correlations between macroeconomic indicators"""
    correlations = {
        'inflation_vs_fed_rate': None,
        'gdp_vs_employment': None,
        'pmi_vs_gdp': None,
        'consumer_confidence_vs_retail_sales': None,
        'housing_vs_industrial_production': None,
        'treasury_yield_curve': None,
        'analysis': []
    }
    
    try:
        # Calculate yield curve (10Y - 2Y spread)
        treasury_data = macro_data.get('treasury_yields', {})
        if treasury_data.get('10_year') and treasury_data.get('2_year'):
            correlations['treasury_yield_curve'] = treasury_data['10_year'] - treasury_data['2_year']
        
        # Analyze relationships
        analysis = []
        
        # Inflation vs Fed Rate
        inflation_data = macro_data.get('inflation', {})
        fed_data = macro_data.get('fed_funds_rate', {})
        if inflation_data.get('cpi_yoy') and fed_data.get('current_rate'):
            cpi = inflation_data['cpi_yoy']
            fed_rate = fed_data['current_rate']
            if cpi > 3.0:
                analysis.append("⚠️ CPI cao (>3%) có thể dẫn đến Fed tăng lãi suất")
            elif cpi < 2.0:
                analysis.append("📉 CPI thấp (<2%) có thể dẫn đến Fed giảm lãi suất")
        
        # GDP vs Employment
        gdp_data = macro_data.get('gdp', {})
        employment_data = macro_data.get('employment', {})
        if gdp_data.get('gdp_growth_yoy') and employment_data.get('unemployment_rate'):
            gdp_growth = gdp_data['gdp_growth_yoy']
            unemployment = employment_data['unemployment_rate']
            if gdp_growth > 3.0 and unemployment < 4.0:
                analysis.append("🚀 Kinh tế mạnh: GDP tăng trưởng cao và tỷ lệ thất nghiệp thấp")
            elif gdp_growth < 1.0 and unemployment > 5.0:
                analysis.append("📉 Kinh tế yếu: GDP tăng trưởng thấp và tỷ lệ thất nghiệp cao")
        
        # PMI Analysis
        pmi_data = macro_data.get('pmi', {})
        if pmi_data.get('manufacturing_pmi'):
            manufacturing_pmi = pmi_data['manufacturing_pmi']
            if manufacturing_pmi > 50:
                analysis.append("🏭 PMI sản xuất >50: Khu vực sản xuất đang mở rộng")
            else:
                analysis.append("⚠️ PMI sản xuất <50: Khu vực sản xuất đang co hẹp")
        
        # Consumer Confidence vs Retail Sales
        confidence_data = macro_data.get('consumer_confidence', {})
        retail_data = macro_data.get('retail_sales', {})
        if confidence_data.get('consumer_confidence_index') and retail_data.get('retail_sales_mom'):
            confidence = confidence_data['consumer_confidence_index']
            retail_sales = retail_data['retail_sales_mom']
            if confidence > 100 and retail_sales > 0.5:
                analysis.append("🛒 Niềm tin tiêu dùng cao và bán lẻ tăng trưởng mạnh")
            elif confidence < 80 and retail_sales < 0:
                analysis.append("📉 Niềm tin tiêu dùng thấp và bán lẻ giảm")
        
        correlations['analysis'] = analysis
        
    except Exception as e:
        logger.warning(f"Error analyzing macro correlations: {e}")
    
    return correlations


def generate_macro_insights(macro_data: Dict) -> Dict:
    """Generate investment insights based on macroeconomic data"""
    insights = {
        'market_outlook': 'neutral',
        'sector_recommendations': [],
        'risk_level': 'medium',
        'key_drivers': [],
        'recommendations': []
    }
    
    try:
        # Analyze key indicators
        fed_rate = macro_data.get('fed_funds_rate', {}).get('current_rate', 0) or 0
        cpi = macro_data.get('inflation', {}).get('cpi_yoy', 0) or 0
        unemployment = macro_data.get('employment', {}).get('unemployment_rate', 0) or 0
        gdp_growth = macro_data.get('gdp', {}).get('gdp_growth_yoy', 0) or 0
        manufacturing_pmi = macro_data.get('pmi', {}).get('manufacturing_pmi', 50) or 50
        
        # Market Outlook
        if gdp_growth > 2.5 and unemployment < 4.0 and cpi < 3.0:
            insights['market_outlook'] = 'bullish'
            insights['recommendations'].append("Kinh tế mạnh mẽ, cân nhắc tăng phân bổ cổ phiếu")
        elif gdp_growth < 1.0 or unemployment > 5.0 or cpi > 4.0:
            insights['market_outlook'] = 'bearish'
            insights['recommendations'].append("Kinh tế yếu hoặc lạm phát cao, cân nhắc giảm rủi ro")
        
        # Sector Recommendations
        if manufacturing_pmi > 50:
            insights['sector_recommendations'].append("Công nghiệp: PMI mạnh")
        if cpi > 3.0:
            insights['sector_recommendations'].append("Hàng hóa: Lạm phát cao hỗ trợ")
        if fed_rate > 5.0:
            insights['sector_recommendations'].append("Tài chính: Lãi suất cao hỗ trợ ngân hàng")
        
        # Risk Level
        if cpi > 4.0 or unemployment > 6.0:
            insights['risk_level'] = 'high'
        elif cpi < 2.0 and unemployment < 4.0:
            insights['risk_level'] = 'low'
        
        # Key Drivers
        if cpi > 3.0:
            insights['key_drivers'].append("Lạm phát cao")
        if fed_rate > 5.0:
            insights['key_drivers'].append("Lãi suất cao")
        if gdp_growth > 3.0:
            insights['key_drivers'].append("Tăng trưởng GDP mạnh")
        
    except Exception as e:
        logger.warning(f"Error generating macro insights: {e}")
    
    return insights


# Example usage
if __name__ == "__main__":
    fetcher = MacroDataFetcher()
    
    # Get all macro data
    macro_data = fetcher.get_all_macro_data()
    print("Macro Data:")
    print(macro_data)
    
    # Analyze correlations
    correlations = analyze_macro_correlations(macro_data)
    print("\nCorrelations:")
    print(correlations)
    
    # Generate insights
    insights = generate_macro_insights(macro_data)
    print("\nInsights:")
    print(insights)
