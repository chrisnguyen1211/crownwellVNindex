import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import re
from typing import Dict, List, Optional
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VietnamStockDataScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def get_stock_overview(self, symbol: str) -> Dict:
        """Get comprehensive stock data from multiple sources"""
        data = {
            'symbol': symbol,
            'free_float': np.nan,
            'market_cap': np.nan,
            'foreign_ownership': np.nan,
            'management_ownership': np.nan,
            'avg_trading_value': np.nan,
            'outstanding_shares': np.nan
        }
        
        try:
            # Try Vietstock first
            vietstock_data = self._scrape_vietstock(symbol)
            data.update(vietstock_data)
            
            # Add delay to be respectful
            time.sleep(1)
            
            # Try CafeF for additional data
            cafef_data = self._scrape_cafef(symbol)
            for key, value in cafef_data.items():
                if pd.isna(data[key]) and not pd.isna(value):
                    data[key] = value
            
        except Exception as e:
            logger.warning(f"Error scraping data for {symbol}: {e}")
        
        return data
    
    def _scrape_vietstock(self, symbol: str) -> Dict:
        """Scrape data from Vietstock.vn"""
        data = {}
        
        try:
            # Try different Vietstock URL patterns
            urls = [
                f"https://finance.vietstock.vn/doanh-nghiep-a/{symbol.lower()}-cong-ty-co-phan.htm",
                f"https://finance.vietstock.vn/doanh-nghiep-a/{symbol.upper()}-cong-ty-co-phan.htm",
                f"https://finance.vietstock.vn/doanh-nghiep-a/{symbol.lower()}.htm",
                f"https://finance.vietstock.vn/doanh-nghiep-a/{symbol.upper()}.htm"
            ]
            
            response = None
            for url in urls:
                try:
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 200 and "Page or Company not found" not in response.text:
                        break
                except:
                    continue
            
            if not response or response.status_code != 200:
                logger.warning(f"Could not access Vietstock for {symbol}")
                return data
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Debug: Print page content for first few symbols
            if symbol in ['FPT', 'MWG', 'VCB']:
                logger.info(f"Vietstock page content for {symbol}: {soup.get_text()[:500]}...")
            
            # Try multiple label variations for each field
            free_float_labels = ["Tỷ lệ cổ phiếu lưu hành", "Cổ phiếu lưu hành", "Free float", "Tỷ lệ lưu hành"]
            for label in free_float_labels:
                free_float_text = self._extract_text_by_label(soup, label)
                if free_float_text:
                    free_float = self._parse_percentage(free_float_text)
                    if free_float is not None:
                        data['free_float'] = free_float
                        break
            
            market_cap_labels = ["Vốn hóa thị trường", "Market cap", "Vốn hóa", "Giá trị vốn hóa"]
            for label in market_cap_labels:
                market_cap_text = self._extract_text_by_label(soup, label)
                if market_cap_text:
                    market_cap = self._parse_market_cap(market_cap_text)
                    if market_cap is not None:
                        data['market_cap'] = market_cap
                        break
            
            foreign_labels = ["Tỷ lệ sở hữu nước ngoài", "Foreign ownership", "Sở hữu nước ngoài", "Tỷ lệ nước ngoài"]
            for label in foreign_labels:
                foreign_text = self._extract_text_by_label(soup, label)
                if foreign_text:
                    foreign_ownership = self._parse_percentage(foreign_text)
                    if foreign_ownership is not None:
                        data['foreign_ownership'] = foreign_ownership
                        break
            
            shares_labels = ["Số cổ phiếu lưu hành", "Outstanding shares", "Cổ phiếu", "Số lượng cổ phiếu"]
            for label in shares_labels:
                shares_text = self._extract_text_by_label(soup, label)
                if shares_text:
                    shares = self._parse_number(shares_text)
                    if shares is not None:
                        data['outstanding_shares'] = shares
                        break
            
        except Exception as e:
            logger.warning(f"Error scraping Vietstock for {symbol}: {e}")
        
        return data
    
    def _scrape_cafef(self, symbol: str) -> Dict:
        """Scrape data from CafeF.vn"""
        data = {}
        
        try:
            # Try different CafeF URL patterns
            urls = [
                f"https://s.cafef.vn/hose/{symbol.lower()}-ctcp.chn",
                f"https://s.cafef.vn/hose/{symbol.upper()}-ctcp.chn",
                f"https://s.cafef.vn/hnx/{symbol.lower()}-ctcp.chn",
                f"https://s.cafef.vn/hnx/{symbol.upper()}-ctcp.chn",
                f"https://s.cafef.vn/upcom/{symbol.lower()}-ctcp.chn",
                f"https://s.cafef.vn/upcom/{symbol.upper()}-ctcp.chn"
            ]
            
            response = None
            for url in urls:
                try:
                    response = self.session.get(url, timeout=10)
                    if response.status_code == 200 and symbol.upper() in response.text:
                        break
                except:
                    continue
            
            if not response or response.status_code != 200:
                logger.warning(f"Could not access CafeF for {symbol}")
                return data
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Debug: Print page content for first few symbols
            if symbol in ['FPT', 'MWG', 'VCB']:
                logger.info(f"CafeF page content for {symbol}: {soup.get_text()[:500]}...")
            
            # Try multiple label variations for trading volume
            volume_labels = ["Khối lượng giao dịch TB", "Khối lượng TB", "Trading volume", "Giao dịch TB", "KLGD TB"]
            for label in volume_labels:
                volume_text = self._extract_text_by_label(soup, label)
                if volume_text:
                    avg_volume = self._parse_trading_volume(volume_text)
                    if avg_volume is not None:
                        data['avg_trading_value'] = avg_volume
                        break

            # Market cap (tỷ đồng) - use as Market Val directly
            market_cap_labels = [
                "Vốn hóa thị trường (tỷ đồng)",
                "Vốn hóa thị trường",
                "Vốn hóa",
                "Market cap"
            ]
            for label in market_cap_labels:
                mc_text = self._extract_text_by_label(soup, label)
                if mc_text:
                    mc_val = self._parse_market_cap(mc_text)
                    if mc_val is not None:
                        data['market_cap'] = mc_val  # billion VND
                        break
            
            # Try multiple label variations for ownership data
            ownership_labels = ["Tỷ lệ sở hữu", "Management ownership", "Sở hữu", "Tỷ lệ sở hữu ban lãnh đạo"]
            for label in ownership_labels:
                ownership_text = self._extract_text_by_label(soup, label)
                if ownership_text:
                    ownership = self._parse_percentage(ownership_text)
                    if ownership is not None:
                        data['management_ownership'] = ownership
                        break
            
        except Exception as e:
            logger.warning(f"Error scraping CafeF for {symbol}: {e}")
        
        return data
    
    def _extract_text_by_label(self, soup: BeautifulSoup, label: str) -> Optional[str]:
        """Extract text value by label"""
        try:
            # Method 1: Look for table rows with label and value
            rows = soup.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                for i, cell in enumerate(cells):
                    cell_text = cell.get_text(strip=True).lower()
                    if label.lower() in cell_text:
                        if i + 1 < len(cells):
                            value = cells[i + 1].get_text(strip=True)
                            if value and value != '':
                                return value
            
            # Method 2: Look for divs with label and value
            divs = soup.find_all('div')
            for div in divs:
                div_text = div.get_text(strip=True).lower()
                if label.lower() in div_text:
                    # Look for next sibling or parent's next sibling
                    next_elem = div.find_next_sibling()
                    if next_elem:
                        value = next_elem.get_text(strip=True)
                        if value and value != '':
                            return value
                    
                    # Check parent's next sibling
                    parent = div.parent
                    if parent:
                        next_sibling = parent.find_next_sibling()
                        if next_sibling:
                            value = next_sibling.get_text(strip=True)
                            if value and value != '':
                                return value
            
            # Method 3: Look for spans with label and value
            spans = soup.find_all('span')
            for span in spans:
                span_text = span.get_text(strip=True).lower()
                if label.lower() in span_text:
                    next_elem = span.find_next_sibling()
                    if next_elem:
                        value = next_elem.get_text(strip=True)
                        if value and value != '':
                            return value
            
            # Method 4: Look for any element containing the label
            all_elements = soup.find_all(text=lambda text: text and label.lower() in text.lower())
            for text_elem in all_elements:
                parent = text_elem.parent
                if parent:
                    # Look for value in same element or next sibling
                    siblings = parent.find_next_siblings()
                    for sibling in siblings:
                        value = sibling.get_text(strip=True)
                        if value and value != '' and value != label:
                            return value
                    
                    # Check if parent has multiple text nodes
                    if parent.get_text(strip=True) != label:
                        full_text = parent.get_text(strip=True)
                        if full_text and full_text != label:
                            return full_text.replace(label, '').strip()
            
        except Exception as e:
            logger.debug(f"Error extracting text for label '{label}': {e}")
        
        return None
    
    def _parse_percentage(self, text: str) -> Optional[float]:
        """Parse percentage from text"""
        try:
            # Remove common text and extract number
            text = text.replace('%', '').replace('percent', '').strip()
            # Extract number with decimal
            match = re.search(r'(\d+\.?\d*)', text)
            if match:
                val = float(match.group(1))
                # Convert to fraction if looks like percent
                if val > 1:
                    val = val / 100.0
                # Clamp to [0,1]
                if val < 0:
                    val = 0.0
                if val > 1:
                    val = 1.0
                return val
        except:
            pass
        return None
    
    def _parse_market_cap(self, text: str) -> Optional[float]:
        """Parse market cap from text (return in billion VND)"""
        try:
            text = text.lower().replace(',', '').strip()
            
            # Extract number
            match = re.search(r'(\d+\.?\d*)', text)
            if not match:
                return None
            
            number = float(match.group(1))
            
            # Convert to billion VND
            if 'tỷ' in text or 'billion' in text:
                return number
            elif 'nghìn tỷ' in text or 'thousand billion' in text:
                return number * 1000
            elif 'triệu' in text or 'million' in text:
                return number / 1000
            else:
                # Assume it's in billion if no unit specified
                return number
                
        except:
            pass
        return None
    
    def _parse_trading_volume(self, text: str) -> Optional[float]:
        """Parse trading volume from text (return in billion VND)"""
        try:
            text = text.lower().replace(',', '').strip()
            
            # Extract number
            match = re.search(r'(\d+\.?\d*)', text)
            if not match:
                return None
            
            number = float(match.group(1))
            
            # Convert to billion VND
            if 'tỷ' in text or 'billion' in text:
                return number
            elif 'triệu' in text or 'million' in text:
                return number / 1000
            else:
                # Assume it's in billion if no unit specified
                return number
                
        except:
            pass
        return None
    
    def _parse_number(self, text: str) -> Optional[float]:
        """Parse number from text"""
        try:
            text = text.replace(',', '').strip()
            match = re.search(r'(\d+\.?\d*)', text)
            if match:
                return float(match.group(1))
        except:
            pass
        return None
    
    def scrape_multiple_stocks(self, symbols: List[str]) -> pd.DataFrame:
        """Scrape data for multiple stocks"""
        results = []
        
        for i, symbol in enumerate(symbols):
            logger.info(f"Scraping {symbol} ({i+1}/{len(symbols)})")
            data = self.get_stock_overview(symbol)
            results.append(data)
            
            # Add delay between requests
            time.sleep(2)
        
        return pd.DataFrame(results)

# Example usage
if __name__ == "__main__":
    scraper = VietnamStockDataScraper()
    
    # Test with a few symbols
    test_symbols = ['FPT', 'MWG', 'VCB']
    df = scraper.scrape_multiple_stocks(test_symbols)
    print(df)
