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
        try:
            # Configure retry strategy
            from urllib3.util.retry import Retry
            from requests.adapters import HTTPAdapter
            retry_strategy = Retry(
                total=2,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "OPTIONS"],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        except Exception:
            pass

    def _get_with_retries(self, url: str, timeout: float = 6.0) -> Optional[requests.Response]:
        try:
            return self.session.get(url, timeout=timeout)
        except requests.RequestException as e:
            logger.debug(f"GET failed {url}: {e}")
            return None
    
    def get_stock_overview(self, symbol: str) -> Dict:
        """Get comprehensive stock data from multiple sources"""
        data = {
            'symbol': symbol,
            'company_name': np.nan,
            'current_price': np.nan,
            'free_float': np.nan,
            'market_cap': np.nan,
            'foreign_ownership': np.nan,
            'management_ownership': np.nan,
            'avg_trading_value': np.nan,
            'outstanding_shares': np.nan,
            'pe_ratio': np.nan,
            'pb_ratio': np.nan,
            'roe': np.nan,
            'roa': np.nan,
            'npl_ratio': np.nan,
            'llr': np.nan
        }
        
        try:
            # Try Vietstock first
            vietstock_data = self._scrape_vietstock(symbol)
            data.update(vietstock_data)

            # Try CafeF for additional data
            cafef_data = self._scrape_cafef(symbol)
            for key, value in cafef_data.items():
                if key in data and pd.isna(data[key]) and not pd.isna(value):
                    data[key] = value
            
            # Try DNSE for Free Float, NPL Ratio, and LLR data
            dnse_data = self._scrape_dnse(symbol)
            for key, value in dnse_data.items():
                if key in data and pd.isna(data[key]) and not pd.isna(value):
                    data[key] = value
                    logger.info(f"DNSE provided {key} for {symbol}: {value}")
                elif key not in data and not pd.isna(value):
                    data[key] = value
                    logger.info(f"DNSE provided new {key} for {symbol}: {value}")
            
            # Fallback: Estimate Free Float from foreign ownership if available
            if 'free_float' in data and pd.isna(data['free_float']) and 'foreign_ownership' in data and not pd.isna(data['foreign_ownership']):
                # Estimate Free Float as inverse of foreign ownership (rough approximation)
                foreign_ownership = data['foreign_ownership']
                if foreign_ownership > 0:
                    estimated_free_float = min(0.95, max(0.05, 1.0 - foreign_ownership))
                    data['free_float'] = estimated_free_float
                    logger.info(f"Estimated Free Float for {symbol} from foreign ownership: {estimated_free_float*100:.1f}%")

        except Exception as e:
            logger.warning(f"Error scraping data for {symbol}: {e}")
        
        return data
    
    def _scrape_vietstock(self, symbol: str) -> Dict:
        """Scrape data from Vietstock.vn"""
        data = {}
        
        try:
            # Try different Vietstock URL patterns
            urls = [
                f"https://finance.vietstock.vn/{symbol.lower()}-ctcp.htm",
                f"https://finance.vietstock.vn/{symbol.upper()}-ctcp.htm",
                f"https://finance.vietstock.vn/{symbol.lower()}.htm",
                f"https://finance.vietstock.vn/{symbol.upper()}.htm",
                f"https://finance.vietstock.vn/doanh-nghiep-a/{symbol.lower()}-cong-ty-co-phan.htm",
                f"https://finance.vietstock.vn/doanh-nghiep-a/{symbol.upper()}-cong-ty-co-phan.htm"
            ]
            
            response = None
            for url in urls:
                response = self._get_with_retries(url, timeout=6.0)
                if response and response.status_code == 200 and "Page or Company not found" not in response.text:
                    break
            
            if not response or response.status_code != 200:
                logger.warning(f"Could not access Vietstock for {symbol}")
                return data
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Debug: reduce noisy logging in production
            # if symbol in ['FPT', 'MWG', 'VCB']:
            #     logger.info(f"Vietstock page content for {symbol}: {soup.get_text()[:500]}...")
            
            # Try to extract company name - be more specific to avoid JS code
            company_name_labels = [
                "Tên công ty", "Company name", "Tên doanh nghiệp", "Tên tổ chức",
                "Tên", "Name", "Công ty", "Company", "Doanh nghiệp", "Organization"
            ]
            for label in company_name_labels:
                company_text = self._extract_text_by_label(soup, label)
                if company_text and len(company_text.strip()) > 5 and not any(js_word in company_text.lower() for js_word in ['$', 'function', 'document', 'ready', 'click', 'hide']):
                    data['company_name'] = company_text.strip()
                    logger.info(f"Found company_name for {symbol}: {company_text.strip()}")
                    break
            
            # Try to extract company name from page title or h1 tags
            if pd.isna(data['company_name']):
                try:
                    # Look for h1 tags that might contain company name
                    h1_tags = soup.find_all('h1')
                    for h1 in h1_tags:
                        text = h1.get_text().strip()
                        if text and len(text) > 5 and any(word in text.lower() for word in ['công ty', 'company', 'ctcp', 'tập đoàn']) and not any(js_word in text.lower() for js_word in ['$', 'function', 'document', 'ready']):
                            data['company_name'] = text
                            logger.info(f"Found company_name from h1 for {symbol}: {text}")
                            break
                    
                    # Look for title tag
                    if pd.isna(data['company_name']):
                        title_tag = soup.find('title')
                        if title_tag:
                            title_text = title_tag.get_text().strip()
                            if title_text and len(title_text) > 5 and not any(js_word in title_text.lower() for js_word in ['$', 'function', 'document', 'ready']):
                                # Clean up title (remove common suffixes)
                                clean_title = title_text.replace(' - Vietstock', '').replace(' - Cổ phiếu', '').replace(' - Stock', '')
                                if clean_title != title_text:
                                    data['company_name'] = clean_title
                                    logger.info(f"Found company_name from title for {symbol}: {clean_title}")
                    
                    # Try to find company name in specific divs or spans
                    if pd.isna(data['company_name']):
                        # Look for divs with class containing 'company', 'name', 'title'
                        company_divs = soup.find_all(['div', 'span'], class_=lambda x: x and any(word in x.lower() for word in ['company', 'name', 'title', 'header']))
                        for div in company_divs:
                            text = div.get_text().strip()
                            if text and len(text) > 5 and any(word in text.lower() for word in ['công ty', 'company', 'ctcp', 'tập đoàn']) and not any(js_word in text.lower() for js_word in ['$', 'function', 'document', 'ready']):
                                data['company_name'] = text
                                logger.info(f"Found company_name from div/span for {symbol}: {text}")
                                break
                except Exception:
                    pass
            
            # Try multiple label variations for each field
            # Current price
            price_labels = [
                "Giá hiện tại", "Current price", "Giá", "Price", "Giá cổ phiếu",
                "Giá đóng cửa", "Closing price", "Giá thị trường", "Market price",
                "Giá giao dịch", "Trading price", "Giá CP", "CP price"
            ]
            for label in price_labels:
                price_text = self._extract_text_by_label(soup, label)
                if price_text:
                    price = self._parse_number(price_text)
                    if price is not None and price > 0:
                        data['current_price'] = price
                        logger.info(f"Found current_price for {symbol}: {price}")
                        break
            
            free_float_labels = [
                "Tỷ lệ cổ phiếu lưu hành", "Cổ phiếu lưu hành", "Free float", "Tỷ lệ lưu hành",
                "Cổ phiếu tự do", "Tỷ lệ cổ phiếu tự do", "Free float ratio", "Tỷ lệ free float",
                "Cổ phiếu đang lưu hành", "Tỷ lệ CP lưu hành", "CP lưu hành"
            ]
            for label in free_float_labels:
                free_float_text = self._extract_text_by_label(soup, label)
                if free_float_text:
                    free_float = self._parse_percentage(free_float_text)
                    if free_float is not None and 0 < free_float <= 1:
                        data['free_float'] = free_float
                        logger.info(f"Found free_float for {symbol}: {free_float}")
                        break
            
            market_cap_labels = ["Vốn hóa thị trường", "Market cap", "Vốn hóa", "Giá trị vốn hóa"]
            for label in market_cap_labels:
                market_cap_text = self._extract_text_by_label(soup, label)
                if market_cap_text:
                    market_cap = self._parse_market_cap(market_cap_text)
                    if market_cap is not None:
                        data['market_cap'] = market_cap
                        break
            
            foreign_labels = [
                "Tỷ lệ sở hữu nước ngoài", "Foreign ownership", "Sở hữu nước ngoài", "Tỷ lệ nước ngoài",
                "% NN sở hữu", "Tỷ lệ NN", "Nước ngoài sở hữu", "Foreign holding", "NN sở hữu",
                "Tỷ lệ sở hữu NN", "Sở hữu NN", "Tỷ lệ ngoại", "Ngoại sở hữu"
            ]
            for label in foreign_labels:
                foreign_text = self._extract_text_by_label(soup, label)
                if foreign_text:
                    foreign_ownership = self._parse_percentage(foreign_text)
                    if foreign_ownership is not None and 0 <= foreign_ownership <= 1:
                        data['foreign_ownership'] = foreign_ownership
                        logger.info(f"Found foreign_ownership for {symbol}: {foreign_ownership}")
                        break
            
            shares_labels = ["KLCP đang lưu hành", "Số cổ phiếu lưu hành", "Outstanding shares", "Cổ phiếu", "Số lượng cổ phiếu"]
            for label in shares_labels:
                shares_text = self._extract_text_by_label(soup, label)
                if shares_text:
                    shares = self._parse_number(shares_text)
                    if shares is not None and shares > 1_000_000:
                        data['outstanding_shares'] = shares
                        break
            
            # Management ownership
            management_labels = [
                "Tỷ lệ sở hữu ban lãnh đạo", "Sở hữu ban lãnh đạo", "Management ownership", "Ban lãnh đạo sở hữu",
                "Tỷ lệ BLĐ", "BLĐ sở hữu", "Sở hữu BLĐ", "Management holding", "Ban lãnh đạo",
                "Tỷ lệ sở hữu quản lý", "Quản lý sở hữu", "Sở hữu quản lý"
            ]
            for label in management_labels:
                management_text = self._extract_text_by_label(soup, label)
                if management_text:
                    management_ownership = self._parse_percentage(management_text)
                    if management_ownership is not None and 0 <= management_ownership <= 1:
                        data['management_ownership'] = management_ownership
                        logger.info(f"Found management_ownership for {symbol}: {management_ownership}")
                        break
            
            # KLGD (trading volume in shares) - Vietstock Finance specific
            klgd_labels = [
                "KLGD", "Khối lượng giao dịch", "Trading Volume", "Khối lượng GD", "Volume",
                "Khối lượng giao dịch TB", "KLGD TB", "Average volume", "Volume TB",
                "Khối lượng GD trung bình", "KLGD trung bình", "Trading volume avg"
            ]
            for label in klgd_labels:
                klgd_text = self._extract_text_by_label(soup, label)
                if klgd_text:
                    klgd_shares = self._parse_number(klgd_text)
                    if klgd_shares is not None and klgd_shares > 0:
                        data['klgd_shares'] = klgd_shares
                        # Calculate avg trading value: KLGD * 10 * current_price / 1B (HOSE)
                        if 'current_price' in data and pd.notna(data['current_price']):
                            avg_volume = (klgd_shares * 10 * data['current_price']) / 1_000_000_000
                            data['avg_trading_value'] = avg_volume
                            logger.info(f"Found avg_trading_value for {symbol}: {avg_volume}")
                        break
            
            # Try to find trading value directly
            trading_value_labels = [
                "Giá trị giao dịch", "Trading value", "Giá trị GD", "Value traded",
                "Giá trị giao dịch TB", "Trading value avg", "Giá trị GD TB", "Value traded avg",
                "Khối lượng giao dịch (tỷ VND)", "Trading volume (billion VND)"
            ]
            for label in trading_value_labels:
                trading_text = self._extract_text_by_label(soup, label)
                if trading_text:
                    trading_value = self._parse_number(trading_text)
                    if trading_value is not None and trading_value > 0:
                        # Convert to billion VND if needed
                        if trading_value > 1000:  # Likely in million VND
                            trading_value = trading_value / 1000
                        data['avg_trading_value'] = trading_value
                        logger.info(f"Found direct avg_trading_value for {symbol}: {trading_value}")
                        break
            
            # P/E and P/B ratios - Vietstock Finance specific patterns
            pe_labels = [
                "P/E cơ bản", "P/E", "PE", "Price to Earning", 
                "Hệ số P/E", "Tỷ số P/E", "Giá trên thu nhập",
                "P/E (TTM)", "P/E trailing", "P/E ratio"
            ]
            for label in pe_labels:
                pe_text = self._extract_text_by_label(soup, label)
                if pe_text:
                    pe_ratio = self._parse_number(pe_text)
                    if pe_ratio is not None and pe_ratio > 0 and pe_ratio < 1000:  # Sanity check
                        data['pe_ratio'] = pe_ratio
                        break
            
            pb_labels = [
                "P/B cơ bản", "P/B", "PB", "Price to Book", 
                "Hệ số P/B", "Tỷ số P/B", "Giá trên giá trị sổ sách",
                "P/BV", "P/B ratio", "Price-to-Book"
            ]
            for label in pb_labels:
                pb_text = self._extract_text_by_label(soup, label)
                if pb_text:
                    pb_ratio = self._parse_number(pb_text)
                    if pb_ratio is not None and pb_ratio > 0 and pb_ratio < 100:  # Sanity check
                        data['pb_ratio'] = pb_ratio
                        break
            
            # ROE and ROA from Vietstock Finance
            roe_labels = [
                "ROEA", "ROE", "Return on Equity", "Tỷ suất sinh lời trên vốn chủ sở hữu",
                "ROE cơ bản", "ROE annualized", "Return on Equity Annualized"
            ]
            for label in roe_labels:
                roe_text = self._extract_text_by_label(soup, label)
                if roe_text:
                    roe_val = self._parse_percentage(roe_text)
                    if roe_val is not None and roe_val > 0:
                        data['roe'] = roe_val
                        break
            
            roa_labels = [
                "ROAA", "ROA", "Return on Assets", "Tỷ suất sinh lời trên tài sản",
                "ROA cơ bản", "ROA annualized", "Return on Assets Annualized"
            ]
            for label in roa_labels:
                roa_text = self._extract_text_by_label(soup, label)
                if roa_text:
                    roa_val = self._parse_percentage(roa_text)
                    if roa_val is not None and roa_val > 0:
                        data['roa'] = roa_val
                        break
            
        except Exception as e:
            logger.warning(f"Error scraping Vietstock for {symbol}: {e}")
        
        return data
    
    def _scrape_cafef(self, symbol: str) -> Dict:
        """Scrape data from CafeF.vn"""
        data = {}
        
        try:
            # Try different CafeF URL patterns - prioritize the working ones
            urls = [
                # cafef.vn du-lieu with company slug patterns (most reliable)
                f"https://cafef.vn/du-lieu/hose/{symbol.lower()}-cong-ty-co-phan-{symbol.lower()}.chn",
                f"https://cafef.vn/du-lieu/hose/{symbol.upper()}-cong-ty-co-phan-{symbol.lower()}.chn",
                f"https://cafef.vn/du-lieu/hose/{symbol.lower()}-cong-ty-co-phan-{symbol.upper()}.chn",
                # Simple symbol pages
                f"https://cafef.vn/du-lieu/hose/{symbol.lower()}.chn",
                f"https://cafef.vn/du-lieu/hose/{symbol.upper()}.chn",
                # s.cafef.vn patterns
                f"https://s.cafef.vn/hose/{symbol.lower()}-ctcp.chn",
                f"https://s.cafef.vn/hose/{symbol.upper()}-ctcp.chn",
                f"https://cafef.vn/du-lieu/hnx/{symbol.lower()}-cong-ty-co-phan-{symbol.lower()}.chn",
                f"https://cafef.vn/du-lieu/upcom/{symbol.lower()}-cong-ty-co-phan-{symbol.lower()}.chn",
            ]
            
            response = None
            for url in urls:
                response = self._get_with_retries(url, timeout=6.0)
                if response and response.status_code == 200 and symbol.upper() in response.text:
                    break
            
            if not response or response.status_code != 200:
                logger.warning(f"Could not access CafeF for {symbol}")
                return data
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Debug: reduce noisy logging in production
            # if symbol in ['FPT', 'MWG', 'VCB']:
            #     logger.info(f"CafeF page content for {symbol}: {soup.get_text()[:500]}...")
            
            # CafeF common fields (explicit labels)
            # 1) Market cap (tỷ đồng) - filter out placeholder values
            mc_text = self._extract_text_by_label(soup, "Vốn hóa thị trường (tỷ đồng)")
            if mc_text:
                mc_val = self._parse_market_cap(mc_text)
                if mc_val is not None and mc_val > 0 and mc_val != 1000:
                    data['market_cap'] = mc_val

            # 2) Foreign ownership (%)
            fo_text = self._extract_text_by_label(soup, "Tỷ lệ sở hữu nước ngoài")
            if fo_text:
                fo_val = self._parse_percentage(fo_text)
                if fo_val is not None:
                    data['foreign_ownership'] = fo_val

            # 3) Outstanding shares
            os_text = None
            for label in ["KLCP đang lưu hành", "Số cổ phiếu lưu hành", "Cổ phiếu lưu hành"]:
                os_text = self._extract_text_by_label(soup, label)
                if os_text:
                    break
            if os_text:
                os_val = self._parse_number(os_text)
                # Filter placeholders and too-small values
                if os_val is not None and os_val > 1_000_000 and os_val != 1000:
                    data['outstanding_shares'] = os_val

            # 4) Free float (if present on CafeF)
            ff_text = None
            for label in ["Tỷ lệ tự do chuyển nhượng", "Free float", "Tỷ lệ cổ phiếu tự do"]:
                ff_text = self._extract_text_by_label(soup, label)
                if ff_text:
                    break
            if ff_text:
                ff_val = self._parse_percentage(ff_text)
                if ff_val is not None:
                    data['free_float'] = ff_val

            # 5) P/E and P/B ratios from CafeF
            pe_labels = ["P/E", "PE", "Price to Earning", "Hệ số P/E", "Tỷ số P/E", "Giá trên thu nhập"]
            for label in pe_labels:
                pe_text = self._extract_text_by_label(soup, label)
                if pe_text:
                    pe_ratio = self._parse_number(pe_text)
                    if pe_ratio is not None and pe_ratio > 0:
                        data['pe_ratio'] = pe_ratio
                        break
            
            pb_labels = ["P/B", "PB", "Price to Book", "Hệ số P/B", "Tỷ số P/B", "Giá trên giá trị sổ sách"]
            for label in pb_labels:
                pb_text = self._extract_text_by_label(soup, label)
                if pb_text:
                    pb_ratio = self._parse_number(pb_text)
                    if pb_ratio is not None and pb_ratio > 0:
                        data['pb_ratio'] = pb_ratio
                        break

            # Try multiple label variations for trading volume
            volume_labels = ["Khối lượng giao dịch TB", "Khối lượng TB", "Trading volume", "Giao dịch TB", "KLGD TB"]
            for label in volume_labels:
                volume_text = self._extract_text_by_label(soup, label)
                if volume_text:
                    avg_volume = self._parse_trading_volume(volume_text)
                    if avg_volume is not None:
                        data['avg_trading_value'] = avg_volume
                        break

            # Fallback market cap label variations if exact one above not found
            if 'market_cap' not in data:
                market_cap_labels = [
                    "Vốn hóa thị trường",
                    "Vốn hóa",
                    "Market cap"
                ]
                for label in market_cap_labels:
                    alt_mc_text = self._extract_text_by_label(soup, label)
                    if alt_mc_text:
                        mc_val = self._parse_market_cap(alt_mc_text)
                        if mc_val is not None:
                            data['market_cap'] = mc_val  # billion VND
                            break

            # Regex fallback: search raw text for pattern near label
            if 'market_cap' not in data:
                try:
                    full_text = soup.get_text(separator=' ', strip=True)
                    # examples: "Vốn hóa thị trường (tỷ đồng): 167,625" or "Vốn hóa thị trường: 167.625 tỷ đồng"
                    patterns = [
                        r"Vốn hóa thị trường\s*\(tỷ đồng\)\s*[:：]?\s*([\d\.,]+)",
                        r"Vốn hóa thị trường\s*[:：]?\s*([\d\.,]+)\s*(tỷ|ty|billion)?"
                    ]
                    for pat in patterns:
                        m = re.search(pat, full_text, flags=re.IGNORECASE)
                        if m:
                            num_txt = m.group(1)
                            mc_val = self._parse_market_cap(num_txt)
                            if mc_val is not None and mc_val > 0 and mc_val != 1000:
                                data['market_cap'] = mc_val
                                break
                except Exception:
                    pass

            # Table-based extraction: find row for the symbol and take the column matching "Vốn hóa TT (Tỷ đồng)"
            if 'market_cap' not in data:
                try:
                    tables = soup.find_all('table')
                    for table in tables:
                        headers = [th.get_text(strip=True) for th in table.find_all('th')]
                        if not headers:
                            continue
                        # Normalize headers
                        headers_lower = [h.lower() for h in headers]
                        if any('vốn hóa tt' in h and 'tỷ' in h for h in headers_lower):
                            # find symbol row
                            for tr in table.find_all('tr'):
                                tds = tr.find_all('td')
                                if not tds:
                                    continue
                                row_text = tr.get_text(' ', strip=True)
                                if symbol.upper() in row_text or f"/{symbol.upper()}-" in row_text:
                                    # find the column index for market cap
                                    mc_idx = None
                                    for idx, h in enumerate(headers_lower):
                                        if 'vốn hóa tt' in h and 'tỷ' in h:
                                            mc_idx = idx
                                            break
                                    if mc_idx is not None and mc_idx < len(tds):
                                        cell = tds[mc_idx].get_text(strip=True)
                                        mc_val = self._parse_market_cap(cell)
                                        if mc_val is not None and mc_val > 0:
                                            data['market_cap'] = mc_val
                                            raise StopIteration
                    
                except StopIteration:
                    pass
                except Exception:
                    pass
            
            # Try multiple label variations for ownership data
            ownership_labels = ["Tỷ lệ sở hữu ban lãnh đạo", "Ban lãnh đạo sở hữu", "Management ownership"]
            for label in ownership_labels:
                ownership_text = self._extract_text_by_label(soup, label)
                if ownership_text:
                    ownership = self._parse_percentage(ownership_text)
                    if ownership is not None and ownership <= 0.8:
                        data['management_ownership'] = ownership
                        break
            
        except Exception as e:
            logger.warning(f"Error scraping CafeF for {symbol}: {e}")
        
        return data
    
    def _extract_text_by_label(self, soup: BeautifulSoup, label: str) -> Optional[str]:
        """Extract text value by label using regex patterns"""
        try:
            import re
            
            # Get full page text
            page_text = soup.get_text()
            
            # Define patterns for different labels
            patterns = {
                'p/e': [r'P/E[:\s]*([\d,]+\.?\d*)', r'P/E cơ bản[:\s]*([\d,]+\.?\d*)', r'Price to Earning[:\s]*([\d,]+\.?\d*)'],
                'p/b': [r'P/B[:\s]*([\d,]+\.?\d*)', r'P/B cơ bản[:\s]*([\d,]+\.?\d*)', r'Price to Book[:\s]*([\d,]+\.?\d*)'],
                'roe': [r'ROEA[:\s]*([\d,]+\.?\d*)', r'ROE[:\s]*([\d,]+\.?\d*)', r'Return on Equity[:\s]*([\d,]+\.?\d*)'],
                'roa': [r'ROAA[:\s]*([\d,]+\.?\d*)', r'ROA[:\s]*([\d,]+\.?\d*)', r'Return on Assets[:\s]*([\d,]+\.?\d*)'],
                'market cap': [r'Vốn hóa thị trường[:\s]*([\d,]+\.?\d*)', r'Market Cap[:\s]*([\d,]+\.?\d*)', r'Vốn hóa[:\s]*([\d,]+\.?\d*)'],
                'free float': [r'Free Float[:\s]*([\d,]+\.?\d*)', r'Tỷ lệ cổ phiếu lưu hành[:\s]*([\d,]+\.?\d*)'],
                'foreign ownership': [r'Foreign Ownership[:\s]*([\d,]+\.?\d*)', r'Tỷ lệ sở hữu nước ngoài[:\s]*([\d,]+\.?\d*)', r'% NN sở hữu[:\s]*([\d,]+\.?\d*)', r'% NN[:\s]*([\d,]+\.?\d*)'],
                'outstanding shares': [r'Outstanding Shares[:\s]*([\d,]+\.?\d*)', r'Số cổ phiếu lưu hành[:\s]*([\d,]+\.?\d*)'],
                'trading volume': [r'KLGD[:\s]*([\d,]+\.?\d*)', r'Khối lượng giao dịch[:\s]*([\d,]+\.?\d*)', r'Trading Volume[:\s]*([\d,]+\.?\d*)']
            }
            
            # Find matching patterns
            label_lower = label.lower()
            for key, pattern_list in patterns.items():
                if key in label_lower:
                    for pattern in pattern_list:
                        matches = re.findall(pattern, page_text, re.IGNORECASE)
                        for match in matches:
                            # Clean the match
                            clean_match = match.replace(',', '')
                            if clean_match and clean_match != '1000' and clean_match != '1':
                                return clean_match
            
            # Fallback: original table-based approach
            rows = soup.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                for i, cell in enumerate(cells):
                    cell_text = cell.get_text(strip=True).lower()
                    if label.lower() in cell_text:
                        if i + 1 < len(cells):
                            value = cells[i + 1].get_text(strip=True)
                            if value and value != '' and value != '1000' and value != '1':
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
                val = float(match.group(1))
                # ignore likely year values
                if 1900 <= val <= 2100:
                    return None
                return val
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
        
        return pd.DataFrame(results)
    
    def _scrape_vndirect(self, symbol: str) -> Dict:
        """Scrape data from VNDirect dstock.vndirect.com.vn"""
        data = {}

        try:
            url = f"https://dstock.vndirect.com.vn/tong-quan/{symbol}"
            response = self._get_with_retries(url)

            if not response or not response.ok:
                return data

            soup = BeautifulSoup(response.content, 'html.parser')

            # Look for Free Float data using the specific structure you found
            free_float = self._extract_free_float_vndirect(soup)
            if free_float is not None:
                data['free_float'] = free_float
                logger.info(f"Found free_float for {symbol}: {free_float}")
            else:
                logger.debug(f"No free_float data found for {symbol} on VNDirect")

            # Look for other metrics that might be available
            market_cap = self._extract_market_cap_vndirect(soup)
            if market_cap is not None:
                data['market_cap'] = market_cap
                logger.info(f"Found market_cap for {symbol}: {market_cap}")

        except Exception as e:
            logger.debug(f"Error scraping VNDirect for {symbol}: {e}")

        return data
    
    def _scrape_dnse(self, symbol: str) -> Dict:
        """Scrape data from DNSE dnse.com.vn"""
        data = {}

        try:
            url = f"https://www.dnse.com.vn/senses/co-phieu-{symbol}"
            response = self._get_with_retries(url)

            if not response or not response.ok:
                return data

            soup = BeautifulSoup(response.content, 'html.parser')

            # Look for Free Float data
            free_float = self._extract_free_float_dnse(soup)
            if free_float is not None:
                data['free_float'] = free_float
                logger.info(f"Found free_float for {symbol} on DNSE: {free_float}")

            # Look for NPL Ratio data
            npl_ratio = self._extract_npl_ratio_dnse(soup, symbol)
            if npl_ratio is not None:
                data['npl_ratio'] = npl_ratio
                logger.info(f"Found npl_ratio for {symbol} on DNSE: {npl_ratio}")

            # Look for LLR data
            llr = self._extract_llr_dnse(soup, symbol)
            if llr is not None:
                data['llr'] = llr
                logger.info(f"Found llr for {symbol} on DNSE: {llr}")

            # EPS (numeric, not percent)
            eps_dnse = self._extract_eps_dnse(soup)
            if eps_dnse is not None:
                data['eps'] = eps_dnse
                logger.info(f"Found eps for {symbol} on DNSE: {eps_dnse}")

            # Dividend Yield (percent)
            div_yield = self._extract_dividend_yield_dnse(soup)
            if div_yield is not None:
                data['dividend_yield'] = div_yield
                logger.info(f"Found dividend_yield for {symbol} on DNSE: {div_yield}")

        except Exception as e:
            logger.debug(f"Error scraping DNSE for {symbol}: {e}")

        return data
    
    def _extract_free_float_dnse(self, soup: BeautifulSoup) -> Optional[float]:
        """Extract Free Float percentage from DNSE page"""
        try:
            import re
            
            # Look for "Tỷ lệ Free float" text and extract the percentage
            # DNSE structure: "Tỷ lệ Free float" followed by percentage
            text = soup.get_text()
            
            # Look for "Tỷ lệ Free float" pattern
            free_float_match = re.search(r'Tỷ lệ Free float[^0-9]*(\d+(?:\.\d+)?)\s*%', text, re.IGNORECASE)
            if free_float_match:
                value = float(free_float_match.group(1))
                logger.info(f"Found Free Float on DNSE: {value}%")
                return value / 100.0
            
            # Alternative: Look for any text containing "Free float" and percentage
            free_float_elements = soup.find_all(text=re.compile(r'Free\s*float', re.IGNORECASE))
            for element in free_float_elements:
                parent = element.parent
                if parent:
                    parent_text = parent.get_text()
                    # Look for percentage in the same element or nearby
                    percentage_match = re.search(r'(\d+(?:\.\d+)?)\s*%', parent_text)
                    if percentage_match:
                        value = float(percentage_match.group(1))
                        if value > 0:
                            logger.info(f"Found Free Float on DNSE: {value}%")
                            return value / 100.0

        except Exception as e:
            logger.debug(f"Error extracting Free Float from DNSE: {e}")

        return None
    
    def _extract_npl_ratio_dnse(self, soup: BeautifulSoup, symbol: str = None) -> Optional[float]:
        """Extract NPL Ratio from DNSE page - only for banks"""
        try:
            import re
            
            # Check if this is a known bank symbol (primary method)
            bank_symbols = {"ACB","BID","CTG","VCB","TCB","TPB","MBB","STB","VIB","VPB","HDB","EIB","SHB","MSB","LPB","NAB","OJB","PGB","SGB","SSB","TAB","VAB","VBB","VCC","VDB","VGB","VIB","VLB","VNB","VPB","VSB","VTB","VUB","VVB","VXB","VYB","VZB"}
            
            is_bank_symbol = symbol in bank_symbols if symbol else False
            
            # Get text for extraction
            text = soup.get_text()
            
            # Secondary check: look for bank-specific terms in text (more specific)
            if not is_bank_symbol:
                bank_indicators = ['ngân hàng', 'bank', 'tín dụng', 'credit']
                is_bank_text = any(indicator in text.lower() for indicator in bank_indicators)
            else:
                is_bank_text = True  # Already confirmed as bank symbol
            
            if not is_bank_symbol and not is_bank_text:
                logger.debug(f"Not a bank page (symbol: {symbol}), skipping NPL ratio extraction")
                return None
            
            # Look for "Tỷ lệ nợ xấu" or "NPL ratio" text and extract the percentage
            npl_match = re.search(r'Tỷ lệ nợ xấu[^0-9]*(\d+(?:\.\d+)?)\s*%', text, re.IGNORECASE)
            if npl_match:
                value = float(npl_match.group(1))
                logger.info(f"Found NPL Ratio on DNSE: {value}%")
                return value / 100.0
            
            # Alternative: Look for any text containing "nợ xấu" and percentage
            npl_elements = soup.find_all(text=re.compile(r'nợ xấu', re.IGNORECASE))
            for element in npl_elements:
                parent = element.parent
                if parent:
                    parent_text = parent.get_text()
                    # Look for percentage in the same element or nearby
                    percentage_match = re.search(r'(\d+(?:\.\d+)?)\s*%', parent_text)
                    if percentage_match:
                        value = float(percentage_match.group(1))
                        if value > 0:
                            logger.info(f"Found NPL Ratio on DNSE: {value}%")
                            return value / 100.0

        except Exception as e:
            logger.debug(f"Error extracting NPL Ratio from DNSE: {e}")

        return None
    
    def _extract_llr_dnse(self, soup: BeautifulSoup, symbol: str = None) -> Optional[float]:
        """Extract LLR (Loan Loss Reserve) from DNSE page - only for banks"""
        try:
            import re
            
            # Check if this is a known bank symbol (primary method)
            bank_symbols = {"ACB","BID","CTG","VCB","TCB","TPB","MBB","STB","VIB","VPB","HDB","EIB","SHB","MSB","LPB","NAB","OJB","PGB","SGB","SSB","TAB","VAB","VBB","VCC","VDB","VGB","VIB","VLB","VNB","VPB","VSB","VTB","VUB","VVB","VXB","VYB","VZB"}
            
            is_bank_symbol = symbol in bank_symbols if symbol else False
            
            # Get text for extraction
            text = soup.get_text()
            
            # Secondary check: look for bank-specific terms in text (more specific)
            if not is_bank_symbol:
                bank_indicators = ['ngân hàng', 'bank', 'tín dụng', 'credit']
                is_bank_text = any(indicator in text.lower() for indicator in bank_indicators)
            else:
                is_bank_text = True  # Already confirmed as bank symbol
            
            print(f"DEBUG: Bank detection for {symbol}: is_bank_symbol={is_bank_symbol}, is_bank_text={is_bank_text}")
            
            if not is_bank_symbol and not is_bank_text:
                print(f"DEBUG: Not a bank page (symbol: {symbol}), skipping LLR extraction")
                return None
            
            # Look for "Tỷ lệ bao phủ nợ xấu" or "LLR" text and extract the percentage
            llr_match = re.search(r'Tỷ lệ bao phủ nợ xấu[^0-9]*(\d+(?:\.\d+)?)\s*%', text, re.IGNORECASE)
            if llr_match:
                value = float(llr_match.group(1))
                logger.info(f"Found LLR on DNSE: {value}%")
                return value / 100.0
            
            # Alternative: Look for any text containing "bao phủ nợ xấu" and percentage
            llr_elements = soup.find_all(text=re.compile(r'bao phủ nợ xấu', re.IGNORECASE))
            for element in llr_elements:
                parent = element.parent
                if parent:
                    parent_text = parent.get_text()
                    # Look for percentage in the same element or nearby
                    percentage_match = re.search(r'(\d+(?:\.\d+)?)\s*%', parent_text)
                    if percentage_match:
                        value = float(percentage_match.group(1))
                        if value > 0:
                            logger.info(f"Found LLR on DNSE: {value}%")
                            return value / 100.0

        except Exception as e:
            logger.debug(f"Error extracting LLR from DNSE: {e}")

        return None

    def _extract_eps_dnse(self, soup: BeautifulSoup) -> Optional[float]:
        """Extract EPS value from DNSE page (unit: VND per share)."""
        try:
            import re
            text = soup.get_text(" ")
            # Try explicit 'EPS' label nearby a number (allow separators)
            m = re.search(r"EPS[^\d]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?)", text, re.IGNORECASE)
            if m:
                raw = m.group(1).replace('.', '').replace(',', '')
                val = float(raw)
                return val
        except Exception as e:
            logger.debug(f"Error extracting EPS from DNSE: {e}")
        return None

    def _extract_dividend_yield_dnse(self, soup: BeautifulSoup) -> Optional[float]:
        """Extract Dividend Yield percentage from DNSE page."""
        try:
            import re
            text = soup.get_text(" ")
            # Vietnamese label: Tỷ suất cổ tức
            m = re.search(r"Tỷ\s*suất\s*cổ\s*tức[^\d]*(\d+(?:[.,]\d+)?)\s*%", text, re.IGNORECASE)
            if m:
                val = float(m.group(1).replace(',', '.')) / 100.0
                return val
            # Alternative generic 'Dividend yield'
            m2 = re.search(r"Dividend\s*Yield[^\d]*(\d+(?:[.,]\d+)?)\s*%", text, re.IGNORECASE)
            if m2:
                val = float(m2.group(1).replace(',', '.')) / 100.0
                return val
        except Exception as e:
            logger.debug(f"Error extracting Dividend Yield from DNSE: {e}")
        return None
    
    def _extract_free_float_vndirect(self, soup: BeautifulSoup) -> Optional[float]:
        """Extract Free Float percentage from VNDirect page"""
        try:
            import re
            
            # Method 1: Look for the specific div with class "row-col__title text-desc" containing "Free float"
            free_float_divs = soup.find_all('div', class_='row-col__title text-desc')
            for div in free_float_divs:
                if 'free float' in div.get_text().lower():
                    logger.debug(f"Found Free Float div: {div.get_text()}")
                    
                    # Look for the value in the next sibling or parent container
                    parent = div.parent
                    if parent:
                        # Look for percentage in the same container
                        container_text = parent.get_text()
                        logger.debug(f"Container text: {container_text}")
                        
                        # Check if it's N/A first
                        if 'N/A' in container_text:
                            logger.debug(f"Free Float is N/A for this symbol")
                            return None
                        
                        # Find percentage in the container
                        percentage_match = re.search(r'(\d+(?:\.\d+)?)\s*%', container_text)
                        if percentage_match:
                            value = float(percentage_match.group(1))
                            if value > 0:  # Valid percentage
                                logger.info(f"Found Free Float: {value}%")
                                return value / 100.0  # Convert to decimal
                        
                        # Also check next siblings
                        next_siblings = div.find_next_siblings()
                        for sibling in next_siblings:
                            sibling_text = sibling.get_text().strip()
                            if sibling_text == 'N/A':
                                logger.debug(f"Free Float sibling is N/A")
                                return None
                            elif '%' in sibling_text and sibling_text != 'N/A':
                                match = re.search(r'(\d+(?:\.\d+)?)\s*%', sibling_text)
                                if match:
                                    value = float(match.group(1))
                                    if value > 0:
                                        logger.info(f"Found Free Float in sibling: {value}%")
                                        return value / 100.0
            
            # Method 2: Look for any div containing "Free float" text
            free_float_elements = soup.find_all(text=re.compile(r'Free\s*Float', re.IGNORECASE))
            for element in free_float_elements:
                logger.debug(f"Found Free Float text: {element}")
                parent = element.parent
                if parent:
                    # Look for percentage in the same element or nearby
                    parent_text = parent.get_text()
                    percentage_match = re.search(r'(\d+(?:\.\d+)?)\s*%', parent_text)
                    if percentage_match:
                        value = float(percentage_match.group(1))
                        if value > 0:
                            logger.info(f"Found Free Float in parent: {value}%")
                            return value / 100.0
                    
                    # Check siblings
                    siblings = parent.find_next_siblings()
                    for sibling in siblings:
                        sibling_text = sibling.get_text().strip()
                        if '%' in sibling_text and sibling_text != 'N/A':
                            match = re.search(r'(\d+(?:\.\d+)?)\s*%', sibling_text)
                            if match:
                                value = float(match.group(1))
                                if value > 0:
                                    logger.info(f"Found Free Float in sibling: {value}%")
                                    return value / 100.0
                                
        except Exception as e:
            logger.debug(f"Error extracting Free Float from VNDirect: {e}")
            
        return None
    
    def _extract_market_cap_vndirect(self, soup: BeautifulSoup) -> Optional[float]:
        """Extract Market Cap from VNDirect page"""
        try:
            # Look for Market Cap in various possible locations
            rows = soup.find_all('tr')
            for row in rows:
                text = row.get_text().lower()
                if 'vốn hóa' in text or 'market cap' in text:
                    cells = row.find_all(['td', 'th'])
                    for cell in cells:
                        cell_text = cell.get_text().strip()
                        # Look for billion VND pattern
                        if 'tỷ' in cell_text or 'billion' in cell_text:
                            import re
                            # Extract number before "tỷ" or "billion"
                            match = re.search(r'(\d+(?:\.\d+)?)\s*(?:tỷ|billion)', cell_text, re.IGNORECASE)
                            if match:
                                return float(match.group(1))
                                
        except Exception as e:
            logger.debug(f"Error extracting Market Cap from VNDirect: {e}")
            
        return None

# Example usage
if __name__ == "__main__":
    scraper = VietnamStockDataScraper()
    
    # Test with a few symbols
    test_symbols = ['FPT', 'MWG', 'VCB']
    df = scraper.scrape_multiple_stocks(test_symbols)
    print(df)
