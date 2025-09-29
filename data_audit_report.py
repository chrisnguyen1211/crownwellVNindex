#!/usr/bin/env python3
"""
Data Source Audit Report for VN Stock Screener
==============================================

This script audits all data sources and calculations used in the VN Stock Screener
to verify accuracy and identify potential issues.
"""

import pandas as pd
import numpy as np
import requests
import time
from typing import Dict, List, Tuple
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_vnstock_api():
    """Test vnstock API data sources and accuracy"""
    print("=" * 60)
    print("VNSTOCK API AUDIT")
    print("=" * 60)
    
    from vnstock import Finance, Listing
    
    test_symbols = ['FPT', 'VCB', 'VNM', 'HPG', 'MWG']
    issues = []
    
    for symbol in test_symbols:
        print(f"\nTesting {symbol}:")
        
        try:
            # Test Finance API
            fin = Finance(symbol=symbol, source='TCBS')
            
            # Test income statement
            income = fin.income_statement(period="year")
            if income.empty:
                issues.append(f"{symbol}: Empty income statement")
            else:
                print(f"  ✓ Income statement: {len(income)} years of data")
                if 'revenue' in income.columns:
                    latest_rev = income['revenue'].iloc[-1] if not income.empty else None
                    print(f"    Latest revenue: {latest_rev} (units: billions VND)")
                else:
                    issues.append(f"{symbol}: Missing 'revenue' column in income statement")
            
            # Test ratios
            ratios = fin.ratio(period="year")
            if ratios.empty:
                issues.append(f"{symbol}: Empty ratios")
            else:
                print(f"  ✓ Ratios: {len(ratios)} years of data")
                latest = ratios.iloc[-1] if not ratios.empty else None
                if latest is not None:
                    pe = latest.get('price_to_earning', np.nan)
                    pb = latest.get('price_to_book', np.nan)
                    roe = latest.get('roe', np.nan)
                    roa = latest.get('roa', np.nan)
                    print(f"    P/E: {pe}, P/B: {pb}, ROE: {roe}, ROA: {roa}")
            
            # Test balance sheet
            bs = fin.balance_sheet(period="year")
            if bs.empty:
                issues.append(f"{symbol}: Empty balance sheet")
            else:
                print(f"  ✓ Balance sheet: {len(bs)} years of data")
                if 'equity' in bs.columns:
                    latest_equity = bs['equity'].iloc[-1] if not bs.empty else None
                    print(f"    Latest equity: {latest_equity} (units: billions VND)")
            
            # Test cash flow
            cf = fin.cash_flow(period="year")
            if cf.empty:
                issues.append(f"{symbol}: Empty cash flow")
            else:
                print(f"  ✓ Cash flow: {len(cf)} years of data")
                cf_cols = [c for c in cf.columns if 'cash' in c.lower() or 'flow' in c.lower()]
                print(f"    Cash flow columns: {cf_cols}")
                
        except Exception as e:
            issues.append(f"{symbol}: API error - {e}")
    
    print(f"\nVNSTOCK API ISSUES FOUND: {len(issues)}")
    for issue in issues:
        print(f"  ❌ {issue}")
    
    return issues

def test_web_scraping():
    """Test web scraping data sources"""
    print("\n" + "=" * 60)
    print("WEB SCRAPING AUDIT")
    print("=" * 60)
    
    from app.web_scraper import VietnamStockDataScraper
    
    scraper = VietnamStockDataScraper()
    test_symbols = ['FPT', 'VCB', 'VNM', 'HPG', 'MWG']
    issues = []
    
    for symbol in test_symbols:
        print(f"\nTesting {symbol}:")
        
        try:
            data = scraper.get_stock_overview(symbol)
            
            # Check each field
            fields_to_check = [
                'free_float', 'market_cap', 'foreign_ownership', 
                'management_ownership', 'avg_trading_value', 'outstanding_shares'
            ]
            
            for field in fields_to_check:
                value = data.get(field, np.nan)
                if pd.isna(value):
                    issues.append(f"{symbol}: Missing {field}")
                else:
                    print(f"  ✓ {field}: {value}")
            
            # Validate market cap
            market_cap = data.get('market_cap', np.nan)
            if pd.notna(market_cap):
                if market_cap <= 0:
                    issues.append(f"{symbol}: Invalid market cap {market_cap}")
                elif market_cap == 1000:
                    issues.append(f"{symbol}: Suspicious market cap value 1000 (likely placeholder)")
                elif market_cap > 1000000:  # > 1M billion VND seems unrealistic
                    issues.append(f"{symbol}: Unrealistically high market cap {market_cap}")
            
            # Validate ownership percentages
            for field in ['free_float', 'foreign_ownership', 'management_ownership']:
                value = data.get(field, np.nan)
                if pd.notna(value):
                    if value < 0 or value > 1:
                        issues.append(f"{symbol}: {field} out of range [0,1]: {value}")
            
        except Exception as e:
            issues.append(f"{symbol}: Scraping error - {e}")
    
    print(f"\nWEB SCRAPING ISSUES FOUND: {len(issues)}")
    for issue in issues:
        print(f"  ❌ {issue}")
    
    return issues

def test_tcbs_api():
    """Test TCBS public API for price data"""
    print("\n" + "=" * 60)
    print("TCBS API AUDIT")
    print("=" * 60)
    
    test_symbols = ['FPT', 'VCB', 'VNM', 'HPG', 'MWG']
    issues = []
    
    for symbol in test_symbols:
        print(f"\nTesting {symbol}:")
        
        try:
            # Test price data
            now = int(time.time())
            start = now - 60*60*24*14  # 14 days ago
            url = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars?ticker={symbol}&type=stock&resolution=1&from={start}&to={now}"
            
            response = requests.get(url, timeout=10)
            if response.ok:
                data = response.json()
                if isinstance(data, dict) and 'data' in data:
                    bars = data['data']
                    if bars and len(bars) > 0:
                        latest = bars[-1]
                        close_price = latest.get('close') or latest.get('c')
                        volume = latest.get('volume') or latest.get('v')
                        print(f"  ✓ Latest price: {close_price}, Volume: {volume}")
                        
                        if close_price and close_price <= 0:
                            issues.append(f"{symbol}: Invalid price {close_price}")
                    else:
                        issues.append(f"{symbol}: No price data returned")
                else:
                    issues.append(f"{symbol}: Invalid API response format")
            else:
                issues.append(f"{symbol}: API request failed - {response.status_code}")
                
        except Exception as e:
            issues.append(f"{symbol}: TCBS API error - {e}")
    
    print(f"\nTCBS API ISSUES FOUND: {len(issues)}")
    for issue in issues:
        print(f"  ❌ {issue}")
    
    return issues

def test_calculations():
    """Test calculation formulas and logic"""
    print("\n" + "=" * 60)
    print("CALCULATION AUDIT")
    print("=" * 60)
    
    from app.helpers import compute_cagr
    
    issues = []
    
    # Test CAGR calculation
    print("\nTesting CAGR calculation:")
    
    # Test case 1: Normal growth
    test_series = pd.Series([100, 110, 121, 133.1], index=[2020, 2021, 2022, 2023])
    cagr = compute_cagr(test_series, years=3)
    expected = 0.10  # 10% CAGR
    if abs(cagr - expected) > 0.01:
        issues.append(f"CAGR calculation error: got {cagr}, expected {expected}")
    else:
        print(f"  ✓ CAGR test 1: {cagr:.2%} (expected 10%)")
    
    # Test case 2: Negative values
    test_series_neg = pd.Series([100, 90, 80, 70], index=[2020, 2021, 2022, 2023])
    cagr_neg = compute_cagr(test_series_neg, years=3)
    print(f"  ✓ CAGR test 2 (decline): {cagr_neg:.2%}")
    
    # Test case 3: Insufficient data
    test_series_short = pd.Series([100, 110], index=[2022, 2023])
    cagr_short = compute_cagr(test_series_short, years=3)
    if not pd.isna(cagr_short):
        issues.append(f"CAGR should return NaN for insufficient data, got {cagr_short}")
    else:
        print(f"  ✓ CAGR test 3 (insufficient data): NaN")
    
    # Test PEG calculation
    print("\nTesting PEG calculation:")
    pe = 15.0
    profit_cagr = 0.10  # 10%
    peg = pe / (profit_cagr * 100)  # Current formula
    expected_peg = 1.5
    if abs(peg - expected_peg) > 0.01:
        issues.append(f"PEG calculation error: got {peg}, expected {expected_peg}")
    else:
        print(f"  ✓ PEG test: {peg} (expected 1.5)")
    
    # Test market cap calculation
    print("\nTesting market cap calculation:")
    price = 100000  # VND per share
    shares = 1000000000  # 1 billion shares
    market_cap = (price * shares) / 1_000_000_000  # Convert to billion VND
    expected_market_cap = 100000  # billion VND
    if abs(market_cap - expected_market_cap) > 0.01:
        issues.append(f"Market cap calculation error: got {market_cap}, expected {expected_market_cap}")
    else:
        print(f"  ✓ Market cap test: {market_cap} billion VND")
    
    print(f"\nCALCULATION ISSUES FOUND: {len(issues)}")
    for issue in issues:
        print(f"  ❌ {issue}")
    
    return issues

def test_data_consistency():
    """Test data consistency across different sources"""
    print("\n" + "=" * 60)
    print("DATA CONSISTENCY AUDIT")
    print("=" * 60)
    
    test_symbol = 'FPT'
    issues = []
    
    try:
        # Get data from vnstock
        from vnstock import Finance
        fin = Finance(symbol=test_symbol, source='TCBS')
        ratios = fin.ratio(period="year")
        income = fin.income_statement(period="year")
        
        # Get data from web scraper
        from app.web_scraper import VietnamStockDataScraper
        scraper = VietnamStockDataScraper()
        scraped = scraper.get_stock_overview(test_symbol)
        
        # Get price from TCBS
        now = int(time.time())
        start = now - 60*60*24*14
        url = f"https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars?ticker={test_symbol}&type=stock&resolution=1&from={start}&to={now}"
        response = requests.get(url, timeout=10)
        
        print(f"\nData consistency check for {test_symbol}:")
        
        if not ratios.empty and not income.empty and response.ok:
            latest_ratios = ratios.iloc[-1]
            latest_income = income.iloc[-1]
            
            # Check P/E consistency
            pe_ratio = latest_ratios.get('price_to_earning', np.nan)
            eps = latest_ratios.get('earning_per_share', np.nan)
            
            if pd.notna(pe_ratio) and pd.notna(eps) and response.ok:
                price_data = response.json()
                if 'data' in price_data and price_data['data']:
                    current_price = price_data['data'][-1].get('close', np.nan)
                    if pd.notna(current_price):
                        implied_pe = current_price / eps if eps > 0 else np.nan
                        if pd.notna(implied_pe):
                            pe_diff = abs(pe_ratio - implied_pe) / pe_ratio
                            print(f"  P/E from ratios: {pe_ratio:.2f}")
                            print(f"  P/E from price/EPS: {implied_pe:.2f}")
                            print(f"  Difference: {pe_diff:.1%}")
                            
                            if pe_diff > 0.2:  # >20% difference
                                issues.append(f"{test_symbol}: Large P/E discrepancy ({pe_diff:.1%})")
        
        # Check market cap consistency
        scraped_market_cap = scraped.get('market_cap', np.nan)
        if pd.notna(scraped_market_cap):
            print(f"  Market cap from scraper: {scraped_market_cap} billion VND")
            
            # Compare with calculated market cap
            if not ratios.empty and response.ok:
                latest_ratios = ratios.iloc[-1]
                eps = latest_ratios.get('earning_per_share', np.nan)
                pe = latest_ratios.get('price_to_earning', np.nan)
                
                if pd.notna(eps) and pd.notna(pe) and response.ok:
                    price_data = response.json()
                    if 'data' in price_data and price_data['data']:
                        current_price = price_data['data'][-1].get('close', np.nan)
                        if pd.notna(current_price):
                            # Estimate shares from revenue
                            if not income.empty and 'revenue' in income.columns:
                                revenue = income['revenue'].iloc[-1]
                                if pd.notna(revenue) and revenue > 0:
                                    estimated_shares = revenue / (eps * 0.1)  # Heuristic
                                    calculated_market_cap = (current_price * estimated_shares) / 1_000_000_000
                                    
                                    print(f"  Calculated market cap: {calculated_market_cap:.0f} billion VND")
                                    
                                    if pd.notna(calculated_market_cap):
                                        cap_diff = abs(scraped_market_cap - calculated_market_cap) / scraped_market_cap
                                        print(f"  Market cap difference: {cap_diff:.1%}")
                                        
                                        if cap_diff > 0.5:  # >50% difference
                                            issues.append(f"{test_symbol}: Large market cap discrepancy ({cap_diff:.1%})")
    
    except Exception as e:
        issues.append(f"Data consistency check failed: {e}")
    
    print(f"\nDATA CONSISTENCY ISSUES FOUND: {len(issues)}")
    for issue in issues:
        print(f"  ❌ {issue}")
    
    return issues

def generate_recommendations():
    """Generate recommendations for data quality improvements"""
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS")
    print("=" * 60)
    
    recommendations = [
        "1. DATA SOURCE PRIORITIZATION:",
        "   - Primary: vnstock API (TCBS source) for financial statements",
        "   - Secondary: CafeF web scraping for market cap and ownership data",
        "   - Tertiary: TCBS public API for real-time price data",
        "",
        "2. DATA VALIDATION:",
        "   - Add sanity checks for market cap values (filter out 1000 placeholder)",
        "   - Validate ownership percentages are within [0,1] range",
        "   - Cross-validate P/E ratios between different sources",
        "",
        "3. ERROR HANDLING:",
        "   - Add retry logic for API failures",
        "   - Implement fallback data sources",
        "   - Add data quality scoring",
        "",
        "4. CALCULATION IMPROVEMENTS:",
        "   - Use more robust CAGR calculation with outlier detection",
        "   - Improve shares outstanding estimation using multiple methods",
        "   - Add confidence intervals for estimated values",
        "",
        "5. PERFORMANCE OPTIMIZATION:",
        "   - Cache API responses to reduce rate limiting",
        "   - Implement parallel processing for multiple symbols",
        "   - Add progress bars for long-running operations",
        "",
        "6. DATA DOCUMENTATION:",
        "   - Document data units and sources for each metric",
        "   - Add data freshness timestamps",
        "   - Create data quality dashboard",
    ]
    
    for rec in recommendations:
        print(rec)

def main():
    """Run complete data audit"""
    print("VN STOCK SCREENER - DATA SOURCE AUDIT")
    print("=" * 60)
    print("This audit will test all data sources and calculations")
    print("to identify accuracy issues and provide recommendations.")
    print("=" * 60)
    
    all_issues = []
    
    # Run all audits
    all_issues.extend(test_vnstock_api())
    all_issues.extend(test_web_scraping())
    all_issues.extend(test_tcbs_api())
    all_issues.extend(test_calculations())
    all_issues.extend(test_data_consistency())
    
    # Generate recommendations
    generate_recommendations()
    
    # Summary
    print("\n" + "=" * 60)
    print("AUDIT SUMMARY")
    print("=" * 60)
    print(f"Total issues found: {len(all_issues)}")
    
    if all_issues:
        print("\nAll issues:")
        for i, issue in enumerate(all_issues, 1):
            print(f"  {i}. {issue}")
    else:
        print("\n✅ No issues found! Data sources appear to be working correctly.")
    
    print("\n" + "=" * 60)
    print("AUDIT COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()
