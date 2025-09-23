#!/usr/bin/env python3

import sys
sys.path.append('app')

from helpers import fetch_income_statement, fetch_ratios
import pandas as pd
import numpy as np

def debug_valuation():
    print("=== Debug Valuation Calculation ===")
    
    symbol = 'FPT'
    print(f"\nTesting {symbol}...")
    
    # Fetch data
    inc = fetch_income_statement(symbol)
    rat = fetch_ratios(symbol)
    
    print(f"Income shape: {inc.shape}")
    print(f"Ratios shape: {rat.shape}")
    
    if not inc.empty:
        print(f"Income columns: {inc.columns.tolist()}")
        print(f"Latest revenue: {inc['revenue'].iloc[-1] if 'revenue' in inc.columns else 'N/A'}")
    
    if not rat.empty:
        print(f"Ratios columns: {rat.columns.tolist()}")
        latest = rat.sort_values(["year"]).tail(1)
        print(f"Latest P/E: {latest['price_to_earning'].iloc[0] if 'price_to_earning' in latest.columns else 'N/A'}")
        print(f"Latest EPS: {latest['earning_per_share'].iloc[0] if 'earning_per_share' in latest.columns else 'N/A'}")
        print(f"Latest P/B: {latest['price_to_book'].iloc[0] if 'price_to_book' in latest.columns else 'N/A'}")
        print(f"Latest Book Value: {latest['book_value_per_share'].iloc[0] if 'book_value_per_share' in latest.columns else 'N/A'}")
    
    # Test market cap calculation
    print(f"\n=== Market Cap Calculation ===")
    
    if not rat.empty:
        latest = rat.sort_values(["year"]).tail(1)
        
        # Method 1: P/E * EPS
        if 'price_to_earning' in latest.columns and 'earning_per_share' in latest.columns:
            pe_ratio = latest['price_to_earning'].iloc[0]
            eps = latest['earning_per_share'].iloc[0]
            print(f"P/E: {pe_ratio}, EPS: {eps}")
            
            if pd.notna(pe_ratio) and pd.notna(eps) and pe_ratio > 0 and eps > 0:
                price_per_share = pe_ratio * eps
                print(f"Price per share: {price_per_share}")
                
                if not inc.empty and 'revenue' in inc.columns:
                    revenue = inc['revenue'].iloc[-1]
                    print(f"Revenue: {revenue}")
                    
                    # Estimate shares
                    estimated_shares = revenue / (eps * 0.1)
                    print(f"Estimated shares: {estimated_shares}")
                    
                    market_cap = (price_per_share * estimated_shares) / 1_000_000_000
                    print(f"Market Cap: {market_cap:.1f}B VND")
        
        # Method 2: P/B * Book Value
        if 'price_to_book' in latest.columns and 'book_value_per_share' in latest.columns:
            pb_ratio = latest['price_to_book'].iloc[0]
            book_value = latest['book_value_per_share'].iloc[0]
            print(f"P/B: {pb_ratio}, Book Value: {book_value}")
            
            if pd.notna(pb_ratio) and pd.notna(book_value) and pb_ratio > 0 and book_value > 0:
                price_per_share = pb_ratio * book_value
                print(f"Price per share (P/B method): {price_per_share}")
    
    # Test current price fetch
    print(f"\n=== Current Price Fetch ===")
    try:
        from vnstock import Stock
        stock = Stock(symbol=symbol, source='TCBS')
        price_data = stock.history(period='1d')
        print(f"Price data shape: {price_data.shape}")
        if not price_data.empty:
            print(f"Price data columns: {price_data.columns.tolist()}")
            print(f"Latest close price: {price_data['close'].iloc[-1]}")
        else:
            print("No price data returned")
    except Exception as e:
        print(f"Error fetching price: {e}")

if __name__ == "__main__":
    debug_valuation()
