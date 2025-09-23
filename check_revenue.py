#!/usr/bin/env python3

import sys
sys.path.append('app')

from helpers import fetch_income_statement
import pandas as pd

def check_revenue():
    print("=== Checking Revenue Data ===")
    
    symbol = 'FPT'
    inc = fetch_income_statement(symbol)
    
    if not inc.empty:
        print(f"Revenue data for {symbol}:")
        print(inc[['year', 'revenue']].head(10))
        
        # Check if revenue is in thousands, millions, or billions
        latest_revenue = inc['revenue'].iloc[-1]
        print(f"\nLatest revenue: {latest_revenue}")
        
        # FPT should have revenue around 60-70 trillion VND
        # If it's 1515, it might be in trillions
        if latest_revenue < 10000:
            print("Revenue appears to be in TRILLIONS VND")
            print(f"Converted to billions: {latest_revenue * 1000:.1f}B VND")
        elif latest_revenue < 1000000:
            print("Revenue appears to be in BILLIONS VND")
            print(f"Revenue: {latest_revenue:.1f}B VND")
        else:
            print("Revenue appears to be in MILLIONS VND")
            print(f"Converted to billions: {latest_revenue / 1000:.1f}B VND")

if __name__ == "__main__":
    check_revenue()
