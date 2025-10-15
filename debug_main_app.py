#!/usr/bin/env python3
"""
Debug script để kiểm tra logic load dữ liệu trong main app
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

import pandas as pd
from supabase_helper import supabase_storage

print("=== DEBUG MAIN APP LOGIC ===")

# Test 1: Supabase connection
print("\n1. Testing Supabase connection...")
try:
    supabase_data = supabase_storage.load_all_exchanges_data()
    if supabase_data:
        total_records = sum(len(df) for df in supabase_data.values())
        print(f"✅ Supabase OK: {total_records} total records")
        for exchange, df in supabase_data.items():
            print(f"   {exchange}: {len(df)} records")
    else:
        print("❌ No data from Supabase")
except Exception as e:
    print(f"❌ Supabase error: {e}")

# Test 2: Simulate app logic exactly
print("\n2. Simulating app logic...")
try:
    metrics = pd.DataFrame()
    
    supabase_data = supabase_storage.load_all_exchanges_data()
    if supabase_data:
        all_data = []
        for exchange, df in supabase_data.items():
            if not df.empty:
                df['exchange'] = exchange
                all_data.append(df)
        
        if all_data:
            metrics = pd.concat(all_data, ignore_index=True)
            print(f"✅ Combined metrics: {len(metrics)} records")
            print(f"   Shape: {metrics.shape}")
            print(f"   Columns: {list(metrics.columns)}")
            print(f"   Sample symbols: {metrics['symbol'].head().tolist()}")
        else:
            print("⚠️ No data to combine")
    else:
        print("⚠️ No Supabase data")
        
    print(f"\nFinal metrics.empty = {metrics.empty}")
    if not metrics.empty:
        print("✅ Data ready for display")
    else:
        print("❌ No data to display")
        
except Exception as e:
    print(f"❌ Error in app logic: {e}")
    import traceback
    traceback.print_exc()






