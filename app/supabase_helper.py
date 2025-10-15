"""
Supabase helper module cho việc lưu trữ và truy xuất dữ liệu cổ phiếu
"""

from supabase import create_client, Client
import pandas as pd
from typing import List, Dict, Optional
import logging

# Supabase configuration
SUPABASE_URL = "https://zwyacdcsvzreauftsrke.supabase.co"
SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inp3eWFjZGNzdnpyZWF1ZnRzcmtlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1OTQ2MjUxMSwiZXhwIjoyMDc1MDM4NTExfQ.UFKF7lq9S5rmqBkBK7e78e1AkcVbse8UGGMjrP0McEs"

logger = logging.getLogger(__name__)

class SupabaseStockStorage:
    """Class để quản lý lưu trữ dữ liệu cổ phiếu trên Supabase"""
    
    def __init__(self):
        self.supabase: Client = create_client(SUPABASE_URL, SERVICE_ROLE_KEY)
        self.table_mapping = {
            'HOSE': 'stocks_hose',
            'HNX': 'stocks_hnx', 
            'UPCOM': 'stocks_upcom',
            'VN30': 'stocks_vn30',
        }
    
    def get_table_name(self, exchange: str) -> str:
        """Lấy tên bảng từ exchange"""
        return self.table_mapping.get(exchange, 'stocks_hose')
    
    def clear_table(self, exchange: str) -> bool:
        """Xóa tất cả dữ liệu trong bảng của exchange"""
        table_name = self.get_table_name(exchange)
        try:
            result = self.supabase.table(table_name).delete().neq('id', 0).execute()
            logger.info(f"✅ Cleared {len(result.data)} records from {table_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to clear table {table_name}: {e}")
            return False
    
    def save_stocks_data(self, df: pd.DataFrame, exchange: str) -> bool:
        """Lưu dữ liệu cổ phiếu vào bảng Supabase"""
        if df.empty:
            logger.warning(f"No data to save for {exchange}")
            return True
        
        table_name = self.get_table_name(exchange)
        
        # Chuẩn bị dữ liệu để insert
        records = []
        for _, row in df.iterrows():
            record = {
                'symbol': row.get('symbol', ''),
                'exchange': exchange,
                'company_name': row.get('company_name', ''),
                
                # Giá cả và định giá
                'price_vnd': float(row.get('price_vnd', 0)) if pd.notna(row.get('price_vnd')) else 0,
                'market_val': float(row.get('market_val', 0)) if pd.notna(row.get('market_val')) else 0,
                'est_val': float(row.get('est_val', 0)) if pd.notna(row.get('est_val')) else 0,
                'pe': float(row.get('pe', 0)) if pd.notna(row.get('pe')) else 0,
                'pb': float(row.get('pb', 0)) if pd.notna(row.get('pb')) else 0,
                'peg': float(row.get('peg', 0)) if pd.notna(row.get('peg')) else 0,
                'eps': float(row.get('eps', 0)) if pd.notna(row.get('eps')) else 0,
                'eps_norm': float(row.get('eps_norm', 0)) if pd.notna(row.get('eps_norm')) else 0,
                'book_value_per_share': float(row.get('book_value_per_share', 0)) if pd.notna(row.get('book_value_per_share')) else 0,
                
                # Tăng trưởng
                'revenue_cagr_3y': float(row.get('revenue_cagr_3y', 0)) if pd.notna(row.get('revenue_cagr_3y')) else 0,
                'profit_cagr_3y': float(row.get('profit_cagr_3y', 0)) if pd.notna(row.get('profit_cagr_3y')) else 0,
                
                # Khả năng sinh lời
                'roe': float(row.get('roe', 0)) if pd.notna(row.get('roe')) else 0,
                'roa': float(row.get('roa', 0)) if pd.notna(row.get('roa')) else 0,
                'gross_margin': float(row.get('gross_margin', 0)) if pd.notna(row.get('gross_margin')) else 0,
                'operating_margin': float(row.get('operating_margin', 0)) if pd.notna(row.get('operating_margin')) else 0,
                
                # Cấu trúc tài chính
                'debt_to_equity': float(row.get('debt_to_equity', 0)) if pd.notna(row.get('debt_to_equity')) else 0,
                'debt_to_asset': float(row.get('debt_to_asset', 0)) if pd.notna(row.get('debt_to_asset')) else 0,
                'current_ratio': float(row.get('current_ratio', 0)) if pd.notna(row.get('current_ratio')) else 0,
                'quick_ratio': float(row.get('quick_ratio', 0)) if pd.notna(row.get('quick_ratio')) else 0,
                
                # Dòng tiền
                'free_cash_flow': float(row.get('free_cash_flow', 0)) if pd.notna(row.get('free_cash_flow')) else 0,
                'operating_cash_flow': float(row.get('operating_cash_flow', 0)) if pd.notna(row.get('operating_cash_flow')) else 0,
                
                # Định giá nâng cao
                'ev_ebitda': float(row.get('ev_ebitda', 0)) if pd.notna(row.get('ev_ebitda')) else 0,
                
                # Cổ tức
                'dividend_yield': float(row.get('dividend_yield', 0)) if pd.notna(row.get('dividend_yield')) else 0,
                
                # Sở hữu và thanh khoản
                'foreign_ownership': float(row.get('foreign_ownership', 0)) if pd.notna(row.get('foreign_ownership')) else 0,
                'free_float': float(row.get('free_float', 0)) if pd.notna(row.get('free_float')) else 0,
                'avg_trading_value': float(row.get('avg_trading_value', 0)) if pd.notna(row.get('avg_trading_value')) else 0,
                
                # Ngân hàng (NPL, LLR)
                'npl_ratio': float(row.get('npl_ratio', 0)) if pd.notna(row.get('npl_ratio')) else 0,
                'llr': float(row.get('llr', 0)) if pd.notna(row.get('llr')) else 0,
            }
            records.append(record)
        
        try:
            # Insert dữ liệu theo batch
            batch_size = 100
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                result = self.supabase.table(table_name).insert(batch).execute()
                logger.info(f"✅ Inserted {len(batch)} records into {table_name}")
            
            logger.info(f"✅ Successfully saved {len(records)} records to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save data to {table_name}: {e}")
            return False
    
    def load_stocks_data(self, exchange: str) -> Optional[pd.DataFrame]:
        """Load dữ liệu cổ phiếu từ bảng Supabase"""
        table_name = self.get_table_name(exchange)
        
        try:
            result = self.supabase.table(table_name).select("*").order('scan_timestamp', desc=True).execute()
            
            if not result.data:
                logger.info(f"No data found in {table_name}")
                return None
            
            df = pd.DataFrame(result.data)
            logger.info(f"✅ Loaded {len(df)} records from {table_name}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to load data from {table_name}: {e}")
            return None
    
    def get_latest_scan_timestamp(self, exchange: str) -> Optional[str]:
        """Lấy timestamp của lần scan gần nhất"""
        table_name = self.get_table_name(exchange)
        
        try:
            result = self.supabase.table(table_name).select('scan_timestamp').order('scan_timestamp', desc=True).limit(1).execute()
            
            if result.data:
                return result.data[0]['scan_timestamp']
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get latest scan timestamp from {table_name}: {e}")
            return None
    
    def save_all_exchanges_data(self, data_dict: Dict[str, pd.DataFrame]) -> bool:
        """Lưu dữ liệu cho tất cả các sàn"""
        success = True
        
        for exchange, df in data_dict.items():
            if exchange in self.table_mapping:
                logger.info(f"💾 Saving {exchange} data ({len(df)} records)...")
                
                # Clear table trước
                if not self.clear_table(exchange):
                    success = False
                    continue
                
                # Save data
                if not self.save_stocks_data(df, exchange):
                    success = False
        
        return success
    
    def load_all_exchanges_data(self) -> Dict[str, pd.DataFrame]:
        """Load dữ liệu cho tất cả các sàn"""
        data_dict = {}
        
        for exchange in self.table_mapping.keys():
            logger.info(f"📥 Loading {exchange} data...")
            df = self.load_stocks_data(exchange)
            if df is not None:
                data_dict[exchange] = df
        
        return data_dict

# Global instance
supabase_storage = SupabaseStockStorage()
