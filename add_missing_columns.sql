-- Thêm các cột còn thiếu vào bảng stocks_hose
ALTER TABLE stocks_hose ADD COLUMN IF NOT EXISTS peg DECIMAL(10,2);
ALTER TABLE stocks_hose ADD COLUMN IF NOT EXISTS eps_norm DECIMAL(15,2);
ALTER TABLE stocks_hose ADD COLUMN IF NOT EXISTS book_value_per_share DECIMAL(15,2);
ALTER TABLE stocks_hose ADD COLUMN IF NOT EXISTS revenue_cagr_3y DECIMAL(8,4);
ALTER TABLE stocks_hose ADD COLUMN IF NOT EXISTS profit_cagr_3y DECIMAL(8,4);
ALTER TABLE stocks_hose ADD COLUMN IF NOT EXISTS gross_margin DECIMAL(8,4);
ALTER TABLE stocks_hose ADD COLUMN IF NOT EXISTS operating_margin DECIMAL(8,4);
ALTER TABLE stocks_hose ADD COLUMN IF NOT EXISTS debt_to_equity DECIMAL(8,4);
ALTER TABLE stocks_hose ADD COLUMN IF NOT EXISTS debt_to_asset DECIMAL(8,4);
ALTER TABLE stocks_hose ADD COLUMN IF NOT EXISTS free_cash_flow DECIMAL(20,2);
ALTER TABLE stocks_hose ADD COLUMN IF NOT EXISTS operating_cash_flow DECIMAL(20,2);
ALTER TABLE stocks_hose ADD COLUMN IF NOT EXISTS ev_ebitda DECIMAL(10,2);

-- Thêm các cột còn thiếu vào bảng stocks_hnx
ALTER TABLE stocks_hnx ADD COLUMN IF NOT EXISTS peg DECIMAL(10,2);
ALTER TABLE stocks_hnx ADD COLUMN IF NOT EXISTS eps_norm DECIMAL(15,2);
ALTER TABLE stocks_hnx ADD COLUMN IF NOT EXISTS book_value_per_share DECIMAL(15,2);
ALTER TABLE stocks_hnx ADD COLUMN IF NOT EXISTS revenue_cagr_3y DECIMAL(8,4);
ALTER TABLE stocks_hnx ADD COLUMN IF NOT EXISTS profit_cagr_3y DECIMAL(8,4);
ALTER TABLE stocks_hnx ADD COLUMN IF NOT EXISTS gross_margin DECIMAL(8,4);
ALTER TABLE stocks_hnx ADD COLUMN IF NOT EXISTS operating_margin DECIMAL(8,4);
ALTER TABLE stocks_hnx ADD COLUMN IF NOT EXISTS debt_to_equity DECIMAL(8,4);
ALTER TABLE stocks_hnx ADD COLUMN IF NOT EXISTS debt_to_asset DECIMAL(8,4);
ALTER TABLE stocks_hnx ADD COLUMN IF NOT EXISTS free_cash_flow DECIMAL(20,2);
ALTER TABLE stocks_hnx ADD COLUMN IF NOT EXISTS operating_cash_flow DECIMAL(20,2);
ALTER TABLE stocks_hnx ADD COLUMN IF NOT EXISTS ev_ebitda DECIMAL(10,2);

-- Thêm các cột còn thiếu vào bảng stocks_upcom
ALTER TABLE stocks_upcom ADD COLUMN IF NOT EXISTS peg DECIMAL(10,2);
ALTER TABLE stocks_upcom ADD COLUMN IF NOT EXISTS eps_norm DECIMAL(15,2);
ALTER TABLE stocks_upcom ADD COLUMN IF NOT EXISTS book_value_per_share DECIMAL(15,2);
ALTER TABLE stocks_upcom ADD COLUMN IF NOT EXISTS revenue_cagr_3y DECIMAL(8,4);
ALTER TABLE stocks_upcom ADD COLUMN IF NOT EXISTS profit_cagr_3y DECIMAL(8,4);
ALTER TABLE stocks_upcom ADD COLUMN IF NOT EXISTS gross_margin DECIMAL(8,4);
ALTER TABLE stocks_upcom ADD COLUMN IF NOT EXISTS operating_margin DECIMAL(8,4);
ALTER TABLE stocks_upcom ADD COLUMN IF NOT EXISTS debt_to_equity DECIMAL(8,4);
ALTER TABLE stocks_upcom ADD COLUMN IF NOT EXISTS debt_to_asset DECIMAL(8,4);
ALTER TABLE stocks_upcom ADD COLUMN IF NOT EXISTS free_cash_flow DECIMAL(20,2);
ALTER TABLE stocks_upcom ADD COLUMN IF NOT EXISTS operating_cash_flow DECIMAL(20,2);
ALTER TABLE stocks_upcom ADD COLUMN IF NOT EXISTS ev_ebitda DECIMAL(10,2);






