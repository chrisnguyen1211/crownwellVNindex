# Hướng dẫn Setup Supabase Tables

## Bước 1: Tạo các bảng trong Supabase

Truy cập vào Supabase Dashboard: https://supabase.com/dashboard/project/zwyacdcsvzreauftsrke

### Option A: Tạo bảng mới (nếu chưa có)
Vào **SQL Editor** và chạy script từ file `create_tables_complete.sql`:

```sql
-- Copy toàn bộ nội dung từ file create_tables_complete.sql
-- Bao gồm đầy đủ các metrics:
-- - Giá cả: price_vnd, market_val, est_val
-- - Định giá: pe, pb, peg, eps, eps_norm, book_value_per_share
-- - Tăng trưởng: revenue_cagr_3y, profit_cagr_3y
-- - Khả năng sinh lời: roe, roa, gross_margin, operating_margin
-- - Cấu trúc tài chính: debt_to_equity, debt_to_asset, current_ratio, quick_ratio
-- - Dòng tiền: free_cash_flow, operating_cash_flow
-- - Định giá nâng cao: ev_ebitda
-- - Cổ tức: dividend_yield
-- - Sở hữu: foreign_ownership, free_float
-- - Thanh khoản: avg_trading_value
-- - Ngân hàng: npl_ratio, llr
```

### Option B: Thêm cột vào bảng hiện có (nếu đã có bảng cũ)
Nếu bạn đã có bảng từ trước, chạy script từ file `add_missing_columns.sql` để thêm các cột còn thiếu:

```sql
-- Copy toàn bộ nội dung từ file add_missing_columns.sql
-- Sẽ thêm các cột: peg, eps_norm, book_value_per_share, revenue_cagr_3y, 
-- profit_cagr_3y, gross_margin, operating_margin, debt_to_equity, 
-- debt_to_asset, free_cash_flow, operating_cash_flow, ev_ebitda
```

## Bước 2: Kiểm tra kết nối

Sau khi tạo bảng, chạy script test:

```bash
python test_supabase_connection.py
```

## Bước 3: Sử dụng

1. **Scan dữ liệu mới**: Bấm nút "🚀 Scan All VN Stocks" để crawl và lưu dữ liệu mới vào Supabase
2. **Load dữ liệu**: Khi refresh trang hoặc thay đổi filter, app sẽ tự động load dữ liệu từ Supabase thay vì scan lại
3. **Tối ưu**: Dữ liệu được lưu theo từng sàn (HOSE, HNX, UPCOM) để query nhanh hơn

## Cấu trúc dữ liệu

- **stocks_hose**: Cổ phiếu sàn HOSE
- **stocks_hnx**: Cổ phiếu sàn HNX  
- **stocks_upcom**: Cổ phiếu sàn UPCOM

Mỗi bảng chứa:
- Thông tin cơ bản: symbol, exchange, company_name
- Giá cả: price_vnd, market_val, est_val
- Chỉ số tài chính: pe, pb, eps, roe, roa, etc.
- Metadata: scan_timestamp, created_at, updated_at
