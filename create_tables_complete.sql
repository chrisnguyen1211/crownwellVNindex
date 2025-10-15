-- Tạo bảng cho cổ phiếu HOSE
CREATE TABLE IF NOT EXISTS stocks_hose (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    exchange VARCHAR(10) NOT NULL DEFAULT 'HOSE',
    company_name TEXT,
    
    -- Giá cả và định giá
    price_vnd DECIMAL(15,2),
    market_val DECIMAL(20,2),
    est_val DECIMAL(20,2),
    pe DECIMAL(10,2),
    pb DECIMAL(10,2),
    peg DECIMAL(10,2),
    eps DECIMAL(15,2),
    eps_norm DECIMAL(15,2),
    book_value_per_share DECIMAL(15,2),
    
    -- Tăng trưởng
    revenue_cagr_3y DECIMAL(8,4),
    profit_cagr_3y DECIMAL(8,4),
    
    -- Khả năng sinh lời
    roe DECIMAL(8,4),
    roa DECIMAL(8,4),
    gross_margin DECIMAL(8,4),
    operating_margin DECIMAL(8,4),
    
    -- Cấu trúc tài chính
    debt_to_equity DECIMAL(8,4),
    debt_to_asset DECIMAL(8,4),
    current_ratio DECIMAL(8,4),
    quick_ratio DECIMAL(8,4),
    
    -- Dòng tiền
    free_cash_flow DECIMAL(20,2),
    operating_cash_flow DECIMAL(20,2),
    
    -- Định giá nâng cao
    ev_ebitda DECIMAL(10,2),
    
    -- Cổ tức
    dividend_yield DECIMAL(8,4),
    
    -- Sở hữu và thanh khoản
    foreign_ownership DECIMAL(8,4),
    free_float DECIMAL(8,4),
    avg_trading_value DECIMAL(20,2),
    
    -- Ngân hàng (NPL, LLR)
    npl_ratio DECIMAL(8,4),
    llr DECIMAL(8,4),
    
    -- Timestamps
    scan_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tạo bảng cho cổ phiếu HNX
CREATE TABLE IF NOT EXISTS stocks_hnx (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    exchange VARCHAR(10) NOT NULL DEFAULT 'HNX',
    company_name TEXT,
    
    -- Giá cả và định giá
    price_vnd DECIMAL(15,2),
    market_val DECIMAL(20,2),
    est_val DECIMAL(20,2),
    pe DECIMAL(10,2),
    pb DECIMAL(10,2),
    peg DECIMAL(10,2),
    eps DECIMAL(15,2),
    eps_norm DECIMAL(15,2),
    book_value_per_share DECIMAL(15,2),
    
    -- Tăng trưởng
    revenue_cagr_3y DECIMAL(8,4),
    profit_cagr_3y DECIMAL(8,4),
    
    -- Khả năng sinh lời
    roe DECIMAL(8,4),
    roa DECIMAL(8,4),
    gross_margin DECIMAL(8,4),
    operating_margin DECIMAL(8,4),
    
    -- Cấu trúc tài chính
    debt_to_equity DECIMAL(8,4),
    debt_to_asset DECIMAL(8,4),
    current_ratio DECIMAL(8,4),
    quick_ratio DECIMAL(8,4),
    
    -- Dòng tiền
    free_cash_flow DECIMAL(20,2),
    operating_cash_flow DECIMAL(20,2),
    
    -- Định giá nâng cao
    ev_ebitda DECIMAL(10,2),
    
    -- Cổ tức
    dividend_yield DECIMAL(8,4),
    
    -- Sở hữu và thanh khoản
    foreign_ownership DECIMAL(8,4),
    free_float DECIMAL(8,4),
    avg_trading_value DECIMAL(20,2),
    
    -- Ngân hàng (NPL, LLR)
    npl_ratio DECIMAL(8,4),
    llr DECIMAL(8,4),
    
    -- Timestamps
    scan_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tạo bảng cho cổ phiếu UPCOM
CREATE TABLE IF NOT EXISTS stocks_upcom (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    exchange VARCHAR(10) NOT NULL DEFAULT 'UPCOM',
    company_name TEXT,
    
    -- Giá cả và định giá
    price_vnd DECIMAL(15,2),
    market_val DECIMAL(20,2),
    est_val DECIMAL(20,2),
    pe DECIMAL(10,2),
    pb DECIMAL(10,2),
    peg DECIMAL(10,2),
    eps DECIMAL(15,2),
    eps_norm DECIMAL(15,2),
    book_value_per_share DECIMAL(15,2),
    
    -- Tăng trưởng
    revenue_cagr_3y DECIMAL(8,4),
    profit_cagr_3y DECIMAL(8,4),
    
    -- Khả năng sinh lời
    roe DECIMAL(8,4),
    roa DECIMAL(8,4),
    gross_margin DECIMAL(8,4),
    operating_margin DECIMAL(8,4),
    
    -- Cấu trúc tài chính
    debt_to_equity DECIMAL(8,4),
    debt_to_asset DECIMAL(8,4),
    current_ratio DECIMAL(8,4),
    quick_ratio DECIMAL(8,4),
    
    -- Dòng tiền
    free_cash_flow DECIMAL(20,2),
    operating_cash_flow DECIMAL(20,2),
    
    -- Định giá nâng cao
    ev_ebitda DECIMAL(10,2),
    
    -- Cổ tức
    dividend_yield DECIMAL(8,4),
    
    -- Sở hữu và thanh khoản
    foreign_ownership DECIMAL(8,4),
    free_float DECIMAL(8,4),
    avg_trading_value DECIMAL(20,2),
    
    -- Ngân hàng (NPL, LLR)
    npl_ratio DECIMAL(8,4),
    llr DECIMAL(8,4),
    
    -- Timestamps
    scan_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Tạo indexes cho performance
CREATE INDEX IF NOT EXISTS idx_stocks_hose_symbol ON stocks_hose(symbol);
CREATE INDEX IF NOT EXISTS idx_stocks_hose_exchange ON stocks_hose(exchange);
CREATE INDEX IF NOT EXISTS idx_stocks_hose_scan_timestamp ON stocks_hose(scan_timestamp);

CREATE INDEX IF NOT EXISTS idx_stocks_hnx_symbol ON stocks_hnx(symbol);
CREATE INDEX IF NOT EXISTS idx_stocks_hnx_exchange ON stocks_hnx(exchange);
CREATE INDEX IF NOT EXISTS idx_stocks_hnx_scan_timestamp ON stocks_hnx(scan_timestamp);

CREATE INDEX IF NOT EXISTS idx_stocks_upcom_symbol ON stocks_upcom(symbol);
CREATE INDEX IF NOT EXISTS idx_stocks_upcom_exchange ON stocks_upcom(exchange);
CREATE INDEX IF NOT EXISTS idx_stocks_upcom_scan_timestamp ON stocks_upcom(scan_timestamp);






