-- Create VN30 table mirroring structure of other exchange tables
CREATE TABLE IF NOT EXISTS stocks_vn30 (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    exchange VARCHAR(10) NOT NULL DEFAULT 'VN30',
    company_name TEXT,
    price_vnd DECIMAL(15,2),
    market_val DECIMAL(20,2),
    est_val DECIMAL(20,2),
    pe DECIMAL(10,2),
    pb DECIMAL(10,2),
    peg DECIMAL(10,2),
    eps DECIMAL(15,2),
    eps_norm DECIMAL(15,2),
    book_value_per_share DECIMAL(15,2),
    revenue_cagr_3y DECIMAL(8,4),
    profit_cagr_3y DECIMAL(8,4),
    roe DECIMAL(8,4),
    roa DECIMAL(8,4),
    gross_margin DECIMAL(8,4),
    operating_margin DECIMAL(8,4),
    debt_to_equity DECIMAL(8,4),
    debt_to_asset DECIMAL(8,4),
    current_ratio DECIMAL(8,4),
    quick_ratio DECIMAL(8,4),
    free_cash_flow DECIMAL(20,2),
    operating_cash_flow DECIMAL(20,2),
    ev_ebitda DECIMAL(10,2),
    dividend_yield DECIMAL(8,4),
    foreign_ownership DECIMAL(8,4),
    free_float DECIMAL(8,4),
    avg_trading_value DECIMAL(20,2),
    npl_ratio DECIMAL(8,4),
    llr DECIMAL(8,4),
    scan_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stocks_vn30_symbol ON stocks_vn30(symbol);
CREATE INDEX IF NOT EXISTS idx_stocks_vn30_exchange ON stocks_vn30(exchange);
CREATE INDEX IF NOT EXISTS idx_stocks_vn30_scan_timestamp ON stocks_vn30(scan_timestamp);






