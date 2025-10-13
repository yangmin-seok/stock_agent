-- schema.sql
CREATE TABLE IF NOT EXISTS market_liquidity (
    trade_date DATE PRIMARY KEY,
    investor_deposits BIGINT,
    credit_balance BIGINT,
    credit_deposit_ratio NUMERIC(5, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE market_liquidity IS '일자별 증시 유동성 지표';
COMMENT ON COLUMN market_liquidity.trade_date IS '거래 일자';
COMMENT ON COLUMN market_liquidity.investor_deposits IS '투자자 예탁금 (억 원)';
COMMENT ON COLUMN market_liquidity.credit_balance IS '신용거래 잔고 (억 원)';
COMMENT ON COLUMN market_liquidity.credit_deposit_ratio IS '신용거래 잔고 비율 (%)';