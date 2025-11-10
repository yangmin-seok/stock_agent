-- schema.sql
CREATE TABLE IF NOT EXISTS investor_trading (
    trade_date DATE PRIMARY KEY,
    individual_trading_value BIGINT,
    foreign_trading_value BIGINT,
    institutional_trading_value BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE investor_trading IS '일자별 증시 유동성 지표';
COMMENT ON COLUMN investor_trading.trade_date IS '거래 일자';
COMMENT ON COLUMN investor_trading.individual_trading_value IS '개인 순매수 금액 (억 원)';
COMMENT ON COLUMN investor_trading.foreign_trading_value IS '외국인 순매수 금액 (억 원)';
COMMENT ON COLUMN investor_trading.institutional_trading_value IS '기관 순매수 금액 (억 원)';