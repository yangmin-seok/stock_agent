-- stock_weekly_candles 테이블 생성
CREATE TABLE IF NOT EXISTS stock_day_candles (
    id SERIAL PRIMARY KEY,
    company_code VARCHAR(10) NOT NULL,
    company_name VARCHAR(50) NOT NULL,
    candle_date DATE NOT NULL, -- 일간 캔들 날짜 (해당 주의 마지막 거래일)
    open INTEGER NOT NULL,  
    high INTEGER NOT NULL,
    low INTEGER NOT NULL,
    close INTEGER NOT NULL,
    volume BIGINT NOT NULL,
    -- 데이터 생성 및 업데이트 타임스탬프
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    foreign_net_buy_amount INTEGER, -- 외국인 순매수 금액(억)
    pension_fund_net_buy_amount INTEGER, -- 연기금 순매수 금액(억)
    -- 각 기업의 특정 날짜 데이터는 유일해야 합니다.
    UNIQUE(company_code, candle_date)
);