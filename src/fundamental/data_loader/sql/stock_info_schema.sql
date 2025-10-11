-- stock_info 테이블 생성
CREATE TABLE IF NOT EXISTS stock_info (
    company_code VARCHAR(10) NOT NULL,
    company_name VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL CHECK (exchange IN ('KOSPI', 'KOSDAQ')),
    market_cap INTEGER, -- 시가총액(억)
    -- 데이터 생성 및 업데이트 타임스탬프
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (company_code, company_name)
);