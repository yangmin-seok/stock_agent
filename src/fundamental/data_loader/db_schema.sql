-- financial_indicators 테이블 생성
-- 기업의 분기별 재무 지표를 저장합니다.
CREATE TABLE IF NOT EXISTS financial_indicators (
    id SERIAL PRIMARY KEY,
    company_code VARCHAR(10) NOT NULL,
    company_name VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL CHECK (exchange IN ('KOSPI', 'KOSDAQ')),
    year INTEGER NOT NULL,
    quarter_code VARCHAR(10) NOT NULL CHECK (quarter_code IN ('0','1', '2', '3', '4')), -- 0: 연간, 1: 1분기, 2: 2분기, 3: 3분기, 4: 4분기
    -- 기초 정보 (Basic Info)
    market_cap INTEGER, -- 시가총액(억)
    sales INTEGER NOT NULL, -- 매출액(억), 
    operating_profit INTEGER NOT NULL, -- 영업이익(억)
    net_income INTEGER NOT NULL, -- 당기순이익(억)
    -- 가치 (Value)
    per NUMERIC(10, 2),
    pbr NUMERIC(10, 2),
    eps NUMERIC(10, 2),
    bps NUMERIC(10, 2),
    ev_ebitda NUMERIC(10, 2),
    ev_sales NUMERIC(10, 2),
    peg NUMERIC(10, 2),
    dividend_yield NUMERIC(5, 2),
    -- 수익성/품질 (Profitability/Quality)
    roe NUMERIC(10, 2),
    roa NUMERIC(10, 2),
    roic NUMERIC(10, 2),
    gross_profit_margin NUMERIC(10, 2),
    operating_profit_margin NUMERIC(10, 2), -- 영업이익률
    net_profit_margin NUMERIC(10, 2),
    -- 성장성 (Growth)
    sales_growth_yoy NUMERIC(10, 2),
    sales_growth_qoq NUMERIC(10, 2),
    eps_growth_yoy NUMERIC(10, 2),
    eps_growth_qoq NUMERIC(10, 2),
    -- 안정성 (Solvency)
    debt_ratio NUMERIC(10, 2),
    current_ratio NUMERIC(10, 2),
    interest_coverage_ratio NUMERIC(10, 2),
    -- 데이터 생성 및 업데이트 타임스탬프
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- 각 기업의 특정 연도, 분기 데이터는 유일해야 합니다.
    UNIQUE(company_code, year, quarter_code)
);