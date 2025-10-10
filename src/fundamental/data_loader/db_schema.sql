-- financial_indicators 테이블 생성
-- 기업의 분기별 재무 지표를 저장합니다.
CREATE TABLE IF NOT EXISTS financial_indicators (
    id SERIAL PRIMARY KEY,
    company_code VARCHAR(10) NOT NULL,
    company_name VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL CHECK (exchange IN ('KOSPI', 'KOSDAQ')),
    year INTEGER NOT NULL,
    quarter_code VARCHAR(10) NOT NULL CHECK (quarter_code IN ('1', '2', '3', '4')),
    -- 가치 (Value)
    per NUMERIC(10, 2),
    pbr NUMERIC(10, 2),
    ev_ebitda NUMERIC(10, 2),
    ev_sales NUMERIC(10, 2),
    peg NUMERIC(10, 2),
    dividend_yield NUMERIC(5, 2),
    -- 수익성/품질 (Profitability/Quality)
    roe NUMERIC(10, 2),
    roa NUMERIC(10, 2),
    roic NUMERIC(10, 2),
    gross_profit_margin NUMERIC(10, 2),
    operating_profit_margin NUMERIC(10, 2),
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

-- 인덱스 추가 (조회 성능 향상)
CREATE INDEX IF NOT EXISTS idx_company_year_quarter ON financial_indicators (company_code, year, quarter_code);

-- updated_at 컬럼 자동 업데이트를 위한 트리거 함수
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ language 'plpgsql';

-- 트리거 생성
-- financial_indicators 테이블에 데이터가 업데이트될 때마다 트리거가 실행됩니다.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'update_financial_indicators_updated_at'
    ) THEN
        CREATE TRIGGER update_financial_indicators_updated_at
        BEFORE UPDATE ON financial_indicators
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    END IF;
END;
$$;