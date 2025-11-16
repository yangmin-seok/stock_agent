-- financial_indicators 테이블 생성
-- 기업의 연간 재무 지표를 저장합니다.
CREATE TABLE IF NOT EXISTS financial_indicators (
    id SERIAL PRIMARY KEY,
    company_code VARCHAR(10) NOT NULL,
    company_name VARCHAR(50) NOT NULL,
    exchange VARCHAR(10) NOT NULL CHECK (exchange IN ('KOSPI', 'KOSDAQ')),
    year INTEGER ,

    -- 기초 정보 (Basic Info)
    sales INTEGER, -- 매출액(억), 
    operating_profit INTEGER, -- 영업이익(억)
    net_income INTEGER, -- 당기순이익(억)

    total_assets INTEGER, -- 자산총계(억)
    total_liabilities INTEGER, -- 부채총계(억)
    total_equity INTEGER, -- 자본총계(억)

    cash_flow_from_operations INTEGER, -- 영업활동현금흐름(억)
    cash_flow_from_investing INTEGER, -- 투자활동현금흐름(억)
    cash_flow_from_financing INTEGER, -- 재무활동현금흐름(억)

    capex INTEGER, -- 자본적지출(억) CAPEX
    fcf INTEGER, -- 잉여현금흐름(억) FCF

    opearting_profit_margin NUMERIC(5, 2), -- 영업이익률(%) OPM
    net_profit_margin NUMERIC(5, 2), -- 순이익률(%) NPM

    roe NUMERIC(5, 2), -- 자기자본순이익률(%) ROE
    roa NUMERIC(5, 2), -- 총자본순이익률(%) ROA

    debt_to_equity_ratio NUMERIC(10, 2), -- 부채비율(%)
    reserve_ratio NUMERIC(10, 2), -- 유보율(%)

    eps INTEGER, -- 주당순이익(원) EPS
    per NUMERIC(10, 2), -- 주가수익비율(배) PER
    bps INTEGER, -- 주당순자산(원) BPS
    pbr NUMERIC(10, 2), -- 주가순자산비율(배) PBR

    dps INTEGER, -- 현금 DPS (원)
    dividend_yield NUMERIC(5, 2), -- 현금배당수익률(%)
    payout_ratio NUMERIC(5, 2), -- 현금배당성향(%)

    -- 데이터 생성 및 업데이트 타임스탬프
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- 각 기업의 특정 연도, 분기 데이터는 유일해야 합니다.
    UNIQUE(company_code, year)
);

COMMENT ON TABLE financial_indicators IS '기업의 연간 재무 지표';
COMMENT ON COLUMN financial_indicators.company_code IS '기업 코드';
COMMENT ON COLUMN financial_indicators.company_name IS '기업 이름';
COMMENT ON COLUMN financial_indicators.exchange IS '거래소 (KOSPI, KOSDAQ)';
COMMENT ON COLUMN financial_indicators.year IS '재무 지표 연도';
COMMENT ON COLUMN financial_indicators.sales IS '매출액(억 원)';
COMMENT ON COLUMN financial_indicators.operating_profit IS '영업이익(억 원)';
COMMENT ON COLUMN financial_indicators.net_income IS '당기순이익(억 원)';
COMMENT ON COLUMN financial_indicators.total_assets IS '자산총계(억 원)';
COMMENT ON COLUMN financial_indicators.total_liabilities IS '부채총계(억 원)';
COMMENT ON COLUMN financial_indicators.total_equity IS '자본총계(억 원)';
COMMENT ON COLUMN financial_indicators.cash_flow_from_operations IS '영업활동현금흐름(억 원)';
COMMENT ON COLUMN financial_indicators.cash_flow_from_investing IS '투자활동현금흐름(억 원)';
COMMENT ON COLUMN financial_indicators.cash_flow_from_financing IS '재무활동현금흐름(억 원)';
COMMENT ON COLUMN financial_indicators.capex IS '자본적지출(억 원)';
COMMENT ON COLUMN financial_indicators.fcf IS '잉여현금흐름(억 원)';
COMMENT ON COLUMN financial_indicators.opearting_profit_margin IS '영업이익률(%)';
COMMENT ON COLUMN financial_indicators.net_profit_margin IS '순이익률(%)';
COMMENT ON COLUMN financial_indicators.roe IS '자기자본순이익률(%)';
COMMENT ON COLUMN financial_indicators.roa IS '총자본순이익률(%)';
COMMENT ON COLUMN financial_indicators.debt_to_equity_ratio IS '부채비율(%)';
COMMENT ON COLUMN financial_indicators.reserve_ratio IS '유보율(%)';
COMMENT ON COLUMN financial_indicators.eps IS '주당순이익(원)';
COMMENT ON COLUMN financial_indicators.per IS '주가수익비율(배)';
COMMENT ON COLUMN financial_indicators.bps IS '주당순자산(원)';
COMMENT ON COLUMN financial_indicators.pbr IS '주가순자산비율(배)';
COMMENT ON COLUMN financial_indicators.dps IS '현금 DPS(원)';   
COMMENT ON COLUMN financial_indicators.dividend_yield IS '현금배당수익률(%)';
COMMENT ON COLUMN financial_indicators.payout_ratio IS '현금배당성향(%)';   
COMMENT ON COLUMN financial_indicators.created_at IS '데이터 생성 일시';
COMMENT ON COLUMN financial_indicators.updated_at IS '데이터 수정 일시';
