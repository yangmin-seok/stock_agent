-- 기존 테이블이 있다면 삭제 (구조 변경을 위해)
DROP TABLE IF EXISTS stock_info;

CREATE TABLE stock_info (
    corp_code VARCHAR(10) PRIMARY KEY, -- DART 고유번호
    company_code VARCHAR(10) NOT NULL, -- 종목코드
    company_name VARCHAR(50) NOT NULL,
    
    sector VARCHAR(100),   -- [변경] 업종 (FDR의 Sector)
    industry TEXT,         -- [변경] 주요 제품/사업내용 (FDR의 Industry)

    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON COLUMN stock_info.corp_code IS 'DART 고유번호';
COMMENT ON COLUMN stock_info.company_code IS '종목코드';
COMMENT ON COLUMN stock_info.company_name IS '회사명';
COMMENT ON COLUMN stock_info.sector IS '표준 산업 분류 (Sector)';
COMMENT ON COLUMN stock_info.industry IS '주요 제품 및 상세 사업 내용 (Industry)';