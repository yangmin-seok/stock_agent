-- stock_info 테이블 생성
CREATE TABLE IF NOT EXISTS stock_info (
    corp_code VARCHAR(10) NOT NULL, -- DART 고유번호
    company_code VARCHAR(10) NOT NULL,
    company_name VARCHAR(50) NOT NULL,
    -- 데이터 생성 및 업데이트 타임스탬프
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (corp_code, company_code, company_name)
);

COMMENT ON TABLE stock_info IS '기업의 기본 정보 테이블';
COMMENT ON COLUMN stock_info.corp_code IS 'DART 고유번호';
COMMENT ON COLUMN stock_info.company_code IS '기업 코드';
COMMENT ON COLUMN stock_info.company_name IS '기업 이름';