import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Any
import logging
from pykrx import stock
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_top_companies(limit: int = 200) -> pd.DataFrame:

    today = datetime.now().strftime('%Y%d%m')

    # KOSPI 종목 정보 가져오기
    df_kospi = stock.get_market_cap_by_ticker(today, market='KOSPI')[:limit]
    df_kospi['exchange'] = 'KOSPI' # exchange 필드 추가

    # KOSDAQ 종목 정보 가져오기
    df_kosdaq = stock.get_market_cap_by_ticker(today, market='KOSDAQ')[:limit]
    df_kosdaq['exchange'] = 'KOSDAQ' # exchange 필드 추가

    df = pd.concat([df_kospi, df_kosdaq])
    df = df.sort_values(by='시가총액', ascending=False)

    # 인덱스(종목코드)를 리셋하고 'company_code' 컬럼으로 만듭니다.
    df = df.reset_index()
    df = df.rename(columns={'티커': 'company_code'})

    # 종목명을 빠르게 추가합니다.
    df['company_name'] = df['company_code'].map(lambda x: stock.get_market_ticker_name(x))

    # 최종적으로 필요한 컬럼만 선택하고 순서를 정리합니다.
    df = df[['company_name', 'company_code', 'exchange', '시가총액']]

    return df

def scrape_financial_data(company: Dict[str, str]) -> Optional[List[Dict[str, Any]]]:
    """
    주어진 종목 코드로 네이버 재무분석 페이지를 스크래핑하여 분기별 재무 데이터를 반환합니다.

    Args:
        company (Dict[str, str]): 'company_name'과 'company_code'를 포함한 기업 정보 딕셔너리

    Returns:
        Optional[List[Dict[str, Any]]]:
            성공 시 DB 스키마에 맞는 재무 데이터 딕셔너리의 리스트를 반환합니다.
            실패 시 None을 반환합니다.
    """
    url = f"https://finance.naver.com/item/main.naver?code={company['company_code']}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        tables = pd.read_html(response.text, encoding='euc-kr')
        # 분기별 재무제표는 보통 4번째 테이블([3])에 위치합니다.
        df = tables[3]

        # --- 데이터 가공 ---
        df.set_index(df.columns[0], inplace=True)
        df.index.name = 'indicator'
        df.columns = [col[1] for col in df.columns]

        # ✅ DB 컬럼에 맞게 전체 지표 매핑
        indicator_map = {
            'PER(배)': 'per', 'PBR(배)': 'pbr', 'EV/EBITDA(배)': 'ev_ebitda',
            '매출액증가율(%)': 'sales_growth_yoy', 'EPS증가율(%)': 'eps_growth_yoy',
            '배당수익률(%)': 'dividend_yield', 'ROE(%)': 'roe', 'ROA(%)': 'roa',
            'ROIC(%)': 'roic', '매출총이익률(%)': 'gross_profit_margin',
            '영업이익률(%)': 'operating_profit_margin', '순이익률(%)': 'net_profit_margin',
            '부채비율': 'debt_ratio', '유동비율': 'current_ratio', '이자보상배율': 'interest_coverage_ratio'
            # 'peg', 'ev_sales', 'sales_growth_qoq', 'eps_growth_qoq' 등 일부 지표는
            # 네이버 재무분석 메인 테이블에 없어 None으로 처리됩니다.
        }
        df.rename(index=indicator_map, inplace=True)
        
        results = []
        for quarter_col in df.columns:
            if '.' not in str(quarter_col):
                continue
            
            try:
                year_month = str(quarter_col).split('(')[0]
                year = int(year_month.split('.')[0])
                month = int(year_month.split('.')[1])
                quarter_code = str((month - 1) // 3 + 1)

                record = {
                    'company_code': company['company_code'],
                    'company_name': company['company_name'],
                    'year': year,
                    'quarter_code': quarter_code
                }
                
                # 모든 DB 컬럼에 대한 기본값(None) 설정
                all_cols = list(indicator_map.values())
                for col in all_cols:
                    record[col] = None

                # 스크래핑한 값을 숫자로 변환하여 record에 추가
                for indicator, value in df[quarter_col].items():
                    if indicator in all_cols:
                        # pd.to_numeric을 사용하여 숫자로 변환, 실패 시 None으로 처리
                        record[indicator] = pd.to_numeric(value, errors='coerce')

                results.append(record)
            except (ValueError, IndexError):
                continue # 날짜 형식이 아니면 건너뛰기
        
        return results

    except Exception as e:
        logger.error(f"⚠️ {company['company_name']}({company['company_code']}) 재무 데이터 스크래핑 중 오류 발생: {e}")
        return None
    
if __name__ == "__main__":
    print("=== 상위 200개 기업 목록 가져오기 ===")
    top_companies = get_top_companies(200) 
    print(f"총 {len(top_companies)}개 기업을 가져왔습니다.\n")
    print(top_companies[:50])  # 상위 5개 기업 출력