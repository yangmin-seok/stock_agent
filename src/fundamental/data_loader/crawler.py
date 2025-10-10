import time
import requests
import pandas as pd
import io
import numpy as np
from typing import List, Dict, Optional, Any
import logging
from pykrx import stock
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_top_companies(limit: int = 200) -> pd.DataFrame:
    """
    KOSPI와 KOSDAQ에서 시가총액 상위 `limit`개의 종목 정보를 가져옵니다.

    Outputs:
        pd.DataFrame: 'company_name', 'company_code', 'exchange', 'market_cap' 컬럼을 포함한 데이터프레임
    """

    #today = datetime.now().strftime('%Y%d%m')
    today = "20251010"

    try:
        # KOSPI 종목 정보
        df_kospi = stock.get_market_cap_by_ticker(today, market='KOSPI').iloc[:limit]
        df_kospi['exchange'] = 'KOSPI'

        # KOSDAQ 종목 정보
        df_kosdaq = stock.get_market_cap_by_ticker(today, market='KOSDAQ').iloc[:limit]
        df_kosdaq['exchange'] = 'KOSDAQ'
    except Exception as e:
        logger.error(f"pykrx를 통해 종목 정보를 가져오는 중 오류 발생: {e}")
        return pd.DataFrame()

    df = pd.concat([df_kospi, df_kosdaq])
    df = df.sort_values(by='시가총액', ascending=False)

    # 인덱스(종목코드)를 리셋하고 'company_code' 컬럼으로 만듭니다.
    df = df.reset_index()
    df = df.rename(columns={'티커': 'company_code'})
    df = df.rename(columns={'시가총액': 'market_cap'})

    # 종목명을 빠르게 추가합니다.
    df['company_name'] = df['company_code'].map(lambda x: stock.get_market_ticker_name(x))

    # market_cap / 1억
    df['market_cap'] = df['market_cap'].apply(lambda x: x // 100000000)

    # 최종적으로 필요한 컬럼만 선택하고 순서를 정리합니다.
    df = df[['company_name', 'company_code', 'exchange', 'market_cap']]

    logger.info(f"✅ 시가총액 상위 {len(df)}개 종목 정보를 성공적으로 가져왔습니다.")

    return df

def crawl_financial_year_data(company: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    주어진 종목의 연간 재무 데이터를 스크래핑하여 DB 스키마에 맞는 DataFrame으로 반환합니다.

    Args:
        company (Dict[str, Any]): 'company_name', 'company_code', 'exchange', 'market_cap' 포함
    
    Returns:
        Optional[pd.DataFrame]: 성공 시 스키마에 맞춰 가공된 재무 데이터 DataFrame, 실패 시 None
    """
    ajax_url = "https://navercomp.wisereport.co.kr/v2/company/ajax/cF1001.aspx"
    referer_url = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={company['company_code']}"

    params = {
        'cmp_cd': company['company_code'],
        'fin_typ': '4',  # K-IFRS(연결)
        'freq_typ': 'Y', # 'Y': 연간 데이터
        'encparam': 'Uk9HN25jMVJoVzhQaVZTc2YrZzdWUT09'
    }
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': referer_url}

    try:
        response = requests.get(ajax_url, params=params, headers=headers)
        response.raise_for_status()

        if not response.text.strip():
            logger.warning(f"{company['company_name']}({company['company_code']}): 서버로부터 빈 응답을 받았습니다.")
            return None

        # 1. 데이터 파싱 및 정제
        tables = pd.read_html(io.StringIO(response.text)) # table tag parsing
        if len(tables) < 2:
            logger.warning(f"{company['company_name']}({company['company_code']}): 재무 데이터 테이블을 찾을 수 없습니다.")
            return None
        
        df = tables[1]
        
        # 2. 컬럼 및 인덱스 정리
        df.columns = df.columns.droplevel(0)
        df.set_index(df.columns[0], inplace=True)
        df = df.loc[:, ~df.columns.str.contains('E')] # 예상(E) 데이터 컬럼 제외
        df.columns = df.columns.str.replace(r'/.*', '', regex=True) # '2020/12(IFRS...)' -> '2020'

        # 3. Wide to Long 포맷으로 변환
        df_long = df.reset_index().melt(id_vars=df.index.name, var_name='year', value_name='value')
        
        # 4. DB 스키마에 맞게 지표 매핑
        indicator_map = {
            '매출액': 'sales',
            '영업이익': 'operating_profit',
            '당기순이익': 'net_income',
            'PER(배)': 'per',
            'PBR(배)': 'pbr',
            '현금배당수익률': 'dividend_yield',
            'ROE(%)': 'roe',
            'ROA(%)': 'roa',
            '영업이익률': 'operating_profit_margin',
            '순이익률': 'net_profit_margin',
            '부채비율': 'debt_ratio',
            'EPS(원)': 'eps',
            'BPS(원)': 'bps',
            '현금배당수익률': 'dividend_yield',
        }
        df_long['indicator'] = df_long[df.index.name].map(indicator_map)
        df_long = df_long.dropna(subset=['indicator'])

        # 5. Long 포맷을 최종 스키마(연도별 행)에 맞게 피벗
        df_pivot = df_long.pivot_table(index='year', columns='indicator', values='value', aggfunc='first').reset_index()

        # 6. 기본 정보 추가
        df_pivot['company_code'] = company['company_code']
        df_pivot['company_name'] = company['company_name']
        df_pivot['exchange'] = company['exchange']
        df_pivot['market_cap'] = company['market_cap']
        df_pivot['quarter_code'] = '0' # 연간 데이터
        
        # 7. 단위 변환 및 데이터 타입 정리
        # 억원 단위 컬럼 처리 (쉼표 제거 후 1억 곱하기)
        unit_cols = ['sales', 'operating_profit', 'net_income']
        for col in unit_cols:
            if col in df_pivot.columns:
                df_pivot[col] = pd.to_numeric(df_pivot[col].astype(str).str.replace(',', ''), errors='coerce') # eg. 1.23억

        # 숫자형으로 변환할 나머지 컬럼들
        numeric_cols = list(set(indicator_map.values()) - set(unit_cols))
        for col in numeric_cols:
             if col in df_pivot.columns:
                df_pivot[col] = pd.to_numeric(df_pivot[col], errors='coerce')
        
        # 8. 스키마에 있는 모든 컬럼을 가지도록 DataFrame 재구성
        schema_columns = [
            'company_code', 'company_name', 'exchange', 'year', 'quarter_code',
            'market_cap', 'sales', 'operating_profit', 'net_income',
            'per', 'pbr', 'eps', 'bps', 'ev_ebitda', 'ev_sales', 'peg', 'dividend_yield',
            'roe', 'roa', 'roic', 'gross_profit_margin', 'operating_profit_margin',
            'net_profit_margin', 'sales_growth_yoy', 'sales_growth_qoq',
            'eps_growth_yoy', 'eps_growth_qoq', 'debt_ratio', 'current_ratio',
            'interest_coverage_ratio'
        ]
        
        final_df = pd.DataFrame(columns=schema_columns)
        for col in schema_columns:
            if col in df_pivot.columns:
                final_df[col] = df_pivot[col]
            else:
                final_df[col] = np.nan # 스키마에 있지만 크롤링 못한 값은 NaN으로 채움

        # 정수형이어야 하는 컬럼들의 타입을 Int64(nullable)로 변경
        int_cols = ['market_cap', 'sales', 'operating_profit', 'net_income']
        for col in int_cols:
             if col in final_df.columns:
                final_df[col] = final_df[col].astype('Int64')

        return final_df

    except requests.exceptions.RequestException as e:
        logger.error(f"⚠️ {company['company_name']}({company['company_code']}) 요청 오류: {e}")
        return None
    except Exception as e:
        logger.error(f"⚠️ {company['company_name']}({company['company_code']}) 데이터 처리 중 오류: {e}")
        return None