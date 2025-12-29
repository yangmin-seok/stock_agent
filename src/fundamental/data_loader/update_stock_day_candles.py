import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import extras
from pykrx import stock
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time

# 실제 환경에 맞게 DB 연결 관련 모듈을 임포트
from src.fundamental.data_loader.db_util import get_db_connection, setup_database
from src.fundamental.data_loader.config import DB_CONFIG

# --- 로깅 설정 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_all_company_codes(conn) -> List[Dict[str, str]]:
    """
    stock_info 테이블에서 모든 회사의 코드와 이름을 가져옵니다.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT company_code, company_name FROM stock_info")
        rows = cur.fetchall()
        # 딕셔너리 리스트 형태로 반환
        return [{"company_code": row[0], "company_name": row[1]} for row in rows]


def get_single_company_data(company: Dict[str, str], from_date: str, to_date: str) -> Optional[pd.DataFrame]:
    """
    [단일 종목 처리]
    특정 회사의 일봉 데이터(OHLCV)와 투자자별(외국인, 연기금) 순매수 데이터를 가져와 병합합니다.
    """
    code = company['company_code']
    name = company['company_name']

    try:
        # 1. OHLCV 데이터 수집
        df_ohlcv = stock.get_market_ohlcv(from_date, to_date, code, 'd')
        if df_ohlcv.empty:
            return None

        # 2. 투자자별 거래실적 수집 (외국인, 연기금 등)
        # 주의: 기간이 길면 pykrx 응답이 느릴 수 있습니다.
        df_trading_value = stock.get_market_trading_value_by_date(from_date, to_date, code, detail=True)

        # 3. 외국인 데이터 정제
        df_foreign = df_trading_value[['외국인']].reset_index()
        # 단위 변환 (원 -> 억원 등 필요 시 조정, 현재는 1억 나누기로 되어있음)
        df_foreign['외국인'] = (df_foreign['외국인'] / 100_000_000).astype(int) 
        df_foreign = df_foreign.rename(columns={'날짜': 'candle_date', '외국인': 'foreign_net_buy_amount'})

        # 4. 연기금 데이터 정제
        df_pension = df_trading_value[['연기금']].reset_index()
        df_pension['연기금'] = (df_pension['연기금'] / 100_000_000).astype(int)
        df_pension = df_pension.rename(columns={'날짜': 'candle_date', '연기금': 'pension_fund_net_buy_amount'})

        # 5. 데이터 병합 (OHLCV + 외국인 + 연기금)
        df_ohlcv = df_ohlcv.reset_index().rename(columns={'날짜': 'candle_date'})
        
        # Left Join으로 병합
        merged_df = pd.merge(df_ohlcv, df_foreign, on='candle_date', how='left')
        merged_df = pd.merge(merged_df, df_pension, on='candle_date', how='left')

        # 회사 정보 추가
        merged_df['company_code'] = code
        merged_df['company_name'] = name

        # 컬럼명 영문 변환 및 필터링
        merged_df = merged_df.rename(columns={
            '시가': 'open',
            '고가': 'high',
            '저가': 'low',
            '종가': 'close',
            '거래량': 'volume'
        })
        
        final_df = merged_df[['company_code', 'company_name', 'candle_date', 'open', 'high', 'low', 'close', 'volume', 'foreign_net_buy_amount', 'pension_fund_net_buy_amount']]
        
        return final_df

    except Exception as e:
        logger.error(f"⚠️ 데이터 수집 실패 - {name}({code}): {e}")
        return None


def save_daily_data_to_db(conn, df: pd.DataFrame):
    """
    DataFrame을 받아 DB에 저장(UPSERT)합니다.
    """
    if df is None or df.empty:
        return

    # NaN -> None 변환 (DB NULL 처리를 위해)
    df = df.replace({np.nan: None})
    data_to_dict = df.to_dict('records')

    columns = data_to_dict[0].keys()
    cols_str = ", ".join([f'"{col}"' for col in columns])
    placeholders = ", ".join([f"%({col})s" for col in columns])

    # 업데이트할 컬럼들
    update_cols = ['open', 'high', 'low', 'close', 'volume', 'foreign_net_buy_amount', 'pension_fund_net_buy_amount']
    update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])
    update_str += ', updated_at = CURRENT_TIMESTAMP' # 업데이트 시간 갱신

    sql = f"""
    INSERT INTO stock_day_candles ({cols_str})
    VALUES ({placeholders})
    ON CONFLICT (company_code, candle_date) DO UPDATE SET
        {update_str};
    """

    with conn.cursor() as cur:
        try:
            psycopg2.extras.execute_batch(cur, sql, data_to_dict)
            conn.commit() # ★ 즉시 커밋하여 저장 확정
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ DB 저장 오류: {e}")
            raise


def update_stock_daily_data():
    """
    [메인 로직]
    전체 종목을 순회하며 '수집 -> 저장'을 반복합니다.
    """
    conn = None
    try:
        logger.info("🗄️ 데이터베이스에 연결합니다...")
        conn = get_db_connection(DB_CONFIG)

        # 테이블 스키마 확인/생성
        setup_database(conn, path='src/fundamental/data_loader/sql/stock_day_candles_schema.sql')
        
        # 전체 종목 리스트 가져오기
        company_list = get_all_company_codes(conn)
        total_count = len(company_list)
        
        if not company_list:
            logger.warning("`stock_info` 테이블에 데이터가 없습니다.")
            return

        logger.info(f"🚀 총 {total_count}개 종목의 일별 데이터 업데이트를 시작합니다.")

        # 날짜 설정 (최근 5년)
        to_date = datetime.now().strftime('%Y%m%d')
        from_date = (datetime.now() - timedelta(days=365 * 5)).strftime('%Y%m%d')

        # --- [핵심 변경] 루프 안에서 수집과 저장을 수행 ---
        for idx, company in enumerate(company_list, start=1):
            company_name = company['company_name']
            company_code = company['company_code']

            # 1. 데이터 수집
            df = get_single_company_data(company, from_date, to_date)

            if df is not None and not df.empty:
                # 2. DB 저장
                save_daily_data_to_db(conn, df)
                logger.info(f"[{idx}/{total_count}] ✅ {company_name}({company_code}) 저장 완료 ({len(df)} rows)")
            else:
                logger.warning(f"[{idx}/{total_count}] ⚠️ {company_name}({company_code}) 데이터 없음")

            # (선택 사항) API 과부하 방지를 위한 미세한 딜레이
            # time.sleep(0.1) 

        logger.info("🎉 모든 작업이 완료되었습니다.")

    except Exception as e:
        logger.error(f"❌ 치명적인 오류 발생: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("🔗 데이터베이스 연결을 종료합니다.")


if __name__ == "__main__":
    update_stock_daily_data()