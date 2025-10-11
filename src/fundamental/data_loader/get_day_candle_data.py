import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import extras
from pykrx import stock
from datetime import datetime
from src.fundamental.data_loader.db_util import get_db_connection, setup_database
from src.fundamental.data_loader.config import DB_CONFIG
from typing import List, Dict, Any
from datetime import timedelta

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_all_company_codes(conn) -> List[Dict[str, str]]:
    """
    stock_info 테이블에서 모든 회사의 코드와 이름을 가져옵니다.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT company_code, company_name FROM stock_info")
        rows = cur.fetchall()
        return [{"company_code": row[0], "company_name": row[1]} for row in rows]

def get_weekly_candle_data(company_list: List[Dict[str, str]]) -> pd.DataFrame:
    """
    주어진 모든 회사에 대한 일봉 데이터를 pykrx를 통해 가져옵니다.
    """
    all_weekly_data = []
    to_date = datetime.now().strftime('%Y%m%d')
    from_date = (datetime.now() - timedelta(days=365*1)).strftime('%Y%m%d')  # 최근 5년치 데이터

    for company in company_list:
        try:
            # 'from_date'를 지정하지 않으면 전체 기간 데이터를 가져옵니다.
            df = stock.get_market_ohlcv(
                from_date, # 1년
                to_date,
                company['company_code'],
                'd' # 일봉 데이터
            )

            if df.empty:
                continue

            df['company_code'] = company['company_code']
            df['company_name'] = company['company_name']
            all_weekly_data.append(df)
            logger.info(f"✅ {company['company_name']}({company['company_code']}) 일봉 데이터 수집 완료")

        except Exception as e:
            logger.error(f"⚠️ {company['company_name']}({company['company_code']}) 일봉 데이터 수집 중 오류: {e}")

    if not all_weekly_data:
        return pd.DataFrame()

    final_df = pd.concat(all_weekly_data)
    final_df = final_df.reset_index()
    final_df = final_df.rename(columns={
        '날짜': 'candle_date',
        '시가': 'open',
        '고가': 'high',
        '저가': 'low',
        '종가': 'close',
        '거래량': 'volume'
    })

    # 필요한 컬럼만 선택
    final_df = final_df[['company_code', 'company_name', 'candle_date', 'open', 'high', 'low', 'close', 'volume']]
    return final_df

def save_day_data_to_db(conn, df: pd.DataFrame):
    """
    일봉 데이터를 데이터베이스에 UPSERT합니다.
    """
    if df.empty:
        logger.info("저장할 일봉 데이터가 없습니다.")
        return

    logger.info(f"💾 총 {len(df)}개의 일봉 데이터를 데이터베이스에 저장(UPSERT)합니다...")
    df = df.replace({np.nan: None})
    data_to_dict = df.to_dict('records')

    columns = data_to_dict[0].keys()
    cols_str = ", ".join([f'"{col}"' for col in columns])
    placeholders = ", ".join([f"%({col})s" for col in columns])

    # ON CONFLICT 시 업데이트할 컬럼들 (고유 키 제외)
    update_cols = ['open', 'high', 'low', 'close', 'volume']
    update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])
    update_str += ', updated_at = CURRENT_TIMESTAMP' # updated_at 타임스탬프 갱신

    sql = f"""
    INSERT INTO stock_day_candles ({cols_str})
    VALUES ({placeholders})
    ON CONFLICT (company_code, candle_date) DO UPDATE SET
        {update_str};
    """

    with conn.cursor() as cur:
        try:
            psycopg2.extras.execute_batch(cur, sql, data_to_dict)
            conn.commit()
            logger.info("🎉 모든 일봉 데이터가 성공적으로 데이터베이스에 저장되었습니다.")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ 데이터베이스 저장 중 오류 발생: {e}")
            raise

def update_stock_weekly_candles():
    """
    전체 프로세스를 실행하여 일봉 데이터를 업데이트합니다.
    """
    conn = None
    try:
        logger.info("🗄️ 데이터베이스에 연결합니다...")
        conn = get_db_connection(DB_CONFIG)

        logger.info("🛠️ `stock_weekly_candles` 테이블 스키마를 설정합니다.")
        setup_database(conn, path='src/fundamental/data_loader/sql/stock_day_candles_schema.sql')

        logger.info("📈 `stock_info` 테이블에서 모든 종목 코드를 가져옵니다...")
        company_list = get_all_company_codes(conn)

        if not company_list:
            logger.warning("`stock_info` 테이블에 데이터가 없습니다. `update_stock_info`를 먼저 실행해주세요.")
            return

        logger.info(f"📊 총 {len(company_list)}개 종목의 주봉 데이터를 수집합니다...")
        weekly_data_df = get_weekly_candle_data(company_list)

        save_day_data_to_db(conn, weekly_data_df)

    finally:
        if conn:
            conn.close()
            logger.info("🔗 데이터베이스 연결을 종료합니다.")

if __name__ == "__main__":
    update_stock_weekly_candles()