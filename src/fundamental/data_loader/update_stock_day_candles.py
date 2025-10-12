import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import extras
from pykrx import stock
from datetime import datetime, timedelta
from typing import List, Dict

# 실제 환경에 맞게 DB 연결 관련 모듈을 임포트해야 합니다.
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
        return [{"company_code": row[0], "company_name": row[1]} for row in rows]


def get_daily_data_with_trading_value(company_list: List[Dict[str, str]]) -> pd.DataFrame:
    """
    주어진 모든 회사에 대한 일봉 데이터(OHLCV)와 외국인 순매수 거래대금 데이터를 pykrx를 통해 가져옵니다.
    """
    all_company_data = []
    to_date = datetime.now().strftime('%Y%m%d')
    from_date = (datetime.now() - timedelta(days=365 * 3)).strftime('%Y%m%d')

    for company in company_list:
        try:
            df_ohlcv = stock.get_market_ohlcv(from_date, to_date, company['company_code'], 'd')
            if df_ohlcv.empty:
                continue

            df_trading_value = stock.get_market_trading_value_by_date(from_date, to_date, company['company_code'])

            df_foreign_net_buy = df_trading_value[['외국인합계']].reset_index()
            df_foreign_net_buy['외국인합계'] = (df_foreign_net_buy['외국인합계'] / 100_000_000).astype(int)
            df_foreign_net_buy = df_foreign_net_buy.rename(columns={'날짜': 'candle_date', '외국인합계': 'foreign_net_buy_amount'})

            df_ohlcv = df_ohlcv.reset_index().rename(columns={'날짜': 'candle_date'})
            merged_df = pd.merge(df_ohlcv, df_foreign_net_buy, on='candle_date', how='left')

            merged_df['company_code'] = company['company_code']
            merged_df['company_name'] = company['company_name']
            all_company_data.append(merged_df)
            logger.info(f"✅ {company['company_name']}({company['company_code']}) 데이터 수집 완료")
        except Exception as e:
            logger.error(f"⚠️ {company['company_name']}({company['company_code']}) 데이터 수집 중 오류: {e}")

    if not all_company_data:
        return pd.DataFrame()

    final_df = pd.concat(all_company_data, ignore_index=True)
    final_df = final_df.rename(columns={
        '시가': 'open',
        '고가': 'high',
        '저가': 'low',
        '종가': 'close',
        '거래량': 'volume'
    })

    final_df = final_df[['company_code', 'company_name', 'candle_date', 'open', 'high', 'low', 'close', 'volume', 'foreign_net_buy_amount']]
    return final_df


def save_daily_data_to_db(conn, df: pd.DataFrame):
    """
    일별 데이터를 데이터베이스에 UPSERT합니다.
    """
    if df.empty:
        logger.info("저장할 데이터가 없습니다.")
        return

    logger.info(f"💾 총 {len(df)}개의 일별 데이터를 데이터베이스에 저장(UPSERT)합니다...")
    df = df.replace({np.nan: None})
    data_to_dict = df.to_dict('records')

    columns = data_to_dict[0].keys()
    cols_str = ", ".join([f'"{col}"' for col in columns])
    placeholders = ", ".join([f"%({col})s" for col in columns])

    update_cols = ['open', 'high', 'low', 'close', 'volume', 'foreign_net_buy_amount']
    update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])
    update_str += ', updated_at = CURRENT_TIMESTAMP'

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
            logger.info("🎉 모든 일별 데이터가 성공적으로 데이터베이스에 저장되었습니다.")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ 데이터베이스 저장 중 오류 발생: {e}")
            raise


def update_stock_daily_data():
    """
    전체 프로세스를 실행하여 일별 주식 데이터를 업데이트합니다.
    """
    conn = None
    try:
        logger.info("🗄️ 데이터베이스에 연결합니다...")
        conn = get_db_connection(DB_CONFIG)

        logger.info("🛠️ `stock_day_candles` 테이블 스키마를 설정합니다.")
        setup_database(conn, path='src/fundamental/data_loader/sql/stock_day_candles_schema.sql')
        
        logger.info("📈 `stock_info`에서 모든 종목 코드를 가져옵니다...")
        company_list = get_all_company_codes(conn)
        
        if not company_list:
            logger.warning("`stock_info` 테이블에 데이터가 없습니다. `update_stock_info`를 먼저 실행해주세요.")
            return

        logger.info(f"📊 총 {len(company_list)}개 종목의 데이터를 수집합니다...")
        final_df = get_daily_data_with_trading_value(company_list)

        save_daily_data_to_db(conn, final_df)

    except Exception as e:
        logger.error(f"❌ 프로세스 실행 중 오류 발생: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("🔗 데이터베이스 연결을 종료합니다.")


if __name__ == "__main__":
    update_stock_daily_data()