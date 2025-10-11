import logging
import psycopg2
from psycopg2 import extras
from src.fundamental.data_loader.crawler import get_top_companies, crawl_financial_year_data
from src.fundamental.data_loader.db_util import get_db_connection, setup_database, save_financial_data
from src.fundamental.data_loader.config import DB_CONFIG
import pandas as pd
import numpy as np

# 로깅 설정: 진행 상황을 터미널에 출력
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def update_stock_info():
    """
    stock_info 테이블을 KOSPI와 KOSDAQ 상위 2000개 기업의 정보로 업데이트합니다.
    """
    top_companies_df = get_top_companies(limit=1000)

    # save db
    conn = None

    logger.info("🗄️ 데이터베이스에 연결합니다...")
    conn = get_db_connection(DB_CONFIG)
    # 해당 파일이 존재하는지 확인이 필요합니다.
    logger.info("🛠️ 데이터베이스 스키마를 설정합니다 (테이블이 없는 경우 생성).")
    setup_database(conn, path='src/fundamental/data_loader/sql/stock_info_schema.sql')
    
    logger.info(f"💾 총 {len(top_companies_df)}개의 연간 재무 데이터를 데이터베이스에 저장(UPSERT)합니다...")

    # save logic
    top_companies_df = top_companies_df.replace({np.nan: None}) # Pandas NA -> None
    data_to_dict = top_companies_df.to_dict('records')

    columns = data_to_dict[0].keys()
    cols_str = ", ".join(f'"{col}"' for col in columns) # 따옴표로 감싸기
    placeholders = ", ".join([f"%({col})s" for col in columns])

    # ON CONFLICT 시 업데이트할 컬럼들 (고유 키 제외)
    update_cols = [col for col in columns if col not in ['company_code', 'company_name']]
    update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

    sql = f"""
    INSERT INTO stock_info ({cols_str})
    VALUES ({placeholders})
    ON CONFLICT (company_code, company_name) DO UPDATE SET
        {update_str};
    """

    with conn.cursor() as cur:
        try:
            psycopg2.extras.execute_batch(cur, sql, data_to_dict) # 배치 삽입
            conn.commit()
        except Exception as e:
            conn.rollback() # 오류 발생 시 트랜잭션 롤백
            print(f"❌ 데이터베이스 저장 중 오류 발생: {e}")
            raise
    logger.info("🎉 모든 데이터가 성공적으로 데이터베이스에 저장되었습니다.")

if __name__ == "__main__":
    update_stock_info()