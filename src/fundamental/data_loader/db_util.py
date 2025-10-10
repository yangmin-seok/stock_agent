# db_manager.py

import psycopg2
from psycopg2 import extras
from psycopg2.extensions import connection
from typing import List, Dict, Any
import pandas as pd
import numpy as np

def get_db_connection(db_config: Dict[str, str]) -> connection:
    """
    설정 정보를 바탕으로 PostgreSQL 데이터베이스 연결 객체를 생성하고 반환합니다.

    Args:
        db_config (Dict[str, str]): 데이터베이스 연결 정보

    Returns:
        connection: psycopg2 연결 객체
    """
    try:
        conn = psycopg2.connect(**db_config)
        print("✅ 데이터베이스에 성공적으로 연결되었습니다.")
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ 데이터베이스 연결에 실패했습니다: {e}")
        raise

def setup_database(conn: connection) -> None:
    """
    데이터베이스에 financial_indicators 테이블과 관련 설정을 생성합니다.
    테이블이 이미 존재하면 생성하지 않습니다.

    Args:
        conn (connection): psycopg2 연결 객체
    """
    # load db schema from file
    with open('src/fundamental/data_loader/db_schema.sql', 'r', encoding='utf-8') as f:
        ddl_script = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl_script)
    conn.commit()
    print("✅ 데이터베이스 테이블 및 설정이 준비되었습니다.")

def save_financial_data(conn: connection, data: List[Dict[str, Any]]) -> None:
    """
    스크래핑한 재무 지표 데이터를 데이터베이스에 저장합니다. (UPSERT 방식)
    UNIQUE 제약 조건(company_code, year, quarter_code)이 충돌하면 UPDATE를 수행합니다.
    """
    if not data:
        return

    # Pandas DataFrame을 사용하여 NaN을 None으로 일괄 변환 (JSON 호환)
    df = pd.DataFrame(data)
    df = df.replace({np.nan: None})
    data_to_insert = df.to_dict('records')

    if not data_to_insert:
        return
        
    # 테이블의 모든 컬럼을 첫 번째 데이터 기준으로 동적으로 생성
    columns = data_to_insert[0].keys()
    cols_str = ", ".join(f'"{col}"' for col in columns) # 따옴표로 감싸기
    placeholders = ", ".join([f"%({col})s" for col in columns])
    
    # ON CONFLICT 시 업데이트할 컬럼들 (고유 키 제외)
    update_cols = [col for col in columns if col not in ['company_code', 'year', 'quarter_code']]
    update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

    sql = f"""
        INSERT INTO financial_indicators ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT (company_code, year, quarter_code) DO UPDATE SET
            {update_str};
    """

    with conn.cursor() as cur:
        try:
            # executemany 대신 execute_batch를 사용하여 성능 향상
            psycopg2.extras.execute_batch(cur, sql, data_to_insert)
            conn.commit()
        except Exception as e:
            conn.rollback() # 오류 발생 시 트랜잭션 롤백
            print(f"❌ 데이터베이스 저장 중 오류 발생: {e}")
            raise
