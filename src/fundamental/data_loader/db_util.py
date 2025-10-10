# db_manager.py

import psycopg2
from psycopg2.extensions import connection
from typing import List, Dict, Any

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

    Args:
        conn (connection): psycopg2 연결 객체
        data (List[Dict[str, Any]]): 저장할 재무 데이터 딕셔너리의 리스트
    """
    if not data:
        return

    sql = """
        INSERT INTO financial_indicators (
            company_code, company_name, year, quarter_code, per, pbr, roe, debt_ratio
            -- ... 모든 컬럼 추가 ...
        ) VALUES (
            %(company_code)s, %(company_name)s, %(year)s, %(quarter_code)s, %(per)s, %(pbr)s, %(roe)s, %(debt_ratio)s
            -- ... 모든 값에 대한 placeholder 추가 ...
        )
        ON CONFLICT (company_code, year, quarter_code) DO UPDATE SET
            per = EXCLUDED.per,
            pbr = EXCLUDED.pbr,
            roe = EXCLUDED.roe,
            debt_ratio = EXCLUDED.debt_ratio;
            -- ... 업데이트할 모든 컬럼 추가 ...
    """
    with conn.cursor() as cur:
        # executemany를 사용하면 여러 데이터를 한 번에 효율적으로 처리할 수 있습니다.
        # 참고: 이 예제에서는 단순화를 위해 INSERT 구문에 일부 컬럼만 포함시켰습니다.
        #       실제 사용 시에는 테이블의 모든 관련 컬럼을 추가해야 합니다.
        psycopg2.extras.execute_batch(cur, sql, data)
    conn.commit()