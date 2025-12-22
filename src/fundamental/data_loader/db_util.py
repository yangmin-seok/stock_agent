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

def setup_database(conn: connection, path: str) -> None:
    """
    데이터베이스에 path에 해당하는 테이블과 관련 설정을 생성합니다.
    테이블이 이미 존재하면 생성하지 않습니다.

    Args:
        conn (connection): psycopg2 연결 객체
    """
    # load db schema from file
    with open(path, 'r', encoding='utf-8') as f:
        ddl_script = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl_script)
    conn.commit()
    print("✅ 데이터베이스 테이블 및 설정이 준비되었습니다.")