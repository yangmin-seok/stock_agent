import logging
import psycopg2
from psycopg2 import extras
from src.fundamental.data_loader.db_util import get_db_connection, setup_database
from src.fundamental.data_loader.config import DB_CONFIG
import pandas as pd
import numpy as np
import requests
import os
import xml.etree.ElementTree as ET
from io import BytesIO
import zipfile
import FinanceDataReader as fdr

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_corp_codes_from_dart():
    """DART API에서 기업 목록 가져오기"""
    logger.info("📈 DART API에서 기업 고유번호 목록을 요청합니다...")
    url = 'https://opendart.fss.or.kr/api/corpCode.xml'
    params = {'crtfc_key': os.environ.get('DART_API_KEY', '')}
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            with z.open('CORPCODE.xml') as f:
                tree = ET.parse(BytesIO(f.read()))
                root = tree.getroot()

        data_list = []
        for item in root.findall('./list'):
            data_list.append({
                'corp_code': item.findtext('corp_code'),
                'corp_name': item.findtext('corp_name'),
                'stock_code': item.findtext('stock_code').strip() or None,
            })
        
        return pd.DataFrame(data_list) if data_list else None

    except Exception as e:
        logger.error(f"DART 데이터 가져오기 실패: {e}")
        return None

def get_krx_industry_map():
    """
    KRX 상세 정보(KRX-DESC)를 조회하여 sector(업종)와 industry(주요제품) 정보를 가져옵니다.
    """
    logger.info("KRX 상세 정보(KRX-DESC)를 조회합니다...")
    try:
        df_krx = fdr.StockListing('KRX-DESC')
        
        # 컬럼명 매핑 (FDR -> DB)
        # Sector -> sector
        # Industry -> industry
        rename_map = {}
        
        # 종목코드 매핑
        if 'Code' in df_krx.columns: rename_map['Code'] = 'company_code'
        elif 'Symbol' in df_krx.columns: rename_map['Symbol'] = 'company_code'
            
        # 업종 및 주요사업 매핑 (소문자로 변환)
        if 'Sector' in df_krx.columns: rename_map['Sector'] = 'sector'
        if 'Industry' in df_krx.columns: rename_map['Industry'] = 'industry'

        df_krx.rename(columns=rename_map, inplace=True)

        # 필수 컬럼 확인
        required_cols = ['company_code', 'sector', 'industry']
        
        # industry(주요제품) 컬럼이 없는 경우 대비 (빈 컬럼 생성)
        if 'industry' not in df_krx.columns:
            df_krx['industry'] = None
        
        if 'sector' not in df_krx.columns:
             # Sector가 없으면 의미가 없으므로 빈 DF 리턴
             logger.error("KRX 데이터에 Sector 정보가 없습니다.")
             return pd.DataFrame()

        # 필요한 컬럼만 추출
        df_result = df_krx[['company_code', 'sector', 'industry']].copy()
        
        # 중복 제거
        df_result = df_result.drop_duplicates(subset=['company_code'])
        
        logger.info(f"✅ KRX 정보 확보: {len(df_result)}개 기업 (sector/industry)")
        return df_result

    except Exception as e:
        logger.error(f"KRX 데이터 가져오기 실패: {e}")
        return pd.DataFrame(columns=['company_code', 'sector', 'industry'])

def update_stock_info():
    """DART + KRX 데이터를 결합하여 DB 업데이트"""
    
    # 1. DART 데이터
    corp_code_df = get_corp_codes_from_dart()
    if corp_code_df is None: return

    logger.info("🐼 데이터 병합 시작...")

    # 2. 전처리
    df_to_save = corp_code_df.rename(columns={'corp_name': 'company_name', 'stock_code': 'company_code'})
    df_to_save = df_to_save.dropna(subset=['company_code'])
    df_to_save['company_code'] = df_to_save['company_code'].astype(str)

    # 3. KRX 데이터 병합 (sector + industry)
    krx_industry_df = get_krx_industry_map()
    krx_industry_df['company_code'] = krx_industry_df['company_code'].astype(str)

    df_to_save = pd.merge(df_to_save, krx_industry_df, on='company_code', how='left')

    # 병합 결과 로깅
    filled_sector = df_to_save['sector'].notna().sum()
    filled_industry = df_to_save['industry'].notna().sum()
    logger.info(f"📊 매핑 결과: Sector({filled_sector}건), Industry({filled_industry}건)")

    # 4. DB 저장
    conn = get_db_connection(DB_CONFIG)
    
    # SQL 파일 실행 (DROP & CREATE 추천)
    setup_database(conn, path='src/fundamental/data_loader/sql/stock_info_schema.sql')

    # NaN 처리
    df_to_save = df_to_save.replace({np.nan: None})
    data_to_dict = df_to_save.to_dict('records')

    if not data_to_dict: return

    columns = data_to_dict[0].keys()
    cols_str = ", ".join(f'"{col}"' for col in columns)
    placeholders = ", ".join([f"%({col})s" for col in columns])
    
    # corp_code 제외하고 업데이트
    update_cols = [col for col in columns if col != 'corp_code']
    update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

    sql = f"""
    INSERT INTO stock_info ({cols_str})
    VALUES ({placeholders})
    ON CONFLICT (corp_code) DO UPDATE SET 
        {update_str},
        updated_at = CURRENT_TIMESTAMP; 
    """

    with conn.cursor() as cur:
        try:
            psycopg2.extras.execute_batch(cur, sql, data_to_dict)
            conn.commit()
            logger.info("🎉 기업 정보(sector/industry 포함) DB 저장 완료.")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ DB 저장 오류: {e}")
            raise
    
    if conn: conn.close()

if __name__ == "__main__":
    update_stock_info()