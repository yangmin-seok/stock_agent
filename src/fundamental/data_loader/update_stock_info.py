import logging
import psycopg2
from psycopg2 import extras
from src.fundamental.data_loader.db_util import get_db_connection, setup_database
from src.fundamental.data_loader.config import DB_CONFIG
import pandas as pd
import numpy as np
import requests
import os
import xml.etree.ElementTree as ET # XML 파싱을 위해 추가
from io import BytesIO # content를 바로 파싱하기 위해 추가
import zipfile  # 👈 [1. 수정] zipfile 라이브러리 임포트

# 로깅 설정: 진행 상황을 터미널에 출력
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_corp_codes_from_dart():
    """
    DART API를 호출하여 모든 기업의 고유번호, 기업명, 종목코드를 받아옵니다.
    결과를 DataFrame으로 반환합니다.
    """
    logger.info("📈 DART API에서 기업 고유번호 목록을 요청합니다...")
    url = 'https://opendart.fss.or.kr/api/corpCode.xml'
    params = {'crtfc_key': os.environ.get('DART_API_KEY', '')}
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status() 

        logger.info("🛰️ API 응답(ZIP)을 수신했습니다. 압축 해제를 시작합니다...")

        xml_content = None # XML 내용을 담을 변수

        # ▼▼▼ [1. 수정] 압축 해제 및 XML 내용 읽기 ▼▼▼
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            # ZIP 파일 내의 'CORPCODE.xml' 파일의 내용을 엽니다.
            try:
                with z.open('CORPCODE.xml') as f:
                    # XML 내용을 변수로 읽어옵니다.
                    xml_content = f.read()
            except KeyError:
                logger.error("ZIP 파일 내에 'CORPCODE.xml' 파일이 없습니다.")
                return None

        # # ▼▼▼ [2. 수정] 디버깅용 XML 파일 저장 ▼▼▼
        # if xml_content:
        #     # 읽어온 XML content(bytes)를 .xml 파일로 저장합니다.
        #     debug_filename = 'dart_response_debug.xml'
        #     with open(debug_filename, 'wb') as f:
        #         f.write(xml_content)
        #     logger.info(f"🐞 디버깅용 XML 파일 저장 완료: {debug_filename}")
        # else:
        #     # 이 경우는 거의 발생하지 않지만, 방어 코드
        #     logger.error("XML 내용을 읽어오지 못해 저장을 건너뜁니다.")
        #     return None

        logger.info("XML 파싱을 시작합니다...")
        
        tree = ET.parse(BytesIO(xml_content))
        root = tree.getroot()

        # API 에러 확인 (DART API는 status 코드로 성공 여부를 알려줌)
        status = root.findtext('status')
        message = root.findtext('message')
        
        # ... (이하 XML 파싱 및 DataFrame 변환 로직은 동일) ...
        data_list = []
        for item in root.findall('./list'):
            data_list.append({
                'corp_code': item.findtext('corp_code'),
                'corp_name': item.findtext('corp_name'),
                'stock_code': item.findtext('stock_code').strip() or None,
            })

        if not data_list:
            logger.warning("API는 성공했으나 파싱된 데이터가 없습니다.")
            return None

        df = pd.DataFrame(data_list)
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"API 요청 중 오류 발생: {e}")
        return None
    except zipfile.BadZipFile as e: 
        logger.error(f"ZIP 파일 압축 해제 중 오류 발생: {e}")
        return None
    except ET.ParseError as e:
        logger.error(f"XML 파싱 중 오류 발생: {e}")
        logger.error(f"수신된 데이터 (일부): {response.content[:200]}...")
        return None

def update_stock_info():
    """
    stock_info 테이블을 DART API에서 받아온 최신 기업 정보로 업데이트합니다.
    """
    
    # DART API로부터 기업 목록 DataFrame 가져오기
    corp_code_df = get_corp_codes_from_dart()

    # ▼▼▼ [수정/추가된 핵심 로직] ▼▼▼
    logger.info("🐼 DataFrame 컬럼명을 DB 스키마에 맞게 변경합니다...")
    
    # 1. DART API 컬럼명 -> DB 컬럼명으로 변경
    df_to_save = corp_code_df.rename(columns={
        'corp_name': 'company_name',
        'stock_code': 'company_code'
    })

    # 2. DB 스키마에서 company_code가 NOT NULL이므로,
    # stock_code가 없는(None) 비상장 기업 등은 제외합니다.
    original_count = len(df_to_save)
    df_to_save = df_to_save.dropna(subset=['company_code'])
    filtered_count = original_count - len(df_to_save)
    if filtered_count > 0:
        logger.info(f"비상장 기업 (company_code=None) {filtered_count}건을 제외했습니다.")
    # ▲▲▲ [수정/추가된 핵심 로직 끝] ▲▲▲

    # (DB 저장 로직 시작)
    conn = None

    logger.info("🗄️ 데이터베이스에 연결합니다...")
    conn = get_db_connection(DB_CONFIG)
    logger.info("🛠️ 데이터베이스 스키마를 설정합니다 (테이블이 없는 경우 생성).")
    setup_database(conn, path='src/fundamental/data_loader/sql/stock_info_schema.sql')
    
    logger.info(f"💾 총 {len(df_to_save)}개의 기업 정보를 데이터베이스에 저장(UPSERT)합니다...")

    # save logic
    df_to_save = df_to_save.replace({np.nan: None}) # Pandas NA -> None
    data_to_dict = df_to_save.to_dict('records')

    if not data_to_dict:
        logger.warning("저장할 데이터가 없습니다.")
        if conn:
            conn.close()
        return

    # 이제 columns는 ['corp_code', 'company_name', 'company_code']가 됩니다.
    columns = data_to_dict[0].keys()
    cols_str = ", ".join(f'"{col}"' for col in columns)
    placeholders = ", ".join([f"%({col})s" for col in columns])

    # ON CONFLICT 시 업데이트할 컬럼들 (고유 키 제외)
    # PK인 'corp_code'를 제외한 컬럼들
    update_cols = [col for col in columns if col not in ['corp_code']]
    update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

    # 스키마의 PRIMARY KEY인 'corp_code'를 기준으로 ON CONFLICT를 수행합니다.
    sql = f"""
    INSERT INTO stock_info ({cols_str})
    VALUES ({placeholders})
    ON CONFLICT (corp_code) DO UPDATE SET 
        {update_str},
        updated_at = CURRENT_TIMESTAMP; 
    """ # updated_at 타임스탬프도 갱신해줍니다.

    with conn.cursor() as cur:
        try:
            psycopg2.extras.execute_batch(cur, sql, data_to_dict) # 배치 삽입
            conn.commit()
        except Exception as e:
            conn.rollback() # 오류 발생 시 트랜잭션 롤백
            print(f"❌ 데이터베이스 저장 중 오류 발생: {e}")
            raise
    logger.info("🎉 모든 데이터가 성공적으로 데이터베이스에 저장되었습니다.")
    if conn:
        conn.close()

if __name__ == "__main__":
    update_stock_info()