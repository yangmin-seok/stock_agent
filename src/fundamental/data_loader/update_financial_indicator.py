# db_update.py

import logging
from src.fundamental.data_loader.crawler import get_top_companies, crawl_financial_year_data
from src.fundamental.data_loader.db_util import get_db_connection, setup_database, save_financial_data
from src.fundamental.data_loader.config import DB_CONFIG
from concurrent.futures import ThreadPoolExecutor, as_completed

# 로깅 설정: 진행 상황을 터미널에 출력
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def update_financial_data():
    """
    KOSPI와 KOSDAQ 상위 200개 기업의 연간 재무 데이터를 크롤링하여
    데이터베이스에 업데이트하는 메인 함수입니다.
    """
    logger.info("🚀 데이터베이스 업데이트 프로세스를 시작합니다.")

    # 1. 대상 기업 목록 가져오기
    logger.info("📊 시가총액 상위 2000개 기업 목록을 가져옵니다...")
    top_companies_df = get_top_companies(limit=1000)

    if top_companies_df.empty:
        logger.error("기업 목록을 가져오는데 실패하여 프로세스를 중단합니다.")
        return

    logger.info(f"✅ 총 {len(top_companies_df)}개 기업의 데이터를 처리합니다.")

    # 2. 각 기업의 재무 데이터 크롤링
    all_financial_data = []
    # for _, company in top_companies_df.iterrows():
    #     company_dict = company.to_dict()
    #     logger.info(f"🔍 '{company_dict['company_name']}'({company_dict['company_code']})의 연간 재무 데이터를 크롤링합니다...")
        
    #     financial_df = crawl_financial_year_data(company_dict)
        
    #     if financial_df is not None and not financial_df.empty:
    #         # DataFrame을 딕셔너리 리스트로 변환하여 저장
    #         all_financial_data.extend(financial_df.to_dict('records'))
    #         logger.info(f"✅ '{company_dict['company_name']}' 데이터 처리 완료.")
    #     else:
    #         logger.warning(f"⚠️ '{company_dict['company_name']}'의 재무 데이터를 가져오지 못했습니다.")
    
    # ThreadPoolExecutor를 사용하여 병렬 작업 실행
    with ThreadPoolExecutor(max_workers=50) as executor:
        # 각 회사에 대한 크롤링 작업을 스케줄링하고 future 객체를 딕셔너리에 저장
        future_to_company = {
            executor.submit(crawl_financial_year_data, company.to_dict()): company.to_dict()
            for _, company in top_companies_df.iterrows()
        }

        # 작업이 완료되는 순서대로 결과 처리
        for future in as_completed(future_to_company):
            company_dict = future_to_company[future]
            company_name = company_dict['company_name']
            
            try:
                financial_df = future.result() # 작업 결과 가져오기
                if financial_df is not None and not financial_df.empty:
                    all_financial_data.extend(financial_df.to_dict('records'))
                    logger.info(f"✅ '{company_name}' 데이터 처리 완료.")
                else:
                    logger.warning(f"⚠️ '{company_name}'의 재무 데이터를 가져오지 못했습니다.")
            except Exception as exc:
                logger.error(f"❌ '{company_name}' 처리 중 오류 발생: {exc}")

    if not all_financial_data:
        logger.warning("크롤링된 재무 데이터가 없어 프로세스를 종료합니다.")
        return

    # 3. 데이터베이스에 저장
    conn = None
    try:
        logger.info("🗄️ 데이터베이스에 연결합니다...")
        conn = get_db_connection(DB_CONFIG)

        # db_schema.sql 파일이 없다면 테이블 생성을 시도하지 않으므로,
        # 해당 파일이 존재하는지 확인이 필요합니다.
        logger.info("🛠️ 데이터베이스 스키마를 설정합니다 (테이블이 없는 경우 생성).")
        setup_database(conn, path='src/fundamental/data_loader/sql/financial_indicators_schema.sql')
        
        logger.info(f"💾 총 {len(all_financial_data)}개의 연간 재무 데이터를 데이터베이스에 저장(UPSERT)합니다...")
        save_financial_data(conn, all_financial_data)
        
        logger.info("🎉 모든 데이터가 성공적으로 데이터베이스에 저장되었습니다.")

    except Exception as e:
        logger.error(f"❌ 데이터베이스 작업 중 오류가 발생했습니다: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("🚪 데이터베이스 연결을 닫습니다.")

    logger.info("🏁 데이터베이스 업데이트 프로세스를 종료합니다.")


if __name__ == "__main__":
    update_financial_data()