# crawler.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import Optional, Dict, Any
import logging
from datetime import datetime
import psycopg2.extras
# 아래 경로는 실제 프로젝트 구조에 맞게 수정해야 합니다.
from src.fundamental.data_loader.db_util import get_db_connection, setup_database
from src.fundamental.data_loader.config import DB_CONFIG, PAGE_NUMBER
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def scrape_naver_investor_trading_value_by_page(sosok: str, page: int) -> Optional[Dict[str, Any]]:
    """
    개인, 외국인, 기관의 순매수, 순매도 데이터를 크롤링하는 함수입니다.
    이 페이지는 보통 최신 거래일의 데이터를 제공합니다.
    """
    date = datetime.now().strftime('%Y%m%d')
    base_url = "https://finance.naver.com/sise/investorDealTrendDay.naver"
    page_url = f"?bizdate={date}&sosok={sosok}&page={page}"
    url = base_url + page_url
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    }
    
    logger.info("네이버 금융에서 투자자별 데이터 크롤링 시작...")
    all_data_in_page = []
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
     
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 데이터 테이블의 모든 데이터 행(tr)을 선택
        # th를 포함하는 헤더 행(상위 3개)은 제외
        data_rows = soup.select('table.type_1 tr')[3:] # slicing to skip header rows 
        actual_data_rows = [row for row in data_rows if row.find('td', class_='date2')]

        # 데이터 행이 없으면 빈 리스트 반환 (마지막 페이지라는 신호)
        if not any(row.find('td', class_='date2') for row in data_rows):
            logger.info(f"P.{page}에서 데이터 행을 찾을 수 없습니다. 크롤링을 중단합니다.")
            return []

        for row in actual_data_rows:
            # 날짜 td가 없는 행(구분선 등)은 건너뛰기
            if not row.find('td', class_='date2'):
                continue

            cells = row.find_all('td')

            # 날짜 포맷팅 (YY.MM.DD -> YYYY-MM-DD)
            date_str = cells[0].text.strip()
            trade_date = datetime.strptime(date_str, '%y.%m.%d').strftime('%Y-%m-%d')
            
            # 개인 순매수/순매도
            individual_trading_value = int(cells[1].text.strip().replace(',', ''))
            
            # 외국인
            foreign_trading_value = int(cells[2].text.strip().replace(',', ''))

            # 기관
            institutional_trading_value = int(cells[3].text.strip().replace(',', ''))
            

            result_dict = {
                "trade_date": trade_date,
                "sosok": sosok,
                "individual_trading_value": individual_trading_value,
                "foreign_trading_value": foreign_trading_value,
                "institutional_trading_value": institutional_trading_value
            }
            all_data_in_page.append(result_dict)

        logger.info(f"✅ P.{page}에서 {len(all_data_in_page)}건의 데이터 파싱 성공.")
        return all_data_in_page

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ P.{page} 네트워크 요청 오류: {e}")
        return []
    except Exception as e:
        logger.error(f"❌ P.{page} 데이터 처리 중 오류: {e}")
        return []


def update_historical_investor_trading_value():
    """
    네이버에서 개인, 외국인, 기관의 순매수, 순매도 데이터를 첫 페이지부터 순차적으로 크롤링하여 DB에 저장합니다.
    """
    logger.info("🚀 증시 유동성 전체 데이터 업데이트 프로세스 시작...")
    
    conn = None
    try:
        conn = get_db_connection(DB_CONFIG)
        setup_database(conn, 'src/fundamental/data_loader/sql/investor_trading_schema.sql')
        logger.info("DB 연결 및 테이블 설정 완료.")

        for sosok in ['01', '02']:  # 01: KOSPI, 02: KOSDAQ
            logger.info(f"🔍 소속 코드 {sosok} 데이터 크롤링 시작...")
            page = 1
            while page <= (PAGE_NUMBER+140):
                # 페이지별 데이터 크롤링
                daily_data_list = scrape_naver_investor_trading_value_by_page(sosok, page)
                
                # 크롤링할 데이터가 더 이상 없으면 루프 종료
                if not daily_data_list:
                    break
                
                # DB 저장 로직 (UPSERT)
                columns = daily_data_list[0].keys()
                cols_str = ", ".join(f'"{col}"' for col in columns)
                placeholders = ", ".join([f"%({col})s" for col in columns])
                # trade_date가 중복될 경우 다른 컬럼들을 업데이트
                update_cols = [col for col in columns if col not in ['trade_date']]
                update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

                # ON CONFLICT 문법으로 UPSERT (INSERT or UPDATE) 구현
                sql = f"""
                    INSERT INTO investor_trading ({cols_str}) 
                    VALUES ({placeholders}) 
                    ON CONFLICT (trade_date, sosok) DO UPDATE SET {update_str};
                """
                
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, sql, daily_data_list)
                    conn.commit()
                    logger.info(f"💾 P.{page}의 데이터 {len(daily_data_list)}건이 성공적으로 저장/업데이트되었습니다.")

                page += 1
                time.sleep(1) # 서버 부하를 줄이기 위해 페이지 요청 간 1초 대기

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ 데이터베이스 처리 중 오류 발생: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("DB 연결 해제.")
    
    logger.info("🎉 모든 작업이 완료되었습니다.")

if __name__ == "__main__":
    update_historical_investor_trading_value()