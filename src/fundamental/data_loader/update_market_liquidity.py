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
from src.fundamental.data_loader.config import DB_CONFIG
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def scrape_naver_market_liquidity_by_page(page: int) -> Optional[Dict[str, Any]]:
    """
    네이버 금융 '증시자금동향' 페이지에서 고객예탁금과 신용잔고 데이터를 크롤링합니다.
    이 페이지는 보통 최신 거래일의 데이터를 제공합니다.
    """
    base_url = "https://finance.naver.com/sise/sise_deposit.naver"
    url = f"{base_url}?page={page}"  # 코스피 전체
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    }
    
    logger.info("네이버 금융에서 증시자금동향 데이터 크롤링 시작...")
    all_data_in_page = []
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
     
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 데이터 테이블의 모든 데이터 행(tr)을 선택
        # th를 포함하는 헤더 행(상위 3개)은 제외
        data_rows = soup.select('table.type_1 tr')[3:] # slicing to skip header rows
     
        # 데이터 행이 없으면 빈 리스트 반환 (마지막 페이지라는 신호)
        if not any(row.find('td', class_='date') for row in data_rows):
            logger.info(f"P.{page}에서 데이터 행을 찾을 수 없습니다. 크롤링을 중단합니다.")
            return []

        for row in data_rows:
            # 날짜 td가 없는 행(구분선 등)은 건너뛰기
            if not row.find('td', class_='date'):
                continue

            cells = row.find_all('td')

            # 날짜 포맷팅 (YY.MM.DD -> YYYY-MM-DD)
            date_str = cells[0].text.strip()
            trade_date = datetime.strptime(date_str, '%y.%m.%d').strftime('%Y-%m-%d')
            
            # 고객예탁금
            deposits_str = cells[1].text.strip().replace(',', '')
            investor_deposits = int(deposits_str)

            # 신용잔고
            credit_str = cells[3].text.strip().replace(',', '')
            credit_balance = int(credit_str)

            # 신용잔고율 계산
            credit_deposit_ratio = (credit_balance / investor_deposits * 100) if investor_deposits != 0 else 0.0

            result_dict = {
                "trade_date": trade_date,
                "investor_deposits": investor_deposits,
                "credit_balance": credit_balance,
                "credit_deposit_ratio": round(credit_deposit_ratio, 2)
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


def update_historical_market_liquidity():
    """
    네이버에서 증시 유동성 데이터를 첫 페이지부터 순차적으로 크롤링하여 DB에 저장합니다.
    """
    logger.info("🚀 증시 유동성 전체 데이터 업데이트 프로세스 시작...")
    
    conn = None
    try:
        conn = get_db_connection(DB_CONFIG)
        setup_database(conn, 'src/fundamental/data_loader/sql/market_liquidity_schema.sql')
        logger.info("DB 연결 및 테이블 설정 완료.")

        page = 1
        while page <= 140:
            # 페이지별 데이터 크롤링
            daily_data_list = scrape_naver_market_liquidity_by_page(page)
            
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
                INSERT INTO market_liquidity ({cols_str}) 
                VALUES ({placeholders}) 
                ON CONFLICT (trade_date) DO UPDATE SET {update_str};
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
    update_historical_market_liquidity()