import logging
import dart_fss as dart
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import re
import time
from tqdm import tqdm
import urllib3
import psycopg2.extras
from datetime import datetime
from dart_fss.errors import NotFoundConsolidated
import os

# 기존 모듈 임포트 유지
from src.fundamental.data_loader.crawler import get_top_companies
from src.fundamental.data_loader.db_util import get_db_connection, setup_database
from src.fundamental.data_loader.config import DB_CONFIG

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)
logger = logging.getLogger(__name__)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- [설정] DART API KEY ---
DART_API_KEY = os.getenv('DART_API_KEY')
dart.set_api_key(DART_API_KEY)

# ==========================================
# 1. 헬퍼 함수들 (전처리 및 값 추출)
# ==========================================
# (기존 로직과 동일하여 그대로 유지)

def safe_int(value):
    try:
        if value is None: return 0
        if isinstance(value, (int, float)):
            if pd.isna(value) or np.isinf(value): return 0
        val_str = str(value).replace(',', '')
        if val_str.strip() == '' or val_str.lower() == 'nan': return 0
        return int(float(val_str))
    except:
        return 0

def preprocess_df(df):
    if df is None or df.empty: return df
    if df.index.names and any('concept_id' in str(name) for name in df.index.names):
        df = df.reset_index()
    if isinstance(df.columns, pd.MultiIndex):
        new_cols = []
        for col in df.columns:
            col_strs = [str(c) for c in col]
            if 'concept_id' in col_strs: new_cols.append('concept_id')
            elif 'label_ko' in col_strs: new_cols.append('label_ko')
            else:
                found = False
                for s in col_strs:
                    if re.match(r'20\d{2}', s):
                        new_cols.append(s)
                        found = True
                        break
                if not found: new_cols.append(str(col[0]))
        df.columns = new_cols
    return df

def get_value(df, concept_id_exact, label_pattern, year_col):
    if df is None or df.empty or year_col not in df.columns: return 0.0
    val = 0.0
    found = False
    if 'concept_id' in df.columns:
        mask = df['concept_id'].astype(str) == concept_id_exact
        if mask.any():
            val = df.loc[mask, year_col].values[0]
            found = True
    if not found and 'label_ko' in df.columns:
        mask = df['label_ko'].astype(str).str.contains(label_pattern, case=False, na=False)
        if mask.any():
            val = df.loc[mask, year_col].values[0]
    try:
        return float(str(val).replace(',', ''))
    except:
        return 0.0

def find_year_columns(df):
    if df is None: return {}
    year_cols = {}
    for col in df.columns:
        matches = re.findall(r'(20\d{2})', str(col))
        if matches and 'concept' not in str(col):
            year_cols[matches[0]] = col
    return year_cols

# ==========================================
# 2. 핵심 로직: 기업 재무 데이터 처리
# ==========================================

def process_company_financials(company_dict, corp_list, start_year=2024):
    company_code = company_dict['company_code']
    company_name = company_dict['company_name']
    exchange = company_dict.get('exchange', 'KOSPI')

    if company_name.endswith('우') or company_name.endswith('우B') or '스팩' in company_name:
        return []

    try:
        corp = corp_list.find_by_stock_code(company_code)
        if not corp: return []
        
        fs = None
        try:
            fs = corp.extract_fs(bgn_de=f'{start_year}0101', report_tp='annual')
        except NotFoundConsolidated:
            try:
                fs = corp.extract_fs(bgn_de=f'{start_year}0101', report_tp='annual', separate=True)
            except Exception:
                return []
        except Exception as e:
            logger.error(f"❌ {company_name}: 데이터 추출 에러 - {e}")
            return []

        if fs is None: return []

        def safe_extract(fs_obj, key):
            try: return fs_obj[key]
            except: return None

        df_bs = preprocess_df(safe_extract(fs, 'bs'))
        df_is = preprocess_df(safe_extract(fs, 'is'))
        df_cis = preprocess_df(safe_extract(fs, 'cis'))
        df_cf = preprocess_df(safe_extract(fs, 'cf'))
        
        map_bs = find_year_columns(df_bs)
        map_is = find_year_columns(df_is)
        map_cis = find_year_columns(df_cis)
        map_cf = find_year_columns(df_cf)
        
        available_years_pl = set(map_is.keys()) | set(map_cis.keys())
        available_years_fin = set(map_bs.keys()) & set(map_cf.keys())
        
        years = sorted(list(available_years_fin & available_years_pl), reverse=True)
        years = [y for y in years if int(y) >= start_year]

        results = []
        scale = 100000000.0 
        
        try:
            if years:
                df_price = fdr.DataReader(company_code, start=f"{min(years)}-01-01")
            else:
                df_price = pd.DataFrame()
        except:
            df_price = pd.DataFrame()

        for year in years:
            c_bs = map_bs.get(year)
            c_is = map_is.get(year)
            c_cis = map_cis.get(year)
            c_cf = map_cf.get(year)
            
            def get_pl_value(concept_id, label_list):
                val = 0.0
                if c_is and df_is is not None:
                    for label in label_list:
                        temp = get_value(df_is, concept_id, label, c_is)
                        if temp != 0: 
                            val = temp
                            break
                if val == 0 and c_cis and df_cis is not None:
                    for label in label_list:
                        temp = get_value(df_cis, concept_id, label, c_cis)
                        if temp != 0:
                            val = temp
                            break
                return val

            sales = get_pl_value('ifrs-full_Revenue', ['매출액', '수익(매출액)', '영업수익'])
            op = get_pl_value('dart_OperatingIncomeLoss', ['영업이익', '영업이익(손실)'])
            ni = get_pl_value('ifrs-full_ProfitLoss', ['당기순이익', '당기순이익(손실)'])
            eps = get_pl_value('ifrs-full_BasicEarningsLossPerShare', ['기본주당이익'])
            
            assets = get_value(df_bs, 'ifrs-full_Assets', '자산총계', c_bs)
            liab = get_value(df_bs, 'ifrs-full_Liabilities', '부채총계', c_bs)
            equity = get_value(df_bs, 'ifrs-full_Equity', '자본총계', c_bs)
            capital = get_value(df_bs, 'ifrs-full_IssuedCapital', '자본금', c_bs)
            
            ocf = get_value(df_cf, 'ifrs-full_CashFlowsFromUsedInOperatingActivities', '영업활동현금흐름', c_cf)
            icf = get_value(df_cf, 'ifrs-full_CashFlowsFromUsedInInvestingActivities', '투자활동현금흐름', c_cf)
            fcf_fin = get_value(df_cf, 'ifrs-full_CashFlowsFromUsedInFinancingActivities', '재무활동현금흐름', c_cf)
            
            ppe = get_value(df_cf, 'ifrs-full_PurchaseOfPropertyPlantAndEquipment', '유형자산의 취득', c_cf)
            intangible = get_value(df_cf, 'ifrs-full_PurchaseOfIntangibleAssets', '무형자산의 취득', c_cf)
            capex = abs(ppe) + abs(intangible)
            fcf = ocf - capex
            
            div_paid = abs(get_value(df_cf, 'ifrs-full_DividendsPaidClassifiedAsFinancingActivities', '배당금의지급', c_cf))
            
            roe = (ni / equity * 100) if equity else 0
            roa = (ni / assets * 100) if assets else 0
            debt_ratio = (liab / equity * 100) if equity else 0
            reserve_ratio = ((equity - capital) / capital * 100) if capital else 0
            payout_ratio = (div_paid / ni * 100) if ni > 0 else 0
            
            bps = 0
            if eps and ni:
                shares = ni / eps
                if shares: bps = equity / shares
            elif equity and capital:
                 pass

            per, pbr = None, None
            if not df_price.empty:
                try:
                    df_year_price = df_price[df_price.index.year == int(year)]
                    if not df_year_price.empty:
                        close = float(df_year_price.iloc[-1]['Close'])
                        if eps > 0: per = round(close / eps, 2)
                        if bps > 0: pbr = round(close / bps, 2)
                except:
                    pass

            data = {
                'company_code': company_code,
                'company_name': company_name,
                'exchange': exchange,
                'year': int(year),
                'sales': safe_int(sales/scale),
                'operating_profit': safe_int(op/scale),
                'net_income': safe_int(ni/scale),
                'total_assets': safe_int(assets/scale),
                'total_liabilities': safe_int(liab/scale),
                'total_equity': safe_int(equity/scale),
                'cash_flow_from_operations': safe_int(ocf/scale),
                'cash_flow_from_investing': safe_int(icf/scale),
                'cash_flow_from_financing': safe_int(fcf_fin/scale),
                'capex': safe_int(capex/scale),
                'fcf': safe_int(fcf/scale),
                'opearting_profit_margin': round(op/sales*100 if sales else 0, 2),
                'net_profit_margin': round(ni/sales*100 if sales else 0, 2),
                'roe': round(roe, 2),
                'roa': round(roa, 2),
                'debt_to_equity_ratio': round(debt_ratio, 2),
                'reserve_ratio': round(reserve_ratio, 2),
                'eps': safe_int(eps),
                'per': per,
                'bps': safe_int(bps),
                'pbr': pbr,
                'dps': None, 
                'dividend_yield': None, 
                'payout_ratio': round(payout_ratio, 2)
            }
            results.append(data)
        return results

    except Exception as e:
        logger.warning(f"⚠️ {company_name}({company_code}) 처리 중 예외: {e}")
        return []

# ==========================================
# 3. DB 저장 함수 (분리됨)
# ==========================================

def save_to_db(conn, data_list):
    """
    데이터 리스트를 받아서 즉시 DB에 UPSERT 수행
    """
    if not data_list:
        return

    try:
        # 데이터의 키를 기반으로 컬럼명 추출
        first_record = data_list[0]
        columns = list(first_record.keys())

        # 쿼리 동적 생성
        cols_str = ", ".join(f'"{col}"' for col in columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])
        
        # Primary Key 설정 (중복 시 업데이트할 컬럼 지정)
        pk_columns = ['company_code', 'year']
        update_cols = [col for col in columns if col not in pk_columns]
        update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

        sql = f"""
            INSERT INTO financial_indicators ({cols_str}) 
            VALUES ({placeholders}) 
            ON CONFLICT (company_code, year) DO UPDATE SET {update_str};
        """
        
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, data_list)
            conn.commit()  # [중요] 즉시 커밋하여 저장 확정
            
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ DB 저장 실패 (Batch Size: {len(data_list)}): {e}")
        raise e  # 메인 루프에서 알 수 있게 예외를 다시 던짐

# ==========================================
# 4. 메인 실행 함수 (수정됨)
# ==========================================

def update_financial_data():
    logger.info("🚀 데이터베이스 업데이트 프로세스 시작")

    # 상위 N개 기업 가져오기
    top_companies_df = get_top_companies(limit=100) 
    
    if top_companies_df.empty:
        logger.error("기업 목록 로드 실패")
        return

    logger.info("📚 DART 기업 목록 초기화 중...")
    try:
        dart_corp_list = dart.get_corp_list()
    except Exception as e:
        logger.error(f"❌ DART 초기화 실패: {e}")
        return

    total_companies = len(top_companies_df)
    logger.info(f"🐢 {total_companies}개 기업 데이터 수집 및 실시간 저장 시작...")
    
    # [변경점 1] DB 연결을 루프 밖에서 미리 수행
    conn = None
    try:
        conn = get_db_connection(DB_CONFIG)
        setup_database(conn, path='src/fundamental/data_loader/sql/financial_indicators_schema.sql')
        logger.info("✅ DB 연결 및 테이블 체크 완료")

        for i, row in tqdm(top_companies_df.iterrows(), total=total_companies, desc="Processing"):
            company_name = row['company_name']
            
            # 재시도 로직
            max_retries = 2
            company_data = [] # 한 기업의 데이터

            for attempt in range(max_retries):
                try:
                    # 데이터 수집
                    company_data = process_company_financials(row.to_dict(), dart_corp_list, start_year=2014)
                    
                    if company_data:
                        # [변경점 2] 수집 직후 DB 저장 호출
                        save_to_db(conn, company_data)
                        logger.info(f"[{i+1}/{total_companies}] ✅ '{company_name}' - {len(company_data)}건 저장 완료")
                    else:
                        logger.info(f"[{i+1}/{total_companies}] ⚠️ '{company_name}' - 데이터 없음")
                    
                    break # 성공하면 재시도 루프 탈출
                
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"🔄 '{company_name}' 재시도 ({attempt+1}/{max_retries})...")
                        time.sleep(3)
                    else:
                        logger.error(f"❌ '{company_name}' 최종 실패: {e}")
            
            # DART API 호출 제한 고려
            time.sleep(1)

    except Exception as e:
        logger.error(f"🔥 치명적 오류 발생: {e}")
    finally:
        # [변경점 3] 모든 작업 종료 후 연결 해제
        if conn:
            conn.close()
            logger.info("🏁 DB 연결 종료 및 프로세스 완료")

if __name__ == "__main__":
    update_financial_data()