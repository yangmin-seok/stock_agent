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

# 기존 프로젝트 모듈 임포트 (사용자 환경에 맞게 유지)
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

# urllib3 경고 메시지 숨기기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- [설정] DART API KEY ---
DART_API_KEY = os.getenv('DART_API_KEY')
dart.set_api_key(DART_API_KEY)

# ==========================================
# 1. 헬퍼 함수들 (전처리 및 값 추출)
# ==========================================

def preprocess_df(df):
    """MultiIndex 컬럼 평탄화 및 메타데이터 정리"""
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
    """값 추출 함수 (Concept ID 우선, 없으면 Label 검색)"""
    if df is None or year_col not in df.columns: return 0
    
    def parse_val(val):
        try: return float(str(val).replace(',', ''))
        except: return 0
    
    # 1. Concept ID 검색
    if 'concept_id' in df.columns:
        mask = df['concept_id'].astype(str) == concept_id_exact
        if mask.any():
            return parse_val(df.loc[mask, year_col].values[0])
            
    # 2. Label 검색
    if 'label_ko' in df.columns:
        mask = df['label_ko'].astype(str).str.contains(label_pattern, case=False, na=False)
        if mask.any():
            return parse_val(df.loc[mask, year_col].values[0])
    return 0

def find_year_columns(df):
    """데이터프레임 컬럼에서 연도(YYYY) 식별"""
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

    # 우선주/스팩 제외
    if company_name.endswith('우') or company_name.endswith('우B') or '스팩' in company_name:
        return []

    try:
        corp = corp_list.find_by_stock_code(company_code)
        if not corp: return []
        
        fs = None
        # 연결재무제표 시도 -> 실패 시 별도재무제표 시도
        try:
            fs = corp.extract_fs(bgn_de=f'{start_year}0101', report_tp='annual')
        except NotFoundConsolidated:
            try:
                fs = corp.extract_fs(bgn_de=f'{start_year}0101', report_tp='annual', separate=True)
            except Exception as e:
                logger.debug(f"ℹ️ {company_name}: 별도재무제표 없음 - {e}")
                return []
        except Exception as e:
            logger.error(f"❌ {company_name}: 데이터 추출 에러 - {e}")
            return []

        if fs is None: return []

        # [핵심] dart-fss 객체에서 안전하게 DataFrame 추출 (AttributeError 방지)
        def safe_extract(fs_obj, key):
            try:
                return fs_obj[key]
            except:
                return None

        # 전처리
        df_bs = preprocess_df(safe_extract(fs, 'bs'))   # 재무상태표
        df_is = preprocess_df(safe_extract(fs, 'is'))   # 손익계산서
        df_cis = preprocess_df(safe_extract(fs, 'cis')) # 포괄손익계산서
        df_cf = preprocess_df(safe_extract(fs, 'cf'))   # 현금흐름표
        
        # 연도 매핑
        map_bs = find_year_columns(df_bs)
        map_is = find_year_columns(df_is)
        map_cis = find_year_columns(df_cis)
        map_cf = find_year_columns(df_cf)
        
        # [핵심] 연도 교집합 로직 개선 (SK하이닉스 대응)
        # IS가 없어도 CIS가 있으면 허용
        available_years_pl = set(map_is.keys()) | set(map_cis.keys())
        available_years_fin = set(map_bs.keys()) & set(map_cf.keys())
        
        # 공통 연도 추출 및 start_year 필터링
        years = sorted(list(available_years_fin & available_years_pl), reverse=True)
        years = [y for y in years if int(y) >= start_year]

        results = []
        scale = 100000000 # 1억 단위로 저장
        
        # 주가 데이터 가져오기 (PER/PBR 계산용)
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
            
            # [핵심] 손익 항목 추출 헬퍼 (IS 우선, 없으면 CIS 조회)
            def get_pl_value(concept_id, label):
                val = 0
                if c_is and df_is is not None:
                    val = get_value(df_is, concept_id, label, c_is)
                # IS값이 0이면 CIS 확인 (SK하이닉스 등 IFRS 대응)
                if val == 0 and c_cis and df_cis is not None:
                    val = get_value(df_cis, concept_id, label, c_cis)
                return val

            # 데이터 추출
            sales = get_pl_value('ifrs-full_Revenue', '매출액')
            op = get_pl_value('dart_OperatingIncomeLoss', '영업이익')
            ni = get_pl_value('ifrs-full_ProfitLoss', '당기순이익')
            eps = get_pl_value('ifrs-full_BasicEarningsLossPerShare', '기본주당이익')
            
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
            
            # 재무 비율 계산
            roe = (ni / equity * 100) if equity else 0
            roa = (ni / assets * 100) if assets else 0
            debt_ratio = (liab / equity * 100) if equity else 0
            reserve_ratio = ((equity - capital) / capital * 100) if capital else 0
            payout_ratio = (div_paid / ni * 100) if ni > 0 else 0
            
            bps = 0
            if eps and ni:
                shares = ni / eps
                if shares: bps = equity / shares

            per, pbr = None, None
            if not df_price.empty:
                try:
                    df_year_price = df_price[df_price.index.year == int(year)]
                    if not df_year_price.empty:
                        # ★ [수정] NumPy 타입을 Python 기본 float으로 강제 변환
                        close = float(df_year_price.iloc[-1]['Close'])
                        
                        if eps > 0: 
                            per = float(round(close / eps, 2)) # float() 감싸기
                        if bps > 0: 
                            pbr = float(round(close / bps, 2)) # float() 감싸기
                except:
                    pass

            data = {
                'company_code': company_code,
                'company_name': company_name,
                'exchange': exchange,
                'year': int(year),
                'sales': int(sales/scale),
                'operating_profit': int(op/scale),
                'net_income': int(ni/scale),
                'total_assets': int(assets/scale),
                'total_liabilities': int(liab/scale),
                'total_equity': int(equity/scale),
                'cash_flow_from_operations': int(ocf/scale),
                'cash_flow_from_investing': int(icf/scale),
                'cash_flow_from_financing': int(fcf_fin/scale),
                'capex': int(capex/scale),
                'fcf': int(fcf/scale),
                'opearting_profit_margin': round(op/sales*100 if sales else 0, 2),
                'net_profit_margin': round(ni/sales*100 if sales else 0, 2),
                'roe': round(roe, 2),
                'roa': round(roa, 2),
                'debt_to_equity_ratio': round(debt_ratio, 2),
                'reserve_ratio': round(reserve_ratio, 2),
                'eps': int(eps),
                'per': per,
                'bps': int(bps),
                'pbr': pbr,
                'dps': None, # 별도 계산 필요 시 추가
                'dividend_yield': None, # 별도 계산 필요 시 추가
                'payout_ratio': round(payout_ratio, 2)
            }
            results.append(data)
        return results

    except Exception as e:
        logger.warning(f"⚠️ {company_name}({company_code}) 처리 중 예외: {e}")
        return []

# ==========================================
# 3. 메인 실행 함수
# ==========================================

def update_financial_data():
    logger.info("🚀 데이터베이스 업데이트 프로세스 시작")

    # 상위 N개 기업 가져오기 (테스트 시 limit 조절 권장)
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

    all_financial_data = []
    total_companies = len(top_companies_df)

    logger.info(f"🐢 {total_companies}개 기업 데이터 수집 시작...")
    
    # 1. 데이터 수집 루프
    for i, row in tqdm(top_companies_df.iterrows(), total=total_companies, desc="Extracting"):
        company_name = row['company_name']
        company_dict = row.to_dict()
        
        # start_year를 기준으로 데이터 추출 (비교식 포함하여 나중에 필터링됨)
        result = process_company_financials(company_dict, dart_corp_list, start_year=2014)
        
        if result:
            all_financial_data.extend(result)
            logger.info(f"[{i+1}/{total_companies}] ✅ '{company_name}' - {len(result)}건 수집 완료")
        else:
            logger.info(f"[{i+1}/{total_companies}] ⚠️ '{company_name}' - 데이터 없음")
            
        time.sleep(1) # 서버 부하 방지용 딜레이

    if not all_financial_data:
        logger.warning("수집된 데이터가 없습니다. 프로세스를 종료합니다.")
        return

    # ==========================================
    # 3. DB 저장
    # ==========================================
    logger.info("🗄️ DB 저장을 시작합니다...")
    conn = None
    try:
        conn = get_db_connection(DB_CONFIG)
        # 테이블 스키마 확인 및 생성
        setup_database(conn, path='src/fundamental/data_loader/sql/financial_indicators_schema.sql')

        first_record = all_financial_data[0]
        columns = list(first_record.keys())

        # INSERT Query 생성
        cols_str = ", ".join(f'"{col}"' for col in columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])
        
        # ON CONFLICT DO UPDATE Query 생성
        pk_columns = ['company_code', 'year']
        update_cols = [col for col in columns if col not in pk_columns]
        update_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])

        sql = f"""
            INSERT INTO financial_indicators ({cols_str}) 
            VALUES ({placeholders}) 
            ON CONFLICT (company_code, year) DO UPDATE SET {update_str};
        """
        
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, all_financial_data)
            conn.commit()
            logger.info(f"🎉 DB 업로드 완료! (총 {len(all_financial_data)}건 처리됨)")

    except Exception as e:
        if conn: conn.rollback()
        logger.error(f"❌ DB 저장 오류: {e}")
    finally:
        if conn: conn.close()
        logger.info("🏁 프로세스 종료")

if __name__ == "__main__":
    update_financial_data()