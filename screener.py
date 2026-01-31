import pandas as pd
import requests
import io
import re
import numpy as np
from typing import Dict, Any, Optional
import logging
from concurrent.futures import ThreadPoolExecutor
import FinanceDataReader as fdr

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_realtime_encparam(company_code: str) -> Optional[str]:
    """네이버 금융 페이지에서 실시간 encparam 추출"""
    url = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={company_code}"
    try:
        res = requests.get(url, timeout=5)
        # 소스코드 내 'encparam': '...' 형태를 정규식으로 추출
        match = re.search(r"encparam\s*:\s*['\"]([^'\"]+)['\"]", res.text)
        return match.group(1) if match else None
    except:
        return None

def get_acceleration_data(company: Dict[str, Any], encparam: str) -> Optional[Dict[str, Any]]:
    ajax_url = "https://navercomp.wisereport.co.kr/v2/company/ajax/cF1001.aspx"
    referer_url = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={company['company_code']}"
    
    params = {
        'cmp_cd': company['company_code'],
        'fin_typ': '4', 'freq_typ': 'Y',
        'encparam': encparam
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': referer_url
    }

    try:
        response = requests.get(ajax_url, params=params, headers=headers, timeout=7)
        tables = pd.read_html(io.StringIO(response.text))
        df = tables[1]
        df.columns = df.columns.droplevel(0)
        df.set_index(df.columns[0], inplace=True)
        
        sales_row = df.loc[df.index.str.contains('매출액')]
        # 데이터 유무 확인을 위한 디버깅 출력
        vals = sales_row.iloc[:, -4:].apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',', ''), errors='coerce')).values[0]
        
        # 추정치가 하나라도 없으면 탈락 (27년 데이터 부재가 주된 원인)
        if np.isnan(vals).any(): 
            return None

        s24, s25, s26, s27 = vals
        g25, g26, g27 = (s25-s24)/s24, (s26-s25)/s25, (s27-s26)/s26
        
        if g27 > g26 > g25 > 0:
            logger.info(f"🎯 찾음: {company['company_name']} ({g25:.1%} < {g26:.1%} < {g27:.1%})")
            return {
                '종목명': company['company_name'],
                '25성장': round(g25*100, 2), '26성장': round(g26*100, 2), '27성장': round(g27*100, 2)
            }
    except:
        return None

def main():
    # 1. encparam 하나 먼저 따오기 (삼성전자 기준)
    enc = get_realtime_encparam('005930')
    if not enc:
        logger.error("encparam 추출 실패")
        return

    # 2. 종목 리스트 (에러 나면 수동 리스트 사용)
    try:
        df_krx = fdr.StockListing('KRX')
        # 상위 100개는 의외로 가속 성장이 없을 수 있으니 300개 정도로 늘림
        target_list = df_krx[['Code', 'Name']].head(300).to_dict('records')
        companies = [{'company_code': c['Code'], 'company_name': c['Name']} for c in target_list]
    except:
        return

    logger.info(f"🚀 실시간 파라미터로 스캔 시작 (enc: {enc[:10]}...)")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = [r for r in list(executor.map(lambda c: get_acceleration_data(c, enc), companies)) if r]

    if results:
        pd.DataFrame(results).to_excel("fast_growth.xlsx", index=False)
        logger.info(f"✅ 저장 완료: {len(results)}개 종목")
    else:
        logger.info("⚠️ 27년 확정 추정치가 있는 '성장 가속' 기업이 현재 리스트에 없습니다.")

if __name__ == "__main__":
    main()