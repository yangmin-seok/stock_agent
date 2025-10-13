SELECT
    si.company_name,                                -- 회사명
    si.company_code,                                -- 종목코드
    SUM(sdc.foreign_net_buy_amount) AS total_net_buy -- 6개월간 외국인 순매수액 합계
FROM
    stock_info AS si
JOIN
    stock_day_candles AS sdc ON si.company_code = sdc.company_code
WHERE
    -- 캔들 날짜(candle_date)를 기준으로 최근 6개월 데이터만 필터링
    sdc.candle_date >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY
    si.company_name, si.company_code
HAVING
    -- 그룹화된 순매수액의 합계가 3000(억)보다 큰 경우만 선택
    SUM(sdc.foreign_net_buy_amount) > 3000 -- GROUP BY filtering
ORDER BY
    total_net_buy DESC; -- 순매수액이 높은 순으로 정렬