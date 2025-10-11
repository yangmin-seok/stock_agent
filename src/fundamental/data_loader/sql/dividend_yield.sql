SELECT fi.company_name, fi.dividend_yield, fi.pbr  FROM public.financial_indicators AS fi
where fi.dividend_yield is not null and fi.pbr is not null and fi.year = 2024 and fi.market_cap <= 10000
order by fi.pbr 
limit 20;