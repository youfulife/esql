# esql
Convert sql to elastic search DSL

# 已经支持
```
"select exchange, max(market_cap) from symbol group by exchange"
"select * from symbol where exchange='nyse' limit 1"
"select * from symbol where exchange='nyse' and sector='Technology' limit 1"
"select * from symbol where last_sale > 985 limit 1"
"select * from symbol where last_sale != 985 limit 1"
"select * from symbol where exchange='nyse' AND sector!='Technology' limit 1"
"select * from symbol where exchange='nyse' OR sector!='Technology' limit 1"
"select * from quote where @timestamp > 1482908284586 limit 1"
"select * from symbol order by name limit 1"
"select count(*) from quote"
"select count(ipo_year) from symbol"
"select count(ipo_year) AS xx from symbol"
"select cardinality(ipo_year) from symbol"
"select sum(market_cap) from symbol"
"select sum(market_cap) from symbol where ipo_year=1998"
"select exchange, count(*) from symbol group by exchange"
"select exchange, max(market_cap) from symbol group by exchange"
"select exchange, sector, max(market_cap) from symbol group by exchange, sector"
"select year, max(adj_close) from quote where symbol='AAPL' group by date_histogram('@timestamp','1y') as year"
"select ipo_year_range, count(*) from symbol group by histogram(ipo_year, 5) as ipo_year_range"
"SELECT ipo_year_range, COUNT(*) FROM symbol GROUP BY range(ipo_year, 2000) AS ipo_year_range"
"SELECT ipo_year, COUNT(*) FROM symbol GROUP BY ipo_year ORDER BY ipo_year LIMIT 3"
"SELECT ipo_year, COUNT(*) AS ipo_count FROM symbol GROUP BY ipo_year ORDER BY ipo_count LIMIT 2"
"SELECT ipo_year, MAX(market_cap) AS max_market_cap FROM symbol GROUP BY ipo_year ORDER BY max_market_cap LIMIT 2"
"SELECT ipo_year_rem, COUNT(*) FROM symbol GROUP BY ipo_year % 5 AS ipo_year_rem"
"SELECT shares_count, COUNT(*) FROM symbol GROUP BY floor(market_cap / last_sale / 1000000)  AS shares_count ORDER BY shares_count LIMIT 3"

```
# Todo
```
"select * from symbol WHERE symbol LIKE 'AAP%'"
"SELECT ipo_year_range, MAX(market_cap) AS max_market_cap FROM symbol GROUP BY histogram(ipo_year, 10) AS ipo_year_range ORDER BY ipo_year_range"

```
