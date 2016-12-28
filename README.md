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
```
# Todo
```
"select * from symbol WHERE symbol LIKE 'AAP%'""
```
