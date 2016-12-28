# esql
Convert sql to elastic search DSL

# 已经支持
sql = "select exchange, max(market_cap) from symbol group by exchange"
sql = "select * from symbol where exchange='nyse' limit 1"
sql = "select * from symbol where exchange='nyse' and sector='Technology' limit 1"
sql = "select * from symbol where last_sale > 985 limit 1"
sql = "select * from symbol where last_sale != 985 limit 1"
sql = "select * from symbol where exchange='nyse' AND sector!='Technology' limit 1"
sql = "select * from symbol where exchange='nyse' OR sector!='Technology' limit 1"
sql = "select * from quote where @timestamp > 1482908284586 limit 1"
sql = "select * from symbol order by name limit 1"

# Todo

select * from symbol WHERE symbol LIKE 'AAP%'
