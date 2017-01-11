# esql
Convert sql to elastic search DSL

Here are currently being developed!!!

# Usage

### build and running
```
# set $GOPATH and $GOROOT
mkdir -p $GOPATH/src/github.com/chenyoufu
cd $GOPATH/src/github.com/chenyoufu
git clone https://github.com/chenyoufu/esql.git
cd esql
go get ./...
go build .
./esql
```

#### Using httpie
```
> http "127.0.0.1:1234?sql=select sum(market_cap) from symbol where ipo_year=1998"
```
output
```
HTTP/1.1 200 OK
Content-Length: 254
Content-Type: application/json; charset=UTF-8
Date: Thu, 05 Jan 2017 08:50:03 GMT

{
  "dsl": {
    "aggs": { "sum(market_cap)": { "sum": { "field": "market_cap" }}},
    "query": { "bool": { "filter": {"script": { "script": "doc['ipo_year'].value == 1998"}}}},
    "from": 0,
    "size": 0,
    "sort": []
  },
  "sql": "select sum(market_cap) from symbol where ipo_year=1998"
}
```

### One time translation
```
./esql -s "select sum(market_cap) from symbol where ipo_year=1998" -p
```
### help
```
Usage of ./esql:
  -c string
    	configuration file (default "cfg.json")
  -p	show pretty
  -s string
    	sql select statement
  -v	show version
```

# Todo
```
//filter aggregation
"select sum(market_cap, "exchange=='nyse'") from symbol where ipo_year=1998"
//like or regex
"select * from symbol WHERE symbol LIKE 'AAP%'"
//none term aggregation order 
"SELECT ipo_year_range, MAX(market_cap) AS max_market_cap FROM symbol GROUP BY histogram(ipo_year, 10) AS ipo_year_range ORDER BY ipo_year_range"

```
