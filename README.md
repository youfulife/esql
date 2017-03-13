# esql
Convert sql to elastic search DSL.

我相信大部分使用ES的开发人员都被ES DSL的语法搞的哭笑不得。今天通过查使用手册写了一个嵌套5层的json，如果不记下来，过两天再用的时候就忘光了。而且ES本身的语法文档并不系统，没有给出DSL的一个明确的文法定义，只是给出了简单的一些示例程序，稍微复杂一些的查询只能自己排列组合尝试，或者通过kibana查看原始的http请求把一大长串的json复制过来。经常在群里看到有人问想查询xxx语法应该怎么写的问题，我也遇到过，所以这个小项目是解决这个问题的初衷。

当然，ES DSL的语法灵活度比SQL是强大的，所以不可能完全用SQL类似的语法来实现ES DSL的所有功能，这个项目也是，目标是能够节省90%的时间就可以了，实在表达不了的再去查看ES的文档就行。

相比于目前类似的开源项目或者es插件来说，本项目的最大优势就是支持函数和表达式的自由运算组合。
用ES官方文档中的描述就是 Pipeline Aggregation。

比如：
```
{
    sql: `select exchange, sum(ipo_year), sum(ipo_year+last_sale)/sum(last_sale) AS yyyy from symbol group by exchange`,
    dsl: `{
            "aggs": {
              "exchange": {
                "aggs": {
                  "sum(ipo_year + last_sale)": {
                    "sum": {
                      "script": "doc['ipo_year'].value + doc['last_sale'].value"
                    }
                  },
                  "sum(ipo_year)": {
                    "sum": {
                      "field": "ipo_year"
                    }
                  },
                  "sum(last_sale)": {
                    "sum": {
                      "field": "last_sale"
                    }
                  },
                  "yyyy": {
                    "bucket_script": {
                      "buckets_path": {
                        "path0": "sum(ipo_year + last_sale)",
                        "path1": "sum(last_sale)"
                      },
                      "script": {
                        "inline": "path0 / path1",
                        "lang": "expression"
                      }
                    }
                  }
                },
                "terms": {
                  "field": "exchange",
                  "size": 0
                }
              }
            },
            "query": {
              "bool": {"filter": {"and": [{"exists": {"field": "exchange"}}]}}
            },
            "size": 0
          }`,
}
```

# 使用

### 编译运行
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
