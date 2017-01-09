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
        "aggs": {
            "sum(market_cap)": {
                "sum": {
                    "script": "doc['market_cap'].value"
                }
            }
        },
        "from": 0,
        "query": {
            "bool": {
                "filter": {
                    "script": {
                        "script": "doc['ipo_year'].value == 1998"
                    }
                }
            }
        },
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


# Supported
```
"select exchange, max(market_cap) from symbol group by exchange"
```
# Todo
```
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
"SELECT ipo_year, COUNT(*) AS ipo_count FROM symbol GROUP BY ipo_year HAVING ipo_count > 200"
"SELECT ipo_year, COUNT(*) AS ipo_count, MAX(last_sale) AS max_last_sale FROM symbol GROUP BY ipo_year HAVING ipo_count > 100 AND max_last_sale <= 10000"
"select exchange, sum(ipo_year+last_sale) from symbol group by exchange"
"select exchange, sum(ipo_year), sum(ipo_year)/sum(last_sale) AS yyyy from symbol group by exchange"
"select exchange, sum(ipo_year), sum(ipo_year*2)/sum(last_sale) AS yyyy from symbol group by exchange"
"select exchange, sum(ipo_year), sum(ipo_year+last_sale)/sum(last_sale) AS yyyy from symbol group by exchange"

//filter aggregation
"select sum(market_cap, "exchange=='nyse'") from symbol where ipo_year=1998"
"select * from symbol WHERE symbol LIKE 'AAP%'"
"SELECT ipo_year_range, MAX(market_cap) AS max_market_cap FROM symbol GROUP BY histogram(ipo_year, 10) AS ipo_year_range ORDER BY ipo_year_range"

```

# BUG
```
"select sum(market_cap)+1 from symbol where ipo_year=1998"
```

# SQL

## Notation
The syntax is specified using Extended Backus-Naur Form ("EBNF").

Notation operators in order of increasing precedence:

```
|   alternation
()  grouping
[]  option (0 or 1 times)
{}  repetition (0 to n times)
```

## Query representation

### Characters

Jepl is Unicode text encoded in [UTF-8](http://en.wikipedia.org/wiki/UTF-8).

```
newline             = /* the Unicode code point U+000A */ .
unicode_char        = /* an arbitrary Unicode code point except newline */ .
```

## Letters and digits

Letters are the set of ASCII characters plus the underscore character _ (U+005F)
is considered a letter.

Only decimal digits are supported.

```
letter              = ascii_letter | "_" .
ascii_letter        = "A" … "Z" | "a" … "z" .
digit               = "0" … "9" .
```

## Identifiers

Identifiers are tokens which refer to topic names, field keys.

The rules:

- must start with an upper or lowercase ASCII character or "_"
- may contain only ASCII letters, decimal digits, and "_"

```
identifier          = ( letter ) { letter | digit }
```

#### Examples:

```
cpu
_cpu_stats
```

## Keywords

```
ALL           AS            NI         IN
SELECT        WHERE         FROM       AND
OR
```

## Literals

### Integers

Jepl supports decimal integer literals.  Hexadecimal and octal literals are not currently supported.

```
int_lit             = ( "1" … "9" ) { digit }
```

### Floats

Jepl supports floating-point literals.  Exponents are not currently supported.

```
float_lit           = int_lit "." int_lit
```

### Strings

String literals must be surrounded by single quotes or double quotes. Strings may contain `'` or `"`
characters as long as they are escaped (i.e., `\'`, `\"`).

```
string_lit          = (`'` { unicode_char } `'`) | (`"` { unicode_char } `"`)
```

### Booleans

```
bool_lit            = TRUE | FALSE
```

### Regular Expressions

```
regex_lit           = "/" { unicode_char } "/"
```

**Comparators:**
`=~` matches against
`!~` doesn't match against

## Statement

```
statement        = select_stmt
```
### SELECT

```
select_stmt      = "SELECT" fields [from_clause] [ where_clause ] [ group_by_clause ]
```

### Fields

```
fields           = field { "," field }

field            = metric_expr [ alias ]

alias            = "AS" identifier

metric_expr      = metric_term { "+" | "-"  metric_term }

metric_term      = metric_factor { "*" | "/" metric_factor }

metric_factor    =  int_lit | float_lit | func "(" arg_expr ")"

func             = "SUM" | "COUNT" | "MAX" | "MIN" | "AVG"

```

### Metric Argument Expression

```
arg_expr         =  arg_term { "+" | "-"  arg_term }

arg_term         = arg_factor { "*" | "/" arg_factor }

arg_factor       = int_lit | float_lit | var_ref | "(" arg_expr ")"
```

### Clauses

```
from_clause      = "FROM" identifier

where_clause     = "WHERE" cond_expr

group_by_clause = "GROUP BY" dimensions
```

### Where Condition Expression
```
cond_expr        = unary_expr { binary_op unary_expr }

unary_expr       = "(" cond_expr ")" | var_ref | literal | list

binary_op        = "+" | "-" | "*" | "/" | "AND" | "OR" | "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" | "!~" | "=~" | "NI" | "IN"

var_ref          = identifier { "." identifier}

list             = "[" literal { "," literal } "]"

literal          = string_lit | int_lit | float_lit | bool_lit | regex_lit

```

### Group By Dimensions
```
dimensions       = var_ref { "," var_ref }
```

#### Examples:

```sql
SELECT sum(tcp.bytes_in+tcp.bytes_out) AS total_bytes FROM packetbeat WHERE uid = 1 AND tcp.src_ip = '127.0.0.1' GROUP BY tcp.dst_ip
```
