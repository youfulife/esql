package sp_test

import (
	"reflect"
	"testing"

	"github.com/bitly/go-simplejson"
	"github.com/chenyoufu/esql/sp"
)

// Ensure the parser can parse strings into Statement ASTs.
func TestTranslator_EsDsl(t *testing.T) {
	// For use in various tests.
	// now := time.Now()

	var tests = []struct {
		sql string
		dsl string
	}{
		{
			sql: `select * from symbol limit 5`,
			dsl: `{
                    "from": 0,
                    "size": 5,
                    "sort": []
                  }`,
		},
		// desc sort
		{
			sql: `select * from symbol order by name desc limit 1`,
			dsl: `{
                    "from": 0,
                    "size": 1,
                    "sort": [
                      {
                        "name": "desc"
                      }
                    ]
                  }`,
		},
		//asc sort
		{
			sql: `select * from symbol order by name limit 1`,
			dsl: `{
                    "from": 0,
                    "size": 1,
                    "sort": [
                      {
                        "name": "asc"
                      }
                    ]
                  }`,
		},
		//where EQ condition
		{
			sql: `select * from symbol where exchange='nyse' limit 1`,
			dsl: `{
                    "from": 0,
                    "query": {
                      "bool": {
                        "filter": {
                          "script": {
                            "script": "doc['exchange'].value == 'nyse'"
                          }
                        }
                      }
                    },
                    "size": 1,
                    "sort": []
                  }`,
		},
		//where GT condition
		{
			sql: `select * from symbol where last_sale > 985 limit 1`,
			dsl: `{
                    "from": 0,
                    "query": {
                      "bool": {
                        "filter": {
                          "script": {
                            "script": "doc['last_sale'].value > 985"
                          }
                        }
                      }
                    },
                    "size": 1,
                    "sort": []
                  }`,
		},
		//where NQ condition
		{
			sql: `select * from symbol where last_sale != 985 limit 1`,
			dsl: `{
                    "from": 0,
                    "query": {
                      "bool": {
                        "filter": {
                          "script": {
                            "script": "doc['last_sale'].value != 985"
                          }
                        }
                      }
                    },
                    "size": 1,
                    "sort": []
                  }`,
		},
		//where AND condition
		{
			sql: `select * from symbol where exchange='nyse' and sector='Technology' limit 3`,
			dsl: `{
                      "from": 0,
                      "query": {
                        "bool": {
                          "filter": {
                            "script": {
                              "script": "doc['exchange'].value == 'nyse' && doc['sector'].value == 'Technology'"
                            }
                          }
                        }
                      },
                      "size": 3,
                      "sort": []
                    }`,
		},
		//where OR condition
		{
			sql: `select * from symbol where exchange='nyse' OR sector!='Technology' limit 1`,
			dsl: `{
                    "from": 0,
                    "query": {
                      "bool": {
                        "filter": {
                          "script": {
                            "script": "doc['exchange'].value == 'nyse' || doc['sector'].value != 'Technology'"
                          }
                        }
                      }
                    },
                    "size": 1,
                    "sort": []
                  }`,
		},
		//condition field has @
		{
			sql: `select * from quote where @timestamp > 1482908284586 limit 1`,
			dsl: `{
                  "from": 0,
                  "query": {
                    "bool": {
                      "filter": {
                        "script": {
                          "script": "doc['@timestamp'].value > 1482908284586"
                        }
                      }
                    }
                  },
                  "size": 1,
                  "sort": []
                }`,
		},
		//count * metric
		{
			sql: `select count(*) from quote`,
			dsl: `{
                    "aggs": {},
                    "from": 0,
                    "size": 0,
                    "sort": []
                  }`,
		},
		//count field metric
		{
			sql: `select count(ipo_year) from symbol`,
			dsl: `{
                    "aggs": {
                      "count(ipo_year)": {
                        "value_count": {
                          "field": "ipo_year"
                        }
                      }
                    },
                    "from": 0,
                    "size": 0,
                    "sort": []
                  }`,
		},
		//count field metric use Alias
		{
			sql: `select count(ipo_year) AS xx from symbol`,
			dsl: `{
                    "aggs": {
                      "xx": {
                        "value_count": {
                          "field": "ipo_year"
                        }
                      }
                    },
                    "from": 0,
                    "size": 0,
                    "sort": []
                  }`,
		},
		//cardinality field metric
		{
			sql: `select cardinality(ipo_year) from symbol`,
			dsl: `{
                    "aggs": {
                      "cardinality(ipo_year)": {
                        "cardinality": {
                          "field": "ipo_year"
                        }
                      }
                    },
                    "from": 0,
                    "size": 0,
                    "sort": []
                  }`,
		},
		//sum field metric
		{
			sql: `select sum(market_cap) from symbol`,
			dsl: `{
                    "aggs": {
                      "sum(market_cap)": {
                        "sum": {
                          "field": "market_cap"
                        }
                      }
                    },
                    "from": 0,
                    "size": 0,
                    "sort": []
                  }`,
		},
		//metric field and filter
		{
			sql: `select sum(market_cap) from symbol where ipo_year=1998`,
			dsl: `{
                    "aggs": {
                      "sum(market_cap)": {"sum": {"field": "market_cap"}}
                    },
                    "from": 0,
                    "query": {
                      "bool": {
                        "filter": {
                          "script": {"script": "doc['ipo_year'].value == 1998"}
                        }
                      }
                    },
                    "size": 0,
                    "sort": []
                  }`,
		},
		//metric field and group by
		{
			sql: `select exchange, count(*) from symbol group by exchange`,
			dsl: `{
                    "aggs": {
                      "exchange": {
                        "aggs": {},
                        "terms": {
                          "field": "exchange",
                          "size": 0
                        }
                      }
                    },
                    "query": {
                      "bool": {
                        "filter": {
                          "and": [{"exists": {"field": "exchange"}}]
                        }
                      }
                    },
                    "size": 0
                  }`,
		},
		//metric field and group by
		{
			sql: `select exchange, count(*) from symbol group by exchange`,
			dsl: `{
                    "aggs": {
                      "exchange": {
                        "aggs": {},
                        "terms": {
                          "field": "exchange",
                          "size": 0
                        }
                      }
                    },
                    "query": {
                      "bool": {
                        "filter": {
                          "and": [{"exists": {"field": "exchange"}}]
                        }
                      }
                    },
                    "size": 0
                  }`,
		},
		//group by groovy function
		{
			sql: `SELECT shares_count, COUNT(*) FROM symbol GROUP BY floor(market_cap / last_sale / 1000000) AS shares_count ORDER BY shares_count LIMIT 3`,
			dsl: `{
				    "aggs": {
				      "shares_count": {
				        "aggs": {},
				        "terms": {
				          "order": [
				            {
				              "_term": "asc"
				            }
				          ],
				          "script": {
				            "inline": "floor(doc['market_cap'].value / doc['last_sale'].value / 1000000)",
				            "lang": "expression"
				          },
				          "size": 3
				        }
				      }
				    },
				    "query": {
				      "bool": {
				        "filter": {
				          "and": [
				            {
				              "exists": {
				                "field": "market_cap"
				              }
				            },
				            {
				              "exists": {
				                "field": "last_sale"
				              }
				            }
				          ]
				        }
				      }
				    },
				    "size": 0
				  }`,
		},
		//histogram aggregation
		{
			sql: `select ipo_year_range, count(*) from symbol group by histogram(ipo_year, 5) as ipo_year_range`,
			dsl: `{
				    "aggs": {
				      "ipo_year_range": {
				        "aggs": {},
				        "histogram": {
				          "field": "ipo_year",
				          "interval": "5",
				          "min_doc_count": 0
				        }
				      }
				    },
					"query": {
                      "bool": {
                        "filter": {
                          "and": [{"exists": {"field": "ipo_year"}}]
                        }
                      }
                    },
				    "size": 0
				  }`,
		},
		//date histogram aggregation
		{
			sql: `select year, max(adj_close) from quote where symbol='AAPL' group by date_histogram('@timestamp','1y') as year`,
			dsl: `{
				    "aggs": {
				      "year": {
				        "aggs": {
				          "max(adj_close)": {
				            "max": {
				              "field": "adj_close"
				            }
				          }
				        },
				        "date_histogram": {
				          "field": "@timestamp",
				          "interval": "1y"
				        }
				      }
				    },
				    "query": {
				      "bool": {
				        "filter": {
				          "script": {
				            "script": "doc['symbol'].value == 'AAPL'"
				          }
				        }
				      }
				    },
				    "size": 0
				  }`,
		},
		//range aggregation
		{
			sql: `SELECT ipo_year_range, COUNT(*) FROM symbol GROUP BY range(ipo_year, 1980, 1990, 2000) AS ipo_year_range`,
			dsl: `{
				    "aggs": {
				      "ipo_year_range": {
				        "aggs": {},
				        "range": {
				          "field": "ipo_year",
				          "keyed": true,
				          "ranges": [
				            {
				              "to": "1980"
				            },
				            {
				              "from": "1980",
				              "to": "1990"
				            },
				            {
				              "from": "1990",
				              "to": "2000"
				            },
				            {
				              "from": "2000"
				            }
				          ]
				        }
				      }
				    },
					"query": {
				      "bool": {"filter": {"and": [{"exists": {"field": "ipo_year"}}]}}
				    },
				    "size": 0
				  }`,
		},
		//metric field and group by
		{
			sql: `select exchange, max(market_cap) from symbol group by exchange`,
			dsl: `{
                    "aggs": {
                      "exchange": {
                        "aggs": {
                          "max(market_cap)": {"max": {"field": "market_cap"}}
                        },
                        "terms": {"field": "exchange", "size": 0}
                      }
                    },
                    "query": {
                      "bool": {
                        "filter": {
                          "and": [{"exists": {"field": "exchange"}}]
                        }
                      }
                    },
                    "size": 0
                  }`,
		},
		//metric field and group by multi field
		{
			sql: `select exchange, sector, max(market_cap) from symbol group by exchange, sector`,
			dsl: `{
                    "aggs": {
                      "exchange": {
                        "aggs": {
                          "sector": {
                            "aggs": {"max(market_cap)": {"max": {"field": "market_cap"}}},
                            "terms": {"field": "sector","size": 0}
                          }
                        },
                        "terms": {"field": "exchange","size": 0}
                      }
                    },
                    "query": {
                      "bool": {
                        "filter": {
                          "and": [
                            {"exists": {"field": "exchange"}},
                            {"exists": {"field": "sector"}}
                          ]
                        }
                      }
                    },
                    "size": 0
                  }`,
		},
		//metric(field op field) and group by multi field
		{
			sql: `select exchange, sum(ipo_year+last_sale) from symbol group by exchange`,
			dsl: `{
				    "aggs": {
				      "exchange": {
				        "aggs": {
				          "sum(ipo_year + last_sale)": {"sum": {"script": "doc['ipo_year'].value + doc['last_sale'].value"}}
				        },
				        "terms": {"field": "exchange","size": 0}
				      }
				    },
				    "query": {
				      "bool": {"filter": {"and": [{"exists": {"field": "exchange"}}]}}
				    },
				    "size": 0
				  }`,
		},
		//metric field and group by field expr
		{
			sql: `SELECT ipo_year_rem, COUNT(*) FROM symbol GROUP BY ipo_year % 5 AS ipo_year_rem`,
			dsl: `{
				    "aggs": {
				      "ipo_year_rem": {
				        "aggs": {},
				        "terms": {
				          "script": "doc['ipo_year'].value % 5",
				          "size": 0
				        }
				      }
				    },
					"query": {
				      "bool": {"filter": {"and": [{"exists": {"field": "ipo_year"}}]}}
				    },
				    "size": 0
				  }`,
		},
		//order by _term
		{
			sql: `SELECT ipo_year, COUNT(*) FROM symbol GROUP BY ipo_year ORDER BY ipo_year LIMIT 3`,
			dsl: `{
				    "aggs": {
				      "ipo_year": {
				        "aggs": {},
				        "terms": {
				          "field": "ipo_year",
				          "order": [
				            {
				              "_term": "asc"
				            }
				          ],
				          "size": 3
				        }
				      }
				    },
					"query": {
				      "bool": {"filter": {"and": [{"exists": {"field": "ipo_year"}}]}}
				    },
				    "size": 0
				  }`,
		},
		//order by _count
		{
			sql: `SELECT ipo_year, COUNT(*) AS ipo_count FROM symbol GROUP BY ipo_year ORDER BY ipo_count LIMIT 2`,
			dsl: `{
				    "aggs": {
				      "ipo_year": {
				        "aggs": {},
				        "terms": {
				          "field": "ipo_year",
				          "order": [
				            {
				              "_count": "asc"
				            }
				          ],
				          "size": 2
				        }
				      }
				    },
					"query": {
				      "bool": {"filter": {"and": [{"exists": {"field": "ipo_year"}}]}}
				    },
				    "size": 0
				  }`,
		},
		//order by metric
		{
			sql: `SELECT ipo_year, MAX(market_cap) AS max_market_cap FROM symbol GROUP BY ipo_year ORDER BY max_market_cap LIMIT 2`,
			dsl: `{
				    "aggs": {
				      "ipo_year": {
				        "aggs": {
				          "max_market_cap": {
				            "max": {
				              "field": "market_cap"
				            }
				          }
				        },
				        "terms": {
				          "field": "ipo_year",
				          "order": [
				            {
				              "max_market_cap": "asc"
				            }
				          ],
				          "size": 2
				        }
				      }
				    },
					"query": {
				      "bool": {"filter": {"and": [{"exists": {"field": "ipo_year"}}]}}
				    },
				    "size": 0
				  }`,
		},
		//having
		{
			sql: `SELECT ipo_year, COUNT(*) AS ipo_count FROM symbol GROUP BY ipo_year HAVING ipo_count > 200`,
			dsl: `{
				    "aggs": {
				      "ipo_year": {
				        "aggs": {
				          "having": {
				            "bucket_selector": {
				              "buckets_path": {
				                "ipo_count": "_count"
				              },
				              "script": {
				                "inline": "ipo_count \u003e 200",
				                "lang": "expression"
				              }
				            }
				          }
				        },
				        "terms": {
				          "field": "ipo_year",
				          "size": 0
				        }
				      }
				    },
					"query": {
					  "bool": {"filter": {"and": [{"exists": {"field": "ipo_year"}}]}}
					},
				    "size": 0
				  }`,
		},
		//having2
		{
			sql: `SELECT ipo_year, COUNT(*) AS ipo_count, MAX(last_sale) AS max_last_sale FROM symbol GROUP BY ipo_year HAVING ipo_count > 100 AND max_last_sale <= 10000`,
			dsl: `{
				    "aggs": {
				      "ipo_year": {
				        "aggs": {
				          "having": {
				            "bucket_selector": {
				              "buckets_path": {
				                "ipo_count": "_count",
				                "max_last_sale": "max_last_sale"
				              },
				              "script": {
				                "inline": "ipo_count > 100 && max_last_sale <= 10000",
				                "lang": "expression"
				              }
				            }
				          },
				          "max_last_sale": {
				            "max": {
				              "field": "last_sale"
				            }
				          }
				        },
				        "terms": {
				          "field": "ipo_year",
				          "size": 0
				        }
				      }
				    },
					"query": {
					  "bool": {"filter": {"and": [{"exists": {"field": "ipo_year"}}]}}
					},
				    "size": 0
				  }`,
		},
		//pipeline aggregation
		{
			sql: `select exchange, sum(ipo_year), sum(ipo_year)/sum(last_sale) AS yyyy from symbol group by exchange`,
			dsl: `{
				    "aggs": {
				      "exchange": {
				        "aggs": {
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
				                "path0": "sum(ipo_year)",
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
		},
		//pipeline aggregation2
		{
			sql: `select exchange, sum(ipo_year), sum(ipo_year*2)/avg(last_sale) AS yyyy from symbol group by exchange`,
			dsl: `{
				    "aggs": {
				      "exchange": {
				        "aggs": {
				          "avg(last_sale)": {
				            "avg": {
				              "field": "last_sale"
				            }
				          },
				          "sum(ipo_year * 2)": {
				            "sum": {
				              "script": "doc['ipo_year'].value * 2"
				            }
				          },
				          "sum(ipo_year)": {
				            "sum": {
				              "field": "ipo_year"
				            }
				          },
				          "yyyy": {
				            "bucket_script": {
				              "buckets_path": {
				                "path0": "sum(ipo_year * 2)",
				                "path1": "avg(last_sale)"
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
		},
		//pipeline aggregation 3
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
		},
		//pipeline aggregation 4
		{
			sql: `select  -5*sum(ipo_year+last_sale*2)  AS yyyy from symbol group by exchange`,
			dsl: `{
				    "aggs": {
				      "exchange": {
				        "aggs": {
				          "sum(ipo_year + last_sale * 2)": {
				            "sum": {
				              "script": "doc['ipo_year'].value + doc['last_sale'].value * 2"
				            }
				          },
				          "yyyy": {
				            "bucket_script": {
				              "buckets_path": {
				                "path0": "sum(ipo_year + last_sale * 2)"
				              },
				              "script": {
				                "inline": "0 - 5 * path0",
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
		},
	}
	for i, tt := range tests {
		dsl, err := sp.EsDsl(tt.sql)
		if err != nil {
			t.Errorf("%d. %s: error\n\n %s", i, tt.sql, err)
		}
		_dsl, _ := simplejson.NewJson([]byte(dsl))
		ttdsl, _ := simplejson.NewJson([]byte(tt.dsl))

		if !reflect.DeepEqual(_dsl.MustMap(), ttdsl.MustMap()) {
			t.Errorf("%d. %q\n\ndsl mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, ttdsl, _dsl)
		}
	}
}
