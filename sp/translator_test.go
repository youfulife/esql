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
