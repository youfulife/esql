package sp

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/bitly/go-simplejson"
)

// ESAgg enum
type ESAgg int

// These are a comprehensive list of es aggregations.
const (
	IllegalAgg ESAgg = iota

	metricBegin
	//metric aggregations method
	Avg
	Cardinality
	ExtendedStats
	GeoBounds
	GeoCentroid
	Max
	Min
	Percentiles
	PercentileRanks
	Stats
	Sum
	Top
	ValueCount
	StarCount // count(*)

	metricEnd

	bucketBegin
	//bucket aggregations method
	DateHistogram
	DateRange
	Filter
	Filters
	GeoDistance
	GeoHashGrid
	Global
	Histogram
	IPRange
	Missing
	Nested
	Range
	ReverseNested
	Sampler
	SignificantTerms
	Terms

	bucketEnd
)

var aggs = [...]string{
	IllegalAgg: "ILLEGAL",

	Avg:             "avg",
	Cardinality:     "cardinality",
	ExtendedStats:   "extended_stats",
	GeoBounds:       "geo_bounds",
	GeoCentroid:     "geo_centroid",
	Max:             "max",
	Min:             "min",
	Percentiles:     "percentiles",
	PercentileRanks: "percentile_ranks",
	Stats:           "stats",
	Sum:             "sum",
	Top:             "top",
	ValueCount:      "value_count",
	StarCount:       "star_count",

	DateHistogram:    "date_histogram",
	DateRange:        "date_range",
	Filter:           "date_range",
	Filters:          "filters",
	GeoDistance:      "geo_distance",
	GeoHashGrid:      "geohash_grid",
	Global:           "global",
	Histogram:        "histogram",
	IPRange:          "ip_range",
	Missing:          "missing",
	Nested:           "nested",
	Range:            "range",
	ReverseNested:    "reverse_nested",
	Sampler:          "sampler",
	SignificantTerms: "significant_terms",
	Terms:            "terms",
}

type Agg struct {
	name   string
	typ    ESAgg
	params map[string]interface{}
}

type Aggs []*Agg

func (s *SelectStatement) isGroupBySort(f string) bool {
	for _, d := range s.Dimensions {
		var _s string
		if d.Alias == "" {
			_s = aggName(d.String())
		} else {
			_s = aggName(d.Alias)
		}
		if _s == f {
			return true
		}
	}
	return false
}

func (s *SelectStatement) isStarCountSort(f string) bool {
	for _, field := range s.Fields {
		fn, ok := field.Expr.(*Call)
		if !ok {
			continue
		}
		switch fn.Name {
		case "count":
			if fn.Args[0].String() == "*" && field.Alias == f {
				return true
			}
		}
	}
	return false
}

func (s *SelectStatement) orders() []map[string]string {
	order := make([]map[string]string, 0, len(s.SortFields))
	for _, sf := range s.SortFields {
		if s.isGroupBySort(sf.Name) {
			sf.Name = "_term"
		}
		if s.isStarCountSort(sf.Name) {
			sf.Name = "_count"
		}
		m := make(map[string]string)
		if sf.Ascending {
			m[sf.Name] = "asc"
		} else {
			m[sf.Name] = "desc"
		}
		order = append(order, m)
	}
	return order
}

//EsDsl return dsl json string
func (s *SelectStatement) EsDsl() string {

	js := simplejson.New()

	if len(s.Dimensions) == 0 {
		//from and size
		js.Set("from", s.Offset)
		js.Set("size", s.Limit)
		//sort
		sort := make([]map[string]string, 0, len(s.SortFields))
		for _, sf := range s.SortFields {
			m := make(map[string]string)
			if sf.Ascending {
				m[sf.Name] = "asc"
			} else {
				m[sf.Name] = "desc"
			}
			sort = append(sort, m)
		}
		js.Set("sort", sort)
	} else {
		js.Set("size", 0)
	}

	//fields
	//scirpt fields

	//query
	if s.Condition != nil {
		branch := []string{"query", "bool", "filter", "script", "script"}
		js.SetPath(branch, s.Condition.String())
	}

	// build aggregates
	path := []string{"aggs"}
	//bucket aggregates
	baggs := s.bucketAggregates()
	for _, a := range baggs {
		_path := append(path, []string{a.name, aggs[a.typ]}...)
		js.SetPath(_path, a.params)

		// if a.typ == Terms {
		// path = append(path, a.name)
		// }
		path = append(path, a.name, "aggs")
	}
	//metric aggregates
	maggs := s.metricAggs()
	for _, a := range maggs {
		if a.typ == StarCount {
			js.SetPath(path, make(map[string]string, 0))
			continue
		}
		_path := append(path, []string{a.name, aggs[a.typ]}...)
		js.SetPath(_path, a.params)
	}

	_s, err := js.MarshalJSON()
	if err != nil {
		return ""
	}
	t, _ := json.MarshalIndent(js.MustMap(), "", "  ")
	fmt.Println(string(t))

	return string(_s)
}

// todo: replace all doc['xxx'].value to xxx
func aggName(s string) string {
	reg := regexp.MustCompile(`\['(.+)'\]`)
	l := reg.FindStringSubmatch(s)
	if len(l) == 0 {
		return s
	}
	if len(l) > 1 {
		return l[1]
	}
	return l[0]
}

func (s *SelectStatement) bucketAggregates() Aggs {
	var aggs Aggs
	for _, dim := range s.Dimensions {
		agg := &Agg{}
		agg.params = make(map[string]interface{})
		if dim.Alias == "" {
			agg.name = aggName(dim.String())
		} else {
			agg.name = aggName(dim.Alias)
		}

		switch expr := dim.Expr.(type) {
		case *Call:
			fn := expr.Name
			switch fn {
			case "range":
				agg.typ = Range
				switch arg0 := expr.Args[0].(type) {
				case *BinaryExpr:
					agg.params["script"] = arg0.String()
				default:
					agg.params["field"] = aggName(arg0.String())
				}
				agg.params["keyed"] = true
				ranges := make([]map[string]string, 0, len(expr.Args))
				args := expr.Args[1:]
				for i, arg := range args {
					m := make(map[string]string)
					if i == 0 {
						m["to"] = arg.String()
					} else {
						m["from"] = args[i-1].String()
						m["to"] = arg.String()
					}
					ranges = append(ranges, m)
				}
				ranges = append(ranges, map[string]string{"from": args[len(args)-1].String()})
				agg.params["ranges"] = ranges

			case "histogram":

				agg.typ = Histogram
				agg.params["field"] = aggName(expr.Args[0].String())
				agg.params["interval"] = expr.Args[1].String()
				agg.params["min_doc_count"] = 0
				// agg.params["min"] = expr.Args[2].String()
				// agg.params["max"] = expr.Args[3].String()
			case "date_histogram":

				agg.typ = DateHistogram
				agg.params["field"] = strings.Trim(expr.Args[0].String(), "'")
				//support `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`
				agg.params["interval"] = strings.Trim(expr.Args[1].String(), "'")
			default:
				panic(fmt.Errorf("not support bucket aggregation"))
			}

		default:
			agg.typ = Terms
			switch term := expr.(type) {
			case *BinaryExpr:
				agg.params["script"] = term.String()
			default:
				agg.params["field"] = aggName(term.String())
			}
			//order
			agg.params["order"] = s.orders()
			agg.params["size"] = s.Limit
		}
		aggs = append(aggs, agg)
	}

	return aggs
}

func (s *SelectStatement) metricAggs() Aggs {
	var aggs Aggs
	for _, field := range s.Fields {
		fn, ok := field.Expr.(*Call)
		if !ok {
			continue
		}
		agg := &Agg{}
		agg.params = make(map[string]interface{})
		if field.Alias == "" {
			agg.name = fmt.Sprintf(`%s(%s)`, fn.Name, aggName(fn.Args[0].String()))
		} else {
			agg.name = aggName(field.Alias)
		}

		switch fn.Name {
		case "avg":
			agg.typ = Avg
			agg.params["script"] = fn.Args[0].String()
		case "cardinality":
			agg.typ = Cardinality
			agg.params["script"] = fn.Args[0].String()
		case "sum":
			agg.typ = Sum
			agg.params["script"] = fn.Args[0].String()
		case "max":
			agg.typ = Max
			agg.params["script"] = fn.Args[0].String()
		case "min":
			agg.typ = Min
			agg.params["script"] = fn.Args[0].String()
		case "top":
			agg.typ = Top
			agg.params["script"] = fn.Args[0].String()
		case "count":
			agg.typ = ValueCount
			if fn.Args[0].String() == "*" {
				agg.typ = StarCount
				agg.params["field"] = ""
			} else {
				agg.params["script"] = fn.Args[0].String()
			}

		case "stats":
			agg.typ = Stats
			agg.params["script"] = fn.Args[0].String()
		case "extended_stats":
			agg.typ = ExtendedStats
			agg.params["script"] = fn.Args[0].String()
		default:
			panic(fmt.Errorf("not support agg aggregation"))
		}

		aggs = append(aggs, agg)
	}

	return aggs
}
