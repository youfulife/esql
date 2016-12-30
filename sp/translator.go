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

//EsDsl return dsl json string
func (s *SelectStatement) EsDsl() string {

	js := simplejson.New()
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
			agg.params["script"] = expr.String()
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
		agg.name = fmt.Sprintf(`%s(%s)`, fn.Name, aggName(fn.Args[0].String()))

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
