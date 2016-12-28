package sp

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bitly/go-simplejson"
)

type EsDsl struct {
	From         int
	Size         int
	Query        string
	Sort         []string
	Fields       []string
	ScriptFields []string
	Aggs         string
}

func (s *SelectStatement) QueryFrom() int {
	return s.Offset
}

func (s *SelectStatement) QuerySize() int {
	return s.Limit
}

func (s *SelectStatement) QueryFilter() string {
	cond := s.Condition
	if cond == nil {
		return ""
	}
	return fmt.Sprintf(`"query": {"bool": {"filter": { "script": { "script": "%s"}}}}`, cond.String())
}

func (s *SelectStatement) QuerySort() string {
	sort := make([]string, 0, len(s.SortFields))
	var a string
	for _, field := range s.SortFields {
		if field.Ascending {
			a = "ASC"
		} else {
			a = "DESC"
		}
		s := fmt.Sprintf(`{"%s": "%s"}`, field.Name, a)
		sort = append(sort, s)
	}
	return fmt.Sprintf(`"sort": [%s]`, strings.Join(sort, ","))
}

// BucketAgg .
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

func (stmt *SelectStatement) EsDsl() string {

	js := simplejson.New()
	path := []string{"aggs"}

	baggs := stmt.BucketAggregates()
	for _, a := range baggs {
		_path := append(path, []string{a.name, aggs[a.typ]}...)
		js.SetPath(_path, a.params)

		if a.typ == Terms {
			path = append(path, a.name)
		}
		path = append(path, "aggs")
	}

	maggs := stmt.MetricAggs()
	for _, a := range maggs {
		_path := append(path, []string{a.name, aggs[a.typ]}...)
		js.SetPath(_path, a.params)
	}

	s, err := js.MarshalJSON()
	if err != nil {
		return ""
	}
	t, _ := json.MarshalIndent(js.MustMap(), "", "  ")
	fmt.Println(string(t))

	return string(s)
}

func (s *SelectStatement) BucketAggregates() Aggs {
	var aggs Aggs
	for _, dim := range s.Dimensions {
		agg := &Agg{}
		agg.params = make(map[string]interface{})
		agg.name = dim.String()

		switch expr := dim.Expr.(type) {
		case *Call:
			fn := expr.Name
			switch fn {
			case "date_histogram":

				agg.typ = DateHistogram
				agg.params["field"] = expr.Args[0].String()
				agg.params["interval"] = expr.Args[1].String()
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

func (s *SelectStatement) MetricAggs() Aggs {
	var aggs Aggs
	for _, field := range s.Fields {
		fn, ok := field.Expr.(*Call)
		if !ok {
			continue
		}
		agg := &Agg{}
		agg.params = make(map[string]interface{})
		agg.name = fn.String()

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
			agg.params["script"] = fn.Args[0].String()
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
