package sp

import (
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
type BucketAgg int

// These are a comprehensive list of bucket aggregations.
const (
	IllegalAgg BucketAgg = iota

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

type Bucket struct {
	name   string
	typ    BucketAgg
	params map[string]interface{}
	child  *Bucket
}

func (b *Bucket) Map() map[string]interface{} {

	// temp := fmt.Sprintf(`{"%s":{"%s":{}}}`, b.name, aggs[b.typ])
	// js, err := simplejson.NewJson([]byte(temp))
	// if err != nil {
	// 	panic("bucket toJson error")
	// }
	js := simplejson.New()
	path := []string{b.name, aggs[b.typ]}

	js.SetPath(path, b.params)

	return js.MustMap()
}

func (b *Bucket) String() string {
	js := simplejson.New()
	path := []string{"aggs"}
	for {
		js.SetPath(path, b.Map())
		if b.child == nil {
			break
		}
		path = append(path, "aggs")
		b = b.child
	}
	s, err := js.MarshalJSON()
	if err != nil {
		return ""
	}

	return string(s)
}

func (s *SelectStatement) TslBucketAggs() string {
	dimensions := s.Dimensions
	bucket := &Bucket{}
	//dummy node
	root := bucket
	for _, dim := range dimensions {
		bkt := &Bucket{}
		bkt.params = make(map[string]interface{})
		if len(dim.Alias) > 0 {
			bkt.name = dim.Alias
		} else {
			bkt.name = dim.String()
		}

		switch expr := dim.Expr.(type) {
		case *Call:
			fn := expr.Name
			switch fn {
			case "date_histogram":

				bkt.typ = DateHistogram
				bkt.params["field"] = expr.Args[0].String()
				bkt.params["interval"] = expr.Args[1].String()
			case "avg":
				bkt.typ = Avg
				bkt.params["script"] = expr.String()
			default:
				panic(fmt.Errorf("not support bucket aggregation"))
			}

		default:
			bkt.typ = Terms
			bkt.params["script"] = expr.String()
		}
		bucket.child = bkt
		bucket = bucket.child
	}
	root = root.child

	return root.String()
}
