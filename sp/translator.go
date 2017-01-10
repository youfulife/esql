package sp

import (
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

	pipelineBegin
	BucketScript
	BucketSelector
	pipelineEnd
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

	BucketScript:   "bucket_script",
	BucketSelector: "bucket_selector",
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
			_s = cleanDocString(d.String())
		} else {
			_s = cleanDocString(d.Alias)
		}
		if _s == f {
			return true
		}
	}
	return false
}

func (s *SelectStatement) isStarCount(f string) bool {
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
		if s.isStarCount(sf.Name) {
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
func EsDsl(sql string) (string, error) {

	stmt, err := ParseStatement(sql)
	if err != nil {
		fmt.Println("stmt err: ", err)
		return "", err
	}
	// fmt.Println(stmt)
	s, ok := stmt.(*SelectStatement)
	if !ok {
		return "", fmt.Errorf("only support select")
	}
	s.RewriteConditions()

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
	fields := s.NamesInDimension()
	// fmt.Println(fields)
	if len(fields) > 0 {
		fieldFilters := make([]map[string]interface{}, 0)
		branch := []string{"query", "bool", "filter", "and"}
		for _, f := range fields {
			_js := simplejson.New()
			existsBranch := []string{"exists", "field"}
			_js.SetPath(existsBranch, f)
			fieldFilters = append(fieldFilters, _js.MustMap())
		}
		js.SetPath(branch, fieldFilters)
	}

	// build Aggregations
	path := []string{"aggs"}
	//bucket Aggregations
	baggs := s.bucketAggregations()
	for _, a := range baggs {
		_path := append(path, []string{a.name, aggs[a.typ]}...)
		js.SetPath(_path, a.params)

		// if a.typ == Terms {
		// path = append(path, a.name)
		// }
		path = append(path, a.name, "aggs")
	}
	//metric Aggregations
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
		return "", err
	}
	// t, _ := json.MarshalIndent(js.MustMap(), "", "  ")
	// fmt.Println(string(t))

	return string(_s), nil
}

// replace all doc['xxx'].value to xxx
func cleanDocString(s string) string {
	reg := regexp.MustCompile(`doc\['(.+?)'\]\.value`)
	l := reg.ReplaceAllString(s, "${1}")
	return l
}

func (s *SelectStatement) BucketSelectorAggregation() *Agg {
	if s.Having == nil {
		return nil
	}
	// fieldAsNames := s.Fields.AliasNames()
	havingNames := s.NamesInHaving()
	agg := &Agg{}
	agg.name = "having"
	agg.typ = BucketSelector
	agg.params = make(map[string]interface{})
	sm := make(map[string]string)
	sm["lang"] = "expression"
	sm["inline"] = cleanDocString(s.Having.String())
	agg.params["script"] = sm
	bm := make(map[string]string)
	for _, name := range havingNames {
		if s.isStarCount(name) {
			bm[name] = "_count"
			continue
		}
		bm[name] = name
	}
	agg.params["buckets_path"] = bm

	return agg
}

func (s *SelectStatement) bucketAggregations() Aggs {
	var aggs Aggs
	for _, dim := range s.Dimensions {
		agg := &Agg{}
		agg.params = make(map[string]interface{})
		if dim.Alias == "" {
			agg.name = cleanDocString(dim.String())
		} else {
			agg.name = cleanDocString(dim.Alias)
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
					agg.params["field"] = cleanDocString(arg0.String())
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
				agg.params["field"] = cleanDocString(expr.Args[0].String())
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
				// terms inline expression
				agg.typ = Terms
				//order
				if len(s.SortFields) > 0 {
					agg.params["order"] = s.orders()
				}
				agg.params["size"] = s.Limit
				m := make(map[string]string, 0)
				m["lang"] = "expression"
				m["inline"] = expr.String()
				agg.params["script"] = m
			}

		default:
			agg.typ = Terms
			switch term := expr.(type) {
			case *BinaryExpr:
				agg.params["script"] = term.String()
			default:
				agg.params["field"] = cleanDocString(term.String())
			}
			//order
			if len(s.SortFields) > 0 {
				agg.params["order"] = s.orders()
			}
			agg.params["size"] = s.Limit
		}
		aggs = append(aggs, agg)
	}

	return aggs
}

// bucketFunctionCalls walks the Field of function calls expr
func bucketFunctionCalls(exp Expr) []*Call {
	switch expr := exp.(type) {
	case *VarRef:
		return nil
	case *Call:
		return []*Call{expr}
	case *BinaryExpr:
		var ret []*Call
		ret = append(ret, bucketFunctionCalls(expr.LHS)...)
		ret = append(ret, bucketFunctionCalls(expr.RHS)...)
		return ret
	case *ParenExpr:
		return bucketFunctionCalls(expr.Expr)
	}

	return nil
}

func (s *SelectStatement) bucketScriptAggs() Aggs {
	var aggs Aggs
	for _, f := range s.Fields {
		switch f.Expr.(type) {
		case *Call, *VarRef, *Wildcard:
			continue
		}

		calls := bucketFunctionCalls(f.Expr)
		bucketsPath := make(map[string]string)
		inlineExpr := cleanDocString(f.Expr.String())

		for i, fn := range calls {
			agg := &Agg{}
			agg.params = make(map[string]interface{})
			agg.name = fmt.Sprintf(`%s(%s)`, fn.Name, cleanDocString(fn.Args[0].String()))
			switch fn.Name {
			case "sum":
				agg.typ = Sum
				agg.params["script"] = fn.Args[0].String()
			default:
				panic(fmt.Errorf("not support agg aggregation"))
			}
			path := fmt.Sprintf("path%d", i)
			bucketsPath[path] = agg.name
			//todo: ugly, should use walk tree method
			inlineExpr = strings.Replace(inlineExpr, agg.name, path, -1)

			aggs = append(aggs, agg)
		}

		agg := &Agg{}
		agg.params = make(map[string]interface{})
		agg.typ = BucketScript
		if f.Alias == "" {
			agg.name = cleanDocString(f.String())
		} else {
			agg.name = cleanDocString(f.Alias)
		}
		sm := make(map[string]string)
		sm["lang"] = "expression"
		sm["inline"] = inlineExpr
		agg.params["script"] = sm
		agg.params["buckets_path"] = bucketsPath

		aggs = append(aggs, agg)

	}
	return aggs
}

func (c *Call) metricAggParams() map[string]interface{} {
	params := make(map[string]interface{})
	switch arg := c.Args[0].(type) {
	case *VarRef:
		params["field"] = arg.String()
	case *BinaryExpr:
		c.RewriteMetricArgs()
		params["script"] = c.Args[0].String()
	default:
		panic(fmt.Errorf("not support metric argument"))
	}
	return params
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
			agg.name = fmt.Sprintf(`%s(%s)`, fn.Name, cleanDocString(fn.Args[0].String()))
		} else {
			agg.name = cleanDocString(field.Alias)
		}

		switch fn.Name {
		case "avg":
			agg.typ = Avg
			agg.params = fn.metricAggParams()
		case "cardinality":
			agg.typ = Cardinality
			agg.params = fn.metricAggParams()
		case "sum":
			agg.typ = Sum
			agg.params = fn.metricAggParams()
		case "max":
			agg.typ = Max
			agg.params = fn.metricAggParams()
		case "min":
			agg.typ = Min
			agg.params = fn.metricAggParams()
		case "top":
			agg.typ = Top
			agg.params = fn.metricAggParams()
		case "count":
			agg.typ = ValueCount
			switch arg := fn.Args[0].(type) {
			case *VarRef:
				agg.params["field"] = arg.String()
			case *Wildcard:
				agg.typ = StarCount
				agg.params["field"] = ""
			case *BinaryExpr:
				fn.RewriteMetricArgs()
				agg.params["script"] = fn.Args[0].String()
			default:
				panic(fmt.Errorf("not support metric argument"))
			}

		case "stats":
			agg.typ = Stats
			agg.params = fn.metricAggParams()
		case "extended_stats":
			agg.typ = ExtendedStats
			agg.params = fn.metricAggParams()
		default:
			panic(fmt.Errorf("not support agg aggregation"))
		}

		aggs = append(aggs, agg)
	}

	//append bucket script aggregation
	aggs = append(aggs, s.bucketScriptAggs()...)
	//append bucket selector aggregation
	pipeAgg := s.BucketSelectorAggregation()
	if pipeAgg != nil {
		aggs = append(aggs, pipeAgg)
	}

	return aggs
}
