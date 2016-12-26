package sp

import (
	"fmt"
	"strings"
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
