package serv

import (
	"encoding/json"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/chenyoufu/esql/sp"
)

//CmdTranslator return string
func CmdTranslator(sql string, pretty bool) string {
	m := make(map[string]interface{}, 1)
	var bs []byte
	var err error

	m["sql"] = sql
	dsl, err := sp.EsDsl(sql)
	if err != nil {
		m["err"] = err.Error()
	}

	js, err := simplejson.NewJson([]byte(dsl))
	if err != nil {
		m["err"] = err.Error()
	} else {
		m["dsl"] = js.MustMap()
	}

	if pretty {
		bs, err = json.MarshalIndent(m, "", "  ")
	} else {
		bs, err = json.Marshal(m)
	}
	return string(bs)
}
