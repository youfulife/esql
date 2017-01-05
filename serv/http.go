package serv

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"io/ioutil"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/chenyoufu/esql/g"
	"github.com/chenyoufu/esql/sp"
	"github.com/toolkits/file"
)

func renderJSON(w http.ResponseWriter, v interface{}, pretty bool) {
	var bs []byte
	var err error
	if pretty {
		bs, err = json.MarshalIndent(v, "", "  ")
	} else {
		bs, err = json.Marshal(v)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Write(bs)
}

func translate(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{}, 1)

	var pretty string
	pretty = r.URL.Query().Get("sql")

	var sql string
	switch r.Method {
	case "GET":
		sql = r.URL.Query().Get("sql")
		if len(sql) == 0 {
			http.Error(w, fmt.Errorf("sql param error").Error(), http.StatusBadRequest)
			return
		}
	case "POST":
		defer r.Body.Close()
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		sql = string(body)
	}

	m["sql"] = sql

	dsl, err := sp.EsDsl(sql)
	if err != nil {
		m["err"] = err.Error()
	} else {
		js, _ := simplejson.NewJson([]byte(dsl))
		m["dsl"] = js.MustMap()
	}

	if pretty == "1" {
		renderJSON(w, m, true)
	} else {
		renderJSON(w, m, false)
	}
	return
}

func configRoutes() {
	http.HandleFunc("/", translate)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		m := make(map[string]string, 1)
		m["health"] = "good"
		renderJSON(w, m, false)
	})

	http.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		m := make(map[string]string, 1)
		m["version"] = g.VERSION
		renderJSON(w, m, false)
	})

	http.HandleFunc("/workdir", func(w http.ResponseWriter, r *http.Request) {
		m := make(map[string]string, 1)
		m["workdir"] = file.SelfDir()
		renderJSON(w, m, false)
	})

}

// Start http server for requests
func Start() {
	if !g.Config().HTTP.Enabled {
		return
	}
	addr := g.Config().HTTP.Listen
	if addr == "" {
		return
	}

	configRoutes()

	log.Println("http listening", addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatalln(err)
	}

}
