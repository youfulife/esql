package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/bitly/go-simplejson"
	"github.com/chenyoufu/esql/g"
	"github.com/chenyoufu/esql/sp"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := flag.String("c", "cfg.json", "configuration file")
	version := flag.Bool("v", false, "show version")
	sql := flag.String("s", "", "sql select statement")
	flag.Parse()

	if *version {
		fmt.Println(g.VERSION)
		os.Exit(0)
	}

	if len(*sql) == 0 {
		os.Exit(0)
	}

	g.ParseConfig(*cfg)
	fmt.Println(g.Config())

	stmt, err := sp.ParseStatement(*sql)
	if err != nil {
		panic(err)
	}
	selectStmt, ok := stmt.(*sp.SelectStatement)
	if !ok {
		panic("Not support stmt")
	}
	fmt.Println(selectStmt)

	fmt.Println(selectStmt.EsDsl())

	js := simplejson.New()
	js.Set("xx", "yy")
	fmt.Println(js)

}
