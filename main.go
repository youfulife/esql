package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/chenyoufu/esql/g"
	"github.com/chenyoufu/esql/serv"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := flag.String("c", "cfg.json", "configuration file")
	version := flag.Bool("v", false, "show version")
	pretty := flag.Bool("p", false, "show pretty")
	sql := flag.String("s", "", "sql select statement")
	flag.Parse()

	if *version {
		fmt.Println(g.VERSION)
		os.Exit(0)
	}

	if len(*sql) != 0 {
		s := serv.CmdTranslator(*sql, *pretty)
		fmt.Println(s)
		os.Exit(0)
	}

	g.ParseConfig(*cfg)
	fmt.Println(g.Config())

	go serv.Start()

	select {}

}
