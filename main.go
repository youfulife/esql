package main

import (
	"flag"
	"fmt"
	"github.com/chenyoufu/esql/g"
	"github.com/chenyoufu/jepl"
	"os"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := flag.String("c", "cfg.json", "configuration file")
	version := flag.Bool("v", false, "show version")
	flag.Parse()

	if *version {
		fmt.Println(g.VERSION)
		os.Exit(0)
	}

	g.ParseConfig(*cfg)
	fmt.Println(g.Config())

	sql := "select mysql_over_time, sum(mysql.bytes_in) / sum(mysql.bytes_in+mysql.bytes_out) AS in_bytes_rate from 'cc-packetbeat-4a859fff6e5c4521aab187eee1cfceb8-2016.12.22' where type='mysql' GROUP BY date_histogram('@timestamp', 'hour') AS mysql_over_time"
	stmt, err := jepl.ParseStatement(sql)
	if err != nil {
		panic(err)
	}
	selectStmt, ok := stmt.(*jepl.SelectStatement)
	if !ok {
		panic("Not support stmt")
	}
	fmt.Println(selectStmt)

}
