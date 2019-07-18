package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var URL = "http://clk.cpa.mobcastlead.com/clk?campaign_id=%s&cid=%s&idfa=%s&ip=%s"

var (
	idFile   string
	from     int64
	qps      int
	campaign string
	cid      string
	lineNum  int64 = 0
)

func main() {
	flag.StringVar(&idFile, "file", "", "path to id file")
	flag.StringVar(&campaign, "campaign", "", "campaign id")
	flag.StringVar(&cid, "cid", "test", "creative id")
	flag.Int64Var(&from, "from", 1, "which line to start read id")
	flag.IntVar(&qps, "qps", 1, "request per second")
	flag.Parse()

	if len(idFile) == 0 {
		flag.Usage()
		return
	}

	if campaign == "" {
		flag.Usage()
		return
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Printf("Last line number is %d \n", lineNum)
		os.Exit(1)
	}()

	syncIdToServer(idFile)

	fmt.Println("END")
}

func syncIdToServer(idFile string) {
	// Load a csv file.
	f, _ := os.Open(idFile)

	rate := time.Second / time.Duration(qps)
	throttle := time.Tick(rate)

	// Create a new reader.
	r := csv.NewReader(f)

	for {
		record, err := r.Read()
		// Stop at EOF.
		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		lineNum++
		if lineNum < from {
			continue
		}

		if len(record) != 2 {
			log.Println("parse line error")
			continue
		}

		idfa := record[0]
		ip := record[1]

		reqUrl := fmt.Sprintf(URL, campaign, cid, idfa, ip)
		if lineNum == from {
			fmt.Printf("Skip  %d line and start to send http to url %s \n", lineNum, reqUrl)
		}

		<-throttle // rate limit our Service.Method RPCs
		resp, err := http.Get(reqUrl)
		if err != nil {
			fmt.Printf("Send to url = %v ERROR=%v \n", reqUrl, err.Error())
		}

		//fmt.Println(time.Now().Format("2006-01-02 15:04:05"))

		defer resp.Body.Close()

	}
}
