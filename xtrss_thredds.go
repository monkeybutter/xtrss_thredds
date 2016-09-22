package main

import (
	"./xtrss"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func readURL(buf chan string, totalBytes, totalReqs *xtrss.ConcCounter, verbose bool) {

	for url := range buf {
		if verbose {
			fmt.Println(url)
		}
		go func() {
			resp, err := http.Get(url)
			if err != nil {
				log.Printf("Error doing GET: %s\n", err)
				return
			}

			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("Error reading HTTP body: %s\n", err)
				return
			}

			totalBytes.Add(len(body))
			totalReqs.Add(1)
		}()
	}
}

func PrintStats(statsInterval int, totalBytes, totalReqs *xtrss.ConcCounter) {
	prevTotalBytes := uint64(0)
	prevTotalReqs := uint64(0)
	for range time.Tick(time.Duration(statsInterval) * time.Second) {
		fmt.Printf("Throughput: %s/s | Requests Received: %d | Total Data Received: %s\n", xtrss.GetHumanSize((totalBytes.Total-prevTotalBytes)/uint64(statsInterval)), totalReqs.Total-prevTotalReqs, xtrss.GetHumanSize(totalBytes.Total))
		prevTotalBytes = totalBytes.Total
		prevTotalReqs = totalReqs.Total
	}
}

func main() {
	host := flag.String("h", "dapds03.nci.org.au", "Thredds host ip.")
	data := flag.String("data", "himawari", "Dataset used to test THREDDS.")
	extent := flag.Int("ext", 1, "Extent of the request.")
	nReqs := flag.Int("n", 1, "Number of concurrent requests submitted to the server.")
	interval := flag.Int("i", 5, "Interval in secods between groups of requests.")
	bin := flag.Bool("b", true, "Binary/Text Response from THREDDS.")
	verbose := flag.Bool("v", false, "Verbose mode for debuging.")

	flag.Parse()

	params := xtrss.Params{Host: *host, Extent: *extent, NReqs: *nReqs, Interval: *interval, Bin: *bin, Verbose: *verbose}

	if params.Verbose {
		fmt.Println("----------------DEBUG----------------")
		fmt.Printf("Thredds Server: %s\n", *host)
		fmt.Printf("Parameters %v\n", params)
		fmt.Println("--------------------------------------")
	}

	var stream chan string	
	switch *data {
	case "himawari":
		stream, _ = xtrss.GetHimStream(params)
	case "magnetic":
		stream, _ = xtrss.GetMagneticStream(params)
	default:
		fmt.Println("-dataset is either 'himawari' or 'magnetic' at the moment")
		return
	}

	totalBytes := xtrss.NewCounter()
	totalReqs := xtrss.NewCounter()

	go PrintStats(params.Interval, &totalBytes, &totalReqs)
	readURL(stream, &totalBytes, &totalReqs, *verbose)
	fmt.Printf("END | Requests Received: %d | Total Data Received: %s\n", totalReqs, xtrss.GetHumanSize(totalBytes.Total))
}
