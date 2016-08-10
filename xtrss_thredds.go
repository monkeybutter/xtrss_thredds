package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

const (
	DB_HOST     = "130.56.243.194"
	DB_USER     = "metadata"
	DB_PASSWORD = "WdrjGgj9AQNC"
	DB_NAME     = "metadata"
	maxExtent   = 11000
)

type Params struct {
	Host     string
	NReqs    int
	Interval int
	Extent   int
	Bin      bool
	Verbose  bool
}

var units map[int]string = map[int]string{0: "B", 1: "kB", 2: "MB", 3: "TB", 4: "PB"}

func getHumanSize(size uint64) string {
	i := 0
	for size > 1024 {
		size = size >> 10
		i += 1
	}

	return fmt.Sprintf("%d %s", size, units[i])
}

var dapBin map[bool]string = map[bool]string{true: "dods", false: "ascii"}

func readURL(buf chan string, totalBytes, totalReqs *uint64) {

	for url := range buf {
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Error doing GET: %s\n", err)
			continue
		}

		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Error reading HTTP body: %s\n", err)
			continue
		}
		*totalBytes += uint64(len(body))
		*totalReqs += 1
	}
}

func FileName2DAP(fileName string, params Params, r *rand.Rand) string {

	baseDAP := "http://" + params.Host + "/thredds/dodsC/rr5/satellite/obs/himawari8/%s/%s/%s/%s/%s/%s.%s?channel_00%s_brf[0:1:0][%d:1:%d][%d:1:%d]"
	xi := 0
	yi := 0

	if params.Extent < maxExtent {
		xi = r.Intn(maxExtent - params.Extent)
		yi = r.Intn(maxExtent - params.Extent)
	}

	parts := strings.Split(fileName, "/")

	return fmt.Sprintf(baseDAP, parts[7], parts[8], parts[9], parts[10], parts[11], parts[12], dapBin[params.Bin], parts[12][29:31], xi, xi+params.Extent-1, yi, yi+params.Extent-1)
}

func LoadBuffer(buf chan string, params Params, closeChan bool, r *rand.Rand, db *sql.DB) {
	if closeChan {
		defer close(buf)
	}

	rows, err := db.Query(`SELECT fi_parent || '/' || fi_name FROM files_rr5 WHERE fi_parent LIKE '/g/data2/rr5/satellite/obs/himawari8/FLDK/201%' AND fi_name LIKE '%P1S-ABOM_BRF_B01-PRJ_GEOS141_1000-HIMAWARI8-AHI.nc' ORDER BY random() LIMIT $1;`, params.NReqs)

	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var fileName string
		err = rows.Scan(&fileName)
		if err != nil {
			log.Fatal(err)
		}
		buf <- FileName2DAP(fileName, params, r)
	}
}

func StreamBuffer(buf chan string, params Params, r *rand.Rand, db *sql.DB) {
	for range time.Tick(time.Duration(params.Interval) * time.Second) {
		go LoadBuffer(buf, params, false, r, db)
	}
}

func GetStats(statsInterval int, totalBytes, totalReqs *uint64) {
	prevTotalBytes := uint64(0)
	prevTotalReqs := uint64(0)
	for range time.Tick(time.Duration(statsInterval) * time.Second) {
		fmt.Printf("Throughput: %s/s | Requests Received: %d | Total Data Received: %s\n", getHumanSize((*totalBytes-prevTotalBytes)/uint64(statsInterval)), *totalReqs-prevTotalReqs, getHumanSize(*totalBytes))
		prevTotalBytes = *totalBytes
		prevTotalReqs = *totalReqs
	}
}

func main() {

	host := flag.String("h", "dapds03.nci.org.au", "Thredds host ip.")
	extent := flag.Int("ext", maxExtent, "Extent of the request.")
	nReqs := flag.Int("n", 1, "Number of concurrent requests submitted to the server.")
	interval := flag.Int("i", 0, "Interval in secods between groups of requests.")
	bin := flag.Bool("b", true, "Binary/Text Response from THREDDS.")
	verbose := flag.Bool("v", false, "Verbose mode for debuging.")

	flag.Parse()

	params := Params{Host: *host, Extent: *extent, NReqs: *nReqs, Interval: *interval, Bin: *bin, Verbose: *verbose}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	if params.Verbose {
		fmt.Println("----------------DEBUG----------------")
		fmt.Printf("Thredds Server: %s\n", *host)
		fmt.Println("--------------------------------------")
	}

	dbinfo := fmt.Sprintf("host= %s user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)
	db, err := sql.Open("postgres", dbinfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	stream := make(chan string, params.NReqs)

	if *interval > 0 {
		go StreamBuffer(stream, params, r, db)
	} else {
		go LoadBuffer(stream, params, true, r, db)
	}

	totalBytes := uint64(0)
	totalReqs := uint64(0)

	go GetStats(1, &totalBytes, &totalReqs)
	readURL(stream, &totalBytes, &totalReqs)
	fmt.Printf("END | Requests Received: %d | Total Data Received: %s\n", totalReqs, getHumanSize(totalBytes))
}
