package main

import (
	"fmt"
	"log"
	"flag"
	"time"
	"sync"
	"strings"
	"io/ioutil"
	"net/http"
	"database/sql"
	_ "github.com/lib/pq"
	"math/rand"
)

const (
    DB_HOST     = "130.56.243.194"
    DB_USER     = "metadata"
    DB_PASSWORD = "WdrjGgj9AQNC"
    DB_NAME     = "metadata"
    maxExtent   = 11000
)

var units map[int]string = map[int]string{0:"B", 1:"kB", 2:"MB", 3:"TB", 4:"PB"}

func getHumanSize(size uint64) string {
	i := 0
	for size > 1024 {
		size = size>>10
		i += 1
	}

	return fmt.Sprintf("%d %s", size, units[i])
}


func getFiles(n int) []string {
	files := []string{}

	dbinfo := fmt.Sprintf("host= %s user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)
	db, err := sql.Open("postgres", dbinfo)
	if err != nil {
		log.Fatal(err)
		fmt.Println(err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT fi_parent || '/' || fi_name FROM files_rr5 WHERE fi_parent LIKE '/g/data2/rr5/satellite/obs/himawari8/FLDK/201%' AND fi_name LIKE '%P1S-ABOM_BRF_B01-PRJ_GEOS141_1000-HIMAWARI8-AHI.nc' ORDER BY random() LIMIT $1;`, n)
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var path string
		err = rows.Scan(&path)
		if err != nil {
			log.Fatal(err)
		}
		files = append(files, path)
	}

	return files
}

var dapBin map[bool]string = map[bool]string{true: "dods", false: "ascii"}

func getURLs(host string, n, extent int, bin bool, r *rand.Rand) []string {

	xi := 0
       	yi := 0

	if extent < maxExtent {
	        xi = r.Intn(maxExtent-extent)
       		yi = r.Intn(maxExtent-extent)
	}

	baseDAP := "http://" + host + "/thredds/dodsC/rr5/satellite/obs/himawari8/%s/%s/%s/%s/%s/%s.%s?channel_00%s_brf[0:1:0][%d:1:%d][%d:1:%d]"
	res := getFiles(n)
	out := []string{}
	for _, fileName := range(res) {
		parts := strings.Split(fileName, "/")
		out = append(out, fmt.Sprintf(baseDAP, parts[7], parts[8], parts[9], parts[10], parts[11], parts[12], dapBin[bin], parts[12][29:31], xi, xi+extent-1, yi, yi+extent-1))
	}
	return out
}

func readURL(url string, total *TotalRead, wg *sync.WaitGroup) {
	defer wg.Done()
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("Error doing GET", err)
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading Body", err)
		return
	}
	total.Lock()
	total.Bytes += uint64(len(body))
	total.Unlock()
}

type TotalRead struct {
	*sync.Mutex
	Bytes uint64
}

func main() {

	host := flag.String("h", "130.56.243.208", "Thredds host ip.")
	extent := flag.Int("ext", maxExtent, "Extent of the request.")
	nReqs := flag.Int("n", 1, "Number of concurrent requests submitted to the server.")
	bin := flag.Bool("b", true, "Binary/Text Response from THREDDS.")
	verbose := flag.Bool("v", false, "Verbose mode for debuging.")

	flag.Parse()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	
	urls := getURLs(*host, *nReqs, *extent, *bin, r)

	if *verbose {
		fmt.Println("----------------DEBUG----------------")
		fmt.Printf("Thredds Server: %s\n", *host)
		fmt.Println("GET Requests:", urls)
		fmt.Println("--------------------------------------")
	}

	var wg sync.WaitGroup
	totalRead := TotalRead{&sync.Mutex{}, 0}

	start := time.Now()
	for _, url := range(urls) {
		wg.Add(1)
		go readURL(url, &totalRead,  &wg)
	}
	
	wg.Wait()
	lapse := time.Since(start).Seconds()
	fmt.Printf("Time: %f seconds\n", lapse)
	fmt.Printf("Data: %d x %s\n", *nReqs, getHumanSize(totalRead.Bytes/uint64(*nReqs)))
	fmt.Printf("Throughput: %s/s\n", getHumanSize(uint64(float64(totalRead.Bytes)/lapse)))
}
