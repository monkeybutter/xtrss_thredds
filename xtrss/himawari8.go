package xtrss

import (
	"fmt"
	"strings"
	"time"
	"log"
	"math/rand"
	"database/sql"
	_ "github.com/lib/pq"
)

const (
	DB_HOST     = "130.56.243.194"
	DB_USER     = "metadata"
	DB_PASSWORD = "WdrjGgj9AQNC"
	DB_NAME     = "metadata"
	maxExtent = 11000
)

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


func GetHimStream(params Params) (chan string, error) {

	stream := make(chan string, params.NReqs)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	
	go func() {
		dbinfo := fmt.Sprintf("host= %s user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)
		db, err := sql.Open("postgres", dbinfo)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		for range time.Tick(time.Duration(params.Interval) * time.Second) {
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
				stream <- FileName2DAP(fileName, params, r)
			}
		}
	}()

	return stream, nil
}
