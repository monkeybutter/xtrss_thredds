package main

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	DB_HOST     = "130.56.243.194"
	DB_USER     = "metadata"
	DB_PASSWORD = "WdrjGgj9AQNC"
	DB_NAME     = "metadata"
	maxExtent   = 11000
)

func LoadBuffer(buf chan string, n int, db *sql.DB) {

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
		buf <- path
	}

	return files
}

func main() {

	dbinfo := fmt.Sprintf("host= %s user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)
	db, err := sql.Open("postgres", dbinfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	stream := make(chan string, 20)

	go func() {
		for range time.Tick(1 * time.Second) {
			go LoadBuffer(stream, 10, db)
		}
	}()

	for n := range stream {
		fmt.Printf("Received %d\n", n)
	}
}
