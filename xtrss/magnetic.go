package xtrss

import (
	"fmt"
	"time"
	"math/rand"
)

const (
	maxXExtent = 50591
	maxYExtent = 41883
)

func MagneticDAP(params Params, r *rand.Rand) string {
	baseDAP := "http://" + params.Host + "/thredds/dodsC/rr2/National_Coverages/magmap_v6_2015/magmap_v6_2015.nc.dods?mag_tmi_anomaly.mag_tmi_anomaly[%d:1:%d][%d:1:%d]"
	xi := 0
	yi := 0

	if params.Extent < maxXExtent {
		xi = r.Intn(maxXExtent - params.Extent)
	}

	if params.Extent < maxYExtent {
		yi = r.Intn(maxYExtent - params.Extent)
	}

	return fmt.Sprintf(baseDAP, yi, yi+params.Extent-1, xi, xi+params.Extent-1)
}

func GetMagneticStream(params Params) (chan string, error) {

	stream := make(chan string, params.NReqs)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	
	go func() {
		for range time.Tick(time.Duration(params.Interval) * time.Second) {
			for i:=0; i<params.NReqs; i++ {
				stream <- MagneticDAP(params, r)
			}
		}
	}()

	return stream, nil
}
