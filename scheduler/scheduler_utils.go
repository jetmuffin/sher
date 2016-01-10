package scheduler

import (
	"bufio"
	"os"

	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
)

func getOfferScalar(offer *mesos.Offer, name string) float64 {
	resources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
		return res.GetName() == name
	})

	value := 0.0
	for _, res := range resources {
		value += res.GetScalar().GetValue()
	}

	return value
}

func logOffers(offers []*mesos.Offer) {
	for _, offer := range offers {
		log.Infof("Received Offer <%v> with cpus=%v mem=%v", offer.Id.GetValue(), getOfferScalar(offer, "cpus"), getOfferScalar(offer, "mem"))
	}
}

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}
