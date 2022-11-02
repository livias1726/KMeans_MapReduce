package main

import (
	"KMeanMR/utils"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

const (
	debug        = true
	network      = "tcp"
	addressLocal = "localhost:5678"
)

type MapArgs struct {
	Centroids utils.Points
	Points    utils.Points
}

type MapResp struct {
	Clusters utils.Clusters
}

type Worker int

// Map -> classify /*-------------------------- REMOTE PROCEDURE - MASTER SIDE ---------------------------------------*/
func (w *Worker) Map(payload []byte, result *[]byte) error {
	var inArgs MapArgs

	// Unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 41)
	if debug {
		log.Printf("Received: %v", string(payload))
		//log.Printf("Unmarshal: Name: %s, Content: %s, Regex: %s", inArgs.File.Name, inArgs.File.Content, inArgs.Regex)
	}

	//map
	var mapRes MapResp
	centroids := inArgs.Centroids
	chunk := inArgs.Points

	mapRes.Clusters = make(utils.Clusters, len(centroids))
	for i, centroid := range centroids {
		mapRes.Clusters[i].Centroid = centroid
	}

	var idx int
	for _, point := range chunk {
		idx = classify(centroids, point)
		mapRes.Clusters[idx].Points = append(mapRes.Clusters[idx].Points, point)
	}

	log.Printf("--> Working on map.\n")

	// Marshalling
	s, err := json.Marshal(&mapRes)
	errorHandler(err, 50)
	if debug {
		log.Printf("MapRes: %v", mapRes)
		log.Printf("Marshaled Data: %s", s)
	}

	//return
	*result = s
	return nil
}

// Reduce -> recenter /*---------------------------------- REMOTE PROCEDURE - MASTER SIDE ----------------------------*/
func (w *Worker) Reduce(payload []byte, result *[]byte) error {
	log.Println("--> Working on reduce")
	*result = payload
	return nil
}

/*------------------------------------------------------- MAIN -------------------------------------------------------*/
func main() {
	worker := new(Worker)
	// Publish the receiver methods
	err := rpc.Register(worker)
	errorHandler(err, 63)

	// Register a HTTP handler
	rpc.HandleHTTP()
	//Listen to TCP connections on port 5678
	listener, err := net.Listen(network, addressLocal)
	errorHandler(err, 69)
	log.Printf("Serving RPC server on port %d", 5678)

	err = http.Serve(listener, nil)
	errorHandler(err, 73)
}

/*------------------------------------------------------ LOCAL FUNCTIONS ---------------------------------------------*/
// Returns the index of the centroid the point is closer to
func classify(centroids utils.Points, point utils.Point) int {
	var idx int
	var tempDist float64

	dist := 0.0
	for i, centroid := range centroids {
		tempDist = utils.GetDistance(point.Coordinates, centroid.Coordinates)
		if dist == 0.0 || dist > tempDist {
			dist = tempDist
			idx = i
		}
	}

	return idx
}

// error handling
func errorHandler(err error, line int) {
	if err != nil {
		log.Fatalf("failure at line %d: %v", line, err)
	}
}
