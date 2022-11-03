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
	debug        = false
	network      = "tcp"
	addressLocal = "localhost:5678"
)

type MapArgs struct {
	Centroids utils.Points
	Points    utils.Points
}

type Worker int

// Map -> classify /*-------------------------- REMOTE PROCEDURE - MASTER SIDE ---------------------------------------*/
func (w *Worker) Map(payload []byte, result *[]byte) error {
	var inArgs MapArgs

	// Unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 34)
	if debug {
		log.Printf("Unmarshalled. %d centroids; %d points to cluster", len(inArgs.Centroids), len(inArgs.Points))
	}
	centroids := inArgs.Centroids
	points := inArgs.Points

	log.Printf("--> Starting local Map.\n")
	var mapRes utils.Clusters
	// prepare clusters with initial centroids
	mapRes = make(utils.Clusters, len(centroids))
	for i, centroid := range centroids {
		mapRes[i].Centroid = centroid
	}
	// classify each given point to a cluster
	var idx int
	for _, point := range points {
		idx = classify(centroids, point)
		mapRes[idx].Points = append(mapRes[idx].Points, point)
	}
	log.Printf("--> Finished local Map.\n")

	// Marshalling
	s, err := json.Marshal(&mapRes)
	errorHandler(err, 50)
	if debug {
		log.Printf("Map result: %v", mapRes)
		log.Printf("Marshalled data: %s", s)
	}

	log.Printf("--> Mapper returning.\n")
	//return
	*result = s
	return nil
}

// Reduce -> recenter /*---------------------------------- REMOTE PROCEDURE - MASTER SIDE ----------------------------*/
func (w *Worker) Reduce(payload []byte, result *[]byte) error {
	var inArgs utils.Cluster

	// Unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 72)
	if debug {
		log.Printf("Unmarshalled cluster with %d points to recenter", len(inArgs.Points))
	}

	log.Printf("--> Starting local Reduce.\n")
	redRes := recenter(inArgs.Points)
	log.Printf("--> Finished local Reduce.\n")

	// Marshalling
	s, err := json.Marshal(&redRes)
	errorHandler(err, 50)
	if debug {
		log.Printf("Reduce result: %v", redRes)
	}

	log.Printf("--> Reducer returning.\n")
	//return
	*result = s
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

// recenter the given cluster
func recenter(points utils.Points) utils.Point {
	var centroid utils.Point
	centroid.Coordinates = make([]float64, len(points[0].Coordinates))

	for _, point := range points {
		for j := 0; j < len(point.Coordinates); j++ {
			centroid.Coordinates[j] += point.Coordinates[j]
		}
	}

	for i := 0; i < len(centroid.Coordinates); i++ {
		centroid.Coordinates[i] = centroid.Coordinates[i] / float64(len(points))
	}

	return centroid
}

// error handling
func errorHandler(err error, line int) {
	if err != nil {
		log.Fatalf("failure at line %d: %v", line, err)
	}
}
