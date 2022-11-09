package main

import (
	"KMeans_MapReduce/utils"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Worker int

// MapInput : valid for both map services
// --> list of current centroids to use
// --> chunk of dataset points to process
type MapInput struct {
	Centroids utils.Points
	//TODO: see if you can avoid resending these at every iteration --> do an initial iteration of points transmission!
	Points utils.Points
}

// InitMapOutput : used as output for the initialization map phase
// --> chunk of dataset points to process
// --> minimum distances of each point from each centroid
type InitMapOutput struct {
	//TODO: see if you can avoid resending the points which are the same as the input
	Points       utils.Points
	MinDistances []float64
}

const (
	debug        = true
	network      = "tcp"
	addressLocal = "localhost:5678"
)

// InitMap
// --> input: set of current chosen initial centroids (from 1 to k-1) and chunk of points to process --> (mu, x)
// --> output: chunk of points processed and their minimum distance from the set of centroids
func (w *Worker) InitMap(payload []byte, result *[]byte) error {
	var inArgs MapInput

	// unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 34)
	if debug {
		log.Printf("--> unmarshalled %d centroids and %d points to cluster",
			len(inArgs.Centroids), len(inArgs.Points))
	}

	initMapOutput := computeMinDistances(inArgs)

	//TODO: use a combiner to get the only the point with maximum distance from each centroid

	// marshalling
	s, err := json.Marshal(&initMapOutput)
	errorHandler(err, 50)

	if debug {
		log.Printf("--> init-mapper returning.\n")
	}

	//return
	*result = s
	return nil
}

// InitReduce --> TODO: bottleneck
// --> input : complete set of points and their minimum distance from the current set of centroids
// --> output: new centroid
func (w *Worker) InitReduce(payload []byte, result *[]byte) error {
	var inArgs InitMapOutput

	// Unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 72)
	if debug {
		log.Printf("--> unmarshalled %d points to choose the centroid from",
			len(inArgs.Points))
	}

	redRes := getFartherPoint(inArgs)

	// Marshalling
	s, err := json.Marshal(&redRes)
	errorHandler(err, 50)

	if debug {
		log.Printf("--> init-reducer returning.\n")
	}

	//return
	*result = s
	return nil
}

// Map -> classify /*-------------------------- REMOTE PROCEDURE - MASTER SIDE ---------------------------------------*/
func (w *Worker) Map(payload []byte, result *[]byte) error {
	var inArgs MapInput

	// unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 34)
	centroids := inArgs.Centroids
	points := inArgs.Points

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

		point.From = point.To
		point.To = idx
		mapRes[idx].Points = append(mapRes[idx].Points, point)
	}

	// Marshalling
	s, err := json.Marshal(&mapRes)
	errorHandler(err, 50)

	log.Printf("--> mapper returning.\n")
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
	var redRes utils.Point
	if len(inArgs.Points) == 0 {
		redRes = inArgs.Centroid
	} else {
		redRes = recenter(inArgs.Points)
	}
	log.Printf("--> Finished local Reduce.\n")

	// Marshalling
	s, err := json.Marshal(&redRes)
	errorHandler(err, 50)

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
	if debug {
		log.Print("--> worker node is online.\n")
	}

	//Listen to TCP connections on port 5678
	listener, err := net.Listen(network, addressLocal)
	errorHandler(err, 69)
	log.Printf("Serving RPC server on port %d", 5678)

	err = http.Serve(listener, nil)
	errorHandler(err, 73)
}

/*------------------------------------------------------ LOCAL FUNCTIONS ---------------------------------------------*/
func computeMinDistances(inArgs MapInput) InitMapOutput {
	var mapOut InitMapOutput

	for _, point := range inArgs.Points {
		// compute the distance between each point and each centroid
		var dist float64
		for i, centroid := range inArgs.Centroids {
			tempDist := utils.GetDistance(point.Coordinates, centroid.Coordinates)
			if i == 0 || dist > tempDist {
				dist = tempDist
			}
		}
		// store the point and its minimum distance from the centroids
		mapOut.Points = append(mapOut.Points, point)
		mapOut.MinDistances = append(mapOut.MinDistances, dist)
	}

	return mapOut
}

// return the farther point wrt the centroids
func getFartherPoint(inArgs InitMapOutput) utils.Point {

	var minDist float64
	var index int

	for i, d := range inArgs.MinDistances {
		if i == 0 || minDist > d {
			minDist = d
			index = i
		}
	}

	return inArgs.Points[index]
}

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
