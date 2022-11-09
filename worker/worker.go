package main

import (
	"KMeans_MapReduce/utils"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Worker struct {
	Id       int
	Combiner Combiner
}

type Combiner struct {
	Id       int
	Combined InitMapOutput
}

// MapInput : valid for both map services
// --> list of current centroids to use
// --> chunk of dataset points to process
type MapInput struct {
	Centroids utils.Points
	Points    utils.Points
	Last      bool
}

// InitMapOutput : used as output for the initialization map phase
// --> chunk of dataset points to process
// --> minimum distances of each point from each centroid
type InitMapOutput struct {
	Points       utils.Points //TODO: see if you can avoid resending the points which are the same as the input
	MinDistances []float64
}

const (
	debug        = false
	network      = "tcp"
	addressLocal = "localhost:5678"
)

// InitMap /*-------------------------- REMOTE PROCEDURE - MASTER SIDE ---------------------------------------*/
func (w *Worker) InitMap(payload []byte, result *[]byte) error {
	var inArgs MapInput

	// unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 34)
	if debug {
		log.Printf("--> unmarshalled %d centroids and %d points to cluster",
			len(inArgs.Centroids), len(inArgs.Points))
	}

	initMapOutput := new(InitMapOutput)
	go computeMinDistances(inArgs, initMapOutput, w.Combiner)

	if !inArgs.Last {
		s, err := json.Marshal(true)
		errorHandler(err, 64)
		if debug {
			log.Print("--> master responding with ack.\n")
		}
		*result = s
		return err
	}

	*initMapOutput = w.Combiner.Combined

	// marshalling
	s, err := json.Marshal(&initMapOutput)
	errorHandler(err, 50)
	if debug {
		log.Printf("Map result: %v", initMapOutput)
		log.Printf("Marshalled data: %s", s)
	}

	log.Printf("--> Init-Mapper returning.\n")
	//return
	*result = s
	return nil
}

// InitCombine : gets only the point with max distance from each chunk received
func (c *Combiner) InitCombine(minDist *InitMapOutput) {
	idx := getFartherPoint(*minDist)
	c.Combined.Points = append(c.Combined.Points, minDist.Points[idx])
	c.Combined.MinDistances = append(c.Combined.MinDistances, minDist.MinDistances[idx])
}

// InitReduce /*---------------------------------- REMOTE PROCEDURE - MASTER SIDE ----------------------------*/
func (w *Worker) InitReduce(payload []byte, result *[]byte) error {
	var inArgs InitMapOutput

	// Unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 72)

	redRes := getFartherPoint(inArgs)

	// Marshalling
	s, err := json.Marshal(&redRes)
	errorHandler(err, 50)
	if debug {
		log.Printf("Reduce result: %v", redRes)
	}

	log.Printf("--> Init-Reducer returning.\n")
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

	log.Printf("Starting local Map.\n")
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
	log.Printf("Finished local Map.\n")

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
func computeMinDistances(inArgs MapInput, outArgs *InitMapOutput, combiner Combiner) {
	for _, point := range inArgs.Points {
		// compute the distance between each point and each centroid
		var dist float64
		for i, centroid := range inArgs.Centroids {
			tempDist := utils.GetDistance(point.Coordinates, centroid.Coordinates)
			if i == 0 || dist > tempDist {
				dist = tempDist
			}
		}

		outArgs.Points = append(outArgs.Points, point)
		outArgs.MinDistances = append(outArgs.MinDistances, dist)
	}

	combiner.InitCombine(outArgs)
}

// return the farther point wrt the centroids
func getFartherPoint(inArgs InitMapOutput) int {

	var minDist float64
	var index int

	for i, d := range inArgs.MinDistances {
		if i == 0 || minDist > d {
			minDist = d
			index = i
		}
	}

	return index
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
