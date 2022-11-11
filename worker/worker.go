package main

import (
	"KMeans_MapReduce/utils"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Mapper struct {
	Centroids utils.Points
	Chunks    []utils.Points
	Dim       int
}

type Worker struct {
	Mappers [maxNodes]Mapper
}

type Combiner struct {
	MapOut     utils.Clusters
	InitMapOut InitMapOutput
}

// MapInput : valid for both map services
// --> list of current centroids to use
// --> chunk of dataset points to process
type MapInput struct {
	Id        int
	Centroids utils.Points
	Points    utils.Points
	NewPoints bool
	First     bool
	Last      bool
}

// InitMapOutput : used as output for the initialization map phase
// --> chunk of dataset points to process
// --> minimum distances of each point from each centroid
type InitMapOutput struct {
	Points       utils.Points
	MinDistances []float64
}

const (
	debug        = true
	network      = "tcp"
	addressLocal = "localhost:5678"
	maxNodes     = 10
)

// InitMap
// --> input: set of current chosen initial centroids (from 1 to k-1) and chunk of points to process --> (mu, x)
// --> output: chunk of points processed and their minimum distance from the set of centroids
func (w *Worker) InitMap(payload []byte, result *[]byte) error {
	// unmarshalling
	var inArgs MapInput
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 34)

	// select mapper
	idx := inArgs.Id
	mapper := &w.Mappers[idx]

	// store data
	if inArgs.First {
		mapper.Centroids = inArgs.Centroids
	}

	if inArgs.NewPoints {
		mapper.Chunks = append(mapper.Chunks, inArgs.Points)
		mapper.Dim += len(inArgs.Points)
	}

	// send ack
	if !inArgs.Last {
		s, err := json.Marshal(true)
		errorHandler(err, 64)
		*result = s
		return err
	}

	if debug {
		log.Printf("--> MAPPER %d: received %d centroid(s) and %d chunk(s) [%d points]",
			idx, len(mapper.Centroids), len(mapper.Chunks), mapper.Dim)
	}

	comb := new(Combiner)
	for i := 0; i < len(mapper.Chunks); i++ {
		// map
		initMapOutput := computeMinDistances(mapper.Chunks[i], mapper.Centroids)
		// combine
		comb.initCombine(initMapOutput)
	}

	// marshalling
	s, err := json.Marshal(comb.InitMapOut)
	errorHandler(err, 50)

	// return
	*result = s
	return nil
}

// InitReduce
// --> input : complete set of points and their minimum distance from the current set of centroids
// --> output: new centroid
func (w *Worker) InitReduce(payload []byte, result *[]byte) error {
	var inArgs InitMapOutput
	var redRes utils.Point

	// unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 72)
	if debug {
		log.Printf("--> REDUCER: received %d points to choose the centroid from",
			len(inArgs.Points))
	}

	if len(inArgs.Points) == 1 {
		redRes = inArgs.Points[0]
	} else {
		redRes, _ = getFartherPoint(inArgs)
	}

	// marshalling
	s, err := json.Marshal(&redRes)
	errorHandler(err, 50)

	// return
	*result = s
	return nil
}

// Map -> classify /*-------------------------- REMOTE PROCEDURE - MASTER SIDE ---------------------------------------*/
func (w *Worker) Map(payload []byte, result *[]byte) error {
	var inArgs MapInput

	// unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 34)

	// select mapper
	id := inArgs.Id
	mapper := &w.Mappers[id]
	mapper.Centroids = inArgs.Centroids

	var mapRes utils.Clusters
	// prepare clusters with initial centroids
	mapRes = make(utils.Clusters, len(mapper.Centroids))
	for i, centroid := range mapper.Centroids {
		mapRes[i].Centroid = centroid
	}
	// classify each given point to a cluster
	var idx int
	//comb := new(Combiner)
	for _, points := range mapper.Chunks {
		// map
		for _, point := range points {
			idx = classify(mapper.Centroids, point)
			point.From = point.To
			point.To = idx
			mapRes[idx].Points = append(mapRes[idx].Points, point)
		}
		// combine
		//comb.combine(mapRes)
	}

	// Marshalling
	s, err := json.Marshal(&mapRes)
	errorHandler(err, 50)

	log.Printf("--> mapper [%d] returning.\n", id)
	// return
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

// Performs a combine phase for a local map output before giving the result to the master
func (c *Combiner) initCombine(inArgs InitMapOutput) {
	combRes, dist := getFartherPoint(inArgs)

	c.InitMapOut.Points = append(c.InitMapOut.Points, combRes)
	c.InitMapOut.MinDistances = append(c.InitMapOut.MinDistances, dist)
}

/*------------------------------------------------------- MAIN -------------------------------------------------------*/
func main() {
	worker := new(Worker)
	// Publish the receiver methods
	err := rpc.Register(worker)
	errorHandler(err, 214)

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
func computeMinDistances(points utils.Points, centroids utils.Points) InitMapOutput {
	var mapOut InitMapOutput

	for _, point := range points {
		// compute the distance between each point and each centroid
		var dist float64
		for i, centroid := range centroids {
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
func getFartherPoint(inArgs InitMapOutput) (utils.Point, float64) {

	var maxDist float64
	var index int

	for i, d := range inArgs.MinDistances {
		if i == 0 || maxDist < d {
			maxDist = d
			index = i
		}
	}

	return inArgs.Points[index], maxDist
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
