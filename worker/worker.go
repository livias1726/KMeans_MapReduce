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
	InitMapOut InitMapOutput
	MapOut     MapOutput
}

type InitMapInput struct {
	MapperId  int
	First     bool
	Centroids utils.Points
	NewPoints bool
	Chunk     utils.Points
	Last      bool
}

type InitMapOutput struct {
	Points       utils.Points
	MinDistances []float64
}

type MapInput struct {
	MapperId  int
	Centroids utils.Points
}

type MapOutput struct {
	ClusterId []int
	Points    []utils.Points
	Len       []int
}

type ReduceInput struct {
	ClusterId int
	Points    utils.Points
	Len       int
}

type ReduceOutput struct {
	ClusterId int
	Point     utils.Point
	Len       int
}

const (
	debug    = true
	network  = "tcp"
	address  = "worker:11091"
	maxNodes = 10
)

// InitMap
// --> input: set of current chosen initial centroids (from 1 to k-1) and chunk of points to process --> (mu, x)
// --> output: chunk of points processed and their minimum distance from the set of centroids
func (w *Worker) InitMap(payload []byte, result *[]byte) error {
	// unmarshalling
	var inArgs InitMapInput
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 72)

	// select mapper
	idx := inArgs.MapperId
	mapper := &w.Mappers[idx]

	// store data
	if inArgs.First {
		mapper.Centroids = inArgs.Centroids
	}
	if inArgs.NewPoints {
		mapper.Chunks = append(mapper.Chunks, inArgs.Chunk)
		mapper.Dim += len(inArgs.Chunk)
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
	for _, chunk := range mapper.Chunks {
		// map
		localOutput := computeMinDistances(chunk, mapper.Centroids)
		// combine
		comb.initCombine(localOutput)
	}

	// marshalling
	s, err := json.Marshal(comb.InitMapOut)
	errorHandler(err, 50)

	// return
	*result = s
	return nil
}

// Performs a combine phase for a local map output before giving the result to the master
func (c *Combiner) initCombine(inArgs InitMapOutput) {
	combRes, dist := getFartherPoint(inArgs)

	c.InitMapOut.Points = append(c.InitMapOut.Points, combRes)
	c.InitMapOut.MinDistances = append(c.InitMapOut.MinDistances, dist)
}

// InitReduce
// --> input : set of points and their minimum distance from the current set of centroids
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
	id := inArgs.MapperId
	mapper := &w.Mappers[id]
	mapper.Centroids = inArgs.Centroids

	// classify each given point to a cluster
	//dim := len(mapper.Centroids)
	comb := new(Combiner)
	for _, chunk := range mapper.Chunks {
		var clusterId []int
		var length []int
		var points utils.Points
		// map
		for _, point := range chunk {
			idx, _ := classify(mapper.Centroids, point)

			clusterId = append(clusterId, idx)
			points = append(points, point)
			length = append(length, 1)
		}
		// combine
		comb.combine(clusterId, points, length)
	}

	// marshalling
	s, err := json.Marshal(&comb.MapOut)
	errorHandler(err, 202)

	// return
	*result = s
	return nil
}

// computes the partial sum of each point in each partial cluster obtained from mapper
func (c *Combiner) combine(clusterId []int, points utils.Points, length []int) {
	//combine
	for i, cid := range clusterId {
		isIn, idx := isIn(cid, c.MapOut.ClusterId)
		if !isIn {
			c.MapOut.ClusterId = append(c.MapOut.ClusterId, cid)
			idx = len(c.MapOut.ClusterId) - 1

			newPoints := new(utils.Points)
			c.MapOut.Points = append(c.MapOut.Points, *newPoints)

			newLength := 0
			c.MapOut.Len = append(c.MapOut.Len, newLength)
		}

		c.MapOut.Points[idx] = append(c.MapOut.Points[idx], points[i])
		c.MapOut.Len[idx] += length[i]
	}
}

func isIn(cid int, id []int) (bool, int) {
	for idx, i := range id {
		if cid == i {
			return true, idx
		}
	}
	return false, -1
}

// Reduce -> recenter /*---------------------------------- REMOTE PROCEDURE - MASTER SIDE ----------------------------*/
func (w *Worker) Reduce(payload []byte, result *[]byte) error {
	var inArgs ReduceInput

	// unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 72)
	if debug {
		log.Printf("REDUCER [%d]: received %d points to recenter.", inArgs.ClusterId, len(inArgs.Points))
	}

	var redRes ReduceOutput
	redRes.ClusterId = inArgs.ClusterId
	redRes.Point = recenter(inArgs.Points, len(inArgs.Points[0].Coordinates))
	redRes.Len = inArgs.Len

	// marshalling
	s, err := json.Marshal(&redRes)
	errorHandler(err, 50)

	//return
	*result = s
	return nil
}

/*------------------------------------------------------- MAIN -------------------------------------------------------*/
func main() {
	worker := new(Worker)

	// publish the methods
	err := rpc.Register(worker)
	errorHandler(err, 214)

	// register a HTTP handler
	rpc.HandleHTTP()

	// listen to TCP connections
	listener, err := net.Listen(network, address)
	errorHandler(err, 69)
	log.Printf("Serving requests on: %s", address)

	err = http.Serve(listener, nil)
	errorHandler(err, 73)
}

/*------------------------------------------------------ LOCAL FUNCTIONS ---------------------------------------------*/
// compute the minimum distance between each point and the set of centroids
func computeMinDistances(points utils.Points, centroids utils.Points) InitMapOutput {
	var mapOut InitMapOutput
	mapOut.Points = make(utils.Points, len(points))
	mapOut.MinDistances = make([]float64, len(points))

	for i, point := range points {
		_, dist := classify(centroids, point)

		// store the point and its minimum distance from the centroids
		mapOut.Points[i] = point
		mapOut.MinDistances[i] = dist
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

// compute the minimum distance between a point and the set of centroids
func classify(centroids utils.Points, point utils.Point) (int, float64) {
	var idx int
	var tempDist float64

	dist := 0.0
	for i, centroid := range centroids {
		tempDist = utils.GetDistance(point.Coordinates, centroid.Coordinates)
		if i == 0 || dist > tempDist {
			dist = tempDist
			idx = i
		}
	}

	return idx, dist
}

// sum the coordinates of the set of points
func recenter(points utils.Points, dim int) utils.Point {

	var res utils.Point

	sum := make([]float64, dim)
	for _, p := range points {
		for i, c := range p.Coordinates {
			sum[i] += c
		}
	}

	res.Coordinates = sum
	return res
}

/*---------------------------------------------------- UTILS ---------------------------------------------------------*/
// error handling
func errorHandler(err error, line int) {
	if err != nil {
		log.Fatalf("failure at line %d: %v", line, err)
	}
}
