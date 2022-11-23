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
	Mappers map[int]map[int]*Mapper //key: req_id, val: {key: chunk_id, val: Mapper}
}

type Combiner struct {
	InitMapOut utils.InitMapOutput
	MapOut     utils.MapOutput
}

const (
	debug   = false
	network = "tcp"
	address = "localhost:11091"
)

// InitMap
// --> input: set of current chosen initial centroids (from 1 to k-1) and chunk of points to process --> (mu, x)
// --> output: chunk of points processed and their minimum distance from the set of centroids
func (w *Worker) InitMap(payload []byte, result *[]byte) error {
	// unmarshalling
	var inArgs utils.InitMapInput
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 72)

	// select mapper
	idx := inArgs.MapperId
	if w.Mappers[idx[0]] == nil {
		w.Mappers[idx[0]] = make(map[int]*Mapper)
	}
	mapperSet := w.Mappers[idx[0]]
	if mapperSet[idx[1]] == nil {
		mapperSet[idx[1]] = new(Mapper)
	}

	// store data
	mapper := mapperSet[idx[1]]
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
func (c *Combiner) initCombine(inArgs utils.InitMapOutput) {
	combRes, dist := getFartherPoint(inArgs)

	c.InitMapOut.Points = append(c.InitMapOut.Points, combRes)
	c.InitMapOut.MinDistances = append(c.InitMapOut.MinDistances, dist)
}

// InitReduce
// --> input : set of points and their minimum distance from the current set of centroids
// --> output: new centroid
func (w *Worker) InitReduce(payload []byte, result *[]byte) error {
	var inArgs utils.InitMapOutput
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
	var inArgs utils.MapInput

	// unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 34)

	// select mapper
	id := inArgs.MapperId
	mapperSet := w.Mappers[id[0]]
	mapper := mapperSet[id[1]]
	mapper.Centroids = inArgs.Centroids

	// classify each given point to a cluster
	comb := new(Combiner)
	for i, chunk := range mapper.Chunks {
		var (
			clusterId []int
			length    []int
			points    utils.Points
		)
		// map
		for _, point := range chunk {
			idx, _ := classify(mapper.Centroids, point)

			clusterId = append(clusterId, idx)
			points = append(points, point)
			length = append(length, 1)
		}
		// combine
		comb.combine(i == 0, clusterId, points, length)
	}

	// marshalling
	s, err := json.Marshal(&comb.MapOut)
	errorHandler(err, 202)

	// return
	*result = s
	return nil
}

// aggregates the clusters obtained from each chunk by the mapper --> shuffle and sort
// computes the partial sum of each point in each partial cluster obtained from mapper --> reduce
func (c *Combiner) combine(first bool, clusterId []int, points utils.Points, length []int) {
	mapOut := &c.MapOut
	if first {
		// allocate space
		mapOut.Clusters = make(map[int]utils.Points)
		mapOut.Len = make(map[int]int)
		mapOut.Sum = make(map[int]utils.Point)
	}

	for i, cid := range clusterId {
		_, ok := mapOut.Clusters[cid]
		if ok {
			// aggregate
			mapOut.Clusters[cid] = append(mapOut.Clusters[cid], points[i])
			mapOut.Len[cid] += length[i]
			// recenter
			mapOut.Sum[cid] = recenter(utils.Points{points[i], mapOut.Sum[cid]}, len(points[i].Coordinates))
		} else {
			// aggregate
			mapOut.Clusters[cid] = utils.Points{points[i]}
			mapOut.Len[cid] = length[i]
			// recenter
			mapOut.Sum[cid] = points[i]
		}
	}
}

// Reduce -> recenter /*---------------------------------- REMOTE PROCEDURE - MASTER SIDE ----------------------------*/
func (w *Worker) Reduce(payload []byte, result *[]byte) error {
	var inArgs utils.ReduceInput

	// unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 72)
	if debug {
		log.Printf("REDUCER [%d]: received %d points to recenter.", inArgs.ClusterId, len(inArgs.Points))
	}

	var redRes utils.ReduceOutput
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
	worker.Mappers = make(map[int]map[int]*Mapper)

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
func computeMinDistances(points utils.Points, centroids utils.Points) utils.InitMapOutput {
	var mapOut utils.InitMapOutput
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
func getFartherPoint(inArgs utils.InitMapOutput) (utils.Point, float64) {

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