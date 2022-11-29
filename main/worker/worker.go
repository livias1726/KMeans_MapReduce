package main

import (
	"KMeans_MapReduce/utils"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
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
	mux        sync.Mutex
}

const (
	debug   = false
	network = "tcp"
	address = "worker:11091"
)

// InitMap
// --> input: set of current chosen initial centroids (from 1 to k-1) and chunk of points to process --> (mu, x)
// --> output: chunk of points processed and their minimum distance from the set of centroids
func (w *Worker) InitMap(payload []byte, result *[]byte) error {
	// unmarshalling
	var inArgs utils.InitMapInput
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, "init-map unmarshalling")
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
	if inArgs.Centroids != nil {
		mapper.Centroids = inArgs.Centroids
	}
	if inArgs.Chunk != nil {
		mapper.Chunks = append(mapper.Chunks, inArgs.Chunk)
		mapper.Dim += len(inArgs.Chunk)
	}
	// send ack
	if !inArgs.Last {
		s, err := json.Marshal(true)
		errorHandler(err, "ack marshalling")
		*result = s
		return err
	}
	if debug {
		log.Printf("--> MAPPER %d: received %d centroid(s) and %d chunk(s) [%d points]",
			idx, len(mapper.Centroids), len(mapper.Chunks), mapper.Dim)
	}
	// process
	comb := new(Combiner)
	var wg sync.WaitGroup
	wg.Add(len(mapper.Chunks))
	for _, chunk := range mapper.Chunks {
		// map
		localOutput := computeMinDistances(chunk, mapper.Centroids)
		// combine
		go func(localOutput utils.InitMapOutput) {
			comb.initCombine(localOutput)
			wg.Done()
		}(localOutput)
	}
	wg.Wait()
	// marshalling
	s, err := json.Marshal(comb.InitMapOut)
	errorHandler(err, "init-map marshalling")
	// return
	*result = s
	return nil
}

// performs a combine phase for a local map output before giving the result to the master
func (c *Combiner) initCombine(inArgs utils.InitMapOutput) {
	combRes, dist := getFartherPoint(inArgs)
	c.mux.Lock()
	c.InitMapOut.Points = append(c.InitMapOut.Points, combRes)
	c.InitMapOut.MinDistances = append(c.InitMapOut.MinDistances, dist)
	c.mux.Unlock()
}

// InitReduce
// --> input : set of points and their minimum distance from the current set of centroids
// --> output: new centroid
func (w *Worker) InitReduce(payload []byte, result *[]byte) error {
	var (
		inArgs utils.InitMapOutput
		redRes utils.Point
	)
	// unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, "init-reduce unmarshalling")
	// get the point at the maximum distance
	if len(inArgs.Points) == 1 {
		redRes = inArgs.Points[0]
	} else {
		redRes, _ = getFartherPoint(inArgs)
	}
	if debug {
		log.Printf("--> reducer results: point %d", redRes.Id)
	}
	// marshalling
	s, err := json.Marshal(&redRes)
	errorHandler(err, "init-reduce marshalling")
	// return
	*result = s
	return nil
}

// Map -> classify /*-------------------------- REMOTE PROCEDURE - MASTER SIDE ---------------------------------------*/
func (w *Worker) Map(payload []byte, result *[]byte) error {
	var inArgs utils.MapInput
	// unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, "map unmarshalling")
	// select mapper
	id := inArgs.MapperId
	mapperSet := w.Mappers[id[0]]
	mapper := mapperSet[id[1]]
	mapper.Centroids = inArgs.Centroids
	// classify each given point to a cluster
	comb := new(Combiner)
	comb.MapOut.Clusters = make(map[int]utils.Points)
	comb.MapOut.Len = make(map[int]int)
	comb.MapOut.Sum = make(map[int]utils.Point)
	var wg sync.WaitGroup
	wg.Add(len(mapper.Chunks))
	for _, chunk := range mapper.Chunks {
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
		go func(clusterId []int, points utils.Points, length []int) {
			comb.combine(clusterId, points, length)
			wg.Done()
		}(clusterId, points, length)
	}
	wg.Wait()
	// marshalling
	s, err := json.Marshal(comb.MapOut)
	errorHandler(err, "map marshalling")
	// return
	*result = s
	return nil
}

// aggregates the clusters obtained from each chunk by the mapper --> shuffle and sort
// computes the partial sum of each point in each partial cluster obtained from mapper --> reduce
func (c *Combiner) combine(clusterId []int, points utils.Points, length []int) {
	c.mux.Lock()
	mapOut := &c.MapOut
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
	c.mux.Unlock()
}

// Reduce -> recenter /*---------------------------------- REMOTE PROCEDURE - MASTER SIDE ----------------------------*/
func (w *Worker) Reduce(payload []byte, result *[]byte) error {
	var inArgs utils.ReduceInput
	// unmarshalling
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, "reduce unmarshalling")
	if debug {
		log.Printf("REDUCER [%d]: received %d points to recenter.", inArgs.ClusterId, len(inArgs.Points))
	}
	// reduce
	var redRes utils.ReduceOutput
	redRes.ClusterId = inArgs.ClusterId
	redRes.Point = recenter(inArgs.Points, len(inArgs.Points[0].Coordinates))
	redRes.Len = inArgs.Len
	// marshalling
	s, err := json.Marshal(&redRes)
	errorHandler(err, "reduce marshalling")
	//return
	*result = s
	return nil
}

/*------------------------------------------------------ MAIN -------------------------------------------------------*/
func main() {
	worker := new(Worker)
	worker.Mappers = make(map[int]map[int]*Mapper)
	// publish the methods
	err := rpc.Register(worker)
	errorHandler(err, "service register")
	// register a HTTP handler
	rpc.HandleHTTP()
	// listen to TCP connections
	listener, err := net.Listen(network, address)
	errorHandler(err, "listener creation")
	log.Printf("Serving requests on: %s", address)
	// serve requests
	err = http.Serve(listener, nil)
	errorHandler(err, "serve request")
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
	var (
		maxDist float64
		index   int
	)
	// compute distances
	for i, d := range inArgs.MinDistances {
		if i == 0 || maxDist < d {
			maxDist = d
			index = i
		}
	}
	// return
	return inArgs.Points[index], maxDist
}

// compute the minimum distance between a point and the set of centroids
func classify(centroids utils.Points, point utils.Point) (int, float64) {
	var (
		tempDist float64
		idx      int
	)
	// compute distances
	dist := 0.0
	for i, centroid := range centroids {
		tempDist = utils.GetDistance(point.Coordinates, centroid.Coordinates)
		if i == 0 || dist > tempDist {
			dist = tempDist
			idx = i
		}
	}
	// return
	return idx, dist
}

// sum the coordinates of the set of points
func recenter(points utils.Points, dim int) utils.Point {
	var res utils.Point
	// get coordinates
	sum := make([]float64, dim)
	for _, p := range points {
		for i, c := range p.Coordinates {
			sum[i] += c
		}
	}
	res.Coordinates = sum
	// return
	return res
}

/*---------------------------------------------------- UTILS ---------------------------------------------------------*/
// error handling logic
func errorHandler(err error, pof string) {
	if err != nil {
		log.Fatalf("%s failure: %v", pof, err)
	}
}
