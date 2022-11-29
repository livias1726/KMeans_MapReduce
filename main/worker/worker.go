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

const (
	debug   = false
	network = "tcp"
	address = "localhost:11091"
)

// InitMap : implements the map task for k-means++ algorithm initial phase
// --> input: set of current chosen initial centroids (from 1 to k-1) and chunk of points to process --> (mu, [x])
// --> output: points processed and their minimum distance from the set of centroids --> ([x], [min_d])
func (w *Worker) InitMap(payload []byte, result *[]byte) error {
	// unmarshalling
	var inArgs utils.MapInput
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, "init-map unmarshalling")
	if debug {
		log.Printf("--> mapper received chunk [%d...] and %d centroids", inArgs.Chunk[0].Id, len(inArgs.Centroids))
	}
	// map
	localOutput := computeMinDistances(inArgs.Chunk, inArgs.Centroids)
	// combine
	localOutput = initCombine(localOutput)
	// marshalling
	s, err := json.Marshal(localOutput)
	errorHandler(err, "init-map marshalling")
	// return
	*result = s
	return nil
}

// Performs a combine phase for a local map output before giving the result to the master
func initCombine(inArgs utils.InitMapOutput) utils.InitMapOutput {
	combRes, dist := getFartherPoint(inArgs)
	inArgs.Points = utils.Points{combRes}
	inArgs.MinDistances = []float64{dist}

	return inArgs
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
	// map -> classify each given point to a cluster
	// map
	localOutput := computeClassification(inArgs.Chunk, inArgs.Centroids)
	// combine -> compute the local sum of the points in each cluster
	outArgs := combine(localOutput)

	// marshalling
	s, err := json.Marshal(outArgs)
	errorHandler(err, "map marshalling")
	// return
	*result = s
	return nil
}

// aggregates the clusters obtained from each chunk by the mapper --> shuffle and sort
// computes the partial sum of each point in each partial cluster obtained from mapper --> reduce
func combine(inArgs utils.MapOutput) utils.MapOutput {
	var mapOut utils.MapOutput
	// allocate space
	mapOut.Clusters = make(map[int]utils.Points)
	mapOut.Len = make(map[int]int)
	mapOut.Sum = make(map[int]utils.Point)

	for cid, points := range inArgs.Clusters {
		_, ok := mapOut.Clusters[cid]
		if ok {
			// aggregate
			mapOut.Clusters[cid] = append(mapOut.Clusters[cid], points...)
			mapOut.Len[cid] += inArgs.Len[cid]
			// recenter
			mapOut.Sum[cid] = recenter(append(points, mapOut.Sum[cid]), len(points[0].Coordinates))
		} else {
			// aggregate
			mapOut.Clusters[cid] = points
			mapOut.Len[cid] = inArgs.Len[cid]
			// recenter
			mapOut.Sum[cid] = recenter(points, len(points[0].Coordinates))
		}
	}
	return mapOut
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
	// marshalling
	s, err := json.Marshal(&redRes)
	errorHandler(err, "reduce marshalling")
	//return
	*result = s
	return nil
}

/*------------------------------------------------------- MAIN -------------------------------------------------------*/
func main() {
	worker := new(Worker)

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

// used to execute the classification of a single chunk
func computeClassification(points utils.Points, centroids utils.Points) utils.MapOutput {
	var mapOut utils.MapOutput
	mapOut.Clusters = make(map[int]utils.Points)
	mapOut.Len = make(map[int]int)
	// map
	for _, point := range points {
		cid, _ := classify(centroids, point)
		_, ok := mapOut.Clusters[cid]
		if ok {
			// aggregate
			mapOut.Clusters[cid] = append(mapOut.Clusters[cid], point)
			mapOut.Len[cid] += 1
		} else {
			// aggregate
			mapOut.Clusters[cid] = utils.Points{point}
			mapOut.Len[cid] = 1
		}
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
