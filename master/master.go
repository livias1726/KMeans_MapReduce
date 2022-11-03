package main

import (
	"KMeanMR/utils"
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"strconv"
	"time"
)

//TODO: plot result

var port string

type Configuration struct {
	CurrentCentroids   utils.Points
	DeltaThreshold     float64
	IterationThreshold int
}

type KMRequest struct {
	Points utils.Points
	K      int
}

type MapInput struct {
	Centroids utils.Points
	Points    utils.Points
}

type MasterServer int

type MasterClient struct {
	numWorkers int
}

const (
	debug         = false
	network       = "tcp"
	address       = "localhost:5678"
	mapService    = "Worker.Map"
	reduceService = "Worker.Reduce"
	maxLoad       = 1000 //every worker operates on a maximum of 'maxLoad' points
)

// KMeans /*---------------------------------- REMOTE PROCEDURE - CLIENT SIDE ---------------------------------------*/
func (m *MasterServer) KMeans(payload []byte, reply *[]byte) error {
	var kmRequest KMRequest
	// Unmarshalling
	err := json.Unmarshal(payload, &kmRequest)
	errorHandler(err, 51)
	if debug {
		log.Printf("Unmarshalled %d points to cluster in %d groups", len(kmRequest.Points), kmRequest.K)
	}

	// initialize the clusters
	mapInput := new(MapInput)
	mapInput.Points = kmRequest.Points
	mapInput.Centroids, err = utils.Init(kmRequest.K, mapInput.Points)

	// initialize the configuration
	conf := new(Configuration)
	conf.CurrentCentroids = mapInput.Centroids
	conf.DeltaThreshold = 0.01
	conf.IterationThreshold = 100

	// call the service
	master := new(MasterClient)
	result := master.KMeans(*mapInput, *conf)

	// Marshalling of result
	s, err := json.Marshal(&result)
	errorHandler(err, 64)
	if debug {
		log.Printf("Marshaled Data: %s", s)
	}

	*reply = s

	return err
}

// KMeans /*------------------------------------- REMOTE PROCEDURE - WORKER SIDE -------------------------------------*/
func (mc *MasterClient) KMeans(mapInput MapInput, configuration Configuration) utils.Clusters {

	var reduceOutput [][]byte
	numIter := 0
	for {
		//MAP PHASE
		log.Println("-->Activate Map Service on workers...")
		mapOutput := mapFunction(mc, mapInput)
		log.Print("...Done: All the workers returned from map -->\n\n")

		//SHUFFLE AND SORT PHASE
		log.Println("-->Do Shuffle and sort...")
		reduceInput, err := shuffleAndSort(mapOutput, mc.numWorkers)
		errorHandler(err, 87)
		log.Print("...Done: Shuffle and sort -->\n\n")

		log.Println("-->Activate Reduce Service on workers...")
		reduceOutput = reduceFunction(mc, reduceInput)
		log.Print("...Done: All the workers returned from reduce -->\n\n")

		numIter++

		delta, newCentroids := computeDelta(reduceOutput, configuration.CurrentCentroids)
		if delta < configuration.DeltaThreshold {
			break
		} else {
			configuration.CurrentCentroids = newCentroids
		}

		if numIter >= configuration.IterationThreshold {
			break
		}
	}

	//TODO: PROCESS RESULTS
	reply := mergeFinalResults(configuration.CurrentCentroids)
	if debug {
		log.Printf("Reply: %v", reply)
	}

	return reply
}

/*------------------------------------------------------- MAP --------------------------------------------------------*/
func mapFunction(mc *MasterClient, args MapInput) [][]byte {
	// prepare map phase
	chunks := getChunks(args.Points, mc)
	kmChannels := make([]*rpc.Call, mc.numWorkers)
	kmResp := make([][]byte, mc.numWorkers)

	// send a chunk to each mapper
	for i, chunk := range chunks {
		// create a TCP connection to localhost on port 5678
		cli, err := rpc.DialHTTP(network, address)
		errorHandler(err, 114)

		mArgs := prepareMapArguments(chunk, args.Centroids)

		// spawn worker connections
		kmChannels[i] = cli.Go(mapService, mArgs, &kmResp[i], nil)
		log.Printf("Mapper #%d spawned.", i)
	}

	// wait for response
	for i := 0; i < mc.numWorkers; i++ {
		<-kmChannels[i].Done
		log.Printf("Mapper #%d completed.\n", i)
	}

	return kmResp
}

/*---------------------------------------------------- REDUCE --------------------------------------------------------*/
func reduceFunction(mc *MasterClient, args utils.Clusters) [][]byte {
	// prepare reduce phase
	mc.numWorkers = len(args)
	kmChannels := make([]*rpc.Call, mc.numWorkers)
	kmResp := make([][]byte, mc.numWorkers)

	// send a cluster to each reducer
	for i, cluster := range args {
		// create a TCP connection to localhost on port 5678
		cli, err := rpc.DialHTTP(network, address)
		errorHandler(err, 145)

		// Marshalling
		rArgs, err := json.Marshal(&cluster)
		errorHandler(err, 149)

		// spawn worker connections
		kmChannels[i] = cli.Go(reduceService, rArgs, &kmResp[i], nil)

		log.Printf("Reducer #%d spawned.", i)
	}

	// wait for response
	for i := 0; i < mc.numWorkers; i++ {
		<-kmChannels[i].Done
		log.Printf("Reducer #%d completed.", i)
	}

	return kmResp
}

/*------------------------------------------------------ MAIN -------------------------------------------------------*/
func main() {
	// Generate a random port for the client
	rand.Seed(time.Now().UTC().UnixNano())
	max := 50005
	min := 50000

	portNum := rand.Intn(max-min) + min
	port = strconv.Itoa(portNum)

	go serveClients() // spawn async server

	master := new(MasterServer)
	// Publish the receiver methods
	err := rpc.Register(master)
	errorHandler(err, 180)

	select {} //infinite loop
}

func serveClients() {
	addr, err := net.ResolveTCPAddr(network, "0.0.0.0:"+port)
	errorHandler(err, 132)

	// Register a HTTP handler
	rpc.HandleHTTP()

	//Listen to TCP connections
	listen, err := net.ListenTCP(network, addr)
	errorHandler(err, 194)

	log.Printf("Serving RPC server on address %s , port %s\n", addr, port)

	for {
		// serve the new client
		rpc.Accept(listen)
	}
}

/*-------------------------------------------- LOCAL FUNCTIONS -------------------------------------------------------*/
/*
 * Distribute an equal amount of points per Mapper, given the 'max load' limit
 */
func getChunks(points utils.Points, mc *MasterClient) []utils.Points {
	var pointsPerWorker int
	numPoints := len(points)
	mc.numWorkers = int(math.Ceil(float64(numPoints / maxLoad)))

	//create and populate chunk buffer
	chunks := make([]utils.Points, mc.numWorkers)
	idx := 0
	for i := 0; i < mc.numWorkers; i++ {
		//add 'pointsPerWorker' points from src to chunk
		if i == mc.numWorkers-1 && numPoints%maxLoad != 0 {
			pointsPerWorker = numPoints % maxLoad
		} else {
			pointsPerWorker = maxLoad
		}

		chunks[i] = points[idx : idx+pointsPerWorker]
		idx = idx + pointsPerWorker + 1
	}

	return chunks
}

/*
 * Prepares a MapInput object with the centroids and the points for each of the Mappers.
 * Returns the marshalled message for the Map.
 */
func prepareMapArguments(chunk utils.Points, centroids utils.Points) interface{} {
	// Arguments
	kmArgs := new(MapInput)
	kmArgs.Centroids = centroids
	kmArgs.Points = chunk

	// Marshaling
	mArgs, err := json.Marshal(&kmArgs)
	errorHandler(err, 259)
	if debug {
		log.Printf("Marshaled Data: %s", mArgs)
	}

	return mArgs
}

/*
 * Merges the partial clusters from every mapper in the actual clusters to reduce (recenter)
 */
func shuffleAndSort(resp [][]byte, dim int) (utils.Clusters, error) {

	var mapRes utils.Clusters
	for i := 0; i < dim; i++ {
		var temp utils.Clusters
		// Unmarshalling
		err := json.Unmarshal(resp[i], &temp)
		errorHandler(err, 273)
		if debug {
			for j := 0; j < len(temp); j++ {
				log.Printf("Worker #%d got %d points in cluster %d.\n", i, len(temp[j].Points), j)
			}
		}

		// Merging
		if len(mapRes) == 0 {
			mapRes = temp
		} else {
			for j := 0; j < len(mapRes); j++ {
				mapRes[j].Points = append(mapRes[j].Points, temp[j].Points...)
			}
		}
	}

	return mapRes, nil
}

/*
 * Compute the amount of changes that have been applied in the latest iteration and returns the new centroids
 */
func computeDelta(resp [][]byte, oldCentroids utils.Points) (float64, utils.Points) {

	var newCentroids utils.Points
	dim := len(oldCentroids)
	delta := 0.0

	for i := 0; i < dim; i++ {
		var centroid utils.Point
		// Unmarshalling
		err := json.Unmarshal(resp[i], &centroid)
		errorHandler(err, 315)

		delta += utils.GetDistance(oldCentroids[i].Coordinates, centroid.Coordinates)
		newCentroids = append(newCentroids, centroid)
	}

	delta = delta / float64(dim)

	return delta, newCentroids
}

func mergeFinalResults(finalCentroids utils.Points) utils.Clusters {

	var result utils.Clusters
	for i := 0; i < len(finalCentroids); i++ {
		cluster := new(utils.Cluster)
		cluster.Centroid = finalCentroids[i]
		//TODO: pass latest points clustering
		result = append(result, *cluster)
	}

	return result
}

// error handling
func errorHandler(err error, line int) {
	if err != nil {
		log.Fatalf("failure at line %d: %v", line, err)
	}
}
