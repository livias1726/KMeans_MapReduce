package main

import (
	"KMeans_MapReduce/utils"
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"strconv"
	"time"
)

var port string

type Configuration struct {
	CurrentCentroids   utils.Points
	InputPoints        []utils.Points
	NumMappers         int
	NumReducers        int
	DeltaThreshold     float64
	IterationThreshold int
}

type KMRequest struct {
	DatasetPoints utils.Points
	K             int
}

type MapInput struct {
	Centroids utils.Points
	Points    utils.Points
}

type InitMapOutput struct {
	Points    utils.Points
	Distances []float64
}

type MasterServer int
type MasterClient int

const (
	debug              = false
	network            = "tcp"
	address            = "localhost:5678"
	mapService1        = "Worker.InitMap"
	reduceService1     = "Worker.InitReduce"
	mapService2        = "Worker.Map"
	reduceService2     = "Worker.Reduce"
	maxLoad            = 1000 //every worker operates on a maximum of 'maxLoad' points
	deltaThreshold     = 0.01
	iterationThreshold = 10
)

// KMeans /*---------------------------------- REMOTE PROCEDURE - CLIENT SIDE ---------------------------------------*/
func (m *MasterServer) KMeans(payload []byte, reply *[]byte) error {
	var kmRequest KMRequest
	// Unmarshalling
	err := json.Unmarshal(payload, &kmRequest)
	errorHandler(err, 51)
	if debug {
		log.Printf("Unmarshalled %d points to cluster in %d groups", len(kmRequest.DatasetPoints), kmRequest.K)
	}

	// initialize the configuration
	conf := new(Configuration)
	initialize(conf, kmRequest)

	// call the service
	master := new(MasterClient)
	result := master.KMeans(*conf)

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
func (mc *MasterClient) KMeans(configuration Configuration) utils.Clusters {

	var reduceOutput [][]byte
	var reply utils.Clusters

	numIter := 0
	for {
		log.Printf("--> Starting iteration #%d... ", numIter+1)

		// map
		if debug {
			log.Println("--> Activate Map Service...")
		}
		mapOutput := mapFunction(configuration, mapService2)
		if debug {
			log.Print("...Done. All the mappers returned. -->\n\n")
		}

		// shuffle and sort
		if debug {
			log.Println("--> Do Shuffle and sort...")
		}
		reduceInput, err := shuffleAndSort(mapOutput, configuration.NumMappers)
		errorHandler(err, 101)
		if debug {
			log.Print("...Done. -->\n\n")
		}

		// reduce
		if debug {
			log.Println("--> Activate Reduce Service...")
		}
		reduceOutput = reduceFunction(reduceService2, nil, reduceInput)
		if debug {
			log.Print("...Done. All the reducers returned. -->\n\n")
		}

		// check update threshold
		delta, newCentroids := computeDelta(reduceOutput, configuration.CurrentCentroids)
		if delta < configuration.DeltaThreshold {
			reply = reduceInput
			break
		} else {
			configuration.CurrentCentroids = newCentroids
			log.Printf(" ...obtained delta is %f.\n\n", delta)
		}

		numIter++
		if numIter >= configuration.IterationThreshold {
			break
		}
	}

	return reply
}

/*------------------------------------------------------- MAP --------------------------------------------------------*/
func mapFunction(conf Configuration, service string) [][]byte {
	// prepare map phase
	channels := make([]*rpc.Call, conf.NumMappers)
	response := make([][]byte, conf.NumMappers)

	// send a chunk to each mapper
	for i, chunk := range conf.InputPoints {
		// create a TCP connection to localhost on port 5678
		cli, err := rpc.DialHTTP(network, address)
		errorHandler(err, 114)

		mapArgs := prepareMapArguments(chunk, conf.CurrentCentroids)

		// spawn worker connections
		channels[i] = cli.Go(service, mapArgs, &response[i], nil)
		if debug {
			log.Printf("Mapper #%d spawned.", i)
		}
	}

	// wait for response
	for i := 0; i < conf.NumMappers; i++ {
		<-channels[i].Done
		if debug {
			log.Printf("Mapper #%d completed.\n", i)
		}
	}

	return response
}

/*---------------------------------------------------- REDUCE --------------------------------------------------------*/
func reduceFunction(service string, initArgs *InitMapOutput, args utils.Clusters) [][]byte {

	if service == reduceService1 {
		return initReduce(*initArgs)
	}

	return reduce(args)
}

func initReduce(arg InitMapOutput) [][]byte {
	// prepare reduce phase
	resp := make([][]byte, 1)

	// create a TCP connection to localhost on port 5678
	cli, err := rpc.DialHTTP(network, address)
	errorHandler(err, 145)

	// Marshalling
	rArgs, err := json.Marshal(&arg)
	errorHandler(err, 149)

	// call reducer synchronously
	err = cli.Call(reduceService1, rArgs, &resp[0])
	errorHandler(err, 199)

	if debug {
		log.Print("Init-Reducer spawned.")
	}

	return resp
}

func reduce(args utils.Clusters) [][]byte {
	// prepare reduce phase
	numReducers := len(args)
	kmChannels := make([]*rpc.Call, numReducers)
	kmResp := make([][]byte, numReducers)

	// send a cluster to each reducer
	for i, cluster := range args {
		// create a TCP connection to localhost on port 5678
		cli, err := rpc.DialHTTP(network, address)
		errorHandler(err, 145)

		// Marshalling
		rArgs, err := json.Marshal(&cluster)
		errorHandler(err, 149)

		// spawn worker connections
		kmChannels[i] = cli.Go(reduceService2, rArgs, &kmResp[i], nil)

		if debug {
			log.Printf("Reducer #%d spawned.", i)
		}
	}

	// wait for response
	for i := 0; i < numReducers; i++ {
		<-kmChannels[i].Done
		if debug {
			log.Printf("Reducer #%d completed.", i)
		}
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
func initialize(configuration *Configuration, request KMRequest) {
	var err error

	configuration.DeltaThreshold = deltaThreshold
	configuration.IterationThreshold = iterationThreshold

	// get first random point from dataset
	configuration.CurrentCentroids, err = utils.Init(1, request.DatasetPoints)
	errorHandler(err, 257)

	// distribute dataset points among the mappers
	getChunks(request.DatasetPoints, configuration)

	// populate the initial set of centroids
	numIter := 0
	for i := 0; i < request.K; i++ {
		log.Printf("--> Starting initialization iteration #%d... ", numIter+1)

		// init-map
		initMapOutput := mapFunction(*configuration, mapService1)

		// unmarshalling
		mapOut := new(InitMapOutput)
		for j := 0; j < len(initMapOutput); j++ {
			var tempMapOut InitMapOutput
			err := json.Unmarshal(initMapOutput[j], &tempMapOut)
			errorHandler(err, 306)

			mapOut.Points = append(mapOut.Points, tempMapOut.Points...)
			mapOut.Distances = append(mapOut.Distances, tempMapOut.Distances...)
		}

		// init-reduce
		initRedOutput := reduceFunction(reduceService1, mapOut, nil)

		// Unmarshalling
		var newCentroid utils.Point
		err = json.Unmarshal(initRedOutput[0], &newCentroid)
		errorHandler(err, 273)

		configuration.CurrentCentroids = append(configuration.CurrentCentroids, newCentroid)

		numIter++
	}
}

/*
 * Distribute an equal amount of points per Mapper, given the 'max load' limit
 */
func getChunks(points utils.Points, conf *Configuration) {
	var pointsPerWorker int
	numPoints := len(points)
	conf.NumMappers = int(math.Ceil(float64(numPoints / maxLoad)))

	//create and populate chunk buffer
	chunks := make([]utils.Points, conf.NumMappers)
	idx := 0
	for i := 0; i < conf.NumMappers; i++ {
		//add 'pointsPerWorker' points from src to chunk
		if i == conf.NumMappers-1 && numPoints%maxLoad != 0 {
			pointsPerWorker = numPoints % maxLoad
		} else {
			pointsPerWorker = maxLoad
		}

		chunks[i] = points[idx : idx+pointsPerWorker]
		idx = idx + pointsPerWorker + 1
	}

	conf.InputPoints = chunks
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
		errorHandler(err, 344)

		delta += utils.GetDistance(oldCentroids[i].Coordinates, centroid.Coordinates)
		newCentroids = append(newCentroids, centroid)
	}

	delta = delta / float64(dim)

	return delta, newCentroids
}

// error handling
func errorHandler(err error, line int) {
	if err != nil {
		log.Fatalf("failure at line %d: %v", line, err)
	}
}
