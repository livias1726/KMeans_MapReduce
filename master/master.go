package main

import (
	"KMeans_MapReduce/utils"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"strconv"
	"time"
)

type MasterServer struct {
	//Clients    []MasterClient //TODO: be able to serve multiple clients simultaneously
	Dataset    utils.Points
	DatasetDim int
}

type MasterClient struct {
	Config Configuration
}

// Configuration :
type Configuration struct {
	Dataset    utils.Points
	K          int
	DatasetDim int
	//----------------------
	CurrentCentroids utils.Points     // list of current centroids to re-send to mappers
	InputPoints      [][]utils.Points // chunks of dataset points divided by mapper
	Mappers          []*rpc.Client    // list of mappers to communicate with
	Reducers         []*rpc.Client    // list of reducers to communicate with
	//----------------------
	NumMappers  int // number of mappers currently active
	NumReducers int // number of reducers currently active
	//----------------------
	DeltaThreshold     float64 // stop condition on updates
	IterationThreshold int     // stop condition on number of iteration
}

// KMRequest : matches with struct on client side
type KMRequest struct {
	Dataset utils.Points
	K       int
	Last    bool
}

// KMResponse : matches with struct on client side
type KMResponse struct {
	Clusters utils.Clusters
	Message  string
}

// MapInput : matches with struct on worker side
type MapInput struct {
	Id        int
	Centroids utils.Points
	Points    utils.Points
	NewPoints bool
	First     bool
	Last      bool
}

// InitMapOutput : matches with struct on worker side
type InitMapOutput struct {
	Points    utils.Points
	Distances []float64
}

const (
	debug              = true
	network            = "tcp"
	address            = "localhost:5678"
	mapService1        = "Worker.InitMap"
	reduceService1     = "Worker.InitReduce"
	mapService2        = "Worker.Map"
	reduceService2     = "Worker.Reduce"
	maxLoad            = 1000 //every worker operates on a maximum of 'maxLoad' points
	maxNodes           = 10
	deltaThreshold     = 0.01
	iterationThreshold = 15
)

// KMeans /*---------------------------------- REMOTE PROCEDURE - CLIENT SIDE ---------------------------------------*/
func (m *MasterServer) KMeans(payload []byte, reply *[]byte) error {
	var kmRequest KMRequest
	var err error
	var s []byte

	// unmarshalling
	err = json.Unmarshal(payload, &kmRequest)
	errorHandler(err, 51)

	// store new points
	m.Dataset = append(m.Dataset, kmRequest.Dataset...)
	m.DatasetDim += len(kmRequest.Dataset)
	if debug {
		log.Printf("--> received %d points to cluster in %d groups.",
			m.DatasetDim, kmRequest.K)
	}

	// send ack
	if !kmRequest.Last {
		s, err = json.Marshal(true)
		errorHandler(err, 106)

		*reply = s
		return err
	}

	// call the service
	master := new(MasterClient)
	//m.Clients = append(m.Clients, *master)

	result, msg := master.KMeans(m.Dataset, kmRequest.K, m.DatasetDim)

	// preparing response
	var resp KMResponse
	resp.Clusters = result
	resp.Message = msg

	// marshalling result
	s, err = json.Marshal(&resp)
	errorHandler(err, 125)

	//cleanup
	m.Dataset = nil
	m.DatasetDim = 0

	// return
	if debug {
		log.Print("--> master returning.\n")
	}
	*reply = s
	return err
}

// KMeans /*------------------------------------- REMOTE PROCEDURE - WORKER SIDE -------------------------------------*/
func (mc *MasterClient) KMeans(dataset utils.Points, k int, dim int) (utils.Clusters, string) {
	conf := new(Configuration)

	// initialize configuration
	conf.Dataset = dataset
	conf.K = k
	conf.DatasetDim = dim

	// divide the dataset among the mappers
	conf.InputPoints, conf.NumMappers = getChunks(conf.Dataset)
	if debug {
		log.Printf("--> %d mappers to spawn with %d chunks.\n",
			len(conf.InputPoints), len(conf.InputPoints[0]))
	}

	// create enough connections to communicate with the mappers
	conf.NumReducers = conf.K
	conf.Mappers, conf.Reducers = createConnections(conf.NumMappers, conf.NumReducers)
	if debug {
		log.Printf("--> initialized %d connections to mapper nodes.\n",
			len(conf.Mappers))
	}

	// perform k-means++
	kMeanspp(conf)
	if debug {
		log.Printf("--> initialized %d centroids with average distance of %f.\n",
			len(conf.CurrentCentroids), utils.GetAvgDistanceOfSet(conf.CurrentCentroids))
	}

	conf.DeltaThreshold = deltaThreshold
	conf.IterationThreshold = iterationThreshold

	// perform k-means
	clusters, logMsg := kMeans(conf)

	return clusters, logMsg
}

/*------------------------------------------------------- MAP --------------------------------------------------------*/
func mapFunction(conf Configuration, service string, iteration int) [][]byte {
	if service == mapService1 {
		return initMap(conf, iteration)
	}

	return mapFunc(conf)
}

func initMap(conf Configuration, iteration int) [][]byte {
	if debug {
		log.Println("--> init-map phase started... ")
	}

	channels := make([]*rpc.Call, conf.NumMappers)
	results := make([][]byte, conf.NumMappers)
	chunksPerMapper := len(conf.InputPoints[0])

	// send chunks and centroids
	newPoints := iteration == 0
	for i := 0; i < chunksPerMapper; i++ {
		first := i == 0
		last := i == chunksPerMapper-1

		for j := 0; j < len(conf.Mappers); j++ {
			cli := conf.Mappers[j]
			mapArgs := prepareInitMapArgs(j, conf.InputPoints[j][i], conf.CurrentCentroids, first, newPoints, last)
			channels[j] = cli.Go(mapService1, mapArgs, &results[j], nil)
		}
		// wait for ack
		if !last { //TODO: does not ack last chunk
			checkAck(conf, channels, results, i, first, newPoints, last)
		}
	}

	// wait for response
	for i := 0; i < conf.NumMappers; i++ {
		<-channels[i].Done
	}

	// return
	if debug {
		log.Println("			...completed.")
	}
	return results
}

func checkAck(conf Configuration, channels []*rpc.Call, results [][]byte, chunkId int,
	first bool, newPoints bool, last bool) {
	var ack bool
	var idx []int
	stop := true
	for j := 0; j < conf.NumMappers; j++ {
		<-channels[j].Done
		err := json.Unmarshal(results[j], &ack)
		errorHandler(err, 106)

		if !ack {
			idx = append(idx, j)
			stop = false
		}
	}

	for !stop {
		// check replies
		stop = true
		idx = nil
		for _, id := range idx {
			<-channels[id].Done
			err := json.Unmarshal(results[id], &ack)
			errorHandler(err, 106)

			if !ack {
				idx = append(idx, id)
				stop = false
			}
		}
		// retry if nack
		for _, id := range idx {
			cli := conf.Mappers[id]
			mapArgs := prepareInitMapArgs(id, conf.InputPoints[id][chunkId], conf.CurrentCentroids,
				first, newPoints, last)
			channels[id] = cli.Go(mapService1, mapArgs, &results[id], nil)
		}
	}
}

func mapFunc(conf Configuration) [][]byte {
	if debug {
		log.Print("--> map phase started... ")
	}

	channels := make([]*rpc.Call, conf.NumMappers)
	results := make([][]byte, conf.NumMappers)
	for j, cli := range conf.Mappers {
		mapArgs := prepareMapArgs(j, conf.CurrentCentroids)
		channels[j] = cli.Go(mapService2, mapArgs, &results[j], nil)
	}
	// wait for response
	for i := 0; i < conf.NumMappers; i++ {
		<-channels[i].Done
	}

	if debug {
		log.Println("			...completed.")
	}

	return results
}

/*---------------------------------------------------- REDUCE --------------------------------------------------------*/
func reduceFunction(conf Configuration, service string, initArgs *InitMapOutput, args utils.Clusters) [][]byte {

	if service == reduceService1 {
		return initReduce(conf, *initArgs)
	}

	return reduce(conf, args)
}

func initReduce(conf Configuration, arg InitMapOutput) [][]byte {
	if debug {
		log.Print("--> init-reduce phase started...")
	}

	// marshalling
	rArgs, err := json.Marshal(&arg)
	errorHandler(err, 149)

	// call reducer synchronously
	resp := make([][]byte, 1) // to be used with 'reduceFunction'
	err = conf.Reducers[0].Call(reduceService1, rArgs, &resp[0])
	errorHandler(err, 199)

	if debug {
		log.Print("			...completed.")
	}

	return resp
}

func reduce(conf Configuration, clusters utils.Clusters) [][]byte {
	if debug {
		log.Print("--> reduce phase started...")
	}

	// prepare reduce phase
	channels := make([]*rpc.Call, conf.NumReducers)
	results := make([][]byte, conf.NumReducers)

	// send a cluster to each reducer
	for i, cli := range conf.Reducers {
		// marshalling
		rArgs, err := json.Marshal(&clusters[i])
		errorHandler(err, 149)
		// call service
		channels[i] = cli.Go(reduceService2, rArgs, &results[i], nil)
	}

	// wait for response
	for i := 0; i < conf.NumReducers; i++ {
		<-channels[i].Done
	}

	// return
	if debug {
		log.Print("			...completed.")
	}
	return results
}

/*------------------------------------------------------ MAIN -------------------------------------------------------*/
func main() {
	// generate a random port for the client
	rand.Seed(time.Now().UTC().UnixNano())
	max := 50005
	min := 50000
	portNum := rand.Intn(max-min) + min
	port := strconv.Itoa(portNum)

	// spawn async server
	go serveClients(port)

	// publish methods
	master := new(MasterServer)
	err := rpc.Register(master)
	errorHandler(err, 180)
	if debug {
		log.Print("--> master node is online.\n")
	}

	select {} //infinite loop
}

func serveClients(port string) {
	addr, err := net.ResolveTCPAddr(network, "0.0.0.0:"+port)
	errorHandler(err, 337)

	// register a HTTP handler
	rpc.HandleHTTP()

	// listen to TCP connections
	listen, err := net.ListenTCP(network, addr)
	errorHandler(err, 344)

	log.Printf("Serving RPCs on address %s\n", addr)

	// serve new clients
	for {
		rpc.Accept(listen)
	}
}

/*-------------------------------------------- LOCAL FUNCTIONS -------------------------------------------------------*/
/*
 * Get chunks of max 1000 points from a request and assign them to each mapper (max 10 mappers)
 */
func getChunks(dataset utils.Points) ([][]utils.Points, int) {

	numPoints := len(dataset)

	// get correct number of mappers to spawn
	numMappers := int(math.Ceil(float64(numPoints) / float64(maxLoad)))
	if numMappers >= maxNodes {
		numMappers = maxNodes
	}

	// initialize rows of input points matrix
	inputPoints := make([][]utils.Points, numMappers)

	// populate chunks
	var j int
	count := 0
	for i := 0; i < numMappers; i++ {
		p1 := ((1 + i) * numPoints) / numMappers
		p2 := (i * numPoints) / numMappers
		points := p1 - p2

		numChunks := int(math.Ceil(float64(points) / float64(maxLoad)))
		inputPoints[i] = make([]utils.Points, numChunks)

		temp := points / maxLoad
		for j = 0; j < temp; j++ {
			inputPoints[i][j] = dataset[count : count+maxLoad]
			count += maxLoad
		}

		temp = points % maxLoad
		if temp != 0 {
			inputPoints[i][j] = dataset[count : count+temp]
			count += temp
		}
	}

	return inputPoints, numMappers
}

func createConnections(numMappers int, numReducers int) ([]*rpc.Client, []*rpc.Client) {
	var err error
	// mappers
	mappers := make([]*rpc.Client, numMappers)
	// create a TCP connection to localhost on port 5678
	for i := 0; i < numMappers; i++ {
		mappers[i], err = rpc.DialHTTP(network, address)
		errorHandler(err, 114)
	}
	// reducers
	reducers := make([]*rpc.Client, numReducers)
	// create a TCP connection to localhost on port 5678
	for i := 0; i < numReducers; i++ {
		reducers[i], err = rpc.DialHTTP(network, address)
		errorHandler(err, 114)
	}

	return mappers, reducers
}

func kMeanspp(conf *Configuration) {
	var err error

	// get first random point from dataset
	conf.CurrentCentroids, err = utils.Init(1, conf.Dataset)
	errorHandler(err, 257)

	// populate the initial set of centroids
	numIter := 0
	for i := 0; i < conf.K-1; i++ {
		log.Printf("K-Means++ (initialization) iteration #%d... ", numIter+1)

		// init-map
		initMapOutput := mapFunction(*conf, mapService1, i)

		// aggregation of results
		mapOut := initShuffleAndSort(initMapOutput)

		// init-reduce -> single reducer
		initRedOutput := reduceFunction(*conf, reduceService1, mapOut, nil)

		// add new centroid
		var newCentroid utils.Point
		err = json.Unmarshal(initRedOutput[0], &newCentroid)
		errorHandler(err, 385)

		conf.CurrentCentroids = append(conf.CurrentCentroids, newCentroid)

		numIter++
	}
}

func initShuffleAndSort(initMapOutput [][]byte) *InitMapOutput {
	mapOut := new(InitMapOutput)

	for j := 0; j < len(initMapOutput); j++ {
		var tempMapOut InitMapOutput
		err := json.Unmarshal(initMapOutput[j], &tempMapOut)
		errorHandler(err, 306)

		mapOut.Points = append(mapOut.Points, tempMapOut.Points...)
		mapOut.Distances = append(mapOut.Distances, tempMapOut.Distances...)
	}

	return mapOut
}

func kMeans(conf *Configuration) (utils.Clusters, string) {
	var reduceOutput [][]byte
	var reply utils.Clusters
	var msg string

	numIter := 0
	for {
		log.Printf("Standard K-Means iteration #%d... ", numIter+1)

		// map
		mapOutput := mapFunction(*conf, mapService2, -1)

		// shuffle and sort
		reduceInput := shuffleAndSort(mapOutput, conf.NumMappers)

		// check update threshold
		delta := computeDelta(reduceInput, conf.DatasetDim)
		if delta < conf.DeltaThreshold {
			reply = reduceInput
			msg = fmt.Sprintf("Algorithm converged to a %f%% percentage of membership change after %d iterations",
				delta, numIter)
			break
		}

		// reduce
		reduceOutput = reduceFunction(*conf, reduceService2, nil, reduceInput)
		conf.CurrentCentroids = computeNewCentroids(reduceOutput)

		numIter++
		if numIter >= conf.IterationThreshold {
			reply = reduceInput
			msg = fmt.Sprintf("Algorithm terminated after reaching the maximum number of iterations (%d). ",
				numIter)
			msg = msg + fmt.Sprintf("Last delta obtained in points classification changes is %f",
				delta)
			break
		}
	}

	return reply, msg
}

/*
 * Merges the partial clusters from every mapper in the actual clusters to reduce (recenter)
 */
func shuffleAndSort(resp [][]byte, dim int) utils.Clusters {
	if debug {
		log.Println("--> shuffle and sort...")
	}

	var mapRes utils.Clusters

	for i := 0; i < dim; i++ {
		var temp utils.Clusters

		// unmarshalling
		err := json.Unmarshal(resp[i], &temp)
		errorHandler(err, 273)

		// merging
		if len(mapRes) == 0 {
			mapRes = temp
		} else {
			for j := 0; j < len(mapRes); j++ {
				mapRes[j].Points = append(mapRes[j].Points, temp[j].Points...)
			}
		}
	}

	if debug {
		log.Print("			...completed.")
	}
	return mapRes
}

/*
 * Prepares a MapInput object for the k-means++ iteration with (eventually) the current centroids and the points
 * for each Mapper.
 */
func prepareInitMapArgs(mapperId int, chunk utils.Points, centroids utils.Points, first bool, newPoints bool,
	last bool) []byte {
	// Arguments
	kmArgs := new(MapInput)
	kmArgs.Id = mapperId

	if !newPoints { // if not first iteration of map -> points already transmitted
		kmArgs.Points = nil
	} else {
		kmArgs.Points = chunk
	}

	kmArgs.First = first
	kmArgs.NewPoints = newPoints
	kmArgs.Last = last

	if !first { // if not first chunk to be sent -> centroids already transmitted
		kmArgs.Centroids = nil
	} else {
		kmArgs.Centroids = centroids
	}

	// Marshaling
	mArgs, err := json.Marshal(&kmArgs)
	errorHandler(err, 259)

	return mArgs
}

/*
 * Prepares a MapInput object for the k-means iteration with the new centroids for each of the Mappers.
 */
func prepareMapArgs(mapperId int, centroids utils.Points) []byte {
	// Arguments
	kmArgs := new(MapInput)
	kmArgs.Id = mapperId
	kmArgs.Centroids = centroids

	// Marshaling
	mArgs, err := json.Marshal(&kmArgs)
	errorHandler(err, 259)

	return mArgs
}

func computeNewCentroids(resp [][]byte) utils.Points {

	var newCentroids utils.Points
	dim := len(resp)

	for i := 0; i < dim; i++ {
		var centroid utils.Point
		// unmarshalling
		err := json.Unmarshal(resp[i], &centroid)
		errorHandler(err, 344)

		newCentroids = append(newCentroids, centroid)
	}

	return newCentroids
}

/*
 * Compute the amount of changes that have been applied in the latest iteration
 */
func computeDelta(newClusters utils.Clusters, datasetDim int) float64 {
	delta := 0.0

	for _, c := range newClusters {
		for _, p := range c.Points {
			if p.From != p.To {
				delta++
			}
		}
	}

	return delta / float64(datasetDim)
}

// error handling
func errorHandler(err error, line int) {
	if err != nil {
		log.Fatalf("failure at line %d: %v", line, err)
	}
}
