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
	maxLoad            = 10000 //every worker operates on a maximum of 'maxLoad' points
	maxNodes           = 10
	deltaThreshold     = 0.01
	iterationThreshold = 10
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
	conf.Mappers = createConnections(conf.NumMappers)
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
	if debug {
		log.Println("--> map started.")
	}

	channels := make([]*rpc.Call, conf.NumMappers)
	results := make([][]byte, conf.NumMappers)
	chunksPerMapper := len(conf.InputPoints[0])

	counter := 0

	var first bool
	var last bool
	newPoints := iteration == 0

	for i := 0; i < chunksPerMapper; i++ {
		first = i == 0
		last = i == chunksPerMapper-1

		for j, cli := range conf.Mappers {
			mapArgs := prepareMapArguments(j, conf.InputPoints[j][i], conf.CurrentCentroids, first, newPoints, last)
			counter += len(conf.InputPoints[j][i])
			channels[j] = cli.Go(service, mapArgs, &results[j], nil)
		}
		// wait for ack
		if !last {
			for j := 0; j < conf.NumMappers; j++ {
				<-channels[j].Done
				//TODO: check ack is successful
			}
		}
	}

	// wait for response
	for i := 0; i < conf.NumMappers; i++ {
		<-channels[i].Done
	}

	if debug {
		log.Println("--> map completed.")
	}

	return results
}

/*---------------------------------------------------- REDUCE --------------------------------------------------------*/
func reduceFunction(service string, initArgs *InitMapOutput, args utils.Clusters) [][]byte {

	if service == reduceService1 {
		return initReduce(*initArgs)
	}

	return reduce(args)
}

func initReduce(arg InitMapOutput) [][]byte {
	if debug {
		log.Print("--> reduce started.")
	}

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
		log.Print("--> reduce completed.")
	}

	return resp
}

func reduce(args utils.Clusters) [][]byte {
	if debug {
		log.Println("--> reduce started ...")
	}

	// prepare reduce phase
	numReducers := len(args)
	kmChannels := make([]*rpc.Call, numReducers)
	kmResp := make([][]byte, numReducers)

	// send a cluster to each reducer
	for i, cluster := range args {
		// create a TCP connection
		cli, err := rpc.DialHTTP(network, address)
		errorHandler(err, 145)

		// marshalling
		rArgs, err := json.Marshal(&cluster)
		errorHandler(err, 149)

		// spawn worker connections
		kmChannels[i] = cli.Go(reduceService2, rArgs, &kmResp[i], nil)
	}

	// wait for response
	for i := 0; i < numReducers; i++ {
		<-kmChannels[i].Done
	}

	if debug {
		log.Println("--> reduce completed.")
	}
	return kmResp
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

func createConnections(numMappers int) []*rpc.Client {
	var err error
	mappers := make([]*rpc.Client, numMappers)
	// create a TCP connection to localhost on port 5678
	for i := 0; i < numMappers; i++ {
		mappers[i], err = rpc.DialHTTP(network, address)
		errorHandler(err, 114)
	}
	return mappers
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
		initRedOutput := reduceFunction(reduceService1, mapOut, nil)

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

func kMeans(configuration *Configuration) (utils.Clusters, string) {
	var reduceOutput [][]byte
	var reply utils.Clusters
	var msg string

	numIter := 0
	for {
		log.Printf("Standard K-Means iteration #%d... ", numIter+1)

		// map
		if debug {
			log.Println("--> activating map service...")
		}
		mapOutput := mapFunction(*configuration, mapService2, -1)
		if debug {
			log.Print("...map service complete. -->\n\n")
		}

		// shuffle and sort
		if debug {
			log.Println("--> shuffle and sort...")
		}
		reduceInput := shuffleAndSort(mapOutput, configuration.NumMappers)
		if debug {
			log.Print("... done. -->\n\n")
		}

		// check update threshold
		delta := computeDelta(reduceInput, configuration.DatasetDim)
		if delta < configuration.DeltaThreshold {
			reply = reduceInput
			msg = fmt.Sprintf("Algorithm converged to a %f%% percentage of membership change after %d iterations",
				delta, numIter)
			break
		}

		// reduce
		if debug {
			log.Println("--> activate reduce service...")
		}
		reduceOutput = reduceFunction(reduceService2, nil, reduceInput)
		if debug {
			log.Print("...reducer service complete. -->\n\n")
		}
		configuration.CurrentCentroids = computeNewCentroids(reduceOutput)
		if debug {
			log.Print("--> new centroids calculated.\n\n")
		}

		numIter++
		if numIter >= configuration.IterationThreshold {
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

	var mapRes utils.Clusters

	for i := 0; i < dim; i++ {
		var temp utils.Clusters

		// unmarshalling
		err := json.Unmarshal(resp[i], &temp)
		errorHandler(err, 273)
		if debug {
			for j := 0; j < len(temp); j++ {
				log.Printf("--> mapper #%d got %d points in cluster %d.\n", i, len(temp[j].Points), j)
			}
		}

		// merging
		if len(mapRes) == 0 {
			mapRes = temp
		} else {
			for j := 0; j < len(mapRes); j++ {
				mapRes[j].Points = append(mapRes[j].Points, temp[j].Points...)
			}
		}
	}

	return mapRes
}

/*
 * Prepares a MapInput object with the centroids and the points for each of the Mappers.
 * Returns the marshalled message for the Map.
 */
func prepareMapArguments(mapperId int, chunk utils.Points, centroids utils.Points, first bool, newPoints bool,
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
