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

type MasterServer int
type MasterClient int

// Configuration :
type Configuration struct {
	DatasetDim         int
	CurrentCentroids   utils.Points   // list of current centroids to re-send to mappers
	InputPoints        []utils.Points // chunks of dataset points divided by mapper
	NumMappers         int            // number of mappers currently active
	NumReducers        int            // number of reducers currently active
	DeltaThreshold     float64        // stop condition on updates
	IterationThreshold int            // stop condition on number of iteration
}

// KMRequest : matches with struct on client side
type KMRequest struct {
	Dataset utils.Points
	K       int
}

// KMResponse : matches with struct on client side
type KMResponse struct {
	Clusters utils.Clusters
	Message  string
}

// MapInput : matches with struct on worker side
type MapInput struct {
	Centroids utils.Points
	Points    utils.Points
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
	deltaThreshold     = 0.01
	iterationThreshold = 10
)

// KMeans /*---------------------------------- REMOTE PROCEDURE - CLIENT SIDE ---------------------------------------*/
func (m *MasterServer) KMeans(payload []byte, reply *[]byte) error {

	var kmRequest KMRequest

	// unmarshalling
	err := json.Unmarshal(payload, &kmRequest)
	errorHandler(err, 51)
	if debug {
		log.Printf("--> unmarshalled %d points to cluster in %d groups.",
			len(kmRequest.Dataset), kmRequest.K)
	}

	// initialize configuration
	conf := new(Configuration)
	initialize(conf, kmRequest)

	// call the service
	master := new(MasterClient)
	result, msg := master.KMeans(conf)

	// preparing response
	var resp KMResponse
	resp.Clusters = result
	resp.Message = msg

	// marshalling result
	s, err := json.Marshal(&resp)
	errorHandler(err, 64)
	if debug {
		log.Print("--> master returning.\n")
	}

	*reply = s
	return err
}

// KMeans /*------------------------------------- REMOTE PROCEDURE - WORKER SIDE -------------------------------------*/
func (mc *MasterClient) KMeans(configuration *Configuration) (utils.Clusters, string) {

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
		mapOutput := mapFunction(*configuration, mapService2)
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

/*------------------------------------------------------- MAP --------------------------------------------------------*/
func mapFunction(conf Configuration, service string) [][]byte {
	if debug {
		log.Println("--> map started.")
	}

	channels := make([]*rpc.Call, conf.NumMappers)
	results := make([][]byte, conf.NumMappers)
	// send a chunk to each mapper
	for i, chunk := range conf.InputPoints {
		// create a TCP connection to localhost on port 5678
		cli, err := rpc.DialHTTP(network, address)
		errorHandler(err, 114)

		mapArgs := prepareMapArguments(chunk, conf.CurrentCentroids)
		// spawn worker connections
		channels[i] = cli.Go(service, mapArgs, &results[i], nil)
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
	// Generate a random port for the client
	rand.Seed(time.Now().UTC().UnixNano())
	max := 50005
	min := 50000

	portNum := rand.Intn(max-min) + min
	port := strconv.Itoa(portNum)

	go serveClients(port) // spawn async server

	master := new(MasterServer)
	// Publish the receiver methods
	err := rpc.Register(master)
	errorHandler(err, 180)
	if debug {
		log.Print("--> master node is online.\n")
	}

	select {} //infinite loop
}

func serveClients(port string) {
	addr, err := net.ResolveTCPAddr(network, "0.0.0.0:"+port)
	errorHandler(err, 132)

	// Register a HTTP handler
	rpc.HandleHTTP()

	// Listen to TCP connections
	listen, err := net.ListenTCP(network, addr)
	errorHandler(err, 194)

	log.Printf("Serving RPC server on address %s , port %s\n", addr, port)

	for {
		// serve the new client
		rpc.Accept(listen)
	}
}

/*-------------------------------------------- LOCAL FUNCTIONS -------------------------------------------------------*/
func initialize(conf *Configuration, request KMRequest) {
	getChunks(conf, request.Dataset)
	if debug {
		log.Printf("--> initialized %d chunks with an average of %d points each.\n",
			len(conf.InputPoints), utils.GetAvgCapacityOfSet(conf.InputPoints))
	}

	kMeanspp(conf, request.Dataset, request.K)
	if debug {
		log.Printf("--> initialized %d centroids with average distance of %f.\n",
			len(conf.CurrentCentroids), utils.GetAvgDistanceOfSet(conf.CurrentCentroids))
	}

	conf.DatasetDim = len(request.Dataset)
	conf.DeltaThreshold = deltaThreshold
	conf.IterationThreshold = iterationThreshold
}

/*
 * Get chunks of max 1000 points from a given request
 */
func getChunks(conf *Configuration, points utils.Points) {
	// distribute dataset points among the mappers
	var pointsPerWorker int
	numPoints := len(points)

	conf.NumMappers = int(math.Ceil(float64(numPoints) / float64(maxLoad)))

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
	//TODO: first round of map connections -> establish a connection and transmit the chunks only 1 time
}

func kMeanspp(conf *Configuration, points utils.Points, k int) {
	var err error

	// get first random point from dataset
	conf.CurrentCentroids, err = utils.Init(1, points)
	errorHandler(err, 257)

	// populate the initial set of centroids
	numIter := 0
	for i := 0; i < k; i++ {
		log.Printf("K-Means++ (initialization) iteration #%d... ", numIter+1)

		// init-map
		initMapOutput := mapFunction(*conf, mapService1)

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

/*
 * Prepares a MapInput object with the centroids and the points for each of the Mappers.
 * Returns the marshalled message for the Map.
 */
func prepareMapArguments(chunk utils.Points, centroids utils.Points) []byte {
	// Arguments
	kmArgs := new(MapInput)
	kmArgs.Centroids = centroids
	kmArgs.Points = chunk

	// Marshaling
	mArgs, err := json.Marshal(&kmArgs)
	errorHandler(err, 259)

	return mArgs
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
