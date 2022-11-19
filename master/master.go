package main

import (
	"KMeans_MapReduce/utils"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
)

//TODO: balance the load on the reducers (and ss phase if possible)

type MasterServer struct {
	Clients map[string]*MasterClient
}

type MasterClient struct {
	Config Configuration
}

// Configuration :
type Configuration struct {
	Dataset            utils.Points
	K                  int
	CurrentCentroids   utils.Points     // list of current centroids to re-send to mappers
	InputPoints        [][]utils.Points // chunks of dataset points divided by mapper
	Mappers            []*rpc.Client    // list of mappers to communicate with
	Reducers           []*rpc.Client    // list of reducers to communicate with
	NumMappers         int              // number of mappers currently active
	NumReducers        int              // number of reducers currently active
	DeltaThreshold     float64          // stop condition on updates
	IterationThreshold int              // stop condition on number of iteration
}

// KMRequest : matches with struct on client side
type KMRequest struct {
	IP      string
	Dataset utils.Points
	K       int
	First   bool
	Last    bool
}

// KMResponse : matches with struct on client side
type KMResponse struct {
	Clusters utils.Clusters
	Message  string
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
	Points    utils.Points
	Distances []float64
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
	debug              = true
	networkProtocol    = "tcp"
	address            = "master:11090"
	workerAddress      = "worker:11091"
	mapService1        = "Worker.InitMap"
	reduceService1     = "Worker.InitReduce"
	mapService2        = "Worker.Map"
	reduceService2     = "Worker.Reduce"
	maxLoad            = 1000 //every worker operates on a maximum of 'maxLoad' points
	maxNodes           = 10
	deltaThreshold     = 0.01
	iterationThreshold = 100
)

// KMeans /*---------------------------------- REMOTE PROCEDURE - CLIENT SIDE ---------------------------------------*/
func (m *MasterServer) KMeans(payload []byte, reply *[]byte) error {
	var (
		kmRequest KMRequest
		err       error
		s         []byte
		resp      KMResponse
	)

	// unmarshalling
	err = json.Unmarshal(payload, &kmRequest)
	errorHandler(err, 51)

	// get client
	if kmRequest.First {
		mc := new(MasterClient)
		m.Clients[kmRequest.IP] = mc
	}
	mc := m.Clients[kmRequest.IP]

	// store new points
	mc.Config.Dataset = append(mc.Config.Dataset, kmRequest.Dataset...)

	// send ack
	if !kmRequest.Last {
		s, err = json.Marshal(true)
		errorHandler(err, 106)

		*reply = s
		return err
	}

	// finalize
	mc.Config.K = kmRequest.K
	if debug {
		log.Printf("--> received %d points from %s to cluster in %d groups.",
			len(mc.Config.Dataset), kmRequest.IP, mc.Config.K)
	}

	// call the service
	result, msg := mc.KMeans()

	// preparing response
	resp.Clusters = result
	resp.Message = msg

	// marshalling result
	s, err = json.Marshal(&resp)
	errorHandler(err, 125)

	//TODO: cleanup client

	// return
	if debug {
		log.Print("--> master returning.\n")
	}
	*reply = s
	return err
}

// KMeans /*------------------------------------- REMOTE PROCEDURE - WORKER SIDE -------------------------------------*/
func (mc *MasterClient) KMeans() (utils.Clusters, string) {
	conf := mc.Config

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
	kMeanspp(&conf, conf.Dataset)
	if debug {
		log.Printf("--> initialized %d centroids with average distance of %f.\n",
			len(conf.CurrentCentroids), utils.GetAvgDistanceOfSet(conf.CurrentCentroids))
	}

	conf.DeltaThreshold = deltaThreshold
	conf.IterationThreshold = iterationThreshold

	// perform k-means
	clusters, logMsg := kMeans(&conf)

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
		if !last {
			checkAck(conf, channels, results, i, first, newPoints, last)
		}
	}

	// wait for response
	for i := 0; i < conf.NumMappers; i++ {
		<-channels[i].Done
	}

	// return
	if debug {
		log.Println("\t\t\t...completed.")
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
		log.Println("\t\t\t...completed.")
	}

	return results
}

/*---------------------------------------------------- REDUCE --------------------------------------------------------*/
func reduceFunction(conf Configuration, service string, initArgs *InitMapOutput, args *MapOutput) [][]byte {

	if service == reduceService1 {
		return initReduce(conf, *initArgs)
	}

	return reduce(conf, *args)
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
		log.Print("\t\t\t...completed.")
	}

	return resp
}

func reduce(conf Configuration, args MapOutput) [][]byte {
	if debug {
		log.Print("--> reduce phase started...")
	}

	// prepare reduce phase
	channels := make([]*rpc.Call, conf.NumReducers)
	results := make([][]byte, conf.NumReducers)

	// send a cluster to each reducer
	for i, cli := range conf.Reducers {
		// marshalling
		rArgs := prepareReduceArgs(args.ClusterId[i], args.Points[i], args.Len[i])
		// call service
		channels[i] = cli.Go(reduceService2, rArgs, &results[i], nil)
	}

	// wait for response
	for i := 0; i < conf.NumReducers; i++ {
		<-channels[i].Done
	}

	// return
	if debug {
		log.Print("\t\t\t...completed.")
	}
	return results
}

/*------------------------------------------------------ MAIN -------------------------------------------------------*/
func main() {
	master := new(MasterServer)
	master.Clients = make(map[string]*MasterClient)

	// publish the methods
	err := rpc.Register(master)
	errorHandler(err, 375)

	// spawn async server
	go serveClients()

	select {} //infinite loop
}

func serveClients() {
	addr, err := net.ResolveTCPAddr(networkProtocol, address)
	errorHandler(err, 337)

	// register a HTTP handler
	rpc.HandleHTTP()

	// listen to TCP connections
	listen, err := net.ListenTCP(networkProtocol, addr)
	errorHandler(err, 344)

	log.Printf("Serving clients on: %s\n", addr)

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
	// create a TCP connection to mappers
	for i := 0; i < numMappers; i++ {
		mappers[i], err = rpc.DialHTTP(networkProtocol, workerAddress)
		errorHandler(err, 454)
	}

	// reducers
	reducers := make([]*rpc.Client, numReducers)
	// create a TCP connection to reducers
	for i := 0; i < numReducers; i++ {
		reducers[i], err = rpc.DialHTTP(networkProtocol, workerAddress)
		errorHandler(err, 462)
	}

	return mappers, reducers
}

func kMeanspp(conf *Configuration, dataset utils.Points) {
	var err error

	// get first random point from dataset
	conf.CurrentCentroids, err = utils.Init(1, dataset)
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

		// add the new centroid
		var newCentroid utils.Point
		err = json.Unmarshal(initRedOutput[0], &newCentroid)
		errorHandler(err, 385)
		conf.CurrentCentroids = append(conf.CurrentCentroids, newCentroid)

		// iterate
		numIter++
	}
}

// merges the nearest points (combined) wrt the centroids obtained from each mapper
func initShuffleAndSort(outs [][]byte) *InitMapOutput {
	initMapOut := new(InitMapOutput)

	var tempMapOut InitMapOutput
	for _, out := range outs {
		err := json.Unmarshal(out, &tempMapOut)
		errorHandler(err, 306)
		// merge
		initMapOut.Points = append(initMapOut.Points, tempMapOut.Points...)
		initMapOut.Distances = append(initMapOut.Distances, tempMapOut.Distances...)
	}

	return initMapOut
}

func kMeans(conf *Configuration) (utils.Clusters, string) {
	var reduceInput MapOutput
	var msg string

	numIter := 1
	for {
		log.Printf("Standard K-Means iteration #%d... ", numIter)

		// map
		mapOutput := mapFunction(*conf, mapService2, 0)
		// shuffle and sort
		reduceInput = shuffleAndSort(mapOutput)
		// reduce
		reduceOutput := reduceFunction(*conf, reduceService2, nil, &reduceInput)
		// check update threshold
		newCentroids := computeNewCentroids(reduceOutput, conf.CurrentCentroids)
		delta := computeDelta(conf.CurrentCentroids, newCentroids)
		if delta < conf.DeltaThreshold {
			msg = fmt.Sprintf("Algorithm converged to a %f%% change in centroids position after %d iterations",
				delta, numIter)
			break
		}
		conf.CurrentCentroids = newCentroids
		// check num of iterations
		if numIter >= conf.IterationThreshold {
			msg = fmt.Sprintf("Algorithm terminated after reaching the maximum number of iterations (%d). ",
				numIter)
			msg = msg + fmt.Sprintf("Last delta obtained in points classification changes is %f",
				delta)
			break
		}
		// iterate
		numIter++
	}

	clusters := make(utils.Clusters, conf.K)
	for i := 0; i < conf.K; i++ {
		clusters[i].Centroid = conf.CurrentCentroids[i]
		clusters[i].Points = reduceInput.Points[i]
	}

	return clusters, msg
}

// merges the partial (combined) clusters from every mapper in the actual clusters to pass to the reducers
func shuffleAndSort(resp [][]byte) MapOutput {
	if debug {
		log.Println("--> shuffle and sort...")
	}

	pMap := make(map[int]utils.Points)
	lMap := make(map[int]int)
	for _, m := range resp {
		// unmarshalling
		var temp MapOutput
		err := json.Unmarshal(m, &temp)
		errorHandler(err, 584)

		// merge
		for j, cid := range temp.ClusterId {
			_, ok := pMap[cid]
			if ok {
				pMap[cid] = append(pMap[cid], temp.Points[j]...)
				lMap[cid] += temp.Len[j]
			} else {
				pMap[cid] = temp.Points[j]
				lMap[cid] = temp.Len[j]
			}
		}
	}

	mapRes := new(MapOutput)
	for k, v := range pMap {
		mapRes.ClusterId = append(mapRes.ClusterId, k)
		mapRes.Points = append(mapRes.Points, v)
		mapRes.Len = append(mapRes.Len, lMap[k])
	}

	if debug {
		log.Print("\t\t\t...completed.")
	}
	return *mapRes
}

/*
 * Prepares a InitMapInput object for the k-means++ iteration with (eventually) the current centroids and the points
 * for each Mapper.
 */
func prepareInitMapArgs(mapperId int, chunk utils.Points, centroids utils.Points, first bool, newPoints bool,
	last bool) []byte {
	// Arguments
	initMapArgs := new(InitMapInput)
	initMapArgs.MapperId = mapperId

	if !newPoints { // if not first iteration of map -> points already transmitted
		initMapArgs.Chunk = nil
	} else {
		initMapArgs.Chunk = chunk
	}

	initMapArgs.First = first
	initMapArgs.NewPoints = newPoints
	initMapArgs.Last = last

	if !first { // if not first chunk to be sent -> centroids already transmitted
		initMapArgs.Centroids = nil
	} else {
		initMapArgs.Centroids = centroids
	}

	// Marshaling
	mArgs, err := json.Marshal(&initMapArgs)
	errorHandler(err, 259)

	return mArgs
}

/*
 * Prepares a InitMapInput object for the k-means iteration with the new centroids for each of the Mappers.
 */
func prepareMapArgs(mapperId int, centroids utils.Points) []byte {
	// arg
	mapArg := new(MapInput)
	mapArg.MapperId = mapperId
	mapArg.Centroids = centroids

	// Marshaling
	mArgs, err := json.Marshal(&mapArg)
	errorHandler(err, 655)

	return mArgs
}

func prepareReduceArgs(clusterId int, points utils.Points, length int) []byte {
	// arg
	redArg := new(ReduceInput)
	redArg.ClusterId = clusterId
	redArg.Points = points
	redArg.Len = length

	rArgs, err := json.Marshal(&redArg)
	errorHandler(err, 149)

	return rArgs
}

func computeNewCentroids(resp [][]byte, oldCentroids utils.Points) utils.Points {

	newCentroids := make(utils.Points, len(oldCentroids))
	copy(newCentroids, oldCentroids)

	var out ReduceOutput
	for _, r := range resp {
		// unmarshalling
		err := json.Unmarshal(r, &out)
		errorHandler(err, 682)

		// compute
		var centroid utils.Point
		centroid.Coordinates = make([]float64, len(out.Point.Coordinates))
		for j, coord := range out.Point.Coordinates {
			centroid.Coordinates[j] = coord / float64(out.Len)
		}

		newCentroids[out.ClusterId] = centroid
	}

	return newCentroids
}

// compute the amount of changes that have been applied in the latest iteration
func computeDelta(oldCentroids utils.Points, newCentroids utils.Points) float64 {
	dim := len(oldCentroids)

	delta := 0.0
	for i := 0; i < dim; i++ {
		delta += utils.GetDistance(oldCentroids[i].Coordinates, newCentroids[i].Coordinates)
	}

	return delta / float64(dim)
}

// error handling
func errorHandler(err error, line int) {
	if err != nil {
		log.Fatalf("failure at line %d: %v", line, err)
	}
}
