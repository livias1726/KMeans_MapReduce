package main

import (
	"KMeans_MapReduce/utils"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"strings"
	"sync"
)

// MasterServer structure is used to store global information for the server
type MasterServer struct {
	NumRequests int                   // number of clients served (and in service) during the master lifecycle
	Requests    map[string]int        // references to concurrent requests (IP, requestNum)
	Clients     map[int]*MasterClient // references to MasterClient instances that process concurrent requests
	mutex       sync.Mutex            // used to operate on NumRequests, Requests and Clients
}

type MasterClient struct {
	Config Configuration
}

// Configuration : keeps the fundamental data for a MasterClient to process a request
type Configuration struct {
	RequestId          int
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

const (
	debug              = false // set to true to activate debug log (added to the default log)
	networkProtocol    = "tcp"
	address            = "localhost:11090"
	workerAddress      = "localhost:11091"
	mapService1        = "Worker.InitMap"
	reduceService1     = "Worker.InitReduce"
	mapService2        = "Worker.Map"
	reduceService2     = "Worker.Reduce"
	maxLoad            = 1000 // maximum number of points each chunk is composed by
	maxNodes           = 10   // maximum number of mappers that can be spawned (could depend on the resources available)
	deltaThreshold     = 0.01 // threshold for the distance between centroids obtained in two consecutive iterations
	iterationThreshold = 100  // threshold for the maximum number of iteration before stopping the processing
)

// KMeans /*---------------------------------- REMOTE PROCEDURE - CLIENT SIDE ---------------------------------------*/
func (m *MasterServer) KMeans(payload []byte, reply *[]byte) error {
	var (
		kmRequest utils.KMRequest
		err       error
		s         []byte
		resp      utils.KMResponse
	)

	// unmarshalling
	err = json.Unmarshal(payload, &kmRequest)
	errorHandler(err, "request unmarshalling")

	// get client
	if kmRequest.First { // the client (the request) is new
		// update the global info
		m.mutex.Lock()

		m.NumRequests++
		m.Requests[kmRequest.IP] = m.NumRequests

		mc := new(MasterClient)
		m.Clients[m.Requests[kmRequest.IP]] = mc

		m.mutex.Unlock()
	}
	mc := m.Clients[m.Requests[kmRequest.IP]]

	// store new points
	mc.Config.Dataset = append(mc.Config.Dataset, kmRequest.Dataset...)

	// send ack
	if !kmRequest.Last {
		s, err = json.Marshal(true)
		errorHandler(err, "ack marshalling")
		if debug {
			log.Print("--> sending ack.\n")
		}
		*reply = s
		return err
	}

	// finalize the MasterClient object to start processing the request
	mc.Config.RequestId = m.Requests[kmRequest.IP]
	mc.Config.K = kmRequest.K
	log.Printf("Received %d points from %s to cluster in %d groups.",
		len(mc.Config.Dataset), kmRequest.IP, mc.Config.K)

	// call the service
	result, msg := mc.KMeans()

	// preparing response
	resp.Clusters = result
	resp.Message = msg

	// marshalling result
	s, err = json.Marshal(&resp)
	errorHandler(err, "result marshalling")

	// clean up
	m.mutex.Lock()
	// the number of requests is not decreased to avoid having different k-means jobs with the same ID on mapper side
	delete(m.Clients, mc.Config.RequestId)
	delete(m.Requests, kmRequest.IP)
	m.mutex.Unlock()

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
		log.Printf("--> %d mappers to spawn with %d chunk(s).", len(conf.InputPoints), len(conf.InputPoints[0]))
	}
	// create enough connections to communicate with the mappers
	conf.NumReducers = conf.K
	conf.Mappers, conf.Reducers = createConnections(conf.NumMappers, conf.NumReducers)
	defer connClose(append(conf.Mappers, conf.Reducers...))
	if debug {
		log.Printf("--> initialized %d connections to mapper nodes.", len(conf.Mappers))
	}
	// perform k-means++
	kMeanspp(&conf, conf.Dataset)
	if debug {
		log.Printf("--> initialized %d centroids with average distance of %f.",
			len(conf.CurrentCentroids), utils.GetAvgDistanceOfSet(conf.CurrentCentroids))
	}
	// set up stopping criteria
	conf.DeltaThreshold = deltaThreshold
	conf.IterationThreshold = iterationThreshold
	// perform k-means
	clusters, logMsg := kMeans(&conf)
	if debug {
		log.Printf("--> %s\n", strings.ToLower(logMsg))
	}
	// return
	return clusters, logMsg
}

/*------------------------------------------------------- K-MEANS ----------------------------------------------------*/
// implements the k-means++ initialization phase to compute the first set of centroids
func kMeanspp(conf *Configuration, dataset utils.Points) {
	var err error
	// get first random point from dataset
	conf.CurrentCentroids = append(conf.CurrentCentroids, dataset[rand.Intn(len(dataset))])
	// populate the initial set of centroids
	numIter := 1
	for i := 0; i < conf.K-1; i++ {
		log.Printf("K-Means++ (initialization) iteration #%d... ", numIter)

		// init-map + shuffle and sort
		initMapOutput := initMapFunction(len(conf.InputPoints[0]), conf.NumMappers, conf.Mappers, conf.InputPoints,
			conf.CurrentCentroids)
		// init-reduce -> single reducer
		initRedOutput := initReduceFunction(conf.Reducers[0], initMapOutput)

		// unmarshalling
		var newCentroid utils.Point
		err = json.Unmarshal(initRedOutput, &newCentroid)
		errorHandler(err, "new centroid unmarshalling")
		// add the new centroid
		conf.CurrentCentroids = append(conf.CurrentCentroids, newCentroid)

		// iterate
		numIter++
	}
}

// implements the standard k-means algorithm to compute the clustering
func kMeans(conf *Configuration) (utils.Clusters, string) {
	var (
		currClusters []map[int]utils.Points
		reduceInputs []utils.ReduceInput
		msg          string
	)
	numIter := 1
	for {
		log.Printf("Standard K-Means iteration #%d... ", numIter)

		// map
		mapOutputs := mapFunction(len(conf.InputPoints[0]), conf.NumMappers, conf.Mappers, conf.InputPoints,
			conf.CurrentCentroids)
		// shuffle and sort
		currClusters, reduceInputs = sort(mapOutputs)
		// reduce
		reduceOutput := reduceFunction(conf.NumReducers, conf.Reducers, reduceInputs)

		// get the new centroids
		newCentroids := computeNewCentroids(reduceOutput, conf.CurrentCentroids)
		// check delta threshold
		delta := computeDelta(conf.CurrentCentroids, newCentroids)
		if delta < conf.DeltaThreshold {
			msg = fmt.Sprintf("Algorithm converged to a %f%% change in centroids position after %d iterations",
				delta, numIter)
			break
		}

		// update the current centroids
		conf.CurrentCentroids = newCentroids
		// check iteration threshold
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
	// prepare results and return
	clusters := prepareClusters(conf.K, conf.CurrentCentroids, currClusters)
	return clusters, msg
}

/*------------------------------------------------------- MAP --------------------------------------------------------*/
// map task gets called with the current set of centroids (chunks, centroids)
func initMapFunction(chunksPerMapper int, numMappers int, mappers []*rpc.Client, chunks [][]utils.Points,
	centroids utils.Points) utils.InitMapOutput {
	if debug {
		log.Println("--> init-map phase started... ")
	}
	var wg sync.WaitGroup
	channels := make([]*rpc.Call, numMappers)
	results := make([]utils.InitMapOutput, chunksPerMapper)
	// send chunks and centroids
	wg.Add(chunksPerMapper)
	for i := 0; i < chunksPerMapper; i++ { // send i-th chunk to every mapper
		replies := make([][]byte, numMappers)
		for j, cli := range mappers {
			mapArgs := prepareInitMapArgs(chunks[j][i], centroids)
			channels[j] = cli.Go(mapService1, mapArgs, &replies[j], nil) // call j-th mapper
		}
		// wait for response
		for k := 0; k < numMappers; k++ {
			<-channels[k].Done
		}
		// process replies asynchronously
		go func(idx int) {
			initShuffle(replies, &results[idx])
			wg.Done()
		}(i)
	}
	// synchronize ss threads to merge results
	wg.Wait()
	// return
	initMapOutput := initSort(results)
	return initMapOutput
}

// prepares a InitMapInput object for the k-means++ map task
func prepareInitMapArgs(chunk utils.Points, centroids utils.Points) []byte {
	// arguments
	initMapArgs := new(utils.InitMapInput)
	initMapArgs.Centroids = centroids
	initMapArgs.Chunk = chunk
	// marshaling
	mArgs, err := json.Marshal(&initMapArgs)
	errorHandler(err, "init-map input marshalling")
	// return
	return mArgs
}

// map task gets called with the current set of centroids (_, centroids)
func mapFunction(chunksPerMapper int, numMappers int, mappers []*rpc.Client, chunks [][]utils.Points,
	centroids utils.Points) []utils.MapOutput {
	if debug {
		log.Print("--> map phase started... ")
	}
	var wg sync.WaitGroup
	channels := make([]*rpc.Call, numMappers)
	results := make([]utils.MapOutput, chunksPerMapper)
	// send chunks and centroids
	wg.Add(chunksPerMapper)
	for i := 0; i < chunksPerMapper; i++ {
		replies := make([][]byte, numMappers)
		for j, cli := range mappers {
			mapArgs := prepareMapArgs(chunks[j][i], centroids)           // send i-th chunk to every mapper
			channels[j] = cli.Go(mapService2, mapArgs, &replies[j], nil) // call j-th mapper
		}
		// wait for response
		for k := 0; k < numMappers; k++ {
			<-channels[k].Done
		}
		// process replies asynchronously
		go func(idx int) {
			shuffle(replies, &results[idx])
			wg.Done()
		}(i)
	}
	// synchronize ss threads to merge results
	wg.Wait()
	// return
	return results
}

// prepares a MapInput object for the k-means map task
func prepareMapArgs(chunk utils.Points, centroids utils.Points) []byte {
	// arg
	mapArg := new(utils.MapInput)
	mapArg.Centroids = centroids
	mapArg.Chunk = chunk
	// marshalling
	mArgs, err := json.Marshal(&mapArg)
	errorHandler(err, "map input marshalling")
	// return
	return mArgs
}

/*----------------------------------------------- SHUFFLE & SORT -----------------------------------------------------*/
// merges the nearest points (combined), wrt the centroids, obtained from each mapper (InitMapOutput)
func initShuffle(replies [][]byte, shuffled *utils.InitMapOutput) {
	if debug {
		log.Println("--> shuffle...")
	}
	var initMapOut utils.InitMapOutput
	for _, out := range replies {
		// unmarshalling
		var temp utils.InitMapOutput
		err := json.Unmarshal(out, &temp)
		errorHandler(err, "init-map output unmarshalling")
		// merge
		initMapOut.Points = append(initMapOut.Points, temp.Points...)
		initMapOut.MinDistances = append(initMapOut.MinDistances, temp.MinDistances...)
	}
	// return
	*shuffled = initMapOut
}

func initSort(output []utils.InitMapOutput) utils.InitMapOutput {
	var initMapOut utils.InitMapOutput
	for _, out := range output {
		initMapOut.Points = append(initMapOut.Points, out.Points...)
		initMapOut.MinDistances = append(initMapOut.MinDistances, out.MinDistances...)
	}
	return initMapOut
}

// merges the partial sums (combined) obtained from the clustering of every mapper to get the set of reduce inputs
func shuffle(replies [][]byte, shuffled *utils.MapOutput) {
	if debug {
		log.Println("--> shuffle...")
	}
	var (
		mapOut utils.MapOutput
		temp   utils.MapOutput
	)
	pMap := make(map[int]utils.Points)
	sMap := make(map[int]utils.Points)
	lMap := make(map[int]int)
	for _, m := range replies {
		// unmarshalling
		err := json.Unmarshal(m, &temp)
		errorHandler(err, "map output unmarshalling")
		// merge
		for cid, points := range temp.Clusters {
			_, ok := pMap[cid]
			if ok { // if the cluster id is already in the maps, append the new results
				pMap[cid] = append(pMap[cid], points...)
				sMap[cid] = append(sMap[cid], temp.Sum[cid]...)
				lMap[cid] += temp.Len[cid]
			} else { // else add the results under the new cluster id
				pMap[cid] = points
				sMap[cid] = temp.Sum[cid]
				lMap[cid] = temp.Len[cid]
			}
		}
	}
	// return
	mapOut.Clusters = pMap
	mapOut.Sum = sMap
	mapOut.Len = lMap
	*shuffled = mapOut
}

func sort(mapOut []utils.MapOutput) ([]map[int]utils.Points, []utils.ReduceInput) {
	clusters := make([]map[int]utils.Points, len(mapOut))
	redIn := make(map[int]utils.ReduceInput)
	for i, mo := range mapOut {
		sums := mo.Sum
		// reduce inputs
		var ri utils.ReduceInput
		for cid, sum := range sums {
			ri.ClusterId = cid
			_, in := redIn[cid] // cid is not in the map
			if !in {
				ri.Points = sum
				ri.Len = mo.Len[cid]
			} else {
				ri.Points = append(redIn[cid].Points, sum...)
				ri.Len = redIn[cid].Len + mo.Len[cid]
			}
			redIn[cid] = ri // overwrite with updated data
		}
		// current clusters
		clusters[i] = mo.Clusters
	}
	// get values
	var reduceInput []utils.ReduceInput
	for _, val := range redIn {
		reduceInput = append(reduceInput, val)
	}
	// return
	if debug {
		log.Print("\t\t\t...sort.")
	}
	return clusters, reduceInput
}

/*---------------------------------------------------- REDUCE --------------------------------------------------------*/
// reduce task gets called with the farther points from the centroids (and the respective minimum distances)
// obtained from 'initShuffleAndSort' (points, minDistances)
func initReduceFunction(reducer *rpc.Client, arg utils.InitMapOutput) []byte {
	if debug {
		log.Print("--> init-reduce phase started...")
	}
	// prepare init reduce arguments - marshalling
	rArgs, err := json.Marshal(&arg)
	errorHandler(err, "init-reduce input marshalling")
	// call reducer synchronously: only 1 reducer is needed and the master cannot continue before receiving its results
	var resp []byte
	err = reducer.Call(reduceService1, rArgs, &resp)
	errorHandler(err, "init-reduce call")
	// return
	if debug {
		log.Print("\t\t\t...completed.")
	}
	return resp
}

// reduce task gets called with the cluster id the reducer works on and the set of sums obtained from the s&s phase
// -> (id, sums)
// it also receives the dimension (len) of the cluster related to the id, but it does not use it
func reduceFunction(numReducers int, reducers []*rpc.Client, args []utils.ReduceInput) [][]byte {
	if debug {
		log.Print("--> reduce phase started...")
	}
	// prepare reduce phase
	channels := make([]*rpc.Call, numReducers)
	results := make([][]byte, numReducers)
	// send a cluster to each reducer
	for i, cli := range reducers {
		// marshalling
		rArgs := prepareReduceArgs(args[i].ClusterId, args[i].Points, args[i].Len)
		// call the service: 1 reducer per cluster
		channels[i] = cli.Go(reduceService2, rArgs, &results[i], nil)
	}
	// wait for response
	for i := 0; i < numReducers; i++ {
		<-channels[i].Done
	}
	// return
	if debug {
		log.Print("\t\t\t...completed.")
	}
	return results
}

// prepares a ReduceInput object for the k-means reduce task
func prepareReduceArgs(clusterId int, points utils.Points, length int) []byte {
	// arguments
	redArg := new(utils.ReduceInput)
	redArg.ClusterId = clusterId
	redArg.Points = points
	redArg.Len = length
	// marshalling
	rArgs, err := json.Marshal(&redArg)
	errorHandler(err, "reduce input marshalling")
	// return
	return rArgs
}

/*------------------------------------------------------ MAIN --------------------------------------------------------*/
func main() {
	// create the server object to receive requests with its global metadata
	master := new(MasterServer)
	master.Clients = make(map[int]*MasterClient)
	master.Requests = make(map[string]int)
	master.NumRequests = 0
	// publish the service
	err := rpc.Register(master)
	errorHandler(err, "service register")
	// spawn async server to call Accept (blocking)
	go serveClients()
	// infinite loop to stay alive
	select {}
}

// thread to receive and dispatch new incoming requests
func serveClients() {
	// resolve master address
	addr, err := net.ResolveTCPAddr(networkProtocol, address)
	errorHandler(err, "tcp address resolution")
	// register a HTTP handler
	rpc.HandleHTTP()
	// listen to TCP connections
	listen, err := net.ListenTCP(networkProtocol, addr)
	errorHandler(err, "tcp listener creation")
	// audit
	log.Printf("Serving clients on: %s\n", addr)
	// infinite loop: serve new incoming requests on master:11090
	for {
		rpc.Accept(listen)
	}
}

/* ----------------------------------------------- UTILS -------------------------------------------------------------*/
// computes chunks of max 'maxLoad' points from the dataset and the necessary number of mappers (max 'maxNodes')
func getChunks(dataset utils.Points) ([][]utils.Points, int) {
	// total number of points to distribute
	numPoints := len(dataset)
	// get correct number of mappers to spawn
	numMappers := int(math.Ceil(float64(numPoints) / float64(maxLoad)))
	if numMappers >= maxNodes {
		numMappers = maxNodes
	}
	// initialize rows of input points matrix (InputPoints[mapper_id][chunk_num])
	inputPoints := make([][]utils.Points, numMappers)
	// populate chunks
	j := 0
	count := 0
	p1 := 0
	for i := 0; i < numMappers; i++ {
		// equally partition points on numMappers
		p2 := ((1 + i) * numPoints) / numMappers
		points := p2 - p1
		p1 = p2
		// get correct number of chunks per mapper
		numChunks := int(math.Ceil(float64(points) / float64(maxLoad)))
		inputPoints[i] = make([]utils.Points, numChunks)
		// populate chunks
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
	// return chunks and the number of mappers to use
	return inputPoints, numMappers
}

// used to avoid dialling mappers and reducers every time a rpc is called
func createConnections(numMappers int, numReducers int) ([]*rpc.Client, []*rpc.Client) {
	var err error
	// create a TCP connection to mappers
	mappers := make([]*rpc.Client, numMappers)
	for i := 0; i < numMappers; i++ {
		mappers[i], err = rpc.DialHTTP(networkProtocol, workerAddress)
		errorHandler(err, "mapper dialling")
	}
	// create a TCP connection to reducers
	reducers := make([]*rpc.Client, numReducers)
	for i := 0; i < numReducers; i++ {
		reducers[i], err = rpc.DialHTTP(networkProtocol, workerAddress)
		errorHandler(err, "reducer dialling")
	}
	// return
	return mappers, reducers
}

// computes the change in the centroids position that occurred in the latest iteration
func computeDelta(oldCentroids utils.Points, newCentroids utils.Points) float64 {
	dim := len(oldCentroids)
	delta := 0.0
	for i := 0; i < dim; i++ {
		delta += utils.GetDistance(oldCentroids[i].Coordinates, newCentroids[i].Coordinates)
	}
	return delta / float64(dim)
}

// computes the new set of centroids from the latest k-means iteration
func computeNewCentroids(resp [][]byte, oldCentroids utils.Points) utils.Points {
	newCentroids := make(utils.Points, len(oldCentroids))
	// reduce output does not have to contain data for every cluster (no mapper added a point to a given cluster)
	// the missing ones are the old ones
	copy(newCentroids, oldCentroids)
	// scan reduce outputs
	var out utils.ReduceOutput
	for _, r := range resp {
		// unmarshalling
		err := json.Unmarshal(r, &out)
		errorHandler(err, "reduce output unmarshalling")
		// compute the new centroid from the reducer output
		var centroid utils.Point
		centroid.Coordinates = make([]float64, len(out.Point.Coordinates))
		for j, coord := range out.Point.Coordinates {
			centroid.Coordinates[j] = coord / float64(out.Len)
		}
		newCentroids[out.ClusterId] = centroid
	}
	// return the new centroids
	return newCentroids
}

// merges the partial clusters from every mapper into the actual clusters (with their centroids) to pass to the client
func prepareClusters(k int, centroids utils.Points, output []map[int]utils.Points) utils.Clusters {
	currClusters := mergeClusters(output)
	clusters := make(utils.Clusters, k)
	for i := 0; i < k; i++ {
		clusters[i].Centroid = centroids[i]
		clusters[i].Points = currClusters[i]
	}
	return clusters
}

func mergeClusters(output []map[int]utils.Points) map[int]utils.Points {
	// create the set of ReduceInput objects
	clusters := make(map[int]utils.Points)
	for _, m := range output {
		for cid, points := range m {
			_, in := clusters[cid]
			if in {
				clusters[cid] = append(clusters[cid], points...)
			} else {
				clusters[cid] = points
			}
		}
	}
	return clusters
}

// used in defer mode to close the connections to worker nodes when computation is over
func connClose(conn []*rpc.Client) {
	for _, cli := range conn {
		func(file io.Closer) {
			err := file.Close()
			errorHandler(err, "connection closure")
		}(cli)
	}
}

// error handling logic
func errorHandler(err error, pof string) {
	if err != nil {
		log.Fatalf("%s failure: %v", pof, err)
	}
}
