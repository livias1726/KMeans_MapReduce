package main

import (
	"KMeanMR/utils"
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	//"sort"
	"strconv"
	"time"
)

var port string

type MapResponse struct {
	Clusters utils.Clusters
}

type MapRequest struct {
	Centroids utils.Points
	Points    utils.Points
}

type ReduceArgs struct {
	Key    string
	Values []string
}

type MasterServer int

type MasterClient struct {
	numWorkers int
}

const (
	debug         = true
	network       = "tcp"
	address       = "localhost:5678"
	mapService    = "Worker.Map"
	reduceService = "Worker.Reduce"
	maxLoad       = 1000 //every worker operates on a maximum of 'maxLoad' points
)

// KMeans /*---------------------------------- REMOTE PROCEDURE - CLIENT SIDE ---------------------------------------*/
func (m *MasterServer) KMeans(payload []byte, reply *[]byte) error {
	var inArgs MapRequest
	// Unmarshalling of request
	err := json.Unmarshal(payload, &inArgs)
	errorHandler(err, 57)
	if debug {
		log.Printf("Unmarshalled: %v", inArgs.Points)
	}

	// call the service
	master := new(MasterClient)
	err = master.KMeans(inArgs) //err = master.KMeans(outArgs)

	/*
		result, err := master.KMeans(clusters)
		errorHandler(err, 64)

		// Marshalling of result
		s, err := json.Marshal(&result)
		if debug {
			log.Printf("Marshaled Data: %s", s)
		}

		*reply = s

	*/
	return err
}

// KMeans /*------------------------------------- REMOTE PROCEDURE - WORKER SIDE -------------------------------------*/
func (mc *MasterClient) KMeans(outArgs MapRequest) error {
	//MAP PHASE
	log.Println("-->Activate Map Service on workers...")
	mapFunction(mc, outArgs) //mapResp := mapFunction(mc, outArgs)
	log.Print("...Done: All the workers returned from map -->\n\n")

	/*
		//SHUFFLE AND SORT PHASE
		log.Println("-->Do Shuffle and sort...")
		mapOutput, err := mergeMapResults(mapResp, mc.numWorkers)
		reduceInput := shuffleAndSort(mapOutput)
		log.Print("...Done: Shuffle and sort -->\n\n")

		//REDUCE PHASE
		log.Println("-->Activate Reduce Service on workers...")
		redResp := reduceFunction(mc, reduceInput)
		log.Print("...Done: All the workers returned from reduce -->\n\n")

		reply, err := mergeFinalResults(redResp, mc.numWorkers)
		if debug {
			log.Printf("Map Data: %s", mapResp)
			log.Printf("Reduced Data: %s", redResp)
			log.Printf("Reply: %s", reply)
		}

		return reply, err

	*/
	return nil
}

/*------------------------------------------------------- MAP --------------------------------------------------------*/
func mapFunction(mc *MasterClient, args MapRequest) [][]byte {
	// chunk the collection using getChunks function
	chunks := getChunks(args.Points, mc)

	//prepare results
	kmChannels := make([]*rpc.Call, mc.numWorkers)
	kmResp := make([][]byte, mc.numWorkers)

	//SEND CHUNKS TO MAPPERS
	for i, chunk := range chunks {
		//create a TCP connection to localhost on port 5678
		cli, err := rpc.DialHTTP(network, address)
		errorHandler(err, 146)

		mArgs := prepareMapArguments(chunk, args.Centroids)

		//spawn worker connections
		kmChannels[i] = cli.Go(mapService, mArgs, &kmResp[i], nil)

		log.Printf("Worker #%d spawned.", i)
	}

	//wait for response
	for i := 0; i < mc.numWorkers; i++ {
		<-kmChannels[i].Done
		log.Printf("Worker #%d completed.\n", i)
	}

	return kmResp
}

/*---------------------------------------------------- REDUCE --------------------------------------------------------*/
func reduceFunction(mc *MasterClient, redIn []ReduceArgs) [][]byte {

	mc.numWorkers = len(redIn)

	//prepare results
	grepChan := make([]*rpc.Call, mc.numWorkers)
	grepResp := make([][]byte, mc.numWorkers)

	//SEND CHUNKS TO REDUCERS
	for i, chunk := range redIn {
		//create a TCP connection to localhost on port 5678
		cli, err := rpc.DialHTTP(network, address)
		errorHandler(err, 110)

		// Marshaling
		rArgs, err := json.Marshal(&chunk)
		errorHandler(err, 114)
		if debug {
			log.Printf("Marshalled Data: %s", rArgs)
		}

		//spawn worker connections
		grepChan[i] = cli.Go(reduceService, rArgs, &grepResp[i], nil)

		log.Printf("Spawned worker connection #%d", i)
	}

	//wait for response
	for i := 0; i < mc.numWorkers; i++ {
		<-grepChan[i].Done
		log.Printf("Worker #%d DONE", i)
	}

	return grepResp
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
 * Prepares a MapRequest object with the centroids and the points for each of the Mappers.
 * Returns the marshalled message for the Map.
 */
func prepareMapArguments(chunk utils.Points, centroids utils.Points) interface{} {
	// Arguments
	kmArgs := new(MapRequest)
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
func mergeMapResults(resp [][]byte, dim int) ([]MapResponse, error) {

	var mapRes []MapResponse

	for i := 0; i < dim; i++ {
		// Unmarshalling
		var temp []MapResponse
		err := json.Unmarshal(resp[i], &temp)
		errorHandler(err, 301)

		if debug {
			log.Printf("Received: %s", resp[i])
			log.Printf("Unmarshal: Key: %v", temp)
		}

		mapRes = append(mapRes, temp...)
	}

	return mapRes, nil
}

/*
func shuffleAndSort(mapRes []MapResponse) []ReduceArgs {
	sort.Slice(mapRes, func(i, j int) bool {
		return mapRes[i].Key < mapRes[j].Key
	})

	var result []ReduceArgs
	var currKey string

	var r *ReduceArgs
	for i, m := range mapRes {
		if currKey != m.Key {
			if i != 0 {
				result = append(result, *r)
			}
			currKey = m.Key
			r = new(ReduceArgs)
			r.Key = currKey
			r.Values = append(r.Values, m.Value)
		} else {
			r.Values = append(r.Values, m.Value)
		}
	}

	result = append(result, *r)
	return result
}

func mergeFinalResults(resp [][]byte, dim int) (*File, error) {
	file := new(File)
	file.Name = "result.txt"

	for i := 0; i < dim; i++ {
		// Unmarshalling
		var outArgs ReduceArgs

		err := json.Unmarshal(resp[i], &outArgs)
		errorHandler(err, 320)
		if debug {
			log.Printf("Received: %s", resp[i])
			log.Printf("Unmarshal: Key: %v", outArgs)
		}
		for k := 0; k < len(outArgs.Values); k++ {
			file.Content += outArgs.Values[k] + "\n"
		}
	}
	return file, nil
}

*/

// error handling
func errorHandler(err error, line int) {
	if err != nil {
		log.Fatalf("failure at line %d: %v", line, err)
	}
}
