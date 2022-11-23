package main

import (
	"KMeans_MapReduce/plot"
	"KMeans_MapReduce/utils"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

const (
	debug      = false // Set to true to activate debug log
	scaled     = false // set to true to scale dataset points coordinates
	datapath   = "main/client/data/"
	network    = "tcp"
	address    = "localhost"
	masterPort = 11090
	service    = "MasterServer.KMeans"
	maxChunk   = 10000
)

/*------------------------------------------------------- MAIN -------------------------------------------------------*/
func main() {

	var (
		reply  []byte
		ack    bool
		cli    *rpc.Client
		err    error
		mArgs  []byte
		result utils.KMResponse
	)

	// connect to the server
	connect(&cli)
	defer fileClose(cli)

	// prepare request
	name := listDatasets(datapath)
	dataset := readDataset(datapath + name)
	dim := len(dataset)

	// send 10k points per message
	numMessages := int(math.Ceil(float64(dim) / float64(maxChunk)))
	counter := 0
	k := new(int)
	*k = 0

	start := time.Now() // take execution time
	for i := 0; i < numMessages; i++ {
		first := i == 0
		last := i == numMessages-1

		// get marshalled request
		if (dim - counter) > maxChunk {
			mArgs = prepareArguments(dataset[counter:counter+maxChunk], k, dim, first, last)
		} else {
			mArgs = prepareArguments(dataset[counter:dim], k, dim, first, last)
		}

		if first {
			fmt.Print("Transmitting data...")
		}
		fmt.Print(".")
		// call the service
		if debug {
			log.Printf("--> client %p calling service %s with a %d bytes message (%d)",
				cli, service, len(mArgs), i)
		}
		err = cli.Call(service, mArgs, &reply)
		errorHandler(err, 87)

		if !last {
			err = json.Unmarshal(reply, &ack)
			errorHandler(err, 106)

			if !ack {
				i-- // retry
			} else {
				counter += maxChunk
			}
		}
	}
	elapsed := time.Since(start)

	// unmarshalling of reply
	err = json.Unmarshal(reply, &result)
	errorHandler(err, 125)

	showResults(result, elapsed)
}

func connect(cli **rpc.Client) {
	log.Print("Connecting to the server...")

	var err error

	port := strconv.Itoa(masterPort)
	*cli, err = rpc.Dial(network, net.JoinHostPort(address, port))
	errorHandler(err, 126)

	if *cli != nil {
		//create a TCP connection to localhost
		net.JoinHostPort(address, port)
		log.Printf("Connected on port %v", masterPort)
	}
}

/*------------------------------------------------------- PRE-PROCESSING ---------------------------------------------*/
func prepareArguments(rawPoints [][]string, k *int, max int, first bool, last bool) []byte {
	var err error
	kmRequest := new(utils.KMRequest)
	kmRequest.IP = getIPAddress()

	// dataset
	kmRequest.Dataset, err = utils.ExtractPoints(rawPoints, scaled)
	errorHandler(err, 102)

	// k
	if *k == 0 {
		kmRequest.K = scanK(max)
		*k = kmRequest.K
	} else {
		kmRequest.K = *k
	}

	// flags
	kmRequest.First = first
	kmRequest.Last = last

	// marshalling
	s, err := json.Marshal(&kmRequest)
	errorHandler(err, 102)

	return s
}

func getIPAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	errorHandler(err, 172)
	defer fileClose(conn)

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String()
}

func listDatasets(dirpath string) string {
	var fileNum int
	fileMap := make(map[int]string)
	fmt.Println("Choose a dataset:")

	// read directory
	file, err := os.ReadDir(dirpath)
	errorHandler(err, 114)
	for i := 0; i < len(file); i++ {
		fmt.Printf("-> (%d) %s\n", i+1, file[i].Name())
		fileMap[i+1] = file[i].Name()
	}

	// input the chosen dataset
	fmt.Print("Select a number: ")
	_, err = fmt.Scanf("%d\n", &fileNum)
	errorHandler(err, 124)

	return fileMap[fileNum]
}

func readDataset(filename string) [][]string {
	//read file content
	file, err := os.Open(filename)
	errorHandler(err, 140)
	defer fileClose(file)
	all, err := csv.NewReader(file).ReadAll()
	errorHandler(err, 163)

	if len(all) == 0 {
		err = errors.New("dataset is empty")
		errorHandler(err, 163)
	}

	return all
}

func scanK(max int) int {
	var k int

	for {
		fmt.Print("Choose the number k of clusters: ")
		_, err := fmt.Scanf("%d\n", &k)
		errorHandler(err, 155)

		if k == 0 || k > max {
			fmt.Println("WARNING: K must be more than 0 and less than the number of instances...")
			continue
		}

		break
	}

	return k
}

/*-------------------------------------------------- RESULTS ---------------------------------------------------------*/
func showResults(result utils.KMResponse, elapsed time.Duration) {
	fmt.Println("\n---------------------------------------- K-Means results --------------------------------------")
	fmt.Printf("INFO: %s.\n\n", result.Message)
	for i := 0; i < len(result.Clusters); i++ {
		fmt.Printf("Cluster %d has %d points.\n",
			i, len(result.Clusters[i].Points))
	}
	fmt.Printf("\nTime elapsed: %v.\n", elapsed)
	plotResults(result.Clusters)
}

func plotResults(result utils.Clusters) {
	pl := new(plot.Plotter)

	err := pl.GenerateBarChart(result)
	errorHandler(err, 227)

	err = pl.GenerateScatterPlot(result)
	errorHandler(err, 231)
}

/*---------------------------------------------------- UTILS ---------------------------------------------------------*/
func fileClose(file io.Closer) {
	func(file io.Closer) {
		err := file.Close()
		errorHandler(err, 131)
	}(file)
}

func errorHandler(err error, line int) {
	if err != nil {
		log.Fatalf("failure at line %d: %v", line, err)
	}
}
