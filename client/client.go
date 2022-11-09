package main

import (
	"KMeans_MapReduce/utils"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// KMRequest : input to master
// --> dataset points extracted locally
// --> number of clusters to create
type KMRequest struct {
	Dataset utils.Points
	K       int
}

// KMResponse : output from master
// --> clusters obtained
// --> message from master wrt the termination of the algorithm
type KMResponse struct {
	Clusters utils.Clusters
	Message  string
}

const (
	debug    = true // Set to true to activate debug log
	datapath = "client/dataset/"
	//outfile  = "k-means.png"
	network = "tcp"
	address = "localhost"
	service = "MasterServer.KMeans"
)

/*------------------------------------------------------- MAIN -------------------------------------------------------*/
func main() {

	var reply []byte
	var cli *rpc.Client
	var err error

	// check for open TCP ports
	checkConnections(&cli)
	defer fileClose(cli)

	// prepare request
	mArgs := prepareArguments()

	// call the service
	if debug {
		log.Printf("--> client %p calling service %v with a %d bytes message...",
			cli, service, len(mArgs))
	}
	start := time.Now()                    // take execution time
	err = cli.Call(service, mArgs, &reply) // sync call
	errorHandler(err, 87)
	elapsed := time.Since(start)
	if debug {
		log.Printf("--> service returned.")
	}

	// unmarshalling of reply
	var result KMResponse
	err = json.Unmarshal(reply, &result)
	errorHandler(err, 125)

	showResults(result, elapsed)
}

func checkConnections(cli **rpc.Client) {
	var err error

	for p := 50000; p <= 50005; p++ {
		port := strconv.Itoa(p)
		*cli, err = rpc.Dial(network, net.JoinHostPort(address, port))
		if err != nil {
			if debug {
				log.Printf("--> port %v is not active", p)
			}
			log.Print("Connecting to master...\n")
			continue
		}

		if *cli != nil {
			//create a TCP connection to localhost
			net.JoinHostPort(address, port)
			log.Printf("Connected on port %v", p)
			break
		}
	}
}

/*------------------------------------------------------- PRE-PROCESSING ---------------------------------------------*/
func prepareArguments() []byte {
	var err error

	name := listDatasets(datapath)
	dataset := readDataset(datapath + name)

	// dataset
	kmRequest := new(KMRequest)
	kmRequest.Dataset, err = utils.ExtractPoints(dataset)
	errorHandler(err, 102)
	if debug {
		log.Printf("--> extracted %d points from dataset file.\n",
			len(kmRequest.Dataset))
	}

	// k
	kmRequest.K = scanK(len(dataset))

	// marshalling
	s, err := json.Marshal(&kmRequest)
	errorHandler(err, 102)

	return s
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
	if debug {
		log.Printf("--> reading from %s ...", filename)
	}

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

func showResults(result KMResponse, elapsed time.Duration) {
	fmt.Println("\n---------------------------------------- K-Means results --------------------------------------")
	fmt.Printf("INFO: %s.\n\n", result.Message)
	for i := 0; i < len(result.Clusters); i++ {
		fmt.Printf("Cluster %d has %d points.\n",
			i, len(result.Clusters[i].Points))
	}
	fmt.Printf("\nTime elapsed: %v.\n", elapsed)

	//plotResults(result) TODO: find a way to plot multi-dimensional data
}

/*
func plotResults(result utils.Clusters) {
	var series []chart.Series

	for i := 0; i < len(result); i++ {
		series = append(series, getSeries(result[i]))
	}

	graph := getChart(series)

	buffer := bytes.NewBuffer([]byte{})
	err := graph.Render(chart.PNG, buffer)
	errorHandler(err, 205)

	err = os.WriteFile(outfile, buffer.Bytes(), 0644)
	errorHandler(err, 208)
}

func getChart(series []chart.Series) chart.Chart {
	c := new(chart.Chart)

	c.Series = series
	c.XAxis.Style.Show = true
	c.YAxis.Style.Show = true

	return *c
}

func getSeries(cluster utils.Cluster) chart.ContinuousSeries {
	c := new(chart.ContinuousSeries)
	c.Style.Show = true
	c.Style.StrokeWidth = chart.Disabled
	c.Style.DotWidth = 5
	c.XValues = cluster.getX()
	c.YValues = cluster.getY()

	return *c
}
*/

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
