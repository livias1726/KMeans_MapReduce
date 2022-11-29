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
	"strings"
	"time"
)

const (
	debug         = false // Set to true to activate debug log
	datapath      = "main/client/data/"
	service       = "MasterServer.KMeans"
	network       = "tcp"
	masterAddress = "localhost"
	masterPort    = 11090
	maxChunk      = 100000
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

	// check for open TCP ports
	connect(&cli)
	defer fileClose(cli)

	// prepare request
	name := listDatasets(datapath + "dataset/")
	dataset := readDataset(datapath + "dataset/" + name)
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

		// audit
		if first {
			fmt.Print("Transmitting data...")
		}
		fmt.Print(".")

		if debug {
			log.Printf("--> client %p calling service %s with a %d bytes message (%d)",
				cli, service, len(mArgs), i)
		}

		// call the service synchronously
		err = cli.Call(service, mArgs, &reply)
		errorHandler(err, "rpc")

		// check ack
		if !last {
			err = json.Unmarshal(reply, &ack)
			errorHandler(err, "ack unmarshalling")
			if debug {
				log.Printf("--> ack received... keep going")
			}
			counter += maxChunk
		}
	}
	elapsed := time.Since(start)

	// unmarshalling of reply
	err = json.Unmarshal(reply, &result)
	errorHandler(err, "result unmarshalling")

	// results
	showResults(result, elapsed)
}

func connect(cli **rpc.Client) {
	fmt.Print("Connecting to the server...")

	var err error

	port := strconv.Itoa(masterPort)
	*cli, err = rpc.Dial(network, net.JoinHostPort(masterAddress, port))
	errorHandler(err, "service dialling")

	if *cli != nil {
		//create a TCP connection to localhost
		net.JoinHostPort(masterAddress, port)
		fmt.Printf(" Connected on port %v\n", masterPort)
	}
}

/*------------------------------------------------------- PRE-PROCESSING ---------------------------------------------*/
func prepareArguments(rawPoints [][]string, k *int, max int, first bool, last bool) []byte {
	var err error
	kmRequest := new(utils.KMRequest)
	kmRequest.IP = getIPAddress()

	// dataset
	kmRequest.Dataset, err = utils.ExtractPoints(rawPoints)
	errorHandler(err, "points extraction")

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
	errorHandler(err, "request marshalling")

	return s
}

func getIPAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	errorHandler(err, "ip retrieval")
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
	errorHandler(err, "directory reader")
	for i := 0; i < len(file); i++ {
		fmt.Printf("-> (%d) %s\n", i+1, file[i].Name())
		fileMap[i+1] = file[i].Name()
	}

	// input the chosen dataset
	fmt.Print("Select a number: ")
	_, err = fmt.Scanf("%d\n", &fileNum)
	errorHandler(err, "stdin")

	return fileMap[fileNum]
}

func readDataset(filename string) [][]string {
	//read file content
	file, err := os.Open(filename)
	errorHandler(err, "file opening")
	defer fileClose(file)
	all, err := csv.NewReader(file).ReadAll()
	errorHandler(err, "csv reader")

	if len(all) == 0 {
		err = errors.New("dataset is empty")
		errorHandler(err, "application")
	}

	return all
}

func scanK(max int) int {
	var k int

	for {
		fmt.Print("Choose the number k of clusters: ")
		_, err := fmt.Scanf("%d\n", &k)
		errorHandler(err, "stdin")

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

	// csv results
	var check string
	fmt.Print("Do you want to see the results into .csv files? [y/n]: ")
	_, err := fmt.Scanf("%s\n", &check)
	errorHandler(err, "stdin")
	if strings.ToLower(check) == "y" {
		saveCSVResults(result.Clusters)
	}

	// plots
	fmt.Print("Do you want to see the plotted results? [y/n]: ")
	_, err = fmt.Scanf("%s\n", &check)
	errorHandler(err, "stdin")
	if strings.ToLower(check) == "y" {
		plotResults(result.Clusters)
	}
}

func saveCSVResults(result utils.Clusters) {
	path := datapath + "results/cluster_"
	for i, cluster := range result {
		csvFile, err := os.Create(path + strconv.Itoa(i) + ".csv")
		errorHandler(err, "csv creation")

		csvWriter := csv.NewWriter(csvFile)
		for _, point := range cluster.Points {
			var row []string
			row = append(row, strconv.Itoa(point.Id))
			for _, coord := range point.Coordinates {
				row = append(row, strconv.FormatFloat(coord, 'E', -1, 64))
			}
			err = csvWriter.Write(row)
			errorHandler(err, "csv writing")
		}

		csvWriter.Flush()
		fileClose(csvFile)
	}
}

func plotResults(result utils.Clusters) {
	pl := new(plot.Plotter)

	err := pl.GenerateBarChart(result, datapath+"plots/")
	errorHandler(err, "bar chart creation")

	err = pl.GenerateScatterPlot(result, datapath+"plots/")
	errorHandler(err, "scatter plot creation")
}

/*---------------------------------------------------- UTILS ---------------------------------------------------------*/
func fileClose(file io.Closer) {
	func(file io.Closer) {
		err := file.Close()
		errorHandler(err, "file closing")
	}(file)
}

func errorHandler(err error, pof string) {
	if err != nil {
		log.Fatalf("%s failure: %v", pof, err)
	}
}
