package main

import (
	"KMeanMR/utils"
	"encoding/json"
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
)

type Dataset struct {
	Name      string
	DataFrame dataframe.DataFrame
}

type KMRequest struct {
	Points utils.Points
	K      int
}

const (
	debug    = false // Set to true to activate debug log
	network  = "tcp"
	address  = "localhost"
	service1 = "MasterServer.KMeans"
)

/*------------------------------------------------------- MAIN -------------------------------------------------------*/
func main() {
	var reply []byte
	var cli *rpc.Client
	var err error
	welcome()

	// check for open TCP ports
	for p := 50000; p <= 50005; p++ {
		port := strconv.Itoa(p)
		cli, err = rpc.Dial(network, net.JoinHostPort(address, port))

		if err != nil {
			if debug {
				log.Printf("Connection error: port %v is not active", p)
			}
			log.Printf("Connecting to master...")
			continue
		}

		if cli != nil {
			//create a TCP connection to localhost
			net.JoinHostPort(address, port)
			log.Printf("Connected on port %v", p)

			if debug {
				log.Printf("client conn: %p", cli)
			}
			break
		}
	}

	// get marshalled request
	mArgs := prepareArguments()

	// call the service
	if debug {
		log.Printf("service: %v", service1)
		log.Printf("args: %v", string(mArgs))
		log.Printf("reply: %p", &reply)
		log.Printf("client: %p", cli)
	}
	cliCall := cli.Go(service1, mArgs, &reply, nil)
	repCall := <-cliCall.Done
	if debug {
		log.Printf("Done %v", repCall)
	}

	// Unmarshalling of reply
	/*
		var result Dataset
		err = json.Unmarshal(reply, &result)
		errorHandler(err, 77)

		// Print grep result on screen

			fmt.Println("")
			fmt.Println("-------------------------- GREP RESULT ------------------------------: ")
			fmt.Println(result.Content)

	*/
	err = cli.Close()
	errorHandler(err, 84)
}

/*------------------------------------------------------- PRE-PROCESSING ---------------------------------------------*/
func prepareArguments() []byte {

	dirpath := "client/dataset/"

	// retrieve dataset
	dataset := new(Dataset)
	dataset.Name = listDatasets(dirpath)
	dataset.DataFrame = readDataset(dirpath + dataset.Name)

	// prepare request for the master
	kmRequest := new(KMRequest)
	kmRequest.Points = utils.ExtractPoints(dataset.DataFrame)
	kmRequest.K = scanK()

	// marshalling
	s, err := json.Marshal(&kmRequest)
	errorHandler(err, 119)
	if debug {
		log.Printf("Marshalled Data: %s ...", string(s[0:10]))
	}

	return s
}

func listDatasets(dirpath string) string {
	var fileNum int
	fileMap := make(map[int]string)
	fmt.Println("Choose a dataset:")

	// Read files directory
	file, err := os.ReadDir(dirpath)
	errorHandler(err, 119)

	for i := 0; i < len(file); i++ {
		fmt.Printf("-> (%d) %s\n", i+1, file[i].Name())
		fileMap[i+1] = file[i].Name()
	}

	// Input the file chosen
	fmt.Print("Select a number: ")
	_, err = fmt.Scanf("%d\n", &fileNum)
	errorHandler(err, 129)
	return fileMap[fileNum]
}

func readDataset(filename string) dataframe.DataFrame {
	//read file content
	if debug {
		log.Printf("Reading dataset %s", filename)
	}

	file, err := os.Open(filename)
	errorHandler(err, 158)

	dataFrame := dataframe.ReadCSV(file)
	dataFrame = dataFrame.Drop(0)
	dataFrame = dataFrame.Drop(0)
	dataFrame = dataFrame.Drop(dataFrame.Ncol() - 1)

	return dataFrame
}

func scanK() int {
	var k int

	fmt.Println("Choose the number k of clusters:")
	_, err := fmt.Scanf("%d\n", &k)
	errorHandler(err, 165)

	return k
}

/*----------------------------------------------------- UTILS --------------------------------------------------------*/
func errorHandler(err error, line int) {
	if err != nil {
		log.Fatalf("failure at line %d: %v", line, err)
	}
}

func welcome() {
	// Welcome
	for i := 0; i <= 3; i++ {
		fmt.Println("*")
	}
	fmt.Println("Author: Livia Simoncini")
	for i := 0; i <= 3; i++ {
		fmt.Println("*")
	}
}
