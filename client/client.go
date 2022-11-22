package main

import (
	"KMeans_MapReduce/utils"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AvraamMavridis/randomcolor"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"io"
	"log"
	"math"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// KMRequest : input to master
type KMRequest struct {
	IP      string       // --> IP of the client
	Dataset utils.Points // --> dataset points extracted locally
	K       int          // --> number of clusters to create
	First   bool         // --> first chunk of the request
	Last    bool         // --> last chunk of the request
}

// KMResponse : output from master
type KMResponse struct {
	Clusters utils.Clusters // --> clusters obtained
	Message  string         // --> message from master wrt the termination of the algorithm
}

const (
	debug      = false // Set to true to activate debug log
	scaled     = false // set to true to scale dataset points coordinates
	datapath   = "client/data/"
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
		result KMResponse
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
	kmRequest := new(KMRequest)
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
func showResults(result KMResponse, elapsed time.Duration) {
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
	generateBarChart(result)
	generateScatterPlot(result)
}

func generateScatterPlot(result utils.Clusters) {
	es := charts.NewScatter3D()
	es.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: "Clustering - Scatter Plot"}),
		charts.WithLegendOpts(
			opts.Legend{
				Show: true,
				Top:  "10%",
			},
		),
		charts.WithToolboxOpts(opts.Toolbox{
			Show: true,
			Feature: &opts.ToolBoxFeature{
				SaveAsImage: &opts.ToolBoxFeatureSaveAsImage{
					Show:  true,
					Type:  "png",
					Title: "k-means_scatter",
				},
			},
		}),
	)

	var dataCentroids []opts.Chart3DData
	color := ""
	for i, cluster := range result {
		resC := reshape(cluster.Centroid.Coordinates, 3)
		dataCentroids = append(dataCentroids, opts.Chart3DData{Value: []interface{}{resC[0], resC[1], resC[2]}})

		data := make([]opts.Chart3DData, 0)
		for _, point := range cluster.Points {
			resP := reshape(point.Coordinates, 3)
			data = append(data, opts.Chart3DData{Value: []interface{}{resP[0], resP[1], resP[2]}})
		}

		name := fmt.Sprintf("Cluster %d", i)
		color = getNewColor(color)
		es.AddSeries(name, data, charts.WithItemStyleOpts(opts.ItemStyle{Color: color}))
	}

	es.AddSeries("Centroids", dataCentroids, charts.WithItemStyleOpts(opts.ItemStyle{Color: "black"}))

	f, _ := os.Create("k-means_scatter.html")
	err := es.Render(io.MultiWriter(f))
	errorHandler(err, 291)
}

func getNewColor(color string) string {
	var res string

	if color == "" {
		res = randomcolor.GetRandomColorInHex()
	} else {
		ok := false
		for !ok {
			res = randomcolor.GetRandomColorInHex()
			if color != res {
				ok = true
			}
		}
	}

	return res
}

func reshape(tensor []float64, dim int) []float64 {
	tensorDim := len(tensor)
	var split int
	res := make([]float64, dim)
	count := 0
	p1 := 0
	for i := 0; i < dim; i++ {
		p2 := ((1 + i) * tensorDim) / dim
		split = p2 - p1
		p1 = p2

		res[i] = 0.0
		for j := 0; j < split; j++ {
			res[i] += tensor[count+j]
		}
		count += split
		res[i] = res[i] / float64(split)
	}

	return res
}

func generateBarChart(result utils.Clusters) {
	bar := charts.NewBar()
	// opts
	bar.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: "Clustering - Bar Chart"}),
		charts.WithToolboxOpts(opts.Toolbox{
			Show:  true,
			Right: "20%",
			Feature: &opts.ToolBoxFeature{
				SaveAsImage: &opts.ToolBoxFeatureSaveAsImage{
					Show:  true,
					Type:  "png",
					Title: "k-means_bar",
				},
				DataView: &opts.ToolBoxFeatureDataView{
					Show:  true,
					Title: "DataView",
					Lang:  []string{"data view", "turn off", "refresh"},
				},
			}},
		),
	)
	// create bars
	var items []opts.BarData
	var xAxis []string
	for i, cluster := range result {
		xAxis = append(xAxis, strconv.Itoa(i))
		items = append(items, opts.BarData{Value: len(cluster.Points)})
	}
	// draw chart
	bar.SetXAxis(xAxis).AddSeries("", items).SetSeriesOptions(
		charts.WithLabelOpts(opts.Label{
			Show:     true,
			Position: "top",
		}),
	)
	// save to file
	f, _ := os.Create("k-means_bar.html")
	err := bar.Render(f)
	errorHandler(err, 261)
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
