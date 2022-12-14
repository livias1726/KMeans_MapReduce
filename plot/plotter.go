package plot

import (
	"KMeans_MapReduce/utils"
	"fmt"
	"github.com/AvraamMavridis/randomcolor"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"io"
	"os"
	"strconv"
)

type Plotter int

func (p *Plotter) GenerateScatterPlot(result utils.Clusters, path string) error {
	es := charts.NewScatter()
	// global options of the scatter
	es.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: "Clustering - Scatter Plot"}),
		charts.WithLegendOpts(
			opts.Legend{
				Show: true,
				Top:  "5%",
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
		charts.WithDataZoomOpts(
			opts.DataZoom{
				Type:       "slider",
				XAxisIndex: 0,
			},
			opts.DataZoom{
				Type:       "slider",
				YAxisIndex: 0,
			},
			opts.DataZoom{
				Type:       "inside",
				XAxisIndex: 0,
			},
			opts.DataZoom{
				Type:       "inside",
				YAxisIndex: 0,
			},
		),
		charts.WithTooltipOpts(opts.Tooltip{
			Show:      true,
			Formatter: "{a}: {b}",
		}),
	)
	// populate the scatter
	var dataCentroids []opts.ScatterData
	color := ""
	for i, cluster := range result {
		// centroid
		resC := reshape(cluster.Centroid.Coordinates, 2)
		dataCentroids = append(dataCentroids, opts.ScatterData{Value: resC})
		// data inside the cluster
		var data []opts.ScatterData
		for _, point := range cluster.Points {
			resP := reshape(point.Coordinates, 2)
			data = append(data, opts.ScatterData{
				Name:  strconv.Itoa(point.Id),
				Value: resP},
			)
		}
		name := fmt.Sprintf("Cluster %d", i)
		color = getNewColor(color)
		es.AddSeries(name, data, charts.WithItemStyleOpts(opts.ItemStyle{Color: color}))
	}
	es.AddSeries("Centroids", dataCentroids, charts.WithItemStyleOpts(opts.ItemStyle{Color: "black"}))
	// save plot
	f, _ := os.Create(path + "k-means_scatter.html")
	err := es.Render(io.MultiWriter(f))

	return err
}

/*
 * used to get different colors
 * 'randomcolor' does not guarantee that every time 'GetRandomColorInHex()' is called a new color is selected
 */
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

/*
 * reshapes the point coordinates to the correct number of dimensions
 * to draw a 2D scatter plot it is necessary to reduce points dimensions to 2 - this means a loss of info
 */
func reshape(tensor []float64, dim int) []float64 {
	tensorDim := len(tensor)
	var split int

	res := make([]float64, dim)
	count := 0
	p1 := 0
	for i := 0; i < dim; i++ {
		// separate equally the points into 'dim' parts
		p2 := ((1 + i) * tensorDim) / dim
		split = p2 - p1
		p1 = p2
		// populate coordinate
		res[i] = 0.0
		for j := 0; j < split; j++ {
			res[i] += tensor[count+j]
		}
		res[i] = res[i] / float64(split)

		count += split
	}

	return res
}

func (p *Plotter) GenerateBarChart(result utils.Clusters, path string) error {
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
					Title: "Data",
					Lang:  []string{"View", "Close", "Refresh"},
				},
			}},
		),
	)
	// create bars
	var items []opts.BarData
	var xAxis []string
	for i, cluster := range result {
		name := fmt.Sprintf("Cluster %d", i)
		xAxis = append(xAxis, name)
		items = append(items, opts.BarData{
			Name:  name,
			Value: len(cluster.Points),
		})
	}
	// draw chart
	bar.SetXAxis(xAxis).AddSeries("	Points", items).SetSeriesOptions(
		charts.WithLabelOpts(opts.Label{
			Show:     true,
			Position: "top",
		}),
	)
	// save to file
	f, _ := os.Create(path + "k-means_bar.html")
	err := bar.Render(f)

	return err
}
