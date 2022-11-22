package utils

import (
	"github.com/pa-m/sklearn/preprocessing"
	"gonum.org/v1/gonum/mat"
	"math"
	"strconv"
)

// Point : represents an instance
type Point struct {
	Id          int
	Coordinates []float64
}
type Points []Point

type Cluster struct {
	Centroid Point
	Points   Points
}
type Clusters []Cluster

func ExtractPoints(dataset [][]string, scaled bool) (Points, error) {
	if scaled {
		return ExtractPointsS(dataset)
	}

	return ExtractPointsNS(dataset)
}

func ExtractPointsNS(dataset [][]string) (Points, error) {
	var points Points

	for i := 0; i < len(dataset); i++ {
		coords := make([]float64, len(dataset[i]))
		for j := 0; j < len(dataset[i]); j++ {
			f, err := strconv.ParseFloat(dataset[i][j], 64)
			if err != nil {
				return points, err
			}

			coords[j] = f
		}

		var p Point
		p.Coordinates = coords
		points = append(points, p)
	}

	return points, nil
}

func ExtractPointsS(dataset [][]string) (Points, error) {
	var points Points

	scaled, err := scaleCoordinates(dataset, len(dataset), len(dataset[0]))
	if err != nil {
		return points, err
	}

	nRows, _ := scaled.Dims()
	for i := 0; i < nRows; i++ {
		coords := scaled.RawRowView(i)

		var p Point
		p.Id = i
		p.Coordinates = coords
		points = append(points, p)
	}

	return points, nil
}

func scaleCoordinates(dataset [][]string, nRows int, nCols int) (*mat.Dense, error) {
	scaler := preprocessing.NewStandardScaler()
	xIn := make([]float64, 0)
	for _, row := range dataset {
		for _, col := range row {
			f, err := strconv.ParseFloat(col, 64)
			if err != nil {
				return nil, err
			}
			xIn = append(xIn, f)
		}
	}

	matIn := mat.NewDense(nRows, nCols, xIn)
	matOut, _ := scaler.FitTransform(matIn, nil)

	return matOut, nil
}

// GetDistance returns the euclidean distance between two points
// --> usage: WORKER.computeMinDistances; WORKER.classify
func GetDistance(p1 []float64, p2 []float64) float64 {
	var dist float64

	for i := 0; i < len(p1); i++ {
		dist += math.Pow(p1[i]-p2[i], 2)
	}

	return math.Sqrt(dist)
}

// GetAvgDistance returns the average distance between a point and a set of points
func GetAvgDistance(p Point, points Points) float64 {
	var d float64
	var l int

	for _, point := range points {
		dist := GetDistance(p.Coordinates, point.Coordinates)
		if dist == 0 {
			continue
		}

		l++
		d += dist
	}

	if l == 0 {
		return 0
	}
	return d / float64(l)
}

func GetAvgDistanceOfSet(points Points) float64 {
	dim := float64(len(points))
	d := 0.0

	for _, p := range points {
		d += GetAvgDistance(p, points)
	}

	return d / dim
}
