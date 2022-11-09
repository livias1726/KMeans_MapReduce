package utils

import (
	"math"
	"strconv"
)

// Point : represents an instance
// --> values representing its position in the features space
// --> index of the cluster which it belonged to
// --> index of the cluster to which it belongs
type Point struct {
	Coordinates []float64
	From        int
	To          int
}
type Points []Point

func ExtractPoints(dataset [][]string) (Points, error) {
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

// GetDistance returns the euclidean distance between two points
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

func GetAvgCapacityOfSet(points []Points) int {
	dim := len(points)
	d := 0

	for _, ps := range points {
		d += len(ps)
	}

	return d / dim
}
