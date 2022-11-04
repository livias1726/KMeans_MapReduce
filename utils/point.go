package utils

import (
	"github.com/go-gota/gota/dataframe"
	"math"
)

type Point struct {
	Coordinates []float64
}
type Points []Point

func ExtractPoints(dataframe dataframe.DataFrame) Points {
	var points Points

	for i := 0; i < dataframe.Nrow(); i++ {
		coords := make([]float64, dataframe.Ncol())
		for j := 0; j < dataframe.Ncol(); j++ {
			coords = append(coords, dataframe.Elem(i, j).Float())
		}

		var p Point
		p.Coordinates = coords
		points = append(points, p)
	}

	return points
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
