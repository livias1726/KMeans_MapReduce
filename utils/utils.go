package utils

import (
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
		p.Id = i
		p.Coordinates = coords
		points = append(points, p)
	}

	return points, nil
}
