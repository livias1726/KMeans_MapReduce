package utils

import (
	"fmt"
	"math/rand"
)

type Cluster struct {
	Centroid Point
	Points   Points
}

type Clusters []Cluster

// Init : dummy initialization by randomly selecting k points from dataset
func Init(k int, dataset Points) (Points, error) {
	var centroids Points

	// check for errors
	if len(dataset) == 0 || len(dataset[0].Coordinates) == 0 {
		return centroids, fmt.Errorf("dataset is empty or invalid")
	}
	if k == 0 || k >= len(dataset) {
		return centroids, fmt.Errorf("k must be more than 0 and less than the cardinality of the dataset")
	}

	// select k random points from dataset
	for i := 0; i < k; i++ {
		centroids = append(centroids, dataset[rand.Intn(len(dataset))])
	}

	return centroids, nil
}

// returns the centroid of a set of points
func (cluster Cluster) setCentroid() error {
	var centroid Point

	var l = len(cluster.Points)
	if l == 0 {
		return fmt.Errorf("empty cluster")
	}

	centroid.Coordinates = make([]float64, len(cluster.Points[0].Coordinates))
	for _, point := range cluster.Points {
		for i, val := range point.Coordinates {
			centroid.Coordinates[i] += val // sum every coordinate in the coordinates of the centroid
		}
	}

	/*
		var mean DatasetPoints
		for _, sum := range centroid.Coordinates {
			mean = append(mean, sum/float64(l))
		}

	*/

	cluster.Centroid = centroid
	return nil
}

// Nearest returns the index of the cluster nearest to point
func (c Clusters) Nearest(point Point) int {
	var idx int
	dist := -1.0

	for i, cluster := range c {
		d := GetDistance(point.Coordinates, cluster.Centroid.Coordinates)
		if dist < 0 || d < dist {
			dist = d
			idx = i
		}
	}

	return idx
}
