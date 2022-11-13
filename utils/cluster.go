package utils

import (
	"math/rand"
)

type Cluster struct {
	Centroid Point
	Points   Points
}

type Clusters []Cluster

// Init : centroids initialization by random selection of 1 point from the dataset
func Init(k int, dataset Points) (Points, error) {
	var centroids Points

	// select k random points from dataset
	for i := 0; i < k; i++ {
		centroids = append(centroids, dataset[rand.Intn(len(dataset))])
	}

	return centroids, nil
}
