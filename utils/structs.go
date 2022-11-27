package utils

/*---------------------------------------------------- K-MEANS -------------------------------------------------------*/

// KMRequest : matches with struct on client side
type KMRequest struct {
	IP      string
	Dataset Points
	K       int
	First   bool
	Last    bool
}

// KMResponse : matches with struct on client side
type KMResponse struct {
	Clusters Clusters
	Message  string
}

/*---------------------------------------------------- MAP-REDUCE ----------------------------------------------------*/

type InitMapInput struct {
	MapperId  [2]int
	Centroids Points
	Chunk     Points
	Last      bool
}

type InitMapOutput struct {
	Points       Points
	MinDistances []float64
}

type MapInput struct {
	MapperId  [2]int
	Centroids Points
}

type MapOutput struct {
	Clusters map[int]Points
	Len      map[int]int
	Sum      map[int]Point
}

type ReduceInput struct {
	ClusterId int
	Points    Points
	Len       int
}

type ReduceOutput struct {
	ClusterId int
	Point     Point
	Len       int
}
