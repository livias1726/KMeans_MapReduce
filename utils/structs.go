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
