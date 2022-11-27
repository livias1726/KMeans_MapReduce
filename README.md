# KMeans - MapReduce
## Local client for remote connection

### How to run the client:
Before running the client, it's necessary to set up master and worker remote peers. (See 'remote' branch README.md file).
When the remote side is running, execute the following commands:
* Set the modules of the repository:
````
go mod init KMeans_MapReduce
````
* Add module requirements and sums:
````
go mod tidy
````
* Run the client:
````
go run main/client/client.go
````

### Steps of execution
The client will search for the server on the chosen port at the IP address given to the EC2 instance used to host it.

It is possible to choose a specific dataset to cluster. These datasets must be
in the directory **main/client/data/dataset/**. Then the user must specify the number of clusters to create.

After that, the client will send the data in chunks to the server and wait for the results.

### Result
When the server returns to the client with the obtained clustering, the user will see a report of the results. Then, 
they can choose to save the results into CSV files (1 per cluster) and/or plots on html files.



