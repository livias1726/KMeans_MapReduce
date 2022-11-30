# KMeans - MapReduce
## Complete local code (without docker configuration files and scripts)

## Running:
To run the program, it is necessary to first set up the modules of the project and add the requirements:
````
go mod init KMeans_MapReduce
````
````
go mod tidy
````

### How to run the master:
Open a new terminal in the root directory (KMeans_MapReduce/) and input the following command:
````
go run main/master/master.go
````

### How to run the worker:
Open a new terminal in the root directory (KMeans_MapReduce/) and input the following command:
````
go run main/worker/worker.go
````

### How to run the client:
Before running the client, it's necessary to set up master and worker peers. When these are running, open another 
terminal in the root directory (KMeans_MapReduce/) and input the following command:
````
go run main/client/client.go
````

### Steps of execution
It is possible to choose a specific dataset to cluster. These datasets must be
in the directory **main/client/data/dataset/**. Then the user must specify the number of clusters to create.

After that, the client will send the data in chunks to the server and wait for the results.

### Result
When the server returns to the client with the obtained clustering, the user will see a report of the results. Then, 
they can choose to save the results into CSV files (1 per cluster) and/or plots on html files.



