# KMeans - MapReduce
## Remote side - stateless implementation

### Environment
To deploy the master and the worker it can be used a Linux instance. In particular, the software was deployed on a AWS
EC2 instance with an Amazon Linux image as OS. Docker must be installed on the host, to use two different containers
to run the master and the worker. 

When the environment is set up, run the script:
````
./start.sh
````
This will call the **docker-compose.yml** to configure the images for the worker and the master using their respective
Dockerfile. The local scripts (**start_master.sh**, **start_worker.sh**) will be executed when the containers start.

### Execution
When up and running, the nodes will wait for incoming requests. A remote client can connect to the master server using 
the virtual machine IP address and the port configured in the Docker configuration for the node (11090).
