# latest golang base image
FROM golang:latest

# Set the Working Directory inside the container
WORKDIR /mapreduce

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Dependencies
# RUN go get -u github.com/colinmarc/hdfs

# RUN go env -w GO111MODULE=on
# RUN go env -w GOPROXY=https://goproxy.io,direct
# ommit errors for `main redeclared ` error

RUN go install; exit 0  

# Expose port to the outside world or use `docker run --expose portnum`
# EXPOSE 11091

# Declare volumes to mount
# VOLUME /TinyDFS/DataNode1  

# Command to run the executable
# ENTRYPOINT ./start.sh ; /bin/bash
