# latest golang base image
FROM golang:latest

# Set the Working Directory inside the container
WORKDIR /mapreduce

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Dependencies
RUN go install; exit 0
