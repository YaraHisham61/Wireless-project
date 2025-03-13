package main

import (
	"Wireless-project/msgs/master"
	"context"
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"

	"google.golang.org/grpc"
)

type Node struct {
	port     string
	currTime time.Time
}

// map of string as key and pair as value that contain string and time.Time
var node_life_tracker = make(map[string]Node)

// Check if the node is still alive and this by checking if it exceeds 1 second as threshold
// If it exceeds threshold, then the node is considered dead
func check_node_life(node_name string) bool {
	return time.Since(node_life_tracker[node_name].currTime) <= 1 * time.Second
}

type MasterServer struct {
	master.UnimplementedMasterServer
}

func (s MasterServer) InitNode(ctx context.Context, in *master.InitRequest) (*master.InitResponse, error) {
	node_name := "Node" + strconv.Itoa(len(node_life_tracker))
	node_life_tracker[node_name] = Node{
		port:     in.GetPort(),
		currTime: time.Now(),
	}
	log.Printf("New node %s connected with port number %s", node_name, in.GetPort())
	return &master.InitResponse{
		Message: node_name,
	}, nil
}
func (s MasterServer) Beat(ctx context.Context, in *master.BeatRequest) (*master.BeatResponse, error) {

	node_name := in.GetNodeName()
	log.Printf("Received Heartbeat from %s", node_name)
	node_life_tracker[node_name] = Node{
		port:     node_life_tracker[node_name].port,
		currTime: time.Now(),
	}
	return &master.BeatResponse{
		Message: "Heartbeat received by Master from you node " + node_name,
	}, nil
}
func (s MasterServer) RequestUpload(ctx context.Context, in *master.UploadRequest) (*master.UploadResponse, error) {

	for {
		i := rand.Intn(len(node_life_tracker))
		node_name := "Node" + strconv.Itoa(i)
		if check_node_life(node_name) {
			return &master.UploadResponse{
				Message: node_life_tracker[node_name].port,
			}, nil
		}
	}

}

func main() {
	lis, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("Cannot start server : %s", err)
	}
	server := grpc.NewServer()
	defer server.Stop()
	master_server := MasterServer{}
	master.RegisterMasterServer(server, master_server)
	err = server.Serve(lis)
	if err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
