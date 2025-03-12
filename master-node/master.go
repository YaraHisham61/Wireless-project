package main

import (
	"Wireless-project/msgs/master"
	"context"
	"google.golang.org/grpc"
	"log"
	"net"
	"strconv"
	"time"
)

type Node struct {
	port     string
	currTime time.Time
}

// map of string as key and pair as value that contain string and time.Time
var node_life_tracker = make(map[string]Node)

func check_node_life(node_name string) {
	if time.Since(node_life_tracker[node_name].currTime) > time.Second {
		log.Printf("Node %s is dead", node_name)
		delete(node_life_tracker, node_name)
	}
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
	log.Printf("New node %s connected with port number %s", node_name,in.GetPort())
	return &master.InitResponse{
		Message: node_name,
	}, nil
}
func (s MasterServer) Beat(ctx context.Context, in *master.BeatRequest) (*master.BeatResponse, error) {
	node_name := in.GetName()
	log.Printf("Received Heartbeat from %s", node_name)
	// Handle the heartbeat updating time
	return &master.BeatResponse{
		Message: "Heartbeat received by Master from you node " + node_name,
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Cannot start server : %s", err)
	}
	server := grpc.NewServer()
	master_server := MasterServer{}
	master.RegisterMasterServer(server, master_server)
	err = server.Serve(lis)
	if err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
