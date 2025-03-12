package main

import (
	"Wireless-project/msgs/master"
	"context"
	"log"
	"net"
	"strconv"
	"time"

	"google.golang.org/grpc"
)
var node_life_tracker = make(map[string]time.Time)
func check_node_life(node_name string) {
	if time.Since(node_life_tracker[node_name]) > time.Second {
		log.Printf("Node %s is dead", node_name)
		delete(node_life_tracker, node_name)
	}
}
type MasterServer struct {
	master.UnimplementedMasterServer
}

func (s MasterServer) Beat(ctx context.Context, in *master.BeatRequest) (*master.BeatResponse, error) {
	node_name := in.GetName()
	if node_name == "Start" {
		node_name = "Node" + strconv.Itoa(len(node_life_tracker))
		node_life_tracker[node_name] = time.Now()
		log.Printf("New node %s connected", node_name)
		return &master.BeatResponse{
			Message: node_name,
		}, nil
	}
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
	master_server := &MasterServer{}
	master.RegisterMasterServer(server, master_server)
	err = server.Serve(lis)
	if err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
