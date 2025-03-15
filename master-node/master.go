package main

import (
	"Wireless-project/msgs/master"
	"context"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"
)

type Node struct {
	port     string
	currTime time.Time
}
type File struct {
	fileName string
	filePath string
	nodeName string
}

// map of string as key and pair as value that contain string and time.Time
var node_life_tracker = make(map[string]Node)
// Map of file name and all file info for all nodes that is uploaded to it
var data_nodes_tracker = make(map[string][]File)
// The list will contain name of node + the filepath of the file that is currently uploading
// Like Node1/videos/video1.mp4
var currently_uploading = make(map[string]bool)

// Check if the node is still alive and this by checking if it exceeds 1 second as threshold
// If it exceeds threshold, then the node is considered dead
func check_node_life(node_name string) bool {
	return time.Since(node_life_tracker[node_name].currTime) <= 1*time.Second
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
			currently_uploading[node_name+"/"+in.GetFilePath()+in.GetFileName()] = true
			return &master.UploadResponse{
				Port: node_life_tracker[node_name].port,
				NodeName: node_name,
			}, nil
		}
	}
}
func (s MasterServer) UploadFinished(ctx context.Context, in *master.DataNodeUploadFinishedRequest) (*master.DataNodeUploadFinishedStatus, error) {
	log.Printf("Upload finished by node %s", in.GetNodeName())
	if data_nodes_tracker[in.GetNodeName()] == nil {
		data_nodes_tracker[in.GetNodeName()] = []File{}
	}
	data_nodes_tracker[in.GetNodeName()] = append(data_nodes_tracker[in.GetNodeName()], File{
		fileName: in.GetFileName(),
		filePath: in.GetFilePath(),
		nodeName: in.GetNodeName(),
	})
	delete(currently_uploading, in.GetNodeName()+"/"+in.GetFilePath()+in.GetFileName())
	return &master.DataNodeUploadFinishedStatus{}, nil
}
func (s MasterServer) ClientUploadCheck(ctx context.Context, in *master.ClientUploadCheckRequest) (*master.ClientUploadCheckResponse, error){
	_,ok := currently_uploading[in.GetNodeName()+"/"+in.GetFilePath()+in.GetFileName()]
	for ok {
		_,ok = currently_uploading[in.GetNodeName()+"/"+in.GetFilePath()+in.GetFileName()]
	}
	return &master.ClientUploadCheckResponse{
		Message: "Upload finished",
	}, nil
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
