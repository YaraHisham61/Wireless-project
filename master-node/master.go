package main

import (
	"Wireless-project/msgs/master"
	"Wireless-project/msgs/user"
	"context"
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
var users = make(map[string]user.UserClient)

// The map maintains the port of a client as a key and the list of files that the client is currently uploading
// The map will contain node_name + file path + name = client_port
var files_tracker = make(map[string]string)

// Check if the node is still alive and this by checking if it exceeds 1 second as threshold
// If it exceeds threshold, then the node is considered dead
func check_node_life(node_name string) bool {
	return time.Since(node_life_tracker[node_name].currTime) <= 1*time.Second
}

type MasterServer struct {
	master.UnimplementedMasterServer
}

func (s *MasterServer) InitNode(ctx context.Context, in *master.InitRequest) (*master.InitResponse, error) {
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
func (s *MasterServer) Beat(ctx context.Context, in *master.BeatRequest) (*master.BeatResponse, error) {
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
func (s *MasterServer) RequestUpload(ctx context.Context, in *master.UploadRequest) (*master.UploadResponse, error) {
	client_port := in.GetClientPort()
	if users[client_port] == nil {
		conn, err := grpc.NewClient("localhost:"+client_port, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Error when calling RequestUpload: %s", err)
		}
		users[client_port] = user.NewUserClient(conn)
	}
	for {
		i := rand.Intn(len(node_life_tracker))
		node_name := "Node" + strconv.Itoa(i)
		if check_node_life(node_name) {
			files_tracker[node_name+"/"+in.FilePath+in.FileName] = client_port
			return &master.UploadResponse{
				Port:     node_life_tracker[node_name].port,
				NodeName: node_name,
			}, nil
		}
	}
}

// When the transferring procedure is finished, the data keeper node will then notify the master tracker
func (s *MasterServer) UploadFinished(ctx context.Context, in *master.DataNodeUploadFinishedRequest) (*master.ReplicateRequest, error) {
	node_name := in.GetNodeName()
	file_path := in.GetFilePath()
	file_name := in.GetFileName()
	// The master tracker then adds the file record to the main look-up table
	if data_nodes_tracker[in.NodeName] == nil {
		data_nodes_tracker[in.NodeName] = []File{}
	}
	data_nodes_tracker[in.NodeName] = append(data_nodes_tracker[in.NodeName], File{
		fileName: file_name,
		filePath: file_path,
		nodeName: node_name,
	})
	user_port := files_tracker[in.NodeName+"/"+file_path+file_name]
	delete(files_tracker, in.NodeName+"/"+file_path+file_name)
	// The master will notify the client with a successful message
	_, err := users[user_port].NotifyUploadFinished(context.Background(), &user.UploadFinishedNotification{
		FilePath: file_path,
		FileName: file_name,
	})
	if err != nil {
		log.Fatalf("Error when calling NotifyUploadFinished: %s", err)
	}
	// The master chooses 2 other nodes to replicate the file transferred
	replicate_ports := make([]string, 2)
	for i := 0; i < 2; i++ {
		for {
			j := rand.Intn(len(node_life_tracker))
			replicate_node_name := "Node" + strconv.Itoa(j)
			if replicate_node_name != node_name && check_node_life(node_name) {
				replicate_ports[i] = node_life_tracker[replicate_node_name].port
				break
			}
		}
	}
	return &master.ReplicateRequest{
		DataNodes: replicate_ports,
	}, nil
}
func (s *MasterServer) RequestDownload(ctx context.Context, in *master.DownloadRequest) (*master.DownloadResponse, error) {
	file_name := in.GetFileName()
	file_path := in.GetFilePath()
	ports_list := make([]string, 0)
	for node, file := range data_nodes_tracker {
		if !check_node_life(node) {
			continue
		}
		for _, f := range file {
			if f.fileName == file_name && f.filePath == file_path {
				ports_list = append(ports_list, node_life_tracker[node].port)
			}
		}
	}
	return &master.DownloadResponse{
		Ports: ports_list,
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
	master.RegisterMasterServer(server, &master_server)
	err = server.Serve(lis)
	if err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
