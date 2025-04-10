package main

import (
	"Wireless-project/msgs/data"
	"Wireless-project/msgs/master"
	"Wireless-project/msgs/user"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	ip       string
	currTime time.Time
	ports    []string
}
type File struct {
	fileName string
	filePath string
}
var life_mutex = sync.Mutex{}
// Master IP
var IP string

// map of string as key and pair as value that contain string and time.Time
var node_life_tracker = make(map[string]Node)
var wait = sync.WaitGroup{}
var availablePorts = make(map[string]bool)

// Map of file name and all file info for all nodes that is uploaded to it
var data_nodes_tracker = make(map[string][]File)
var users = make(map[string]user.UserClient)
var original_file_source = make(map[File]string)

// The map maintains the ip of a client as a key and the list of files that the client is currently uploading
// The map will contain node_name + file path + name = node_ip
var files_tracker = make(map[string]string)
var port_tracker = make(map[string]string)

// Check if the node is still alive and this by checking if it exceeds 1 second as threshold
// If it exceeds threshold, then the node is considered dead
func check_node_life(node_name string) bool {
	life_mutex.Lock()
	defer life_mutex.Unlock()
	return time.Since(node_life_tracker[node_name].currTime) <= 1*time.Second
}

type MasterServer struct {
	master.UnimplementedMasterServer
}

func senderReplication(client data.DataClient, f File, receiver_ip string) {
	a, err := client.ReplicateNotify(context.Background(), &data.ReplicateNotification{
		FileName:    f.fileName,
		FilePath:    f.filePath,
		Source:      true,
		Destination: receiver_ip,
	})
	if err != nil {
		fmt.Print("SenderReplication --> %s", err)
		wait.Done()

		return
		// fmt.Print("SenderReplication --> %s", err)
	}
	fmt.Println("The message returned from SenderReplication --> " + a.Message)
	wait.Done()
}
func receiverReplication(client data.DataClient, f File) {
	a, err := client.ReplicateNotify(context.Background(), &data.ReplicateNotification{
		FileName:    f.fileName,
		FilePath:    f.filePath,
		Source:      false,
		Destination: "",
	})
	if err != nil {
		fmt.Print("ReceiverReplication --> %s", err)
		return
	}
	fmt.Println("The message returned from ReceiverReplication --> " + a.Message)
	wait.Done()
}
func notifyMachineDataTransfer(sender_ip string, receiver_ip string, f File) {
	sen_conn, err := grpc.NewClient(sender_ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Print("CheckReplication -> Error when calling RequestUpload: %s", err)
		return
	}
	defer sen_conn.Close()
	data_client := data.NewDataClient(sen_conn)
	wait.Add(1)
	go senderReplication(data_client, f, receiver_ip)
	dest_conn, err := grpc.NewClient(receiver_ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Print("CheckReplication -> Error when calling RequestUpload: %s", err)
		return
	}
	defer dest_conn.Close()
	d_client := data.NewDataClient(dest_conn)
	wait.Add(1)
	go receiverReplication(d_client, f)
	wait.Wait()
}
func check_replication() {
	for {
		time.Sleep(10000 * time.Millisecond)
		fmt.Println("============ Check Replication Started ===============")

		for file, source_machine := range original_file_source {
			if !check_node_life(source_machine) {
				continue
			}
			count := 0
			nodes_without_file := make([]string, 0)
			for data_node, node_files := range data_nodes_tracker {
				is_found := false
				if !check_node_life(data_node) || source_machine == data_node {
					continue
				}
				for _, f := range node_files {
					if f.fileName == file.fileName && f.filePath == file.filePath {
						is_found = true
						count++
						break
					}
				}
				if count >= 2 {
					break
				}
				if !is_found {
					nodes_without_file = append(nodes_without_file, data_node)
				}
			}
			threshold := 2
			threshold -= count
			nodes_count := len(nodes_without_file)

			fmt.Printf("For file %s count = %d,threshold = %d, nodes_count = %d\n", file.filePath+file.fileName, count, threshold, nodes_count)
			x0 := -1
			x1 := -1
			source_machine_ip := node_life_tracker[source_machine].ip
			for threshold != 0 && nodes_count != 0 {
				i := rand.Intn(nodes_count)
				for i == x0 || i == x1 {
					i = rand.Intn(nodes_count)
				}
				if x0 == -1 {
					x0 = i
				}
				if x1 == -1 {
					x1 = i
				}
				fmt.Printf("i = %d\n", i)
				receiver_ip := node_life_tracker[nodes_without_file[i]].ip
				notifyMachineDataTransfer(source_machine_ip, receiver_ip, file)
				data_nodes_tracker[nodes_without_file[i]] = append(data_nodes_tracker[nodes_without_file[i]], File{
					fileName: file.fileName,
					filePath: file.filePath,
				})
				threshold--
			}

		}
		fmt.Println("============ Check Replication Finished ===============")
	}
}

func (s *MasterServer) InitNode(ctx context.Context, in *master.InitRequest) (*master.InitResponse, error) {
	node_name := "Node" + strconv.Itoa(len(node_life_tracker))
	node_life_tracker[node_name] = Node{
		ip:       in.GetIP(),
		ports:    in.GetPorts(),
		currTime: time.Now(),
	}
	if data_nodes_tracker[node_name] == nil {
		data_nodes_tracker[node_name] = []File{}
	}
	for _, port := range in.GetPorts() {
		availablePorts[in.IP+":"+port] = true
	}
	log.Printf("Node %s is initialized with IP %s with ports [%s, %s, %s]\n", node_name, in.GetIP(), in.GetPorts()[0], in.GetPorts()[1], in.GetPorts()[2])
	return &master.InitResponse{
		Message: node_name,
	}, nil
}
func (s *MasterServer) Beat(ctx context.Context, in *master.BeatRequest) (*master.BeatResponse, error) {
	node_name := in.GetNodeName()
	log.Printf("Received Heartbeat from %s\n", node_name)
	node_life_tracker[node_name] = Node{
		ip:       node_life_tracker[node_name].ip,
		currTime: time.Now(),
	}
	return &master.BeatResponse{
		Message: "Heartbeat received by Master from you node " + node_name,
	}, nil
}
func (s *MasterServer) RequestUpload(ctx context.Context, in *master.UploadRequest) (*master.UploadResponse, error) {
	life_mutex.Lock()
	defer life_mutex.Unlock()
	client_ip := in.GetClientIP()
	if users[client_ip] == nil {
		conn, err := grpc.NewClient(client_ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Print("Error when calling RequestUpload: %s", err)
			return nil, err
		}
		users[client_ip] = user.NewUserClient(conn)
	}
	for {
		i := rand.Intn(len(node_life_tracker))
		node_name := "Node" + strconv.Itoa(i)
		if check_node_life(node_name) {
			sendIP := node_life_tracker[node_name].ip
			found := false
			for _, port := range node_life_tracker[node_name].ports {
				if availablePorts[node_life_tracker[node_name].ip+":"+port] {
					found = true
					availablePorts[node_life_tracker[node_name].ip+":"+port] = false
					sendIP = node_life_tracker[node_name].ip + ":" + port
					break
				}
			}
			if !found {
				continue
			}
			files_tracker[node_name+"/"+in.FilePath+in.FileName] = client_ip
			port_tracker[node_name+"/"+in.FilePath+in.FileName] = sendIP
			return &master.UploadResponse{
				IP:       sendIP,
				NodeName: node_name,
			}, nil
		}
	}
}

// When the transferring procedure is finished, the data keeper node will then notify the master tracker
func (s *MasterServer) UploadFinished(ctx context.Context, in *master.DataNodeUploadFinishedRequest) (*master.ReplicateRequest, error) {
	// node_name := in.GetNodeName()
	file_path := in.GetFilePath()
	file_name := in.GetFileName()
	// The master tracker then adds the file record to the main look-up table

	ff := File{
		fileName: file_name,
		filePath: file_path,
	}
	data_nodes_tracker[in.NodeName] = append(data_nodes_tracker[in.NodeName], ff)
	original_file_source[ff] = in.NodeName
	node_ip_port := port_tracker[in.NodeName+"/"+file_path+file_name]
	delete(port_tracker, in.NodeName+"/"+file_path+file_name)
	user_ip := files_tracker[in.NodeName+"/"+file_path+file_name]
	delete(files_tracker, in.NodeName+"/"+file_path+file_name)
	// The master will notify the client with a successful message
	_, err := users[user_ip].NotifyUploadFinished(context.Background(), &user.UploadFinishedNotification{
		FilePath: file_path,
		FileName: file_name,
	})
	if err != nil {
		fmt.Print("UploadFinished -> Error when calling NotifyUploadFinished: %s", err)
		return nil, err
	}
	// // The master chooses 2 other nodes to replicate the file transferred
	// replicate_ports := make([]string, 2)
	// for i := 0; i < 2; i++ {
	// 	for {
	// 		j := rand.Intn(len(node_life_tracker))
	// 		replicate_node_name := "Node" + strconv.Itoa(j)
	// 		if replicate_node_name != node_name && check_node_life(node_name) {
	// 			replicate_ports[i] = node_life_tracker[replicate_node_name].ip
	// 			break
	// 		}
	// 	}
	// }
	availablePorts[node_ip_port] = true
	return &master.ReplicateRequest{
		// DataNodes: replicate_ports,
		DataNodes: []string{},
	}, nil
}
func (s *MasterServer) RequestDownload(ctx context.Context, in *master.DownloadRequest) (*master.DownloadResponse, error) {
	file_name := in.GetFileName()
	file_path := in.GetFilePath()
	ports_list := make([]string, 0)
	names_list := make([]string, 0)
	for node, file := range data_nodes_tracker {
		if !check_node_life(node) {
			continue
		}
		for _, f := range file {
			if f.fileName == file_name && f.filePath == file_path {
				ports_list = append(ports_list, node_life_tracker[node].ip)
				names_list = append(names_list, node)
			}
		}
	}
	return &master.DownloadResponse{
		IPs:       ports_list,
		NodeNames: names_list,
	}, nil
}
func getPreferredIP() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, i := range interfaces {
		if i.Flags&net.FlagUp == 0 || i.Flags&net.FlagLoopback != 0 {
			continue // Skip down or loopback interfaces
		}

		addrs, err := i.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}

			if ip.To4() != nil {
				return ip.String(), nil
			}
		}
	}

	return "", fmt.Errorf("no valid IP found")
}
func main() {
	// rand.Seed(time.Now().UnixNano())
	ip, err := getPreferredIP()
	if err != nil {
		fmt.Print("Error getting local IP: %s", err)
		return
	}
	IP = ip + ":8080"
	fmt.Println("Master IP: ", IP)
	server := grpc.NewServer()
	defer server.Stop()
	master_server := MasterServer{}
	master.RegisterMasterServer(server, &master_server)
	lis, err := net.Listen("tcp", IP)
	if err != nil {
		fmt.Print("Cannot start server : %s", err)
		return
	}
	go check_replication()
	err = server.Serve(lis)
	if err != nil {
		fmt.Print("Failed to serve: %v", err)
		return
	}
}
