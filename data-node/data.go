package main

import (
	"Wireless-project/msgs/data"
	"Wireless-project/msgs/master"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)


var name string
var client_master master.MasterClient
var upload_file_mutex sync.Mutex
var replicate_mutex sync.Mutex
var IP string
type DataServer struct {
	data.UnimplementedDataServer
	currentFile   *os.File
	fileName      string
	filePath      string
	replicateFile *os.File
}

func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil { // IPv4 only
				return ipNet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("no local IP found")
}
func sendHeartbeat(client master.MasterClient) {
	for {
		_, err := client.Beat(context.Background(), &master.BeatRequest{NodeName: name})
		if err != nil {
			log.Fatalf("Error when calling Beat: %s", err)
		}
		time.Sleep(950 * time.Millisecond)
	}
}
func initConn() error {
	response, err := client_master.InitNode(context.Background(), &master.InitRequest{IP: IP})
	if err != nil {
		// terminate program
		log.Fatalf("Error when calling InitNode: %s", err)
		return err
	}
	name = response.GetMessage()
	fmt.Println("My name is assigned as " + name)
	return nil
}
func (s *DataServer) ReplicateNotify(ctx context.Context, in *data.ReplicateNotification) (*data.ReplicateNotificationStatus, error) {
	is_source := in.GetSource()
	file_name := in.GetFileName()
	file_path := in.GetFilePath()
	if is_source {
		// I am the source node of file so I will be the sender
		ip := in.GetDestination()
		conn, err := grpc.NewClient(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Cannot start data : %s", err)
		}
		defer conn.Close()
		client := data.NewDataClient(conn)
		fmt.Printf("Replicating file %s to data node %s\n", file_path+file_name, ip)
		client_stream, err := client.NodeToNodeReplicate(context.Background())
		if err != nil {
			log.Printf("Error when calling NodeToNodeReplicate: %s", err)
		}
		file, err := os.Open(filepath.Join("./uploads/"+name+"/"+file_path, file_name))
		if err != nil {
			log.Printf("Error when opening file: %s", err)
		}
		// info,e:= file.Stat()
		// if e!= nil{
		// 	log.Fatalf("Error when opening file: %s", err)
		// }
		// file_size := info.Size()
		buffer := make([]byte, 1024*1024)
		for {
			n, err := file.Read(buffer)
			if err == io.EOF {
				fmt.Printf("File %s uploaded to data node %s\n", file_path+file_name, ip)
				err = client_stream.CloseSend()
				if err != nil {
					log.Printf("Failed to close stream: %v", err)
				}
				break
			}
			if err != nil {
				log.Printf("Failed to read video file: %v", err)
			}
			err = client_stream.Send(&data.VideoChunk{Data: buffer[:n]})
			if err != nil {
				log.Printf("Failed to send video chunk: %v", err)
			}
			time.Sleep(time.Millisecond * 100) // Optional throttling
		}
		fmt.Println("File replicated successfully")
		return &data.ReplicateNotificationStatus{
			Message: name+": Replication done",
		}, nil
	} else {
		// I am the destination of the data node
		replicate_mutex.Lock()
		path := filepath.Join("./uploads/"+name+"/"+file_path, file_name)
		file, err := os.Create(path)
		if err != nil {
			e := os.MkdirAll("./uploads/"+name+"/"+file_path, os.ModePerm)
			if e != nil {
				log.Printf("Error when creating file: %s", err)
			}
			file, _ = os.Create(path)
		}
		s.replicateFile = file
		fmt.Println("File replication received successfully")
		return &data.ReplicateNotificationStatus{
			Message: name+": Replication done",
		}, nil
	}
}
func (s *DataServer) NodeToNodeReplicate(stream grpc.ClientStreamingServer[data.VideoChunk, data.ReplicateStatus]) error {
	defer replicate_mutex.Unlock()
	defer s.replicateFile.Close()
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			
			return stream.SendAndClose(&data.ReplicateStatus{Message: "File uploaded successfully"})
		}
		if err != nil {
			return err
		}
		_, writeErr := s.replicateFile.Write(chunk.Data)
		if writeErr != nil {
			return writeErr
		}
	}

}

func (s *DataServer) EstablishUploadConnection(ctx context.Context, in *data.VideoUploadData) (*data.UploadStatus, error) {
	// This mutex to prevent multi users from uploading in same time
	upload_file_mutex.Lock()
	path := filepath.Join("./uploads/"+name+"/"+in.GetFilePath(), in.GetFileName())
	file, err := os.Create(path)
	if err != nil {
		e := os.MkdirAll("./uploads/"+name+"/"+in.GetFilePath(), os.ModePerm)
		if e != nil {
			log.Fatalf("Error when creating file: %s", err)
		}
		file, _ = os.Create(path)
	}
	s.currentFile = file
	s.fileName = in.GetFileName()
	s.filePath = in.GetFilePath()
	return &data.UploadStatus{Message: "File initialized successfully in location " + path}, nil
}
func (s *DataServer) UploadVideo(stream grpc.ClientStreamingServer[data.VideoChunk, data.UploadStatus]) error {
	defer upload_file_mutex.Unlock()
	defer s.currentFile.Close()
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			_, err := client_master.UploadFinished(context.Background(), &master.DataNodeUploadFinishedRequest{
				NodeName: name,
				FilePath: s.filePath,
				FileName: s.fileName,
			})
			if err != nil {
				log.Fatalf("Error when calling UploadFinished: %s", err)
			}
			return stream.SendAndClose(&data.UploadStatus{Message: "File uploaded successfully"})
		}
		if err != nil {
			return err
		}
		_, writeErr := s.currentFile.Write(chunk.Data)
		if writeErr != nil {
			return writeErr
		}
	}

}
func (s *DataServer) DownloadVideo(in *data.DownloadVideoRequest, stream data.Data_DownloadVideoServer) error {
	file_path := in.GetFilePath()
	file_name := in.GetFileName()
	file, err := os.Open(filepath.Join("./uploads/"+name+"/"+file_path, file_name))
	if err != nil {
		return err
	}
	defer file.Close()
	buffer := make([]byte, 1024)
	for {
		n, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := stream.Send(&data.VideoChunk{Data: buffer[:n]}); err != nil {
			return err
		}
	}
	return nil
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
	if len(os.Args) < 2 {
		log.Fatalf("Please provide the port number")
		return
	}
	if len(os.Args) < 3 {
		log.Fatalf("Please provide the master server ip")
		return
	}
	port := os.Args[1]
	master_ip := os.Args[2]
	ip, err := getPreferredIP()
	if err != nil {
		log.Fatalf("Error when getting local IP: %s", err)
		return
	}
	IP = ip + ":" + port
	fmt.Println("Data Node IP: ", IP)
	upload_file_mutex = sync.Mutex{}
	replicate_mutex = sync.Mutex{}
	conn, err := grpc.NewClient(master_ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Cannot start data : %s", err)
	}
	defer conn.Close()
	client_master = master.NewMasterClient(conn)
	initConn := initConn()
	if initConn != nil {
		return
	}
	server := grpc.NewServer()
	data_server := DataServer{}
	data.RegisterDataServer(server, &data_server)
	lis, err := net.Listen("tcp", IP)
	if err != nil {
		log.Fatalf("Cannot start server : %s", err)
	}
	defer server.Stop()
	go sendHeartbeat(client_master)
	err = server.Serve(lis)
	if err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
