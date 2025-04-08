package main

import (
	"Wireless-project/msgs/data"
	"Wireless-project/msgs/master"
	"Wireless-project/msgs/user"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var IP string

// The map will contain the files that are currently uploading to prevent from downloading it
var currently_uploading = make(map[string]bool)
var upload_file_mutex = sync.Mutex{}

type UserServer struct {
	user.UnimplementedUserServer
}

func (s *UserServer) NotifyUploadFinished(ctx context.Context, in *user.UploadFinishedNotification) (*user.Nothing, error) {
	log.Println("Upload for " + in.GetFilePath() + in.GetFileName() + " finished successfully")
	delete(currently_uploading, in.GetFilePath()+in.GetFileName())
	return &user.Nothing{}, nil
}
func getDataNodePort(request master.MasterClient, fileName string, filePath string) (string, string) {
	res, err := request.RequestUpload(context.Background(), &master.UploadRequest{
		FileName: fileName,
		FilePath: filePath,
		ClientIP: IP,
	})
	if err != nil {
		log.Fatalf("Error when calling RequestUpload: %s", err)
	}
	data_node_ip := res.IP
	data_node_name := res.NodeName
	return data_node_ip, data_node_name
}
func uploadVideo(client_master master.MasterClient, name string, path string) {
	upload_file_mutex.Lock()
	defer upload_file_mutex.Unlock()

	ip, nodeName := getDataNodePort(client_master, name, path)
	fmt.Println("Connecting to data node with ip : " + ip)
	data_conn, err_data := grpc.NewClient(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err_data != nil {
		fmt.Printf("Error connecting to data node: %s\n", err_data)
		return
	}
	defer data_conn.Close()
	data_client := data.NewDataClient(data_conn)
	f, err := os.Open("../" + path + name)
	if err != nil {
		fmt.Printf("Error opening file: %s\n", err)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		fmt.Printf("Error getting file info: %s\n", err)
		return
	}
	//* Connection establishment
	_, err = data_client.EstablishUploadConnection(context.Background(),
		&data.VideoUploadData{FilePath: path,
			FileName: name,
			FileSize: info.Size(),
		})
	if err != nil {
		fmt.Printf("Error establishing upload connection: %s\n", err)
		return
	}
	currently_uploading[nodeName+"/"+path+name+".mp4"] = true
	stream, err := data_client.UploadVideo(context.Background())
	if err != nil {
		fmt.Printf("Error starting upload stream: %s\n", err)
		delete(currently_uploading, nodeName+"/"+path+name+".mp4")
		return
	}
	buf := make([]byte, 1024*1024)
	for {
		n, err := f.Read(buf)
		if err == io.EOF {
			err = stream.CloseSend()
			if err != nil {
				fmt.Printf("Error closing upload stream: %s\n", err)
				delete(currently_uploading, nodeName+"/"+path+name+".mp4")
				return
			}
			break
		}
		if err != nil {
			fmt.Printf("Error reading file: %s\n", err)
			delete(currently_uploading, nodeName+"/"+path+name+".mp4")
			return
		}
		err = stream.Send(&data.VideoChunk{Data: buf[:n]})
		if err != nil {
			fmt.Printf("Error sending chunk: %s\n", err)
			delete(currently_uploading, nodeName+"/"+path+name+".mp4")
			return
		}
		time.Sleep(time.Millisecond * 100) // Optional throttling
	}
}
func requestVideoDownload(request master.MasterClient, fileName string, filePath string) ([]string, []string) {
	res, err := request.RequestDownload(context.Background(), &master.DownloadRequest{
		FileName: fileName,
		FilePath: filePath,
		ClientIP: IP,
	})
	if err != nil {
		log.Fatalf("Error when calling RequestDownload: %s", err)
	}
	return res.GetIPs(), res.GetNodeNames()
}
func downloadVideo(ip string, nodeName string, fileName string, filePath string) {
	conn, err := grpc.NewClient(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("Error connecting to data node %s: %s\n", nodeName, err)
		return
	}
	defer conn.Close()
	data_client := data.NewDataClient(conn)
	stream, err := data_client.DownloadVideo(context.Background(), &data.DownloadVideoRequest{
		FileName: fileName,
		FilePath: filePath,
	})
	if err != nil {
		fmt.Printf("Error starting download from node %s: %s\n", nodeName, err)
		return
	}
	path := "../downloads/" + nodeName + "/" + filePath
	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		fmt.Printf("Error creating download directory: %s\n", err)
		return
	}
	file, err := os.Create(path + fileName)
	if err != nil {
		fmt.Printf("Error creating download file: %s\n", err)
		return
	}
	defer file.Close()
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Error receiving chunk from node %s: %s\n", nodeName, err)
			return
		}
		_, err = file.Write(chunk.Data)
		if err != nil {
			fmt.Printf("Error writing chunk to file: %s\n", err)
			return
		}
	}
	log.Println("Downloaded file " + fileName + " from node " + nodeName + " with ip: " + ip)
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
		log.Fatalf("Please provide the master server ip")
		return
	}
	master_ip := os.Args[1]
	conn, err := grpc.NewClient(master_ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Cannot start client : %s", err)
	}
	server := grpc.NewServer()
	user.RegisterUserServer(server, &UserServer{})
	client_master := master.NewMasterClient(conn)
	defer server.Stop()
	defer conn.Close()
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("Cannot start server : %s", err)
	}
	port := strconv.Itoa(lis.Addr().(*net.TCPAddr).Port)
	fmt.Println("Client node started on port " + port)
	ip, err := getPreferredIP()
	IP = ip + ":" + port
	if err != nil {
		log.Fatalf("Error getting local IP: %s", err)
	}
	fmt.Println("Client node started on IP " + ip)
	go server.Serve(lis)
	upload_file_mutex = sync.Mutex{}
	answer := -1
	for answer != 0 {
		fmt.Println("Hello there! What would you like to do?")
		fmt.Println("1. Upload a video")
		fmt.Println("2. Download a video")
		fmt.Println("0. Exit")
		fmt.Scan(&answer)
		if answer == 0 {
			return
		} else if answer == 1 {
			fmt.Println("Enter the path of the video file you want to upload")
			var path string
			fmt.Scan(&path)
			fmt.Println("Enter the name of the video file you want to upload")
			var name string
			fmt.Scan(&name)
			if path[len(path)-1:] != "/" {
				path += "/"
			}
			if name[len(name)-4:] != ".mp4" {
				name += ".mp4"
			}
			go uploadVideo(client_master, name, path)
		} else if answer == 2 {
			fmt.Println("Enter the path of the video file you want to download")
			var path string
			fmt.Scan(&path)
			fmt.Println("Enter the name of the video file you want to download")
			var name string
			fmt.Scan(&name)
			if path[len(path)-1:] != "/" {
				path += "/"
			}
			if name[len(name)-4:] != ".mp4" {
				name += ".mp4"
			}
			download_ips, nodes := requestVideoDownload(client_master, name, path)
			log.Println(nodes)
			for i, ip := range download_ips {
				go downloadVideo(ip, nodes[i], name, path)
			}
		} else {
			fmt.Println("Invalid input")
		}
	}
}
