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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var port string

// The map will contain the files that are currently uploading to prevent from downloading it
var currently_uploading = make(map[string]bool)

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
		FileName:   fileName,
		FilePath:   filePath,
		ClientPort: port,
	})
	if err != nil {
		log.Fatalf("Error when calling RequestUpload: %s", err)
	}
	data_node_port := res.Port
	data_node_name := res.NodeName
	return data_node_port, data_node_name
}
func uploadVideo(data_client data.DataClient, f *os.File, fileSize int64) {
	stream, err := data_client.UploadVideo(context.Background())
	if err != nil {
		log.Fatalf("Error when calling UploadVideo: %s", err)
	}
	buf := make([]byte, 1024*1024)
	for {
		n, err := f.Read(buf)
		if err == io.EOF {
			err = stream.CloseSend()
			if err != nil {
				log.Fatalf("Failed to close stream: %v", err)
			}
			break
		}
		if err != nil {
			log.Fatalf("Failed to read video file: %v", err)
		}
		err = stream.Send(&data.VideoChunk{Data: buf[:n]})
		if err != nil {
			log.Fatalf("Failed to send video chunk: %v", err)
		}
		time.Sleep(time.Millisecond * 100) // Optional throttling
	}
}
func requestVideoDownload(request master.MasterClient, fileName string, filePath string) []string {
	res, err := request.RequestDownload(context.Background(), &master.DownloadRequest{
		FileName:   fileName,
		FilePath:   filePath,
		ClientPort: port,
	})
	if err != nil {
		log.Fatalf("Error when calling RequestDownload: %s", err)
	}
	return res.GetPorts()
}
func downloadVideo(port string, fileName string, filePath string) {
	conn, err := grpc.NewClient("localhost:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Cannot start client : %s", err)
	}
	defer conn.Close()
	data_client := data.NewDataClient(conn)
	stream, err := data_client.DownloadVideo(context.Background(), &data.DownloadVideoRequest{
		FileName: fileName,
		FilePath: filePath,
	})
	if err != nil {
		log.Fatalf("Error when calling DownloadVideo: %s", err)
	}
	path:= "../downloads/" + port + "/" + filePath
	os.MkdirAll(path, os.ModePerm)
	file, err := os.Create(path + fileName)
	if err != nil {
		log.Fatalf("Error when creating file: %s", err)
	}
	defer file.Close()
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error when receiving video chunk: %s", err)
		}
		_, err = file.Write(chunk.Data)
		if err != nil {
			log.Fatalf("Error when writing video chunk to file: %s", err)
		}
	}
}
func main() {

	conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	port = strconv.Itoa(lis.Addr().(*net.TCPAddr).Port)
	fmt.Println("Client node started on port " + port)
	go server.Serve(lis)
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
			port, nodeName := getDataNodePort(client_master, name, path)
			fmt.Println("Connecting to data node with port " + port)
			data_conn, err_data := grpc.NewClient("localhost:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("Cannot start client : %s", err_data)
			}
			defer data_conn.Close()
			data_client := data.NewDataClient(data_conn)
			f, err := os.Open("../" + path + name)
			if err != nil {
				log.Fatalf("Error when opening file: %s", err)
			}
			info, err := f.Stat()
			if err != nil {
				log.Fatalf("Error when getting file info: %s", err)
			}
			//* Connection establishment
			_, err = data_client.EstablishUploadConnection(context.Background(),
				&data.VideoUploadData{FilePath: path,
					FileName: name,
					FileSize: info.Size(),
				})
			if err != nil {
				log.Fatalf("Error when calling EstablishUploadConnection: %s", err)
			}
			currently_uploading[nodeName+"/"+path+name+".mp4"] = true
			//* File upload
			uploadVideo(data_client, f, info.Size())
			f.Close()
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
			ports := requestVideoDownload(client_master, name, path)
			log.Println(ports)
			for _, port := range ports {
				go downloadVideo(port, name, path)
			}
		} else {
			fmt.Println("Invalid input")
		}
	}
}
