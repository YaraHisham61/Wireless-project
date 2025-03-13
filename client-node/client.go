package main

import (
	"Wireless-project/msgs/data"
	"Wireless-project/msgs/master"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func getDataNodePort(request master.MasterClient) string {
	res, err := request.RequestUpload(context.Background(), &master.UploadRequest{})
	if err != nil {
		log.Fatalf("Error when calling RequestUpload: %s", err)
	}
	data_node_port := res.Message
	return data_node_port
}
func uploadVideo(data_client data.DataClient,f *os.File , fileSize int64) {
	stream, err := data_client.UploadVideo(context.Background())
	if err != nil {
		log.Fatalf("Error when calling UploadVideo: %s", err)
	}
	buf := make([]byte, fileSize)
	for {
		n, err := f.Read(buf)
		if err == io.EOF {
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
func main() {
	conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Cannot start client : %s", err)
	}
	defer conn.Close()
	client_master := master.NewMasterClient(conn)
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
			port := getDataNodePort(client_master)
			fmt.Println("Connecting to data node with port " + port)
			data_conn, err_data := grpc.NewClient("localhost:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("Cannot start client : %s", err_data)
			}
			defer data_conn.Close()
			data_client := data.NewDataClient(data_conn)
			fmt.Println("Connected to data node")
			fmt.Println("Enter the path of the video file you want to upload")
			var path string
			fmt.Scan(&path)
			fmt.Println("Enter the name of the video file you want to upload")
			var name string
			fmt.Scan(&name)
			f, err := os.Open("../videos/" + name + ".mp4")
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
					FileName: name + ".mp4",
					FileSize: info.Size(),
				})
			if err != nil {
				log.Fatalf("Error when calling EstablishUploadConnection: %s", err)
			}
			fmt.Println("Connection established")
			//* File upload
			uploadVideo(data_client,f, info.Size())
			fmt.Println("File uploaded successfully")
			f.Close()
		} else if answer == 2 {
		} else {
			fmt.Println("Invalid input")
		}
	}
}
