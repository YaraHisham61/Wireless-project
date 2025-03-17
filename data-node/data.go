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
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func sendHeartbeat(client master.MasterClient) {
	for {
		_, err := client.Beat(context.Background(), &master.BeatRequest{NodeName: name})
		if err != nil {
			log.Fatalf("Error when calling Beat: %s", err)
		}
		time.Sleep(950 * time.Millisecond)
	}
}

var name string
var port string
var client_master master.MasterClient

type DataServer struct {
	data.UnimplementedDataServer
	currentFile *os.File
	fileSize    int64
	fileName    string
	filePath    string
}

func initConn() error {
	response, err := client_master.InitNode(context.Background(), &master.InitRequest{Port: port})
	if err != nil {
		// terminate program
		log.Fatalf("Error when calling InitNode: %s", err)
		return err
	}
	name = response.GetMessage()
	fmt.Println("My name is assigned as " + name)
	return nil
}
func (s *DataServer) EstablishUploadConnection(ctx context.Context, in *data.VideoUploadData) (*data.UploadStatus, error) {
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
	s.fileSize = in.GetFileSize()
	s.fileName = in.GetFileName()
	s.filePath = in.GetFilePath()
	return &data.UploadStatus{Message: "File initialized successfully in location " + path + " and with size " + strconv.Itoa(int(s.fileSize))}, nil
}
func (s *DataServer) UploadVideo(stream grpc.ClientStreamingServer[data.VideoChunk, data.UploadStatus]) error {
	defer s.currentFile.Close()
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			client_master.UploadFinished(context.Background(), &master.DataNodeUploadFinishedRequest{
				NodeName: name,
				FilePath: s.filePath,
				FileName: s.fileName,
			})

			return stream.SendAndClose(&data.UploadStatus{})
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
func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Please provide the port number")
		return
	}
	port = os.Args[1]
	conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	lis, err := net.Listen("tcp", "localhost:"+port)
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
