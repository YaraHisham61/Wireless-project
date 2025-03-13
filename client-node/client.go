package main

import (
	"Wireless-project/msgs/master"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
)

func getDataNodePort(request master.MasterClient) string {
	res, err := request.RequestUpload(context.Background(), &master.UploadRequest{})
	if err != nil {
		log.Fatalf("Error when calling RequestUpload: %s", err)
	}
	data_node_port := res.Message
	fmt.Println("Connecting to data node with port " + data_node_port)
	return data_node_port
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
			getDataNodePort(client_master)
		} else if answer == 2 {
		} else {
			fmt.Println("Invalid input")
		}
	}
}
