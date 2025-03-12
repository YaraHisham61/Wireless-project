package main

import (
	"Wireless-project/msgs/master"
	"context"
	"fmt"
	"log"
	"time"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"os"
)

func sendHeartbeat(client master.MasterClient) {
	for {
		_, err := client.Beat(context.Background(), &master.BeatRequest{Name: name})
		if err != nil {
			log.Fatalf("Error when calling Beat: %s", err)
		}
		time.Sleep(1 * time.Second)
	}
}
var name string
var port string

func initConn(client master.MasterClient) (error){
	response, err := client.InitNode(context.Background(), &master.InitRequest{Port: port})
	if err != nil {
		// terminate program
		log.Fatalf("Error when calling Beat: %s", err)
		return err
	}
	name := response.GetMessage()
	fmt.Println("My name is assigned as " + name)
	return nil
}

func main(){
	if len(os.Args) < 2 {
		log.Fatalf("Please provide the port number")
		return
	}
	port = os.Args[1]
	conn, err := grpc.NewClient("localhost:8080",grpc.WithTransportCredentials( insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Cannot start data : %s", err)
	}
	defer conn.Close()
	client_master := master.NewMasterClient(conn)
	initConn := initConn(client_master)
	if initConn != nil {
		return
	}
	go sendHeartbeat(client_master)
	select{}
}
