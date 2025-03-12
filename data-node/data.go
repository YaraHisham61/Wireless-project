package main

import (
	"Wireless-project/msgs/master"
	"context"
	"fmt"
	"log"
	"time"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
func initConn(client master.MasterClient) (error){
	response, err := client.Beat(context.Background(), &master.BeatRequest{Name: "Start"})
	if err != nil {
		// terminate program
		log.Fatalf("Error when calling Beat: %s", err)
		return err
	}
	name := response.GetMessage()
	fmt.Println("My name is assigned as " + name)
	return nil
}
func printVals(){
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)
		fmt.Println(i)
	}
}

func main(){
	conn, err := grpc.NewClient("localhost:8080",grpc.WithTransportCredentials( insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Cannot start client : %s", err)
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
