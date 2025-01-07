package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	. "github.com/suchit07-git/chordkv/chord"
	. "github.com/suchit07-git/chordkv/client"
	pb "github.com/suchit07-git/chordkv/rpc"
	"google.golang.org/grpc"
)

func getIpAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ipAddress := conn.LocalAddr().(*net.UDPAddr).IP
	return ipAddress.String()
}

func main() {
	if len(os.Args) != 4 {
		panic("Usage: go run server/main.go <port> <bootstrap_server_address> <bootstrap_server_port>")
	}
	address := getIpAddress()
	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic("Invalid server port")
	}
	bootstrapAddress := os.Args[2]
	bootstrapPort, err := strconv.Atoi(os.Args[3])
	if err != nil {
		panic("Invalid bootstrap server port")
	}
	node := NewChordNode(address, int32(port))
	bootstrapNode := NewChordNode(bootstrapAddress, int32(bootstrapPort))
	if address != bootstrapAddress || port != bootstrapPort {
		node.Join(bootstrapNode)
	}
	listener, err := net.Listen("tcp", address+":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", port, err)
	}
	server := grpc.NewServer()
	pb.RegisterChordServiceServer(server, NewChordServiceImpl(node))
	log.Printf("Server started on %s:%d\n", address, port)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
	go node.RunBackgroundTasks()
	client := NewChordClient(address + ":" + strconv.Itoa(port))
	for {
		fmt.Println("Commands:")
		fmt.Println("- store <key> <value>")
		fmt.Println("- retrieve <key>")
		fmt.Println("- delete <key>")
		fmt.Println("- exit")
		var command string
		if strings.HasPrefix(command, "store") {
			var key, value string
			fmt.Sscanf(command, "store %s %s", &key, &value)
			client.StoreKeyValuePair(key, value)
		} else if strings.HasPrefix(command, "retrieve") {
			var key string
			fmt.Scan(&key)
			client.Retrieve(key)
		} else if strings.HasPrefix(command, "delete") {
			var key string
			fmt.Scan(&key)
			client.DeleteKey(key)
		} else if strings.HasPrefix(command, "exit") {
			log.Printf("Exiting...")
		} else {
			fmt.Println("Invalid command. Please try again.")
		}
	}
}
