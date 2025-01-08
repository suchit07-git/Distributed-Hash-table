package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

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

func serve(node *ChordNode, address string, port int) {
	server := grpc.NewServer()
	pb.RegisterChordServiceServer(server, NewChordServiceImpl(node))
	listener, err := net.Listen("tcp", address+":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", port, err)
	}
	log.Printf("Server started on %s:%d\n", address, port)
	var wg sync.WaitGroup
	wg.Add(1)
	go node.RunBackgroundTasks(&wg)
	wg.Add(1)
	go GetInput(node, &wg, address, port)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
	wg.Wait()
	server.Stop()
}

func GetInput(node *ChordNode, wg *sync.WaitGroup, address string, port int) {
	defer wg.Done()
	client := NewChordClient(address + ":" + strconv.Itoa(port))
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("Commands:")
		fmt.Println("- store <key> <value>")
		fmt.Println("- retrieve <key>")
		fmt.Println("- delete <key>")
		fmt.Println("- exit")
		scanner.Scan()
		command := scanner.Text()
		if strings.HasPrefix(command, "store") {
			fmt.Println("Enter the key:")
			scanner.Scan()
			key := scanner.Text()
			fmt.Println("Enter the value:")
			scanner.Scan()
			value := scanner.Text()
			client.StoreKeyValuePair(key, value)
		} else if strings.HasPrefix(command, "retrieve") {
			fmt.Print("Enter the key: ")
			scanner.Scan()
			key := scanner.Text()
			client.Retrieve(key)
		} else if strings.HasPrefix(command, "delete") {
			fmt.Print("Enter the key: ")
			scanner.Scan()
			key := scanner.Text()
			client.DeleteKey(key)
		} else if strings.HasPrefix(command, "exit") {
			log.Printf("Exiting...")
			os.Exit(0)
		} else {
			fmt.Println("Invalid command. Please try again.")
		}
	}
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
	serve(node, address, port)
}
