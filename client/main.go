package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	. "github.com/suchit07-git/chordkv/chord"
	pb "github.com/suchit07-git/chordkv/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ChordClient struct {
	address string
	stub    pb.ChordServiceClient
}

func NewChordClient(address string) *ChordClient {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to node: %v", err)
	}
	stub := pb.NewChordServiceClient(conn)
	return &ChordClient{address: address, stub: stub}
}

func (client *ChordClient) storeKeyValuePair(key string, value string) {
	hash := Sha1Hash(key)
	responsibleNode := client.FindSuccessor(hash)
	address := responsibleNode.Address + ":" + string(responsibleNode.Port)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to responsible node: %v", err)
	}
	defer conn.Close()
	stub := pb.NewChordServiceClient(conn)
	ok, err := stub.Put(context.Background(), &pb.PutRequest{Key: key, Value: value})
	if err != nil || !ok.Success {
		log.Fatalf("Failed to store key-value pair: %v", err)
	}
	fmt.Printf("Stored key %s pair successfully at node %d\f", key, responsibleNode.Id)
}

func (client *ChordClient) FindSuccessor(id int64) *pb.Node {
	response, err := client.stub.FindSuccessor(context.Background(), &pb.FindSuccessorRequest{Id: id})
	if err != nil {
		log.Fatalf("FindSuccessor call failed: %v", err)
	}
	return response
}

func (client *ChordClient) Retrieve(key string) {
	response, err := client.stub.Get(context.Background(), &pb.GetRequest{Key: key})
	if err != nil {
		log.Fatalf("Failed to retrieve key: %v", err)
	}
	if response.Value == "" {
		fmt.Printf("Key %s not found\n", key)
	} else {
		fmt.Printf("Key %s found with value %s\n", key, response.Value)
	}
}

func (client *ChordClient) DeleteKey(key string) {
	response, err := client.stub.Delete(context.Background(), &pb.GetRequest{Key: key})
	if err != nil {
		log.Fatalf("Failed to delete key: %v", err)
	}
	if response == nil {
		fmt.Printf("Key %s not found\n", key)
	} else {
		fmt.Printf("Key %s deleted successfully\n", key)
	}
}

func (client *ChordClient) Notify(id int64, address string) {
	_, err := client.stub.Notify(context.Background(), &pb.Node{Id: id, Address: address})
	if err != nil {
		log.Fatalf("Notify call failed: %v", err)
	}
}

func (client *ChordClient) GetPredecessor() *pb.Node {
	response, err := client.stub.GetPredecessor(context.Background(), &emptypb.Empty{})
	if err != nil {
		log.Fatalf("Failed to get predecessor: %v", err)
	}
	return response
}

func main() {
	if len(os.Args) < 3 {
		panic("Usage: go run main.go <address> <port> <operation> [key] [value]")
	}
	address := os.Args[1]
	port := os.Args[2]
	address = net.JoinHostPort(address, port)
	client := NewChordClient(address)
	op := os.Args[3]
	switch op {
	case "store":
		if len(os.Args) < 6 {
			panic("Usage: go run main.go <address> <port> store <key> <value>")
		}
		key, value := os.Args[4], os.Args[5]
		client.storeKeyValuePair(key, value)

	case "retrieve":
		if len(os.Args) < 5 {
			panic("Usage: go run main.go <address> <port> retrieve <key>")
		}
		key := os.Args[4]
		client.Retrieve(key)

	case "delete":
		if len(os.Args) < 5 {
			panic("Usage: go run main.go <address> <port> delete <key>")
		}
		key := os.Args[4]
		client.DeleteKey(key)

	default:
		panic("Unknown operation. Supported operations: store, retrieve, delete")
	}
}
