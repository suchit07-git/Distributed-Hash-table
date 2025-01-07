package chord

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"log"
	"math/big"
	"strconv"
	"time"

	pb "github.com/suchit07-git/chordkv/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

const nBits = 32

type ChordNode struct {
	id          int64
	address     string
	port        int32
	fingerTable map[int]*ChordNode
	predecessor *ChordNode
	successor   *ChordNode
	kvstore     map[string]string
}

func NewChordNode(address string, port int32) *ChordNode {
	node := &ChordNode{
		id:          -1,
		address:     address,
		port:        port,
		fingerTable: make(map[int]*ChordNode),
		predecessor: nil,
		successor:   nil,
		kvstore:     make(map[string]string),
	}
	node.id = Sha1Hash(address + ":" + strconv.Itoa(int(port)))
	return node
}

func (node *ChordNode) FindSuccessor(id int64) *ChordNode {
	if id >= node.id && id <= node.successor.id {
		return node.successor
	}
	closestNode := node.ClosestPrecedingNode(id)
	address := closestNode.address + ":" + strconv.Itoa(int(closestNode.port))
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to node: %v", err)
	}
	defer conn.Close()
	client := pb.NewChordServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := client.FindSuccessor(ctx, &pb.FindSuccessorRequest{Id: id})
	if err != nil {
		log.Fatalf("Couldn't find successor: %v", err)
	}
	return &ChordNode{id: response.Id, address: response.Address, port: response.Port}
}

func (node *ChordNode) ClosestPrecedingNode(id int64) *ChordNode {
	for i := nBits - 1; i >= 0; i-- {
		if node.fingerTable[i] != nil {
			if node.fingerTable[i].id >= node.id && node.fingerTable[i].id <= id {
				return node.fingerTable[i]
			}
		}
	}
	return node
}

func (node *ChordNode) Notify(n *ChordNode) {
	if node.predecessor == nil || (n.id >= node.predecessor.id && n.id <= node.id) {
		node.predecessor = n
	}
}

func (node *ChordNode) Store(key string, value string) {
	node.kvstore[key] = value
}

func Sha1Hash(key string) int64 {
	hashser := sha1.New()
	hashser.Write([]byte(key))
	hashBytes := hashser.Sum(nil)
	hashHex := hex.EncodeToString(hashBytes)
	hashBigInt := new(big.Int)
	hashBigInt.SetString(hashHex, 16)
	modulus := new(big.Int).SetUint64(1 << 32)
	hashMod := new(big.Int).Mod(hashBigInt, modulus)
	return hashMod.Int64()
}

func (node *ChordNode) Retrieve(key string) string {
	hash_key := Sha1Hash(key)
	responsibleNode := node.FindSuccessor(hash_key)
	if responsibleNode.id == node.id {
		return node.kvstore[key]
	}
	address := responsibleNode.address + ":" + strconv.Itoa(int(responsibleNode.port))
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to node: %v", err)
	}
	defer conn.Close()
	client := pb.NewChordServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := client.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		log.Fatalf("Coudn't retrieve value for key %s: %v", key, err)
	}
	return response.Value
}

func (node *ChordNode) Delete(key string) bool {
	hash_key := Sha1Hash(key)
	responsibleNode := node.FindSuccessor(hash_key)
	if responsibleNode.id == node.id {
		_, exists := node.kvstore[key]
		if !exists {
			log.Printf("Key %s not found", key)
			return false
		}
		delete(node.kvstore, key)
	}
	address := responsibleNode.address + ":" + strconv.Itoa(int(responsibleNode.port))
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to node: %v", err)
	}
	defer conn.Close()
	client := pb.NewChordServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = client.Delete(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		log.Fatalf("Coudn't delete value for key %s: %v", key, err)
	}
	return true
}

func (node *ChordNode) Join(bootstrapNode *ChordNode) {
	if bootstrapNode != nil {
		address := bootstrapNode.address + ":" + strconv.Itoa(int(bootstrapNode.port))
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Failed to connect to node: %v", err)
		}
		defer conn.Close()
		client := pb.NewChordServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := client.FindSuccessor(ctx, &pb.FindSuccessorRequest{Id: node.id})
		if err != nil {
			log.Fatalf("Couldn't find successor: %v", err)
		}
		node.successor = &ChordNode{id: response.Id, address: response.Address, port: response.Port}
		node.FixFingers()
	} else {
		node.successor = node
	}
	node.predecessor = nil
}

func (node *ChordNode) FixFingers() {
	for i := 0; i < nBits; i++ {
		node.fingerTable[i] = node.FindSuccessor((node.id + (1 << i)) % (1 << nBits))
	}
}

func (node *ChordNode) Stabilize() {
	if node.successor != nil {
		address := node.successor.address + ":" + strconv.Itoa(int(node.successor.port))
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to connect to successor: %v", err)
			return
		}
		defer conn.Close()
		client := pb.NewChordServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err := client.GetPredecessor(ctx, &emptypb.Empty{})
		if err != nil {
			log.Printf("Couldn't get predecessor: %v", err)
			return
		}
		var x *ChordNode
		if response.Id != -1 {
			x = &ChordNode{id: response.Id, address: response.Address, port: response.Port}
		}
		if x != nil && (x.id >= node.id && x.id <= node.successor.id) {
			node.successor = x
		}
		_, err = client.Notify(ctx, &pb.Node{Id: node.id, Address: node.address, Port: node.port})
		if err != nil {
			log.Printf("Couldn't notify successor: %v", err)
		}
	}
}

func (node *ChordNode) RunBackgroundTasks() {
	for {
		node.Stabilize()
		node.FixFingers()
		time.Sleep(time.Second)
	}
}
