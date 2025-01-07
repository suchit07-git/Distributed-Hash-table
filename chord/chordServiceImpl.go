package chord

import (
	"context"
	"errors"
	"fmt"
	"log"

	pb "github.com/suchit07-git/chordkv/rpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type ChordServiceServer struct {
	pb.UnimplementedChordServiceServer
	node *ChordNode
}

func (s *ChordServiceServer) FindSuccessor(ctx context.Context, req *pb.FindSuccessorRequest) (*pb.Node, error) {
	successor := s.node.FindSuccessor(req.Id)
	return &pb.Node{Id: successor.id, Address: successor.address, Port: successor.port}, nil
}

func (s *ChordServiceServer) GetPredecessor(ctx context.Context, req *emptypb.Empty) (*pb.Node, error) {
	predecessor := s.node.predecessor
	if predecessor != nil {
		return &pb.Node{Id: predecessor.id, Address: predecessor.address, Port: predecessor.port}, nil
	}
	return &pb.Node{Id: -1, Address: "", Port: -1}, errors.New("No predecessor found")
}

func (s *ChordServiceServer) Notify(ctx context.Context, req *pb.Node) (*emptypb.Empty, error) {
	s.node.Notify(&ChordNode{id: req.Id, address: req.Address, port: req.Port})
	return &emptypb.Empty{}, nil
}

func (s *ChordServiceServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	log.Printf("Get Request for key: %s", req.Key)
	value := s.node.Retrieve(req.Key)
	if value == "" {
		return &pb.GetResponse{Value: ""}, errors.New("Key not found")
	}
	fmt.Printf("Retrieved value for key %s: %s\n", req.Key, value)
	return &pb.GetResponse{Value: value}, nil
}

func (s *ChordServiceServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("Put Request for key: %s, value: %s", req.Key, req.Value)
	s.node.Store(req.Key, req.Value)
	return &pb.PutResponse{Success: true}, nil
}

func (s *ChordServiceServer) Delete(ctx context.Context, req *pb.GetRequest) (*emptypb.Empty, error) {
	log.Printf("Delete Request for key: %s", req.Key)
	exists := s.node.Delete(req.Key)
	if !exists {
		return &emptypb.Empty{}, errors.New("Key doesn't exist")
	}
	return &emptypb.Empty{}, nil
}
