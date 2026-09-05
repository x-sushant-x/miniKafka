package raft

import (
	"context"
	"log"
	"net"
	"sync"

	zeroLog "github.com/rs/zerolog/log"
	"github.com/x-sushant-x/miniKafka/raft/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	pb.UnimplementedRaftServiceServer

	mu       sync.Mutex
	id       string
	host     string
	port     string
	cluster  map[string]string
	peerRPCs map[string]pb.RaftServiceClient

	raft *Raft
}

func NewServer(id, host, port string, cluster map[string]string) *Server {
	return &Server{
		id:       id,
		host:     host,
		port:     port,
		cluster:  cluster,
		peerRPCs: make(map[string]pb.RaftServiceClient),
	}
}

func (s *Server) RequestVote(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteResp, error) {
	return s.raft.HandleRequestVote(req)
}

func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	return s.raft.HandleAppendEntries(req)
}

func (s *Server) SubmitCommand(ctx context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	resp := s.raft.Submit(req.Command)
	return &pb.SubmitResponse{
		IsSuccess: resp,
	}, nil
}

func (s *Server) Serve() {
	ln, err := net.Listen("tcp", ":"+s.port)
	if err != nil {
		panic("unable to serve: " + err.Error())
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, s)

	zeroLog.Info().Str("node", s.id).Str("port", s.port).Msg("Raft Server Running")
	log.Fatal(grpcServer.Serve(ln))
}

func (s *Server) ConnectToAllPeers() {
	zeroLog.Info().Msgf("Node: %s trying to connect to all peers.", s.id)

	for id, addr := range s.cluster {
		if id == s.id {
			continue
		}

		if err := s.connectToPeer(id, addr); err != nil {
			zeroLog.Err(err).Msgf("Node: %s failed to connect to peer %s.", s.id, id)
		}
	}
}

func (s *Server) connectToPeer(id string, address string) error {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		return err
	}

	conn.Connect()

	for {
		state := conn.GetState()

		if state == connectivity.Ready {
			break
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.peerRPCs[id] = pb.NewRaftServiceClient(conn)

	zeroLog.Info().Msgf("Connected to node %s", id)
	return nil
}
