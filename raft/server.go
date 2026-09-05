package raft

import (
	"context"
	"errors"
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

	mu       sync.RWMutex
	id       string
	host     string
	port     string
	cluster  map[string]string
	peerRPCs map[string]pb.RaftServiceClient
	rafts    map[string]*Raft
}

func NewServer(id, host, port string, cluster map[string]string) *Server {
	return &Server{
		id:       id,
		host:     host,
		port:     port,
		cluster:  cluster,
		peerRPCs: make(map[string]pb.RaftServiceClient),
		rafts:    make(map[string]*Raft),
	}
}

func (s *Server) AddRaft(groupID string, raft *Raft) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rafts[groupID] = raft
}

func (s *Server) RequestVote(ctx context.Context, req *pb.RequestVoteReq) (*pb.RequestVoteResp, error) {
	s.mu.RLock()
	raft := s.rafts[req.GroupId]
	s.mu.RUnlock()

	if raft == nil {
		return nil, errors.New("raft group not found for: " + req.GroupId)
	}

	return raft.HandleRequestVote(req)
}

func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	s.mu.RLock()
	raft := s.rafts[req.GroupId]
	s.mu.RUnlock()

	if raft == nil {
		return nil, errors.New("raft group not found for: " + req.GroupId)
	}

	return raft.HandleAppendEntries(req)
}

func (s *Server) SubmitCommand(ctx context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	s.mu.RLock()
	raft := s.rafts[req.GroupId]
	s.mu.RUnlock()

	if raft == nil {
		return nil, errors.New("raft group not found for: " + req.GroupId)
	}

	resp := raft.Submit(req.Command)
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
