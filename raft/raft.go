package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	zeroLog "github.com/rs/zerolog/log"
	"github.com/x-sushant-x/miniKafka/raft/proto/pb"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
	Dead
)

type Raft struct {
	mu            sync.Mutex
	state         State
	lastEventTime time.Time
	votedFor      string
	term          int64
	server        *Server

	log         []LogEntry
	commitIndex int
	lastApplied int

	// Leader Only
	nextIndex  map[string]int
	matchIndex map[string]int

	// Channel on which commands will be sent to application logic.
	applyChan chan ApplyMessage

	groupID string // topic_name-parition_number
}

func NewRaft(server *Server, applyChan chan ApplyMessage, groupID string) *Raft {
	r := &Raft{
		state:         Follower,
		lastEventTime: time.Now(),
		votedFor:      "-1",
		term:          0,
		log:           make([]LogEntry, 0),
		nextIndex:     make(map[string]int),
		matchIndex:    make(map[string]int),
		applyChan:     applyChan,
		server:        server,
		groupID:       groupID,
	}

	// Adding dummy entry to log to make things simple.
	r.log = append(r.log, LogEntry{})
	return r
}

func generateTimeout() time.Duration {
	randomTime := rand.Intn(150)
	raftRandomTime := randomTime + 150
	return time.Duration(raftRandomTime * int(time.Millisecond))
}

func (r *Raft) StartElectionLoop() {
	ticker := time.NewTicker(time.Millisecond * 10)
	defer ticker.Stop()

	timeout := generateTimeout()
	zeroLog.Info().Int64("timeout", timeout.Milliseconds()).Msg("Starting Election Loop")

	for {
		<-ticker.C

		r.mu.Lock()
		state := r.state
		elapsed := time.Since(r.lastEventTime)
		r.mu.Unlock()

		if state == Dead {
			continue
		}

		if state != Candidate && state != Follower {
			continue
		}

		if elapsed >= timeout {
			log.Println("Starting Election")
			r.startElection()
			timeout = generateTimeout()
		}
	}
}

func (r *Raft) startElection() {
	r.mu.Lock()
	r.state = Candidate
	r.term++
	r.votedFor = r.server.id
	savedTerm := r.term
	totalVotesReceived := 1
	r.lastEventTime = time.Now()
	lastLogIndex := int64(len(r.log) - 1)
	lastLogTerm := r.log[lastLogIndex].Term
	r.mu.Unlock()

	for peerID, peerRPC := range r.server.peerRPCs {
		go func(peerID string, peerRPC pb.RaftServiceClient) {
			log.Println("Requesting vote from: " + peerID)

			req := &pb.RequestVoteReq{
				Term:         savedTerm,
				CandidateID:  r.server.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
				GroupId:      r.groupID,
			}

			resp, err := peerRPC.RequestVote(context.Background(), req)
			if err != nil {
				log.Printf("vote RPC to %s failed: %v", peerID, err)
				return
			}

			log.Printf(
				"reply from %s granted=%v term=%d",
				peerID,
				resp.VoteGranted,
				resp.Term,
			)

			r.mu.Lock()
			defer r.mu.Unlock()

			if r.state != Candidate {
				if r.state == Leader {
					log.Println("election already won")
				} else {
					log.Println("someone else became leader")
				}
				return
			}

			if resp.Term > savedTerm {
				r.becameFollower(resp.Term)
				return
			}

			if resp.Term == savedTerm {
				if resp.VoteGranted {
					log.Println("Vote granted by: " + peerID)

					totalVotesReceived++
					clusterSize := len(r.server.peerRPCs) + 1

					if totalVotesReceived > clusterSize/2 && r.state == Candidate {
						log.Println("Won Election")
						r.becameLeader()
					}
				}
			}

		}(peerID, peerRPC)
	}
}

func (r *Raft) HandleRequestVote(req *pb.RequestVoteReq) (*pb.RequestVoteResp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	lastLogIndex := int64(len(r.log) - 1)
	lastLogTerm := r.log[lastLogIndex].Term

	resp := &pb.RequestVoteResp{}

	if r.state == Dead {
		resp.Term = r.term
		resp.VoteGranted = false
		return resp, nil
	}

	if req.Term > r.term {
		r.becameFollower(req.Term)
	}

	isCandidateUpToDate := req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if r.term == req.Term && (r.votedFor == "-1" || r.votedFor == req.CandidateID) && isCandidateUpToDate {
		resp.VoteGranted = true
		r.votedFor = req.CandidateID
		r.lastEventTime = time.Now()
	} else {
		resp.VoteGranted = false
	}

	resp.Term = r.term
	return resp, nil
}

func (r *Raft) HandleAppendEntries(req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &pb.AppendEntriesResponse{
		Success: false,
		Term:    r.term,
	}

	if req.Term < r.term {
		return resp, nil
	}

	r.lastEventTime = time.Now()

	if req.Term > r.term {
		r.becameFollower(req.Term)
	}

	if r.state != Follower {
		r.becameFollower(req.Term)
	}

	lastLogIndex := int64(len(r.log) - 1)

	// Leader is expecting follower to have PrevLogIndex but it does not even have that. So it will reply false. Leader will then
	// decrement index by 1 and resend the entry.
	// This will keep going until leader sends entry that was actually present in follower log.
	if req.PrevLogIndex > lastLogIndex {
		return resp, nil
	}

	if req.PrevLogTerm != r.log[req.PrevLogIndex].Term {
		return resp, nil
	}

	// Appending new entries
	for i, entry := range req.Entries {
		index := req.PrevLogIndex + 1 + int64(i)

		// There are existing entries that are conflicting with leader new entries. We need to discard them.
		if index <= int64(len(r.log)-1) {
			if r.log[index].Term != entry.Term {
				r.log = r.log[:index]

				for j := i; j < len(req.Entries); j++ {
					r.log = append(r.log, LogEntry{
						Term:    req.Entries[j].Term,
						Command: req.Entries[j].Command,
					})
				}

				break
			}

			continue
		}

		r.log = append(r.log, LogEntry{
			Term:    entry.Term,
			Command: entry.Command,
		})
	}

	if req.LeaderCommit > int64(r.commitIndex) {
		r.commitIndex = min(int(req.LeaderCommit), len(r.log)-1)
	}

	resp.Term = r.term
	resp.Success = true

	return resp, nil
}

func (r *Raft) becameFollower(term int64) {
	r.term = term
	r.state = Follower
	r.votedFor = "-1"
	r.lastEventTime = time.Now()
}

func (r *Raft) becameLeader() {
	r.state = Leader

	for peerID := range r.server.peerRPCs {
		r.nextIndex[peerID] = len(r.log)
		r.matchIndex[peerID] = 0
	}

	go func() {
		ticker := time.NewTicker(time.Millisecond * 50)
		defer ticker.Stop()

		for {
			r.sendHeartBeats()
			<-ticker.C

			r.mu.Lock()
			if r.state != Leader {
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
		}
	}()
}

func (r *Raft) sendHeartBeats() {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	savedTerm := r.term
	leaderID := r.server.id
	r.mu.Unlock()

	for peerID, peerRPC := range r.server.peerRPCs {
		r.replicateToPeer(peerID, peerRPC, savedTerm, leaderID)
	}
}

func (r *Raft) replicateToPeer(peerID string, peerRPC pb.RaftServiceClient, savedTerm int64, leaderID string) {
	r.mu.Lock()

	next := r.nextIndex[peerID]
	prevLogIndex := next - 1
	prevLogTerm := r.log[prevLogIndex].Term
	entries := make([]LogEntry, len(r.log[next:]))
	copy(entries, r.log[next:])
	pbEntries := []*pb.LogEntry{}
	leaderCommit := r.commitIndex

	r.mu.Unlock()

	for _, entry := range entries {
		pbEntries = append(pbEntries, &pb.LogEntry{
			Term:    entry.Term,
			Command: entry.Command,
		})
	}

	req := &pb.AppendEntriesRequest{
		Term:         savedTerm,
		LeaderId:     leaderID,
		PrevLogIndex: int64(prevLogIndex),
		PrevLogTerm:  prevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: int64(leaderCommit),
		GroupId:      r.groupID,
	}

	// As of now we will hit clients RPCs synchronously to keep things simple.
	resp, err := peerRPC.AppendEntries(context.Background(), req)
	if err == nil {
		r.mu.Lock()

		if r.state != Leader {
			r.mu.Unlock()
			return
		}

		if resp.Term > savedTerm {
			r.becameFollower(resp.Term)
			r.mu.Unlock()
			return
		}

		if !resp.Success {
			if r.nextIndex[peerID] > 1 {
				r.nextIndex[peerID]--
			}
		} else {
			lastReplicatedIndex := int(req.PrevLogIndex) + len(pbEntries)
			r.nextIndex[peerID] = lastReplicatedIndex + 1
			r.matchIndex[peerID] = lastReplicatedIndex

			r.advanceLeaderCommit()
		}

		r.mu.Unlock()
	}
}

func (r *Raft) advanceLeaderCommit() {
	clusterSize := len(r.server.peerRPCs) + 1

	for N := len(r.log) - 1; N > r.commitIndex; N-- {
		replicatedCount := 1

		for peerID := range r.server.peerRPCs {
			if r.matchIndex[peerID] >= N {
				replicatedCount++
			}
		}

		if replicatedCount > clusterSize/2 && r.log[N].Term == r.term {
			r.commitIndex = N
			break
		}
	}
}

func (r *Raft) ApplyLoop() {
	for {
		r.mu.Lock()

		if r.lastApplied < r.commitIndex {
			r.lastApplied++
			index := r.lastApplied
			command := r.log[index].Command

			r.mu.Unlock()

			r.applyChan <- ApplyMessage{
				Index:   int64(index),
				Command: command,
			}

			continue
		}

		r.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (r *Raft) Submit(command []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return false
	}

	entry := LogEntry{
		Term:    r.term,
		Command: command,
	}

	r.log = append(r.log, entry)

	return true
}
