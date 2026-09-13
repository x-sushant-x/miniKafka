package raft

import (
	"context"

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
	mu    sync.Mutex
	state State
	// lastEventTime    time.Time
	electionDeadline time.Time
	votedFor         string
	term             int64
	server           *Server
	log              *RaftLog
	commitIndex      int
	lastApplied      int

	// Leader Only
	nextIndex  map[string]int
	matchIndex map[string]int

	// Channel on which commands will be sent to application logic.
	applyChan chan ApplyMessage

	groupID string // topic_name-parition_number
}

func NewRaft(server *Server, applyChan chan ApplyMessage, groupID, raftStorageDir string) *Raft {
	r := &Raft{
		state: Follower,
		// lastEventTime:    time.Now(),
		electionDeadline: time.Now().Add(generateTimeout()),
		votedFor:         "-1",
		term:             0,
		nextIndex:        make(map[string]int),
		matchIndex:       make(map[string]int),
		applyChan:        applyChan,
		server:           server,
		groupID:          groupID,
	}

	raftLog, err := openRaftLog(raftStorageDir)
	if err != nil {
		zeroLog.Fatal().Err(err).Msg("unable to open raftLog")
	}

	r.log = raftLog

	if err := r.loadPersistentState(raftStorageDir); err != nil {
		zeroLog.Fatal().Err(err).Msg("unable to load raft state")
	}

	/* Give an existing leader enough time to send a heartbeat
	before this restarted node attempts an election. */
	r.electionDeadline = time.Now().Add(2000 * time.Millisecond)
	return r
}

func generateTimeout() time.Duration {
	randomTime := rand.Intn(150)
	raftRandomTime := randomTime + 150
	return time.Duration(raftRandomTime * int(time.Millisecond))
}

// Whenever something meaningful happen we will move election deadline forward.
func (r *Raft) StartElectionLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	r.mu.Lock()
	deadline := r.electionDeadline
	r.mu.Unlock()

	zeroLog.Info().
		Dur("timeout", time.Until(deadline)).
		Msg("Starting Election Loop")

	for range ticker.C {
		r.mu.Lock()

		if r.state == Dead {
			r.mu.Unlock()
			continue
		}

		if (r.state == Follower || r.state == Candidate) && time.Now().After(r.electionDeadline) {

			r.mu.Unlock()

			zeroLog.Info().Msg("Starting Election")
			r.startElection()

			continue
		}

		r.mu.Unlock()
	}
}

func (r *Raft) resetElectionDeadline() {
	now := time.Now()

	// r.lastEventTime = now
	r.electionDeadline = now.Add(generateTimeout())
}

func (r *Raft) startElection() {
	r.mu.Lock()
	r.state = Candidate
	r.term++
	r.votedFor = r.server.id
	savedTerm := r.term

	if err := r.persistStateLocked(); err != nil {
		zeroLog.Err(err).Msg("failed to persist election state")
	}

	r.resetElectionDeadline()
	totalVotesReceived := 1
	lastLogIndex := r.log.LastIndex()
	lastLogTerm := r.log.LastTerm()
	r.mu.Unlock()

	for peerID, peerRPC := range r.server.peerRPCs {
		go func(peerID string, peerRPC pb.RaftServiceClient) {
			zeroLog.Info().Msg("Requesting vote from: " + peerID)

			req := &pb.RequestVoteReq{
				Term:         savedTerm,
				CandidateID:  r.server.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
				GroupId:      r.groupID,
			}

			resp, err := peerRPC.RequestVote(context.Background(), req)
			if err != nil {
				zeroLog.Info().Msgf("vote RPC to %s failed: %v", peerID, err)
				return
			}

			zeroLog.Info().Msgf(
				"reply from %s granted=%v term=%d",
				peerID,
				resp.VoteGranted,
				resp.Term,
			)

			r.mu.Lock()
			defer r.mu.Unlock()

			if r.state != Candidate || r.term != savedTerm {
				if r.state == Leader {
					zeroLog.Info().Msg("election already won")
				} else {
					zeroLog.Info().Msg("someone else became leader")
				}
				return
			}

			if resp.Term > savedTerm {
				zeroLog.Info().
					Int64("term", resp.Term).
					Int64("savedTerm", savedTerm).
					Msg("Becaming follower because received higher term from RequestVote")
				r.becameFollower(resp.Term)
				return
			}

			if resp.Term == savedTerm {
				if resp.VoteGranted {
					zeroLog.Info().Msg("Vote granted by: " + peerID)

					totalVotesReceived++
					clusterSize := len(r.server.peerRPCs) + 1

					if totalVotesReceived > clusterSize/2 && r.state == Candidate {
						zeroLog.Info().Msg("Won Election")
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

	lastLogIndex := r.log.LastIndex()
	lastLogTerm := r.log.LastTerm()

	resp := &pb.RequestVoteResp{}

	if r.state == Dead {
		resp.Term = r.term
		resp.VoteGranted = false
		return resp, nil
	}

	if req.Term > r.term {
		zeroLog.Info().
			Int64("reqTerm", req.Term).
			Int64("r.term", r.term).
			Msg("Becaming follower because someone else sent RequestVote with higher term.")
		r.becameFollower(req.Term)
	}

	isCandidateUpToDate := req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if r.term == req.Term && (r.votedFor == "-1" || r.votedFor == req.CandidateID) && isCandidateUpToDate {
		r.votedFor = req.CandidateID

		if err := r.persistStateLocked(); err != nil {
			zeroLog.Err(err).Msg("failed to persist vote")
			resp.VoteGranted = false
		} else {
			resp.VoteGranted = true
			r.resetElectionDeadline()
		}
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

	if req.Term > r.term {
		zeroLog.Info().
			Int64("reqTerm", req.Term).
			Int64("r.term", r.term).
			Msg("Becaming follower because got higher term in HandleAppendEntries")
		r.becameFollower(req.Term)
	} else if r.state != Follower {
		// Same term: step down without resetting votedFor.
		r.state = Follower
	}

	r.resetElectionDeadline()

	lastLogIndex := r.log.LastIndex()

	// Leader is expecting follower to have PrevLogIndex but it does not even have that. So it will reply false. Leader will then
	// decrement index by 1 and resend the entry.
	// This will keep going until leader sends entry that was actually present in follower log.
	if req.PrevLogIndex > lastLogIndex {
		return resp, nil
	}

	prevEntry, err := r.log.Get(int(req.PrevLogIndex))
	if err != nil {
		return resp, err
	}

	if req.PrevLogTerm != prevEntry.Term {
		return resp, nil
	}

	// Appending new entries
	for i, incoming := range req.Entries {
		index := req.PrevLogIndex + 1 + int64(i)

		// There are existing entries that are conflicting with leader new entries. We need to discard them.
		if index <= r.log.LastIndex() {
			existing, err := r.log.Get(int(index))
			if err != nil {
				return resp, err
			}

			if existing.Term != incoming.Term {
				if err := r.log.TruncateFrom(int(index)); err != nil {
					return resp, err
				}

				for j := i; j < len(req.Entries); j++ {
					entry := LogEntry{
						Term:    req.Entries[j].Term,
						Command: req.Entries[j].Command,
					}

					if _, err := r.log.Append(entry); err != nil {
						return resp, err
					}
				}
				break
			}
			continue
		}

		entry := LogEntry{
			Term:    incoming.Term,
			Command: incoming.Command,
		}

		if _, err := r.log.Append(entry); err != nil {
			return resp, err
		}
	}

	if req.LeaderCommit > int64(r.commitIndex) {
		lastIndex := r.log.LastIndex()
		r.commitIndex = min(int(req.LeaderCommit), int(lastIndex))
	}

	resp.Term = r.term
	resp.Success = true

	return resp, nil
}

func (r *Raft) becameFollower(term int64) {
	if term > r.term {
		r.term = term
		r.votedFor = "-1"

		if err := r.persistStateLocked(); err != nil {
			zeroLog.Err(err).Msg("failed to persist follower state")
		}
	}

	r.state = Follower
	r.resetElectionDeadline()
}

func (r *Raft) becameLeader() {
	r.state = Leader

	for peerID := range r.server.peerRPCs {
		r.nextIndex[peerID] = int(r.log.LastIndex() + 1)
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
		go r.replicateToPeer(peerID, peerRPC, savedTerm, leaderID)
	}
}

func (r *Raft) replicateToPeer(peerID string, peerRPC pb.RaftServiceClient, savedTerm int64, leaderID string) {
	r.mu.Lock()

	if r.state != Leader {
		r.mu.Unlock()
		return
	}

	next := r.nextIndex[peerID]
	prevLogIndex := next - 1

	prevEntry, err := r.log.Get(prevLogIndex)
	if err != nil {
		r.mu.Unlock()
		zeroLog.Err(err).Msgf("failed to get prev log entry for peer %s", peerID)
		return
	}

	lastIndex := int(r.log.LastIndex())
	prevLogTerm := prevEntry.Term

	var entries []LogEntry

	if next <= lastIndex {
		entries, err = r.log.GetRange(next, lastIndex+1)
		if err != nil {
			r.mu.Unlock()
			zeroLog.Err(err).Msgf("failed to read log range for peer %s", peerID)
			return
		}
	}

	leaderCommit := r.commitIndex

	r.mu.Unlock()

	pbEntries := make([]*pb.LogEntry, 0, len(entries))

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
			zeroLog.Info().
				Int64("reqTerm", req.Term).
				Int64("r.term", r.term).
				Msg("Becaming follower because got higher term while sending AppendEntries as heartbeat.")
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
	lastIndex := int(r.log.LastIndex())

	for N := lastIndex; N > r.commitIndex; N-- {
		replicatedCount := 1

		for peerID := range r.server.peerRPCs {
			if r.matchIndex[peerID] >= N {
				replicatedCount++
			}
		}

		if replicatedCount <= clusterSize/2 {
			continue
		}

		entry, err := r.log.Get(N)
		if err != nil {
			zeroLog.Info().Msgf("failed to read log entry %d: %v", N, err)
			continue
		}

		// Raft rule: only commit entries from current term
		// using the normal majority mechanism.
		if entry.Term == r.term {
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
			entry, err := r.log.Get(index)
			if err != nil {
				r.mu.Unlock()
				zeroLog.Err(err).Msgf("failed to read committed log entry %d", index)
				continue
			}

			r.mu.Unlock()

			r.applyChan <- ApplyMessage{
				Index:   int64(index),
				Command: entry.Command,
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

	_, err := r.log.Append(entry)
	if err != nil {
		zeroLog.Info().Msgf("failed to append raft entry: %v", err)
		return false
	}

	return true
}

func (r *Raft) SetLastApplied(offset uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.lastApplied = int(offset)
	if r.commitIndex < r.lastApplied {
		r.commitIndex = r.lastApplied
	}
}
