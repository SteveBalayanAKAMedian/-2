package raft

import (
	"context"
	"log/slog"
)

func (s *RaftServer) beginElection() {
	s.mu.Lock()
	defer s.mu.Unlock()

	slog.Info("Begin election...", "node", s.id)

	s.currentTerm++
	s.state = CANDIDATE
	s.lastVotedFor = s.id

	votes := 1 // self voted

	s.electionTimer = s.Tick(s.electionTimer, s.electionTimeout, s.beginElection)

	for _, peer := range s.peers {
		go func(peer string) {
			request := {}

			response, err := sendRequestVote(peer, request)
			if err == nil && response.VoteGranted {
				s.mu.Lock()
				defer s.mu.Unlock()
				slog.Info("Vote granted!", "node", s.id, "votes", votes)
				votes++
				if votes > len(s.peers)/2 && s.state == CANDIDATE {
					s.becomeLeader()
				}
			}
		}(peer)
	}
}

func (s *RaftServer) becomeLeader() {
	slog.Info("Becoming leader", "node", s.id)

	s.state = LEADER
	s.leaderID = s.id

	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}
}

func (s *RaftServer) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {

}