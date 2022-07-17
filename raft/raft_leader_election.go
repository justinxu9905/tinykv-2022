package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		From: r.id,
		To: to,
		Term: r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
	})
}

func (r *Raft) bcastHeartbeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

func (r *Raft) handleHup() {
	r.becomeCandidate()

	if len(r.Prs) == 1 {
		r.becomeLeader()
	}

	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)

	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: peer,
			Term: r.Term,
			Index: lastLogIndex,
			LogTerm: lastLogTerm,
			MsgType: pb.MessageType_MsgRequestVote,
		})
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: m.From,
			Term: r.Term,
			Reject: true,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
		})
		return
	} else if m.Term == r.Term {
		if r.Vote == m.From {
			r.msgs = append(r.msgs, pb.Message{
				From: r.id,
				To: m.From,
				Term: r.Term,
				MsgType: pb.MessageType_MsgRequestVoteResponse,
			})
			return
		}
		if r.Vote != 0 && r.Vote != m.From {
			r.msgs = append(r.msgs, pb.Message{
				From: r.id,
				To: m.From,
				Term: r.Term,
				Reject: true,
				MsgType: pb.MessageType_MsgRequestVoteResponse,
			})
			return
		}
	}

	if m.Term > r.Term {
		r.Vote = 0
		r.becomeFollower(m.Term, None) // cannot make sure who is leader, just update term
	}

	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	if lastLogTerm > m.LogTerm || (lastLogTerm == m.LogTerm && lastLogIndex > m.Index) {
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: m.From,
			Term: r.Term,
			Reject: true,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
		})
		return
	}

	r.Vote = m.From
	r.becomeFollower(m.Term, None)
	r.msgs = append(r.msgs, pb.Message{
		From: r.id,
		To: m.From,
		Term: r.Term,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
	})
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	n := len(r.Prs)
	grant := 0
	votes := len(r.votes)
	for _, v := range r.votes {
		if v {
			grant++
		}
	}
	if grant > n/2 {
		r.becomeLeader()
	} else if votes-grant > n/2 {
		r.becomeFollower(r.Term, None)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: m.From,
			Term: r.Term,
			Reject: true,
			MsgType: pb.MessageType_MsgHeartbeatResponse,
		})
		return
	}

	r.becomeFollower(m.Term, m.From)
	r.msgs = append(r.msgs, pb.Message{
		From: r.id,
		To: m.From,
		Term: r.Term,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	})
}