package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	var prevLogTerm, prevLogIndex uint64
	var entries []*pb.Entry
	nextIdx := r.Prs[to].Next
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)

	if nextIdx > lastLogIndex {
		prevLogTerm = lastLogTerm
		prevLogIndex = lastLogIndex
	} else {
		for _, entry := range r.RaftLog.entries[nextIdx-r.RaftLog.first:] {
			entries = append(entries, &pb.Entry{
				Index: entry.Index,
				Term: entry.Term,
				Data: entry.Data,
			})
		}
		prevLogIndex = nextIdx - 1
		prevLogTerm, _ = r.RaftLog.Term(prevLogIndex)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From: r.id,
		To: to,
		Term: r.Term,
		LogTerm: prevLogTerm,
		Index: prevLogIndex,
		Entries: entries,
		Commit: r.RaftLog.committed,
	})
	return false
}

func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	r.appendEntries(m.Entries)
}

func (r *Raft) appendEntries(entries []*pb.Entry) {
	for _, entry := range entries {
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			Index: r.RaftLog.LastIndex()+1,
			Term: r.Term,
			Data: entry.Data,
		})
	}
	r.Prs[r.id].Next = r.RaftLog.LastIndex()+1
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.tryCommit()

	r.bcastAppend()
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: m.From,
			Term: r.Term,
			Reject: true,
			MsgType: pb.MessageType_MsgAppendResponse,
		})
		return
	}

	r.becomeFollower(m.Term, m.From)

	prevLogIndex := m.Index
	lastLogIndex := r.RaftLog.LastIndex()
	if prevLogIndex > lastLogIndex {
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: m.From,
			Reject: true,
			Term: r.Term,
			Index: lastLogIndex,
			MsgType: pb.MessageType_MsgAppendResponse,
		})
		return
	}
	logTermAtPrevIndex, _ := r.RaftLog.Term(prevLogIndex)
	if logTermAtPrevIndex != m.LogTerm {
		term := logTermAtPrevIndex
		idx := prevLogIndex
		for idx > r.RaftLog.committed {
			curTerm, _ := r.RaftLog.Term(idx)
			if curTerm != term {
				break
			}
			idx -= 1
		}
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: m.From,
			Reject: true,
			Term: r.Term,
			Index: idx,
			MsgType: pb.MessageType_MsgAppendResponse,
		})
		return
	}

	for _, entry := range m.Entries {
		if entry.Index < r.RaftLog.first {
			continue
		}
		if entry.Index <= r.RaftLog.LastIndex() {
			logTerm, _ := r.RaftLog.Term(entry.Index)
			if logTerm != entry.Term {
				idx := entry.Index-r.RaftLog.first
				r.RaftLog.entries[idx] = *entry
				r.RaftLog.entries = r.RaftLog.entries[:idx+1]
				r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
			}
		} else {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
	}

	r.msgs = append(r.msgs, pb.Message{
		From: r.id,
		To: m.From,
		Term: r.Term,
		Index: r.RaftLog.LastIndex(),
		MsgType: pb.MessageType_MsgAppendResponse,
	})

	if r.RaftLog.committed < m.Commit {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}

	if r.State != StateLeader || r.Term < m.Term {
		return
	}

	if !m.Reject {
		if m.Index + 1 > r.Prs[m.From].Next {
			r.Prs[m.From].Next = m.Index + 1
			r.Prs[m.From].Match = m.Index
		}
		oldCommitted := r.RaftLog.committed
		r.tryCommit()
		if r.RaftLog.committed > oldCommitted {
			r.bcastAppend()
		}
		return
	}

	if m.Index + 1 > 0 {
		r.Prs[m.From].Next = m.Index + 1 //m.index means last index
		r.sendAppend(m.From)
	}
}