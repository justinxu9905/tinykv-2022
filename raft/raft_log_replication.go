package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

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
	r.tryToCommit()

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

	logTermAtPrevIndex, _ := r.RaftLog.Term(m.Index)
	lastLogIndex := r.RaftLog.LastIndex()
	if m.Index > lastLogIndex {
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: m.From,
			Reject: true,
			Index: lastLogIndex,
			MsgType: pb.MessageType_MsgAppendResponse,
		})
		return
	} else if logTermAtPrevIndex == m.LogTerm {
		for _, entry := range m.Entries {
			r.RaftLog.entries = append(r.RaftLog.entries[0:m.Index], *entry)
		}
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: m.From,
			Index: r.RaftLog.LastIndex(),
			MsgType: pb.MessageType_MsgAppendResponse,
		})
	} else {
		term := logTermAtPrevIndex
		idx := m.Index
		for idx > r.RaftLog.committed && r.RaftLog.entries[idx].Term == term {
			idx -= 1
		}
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: m.From,
			Reject: true,
			Index: idx,
			MsgType: pb.MessageType_MsgAppendResponse,
		})
		return
	}

	if r.RaftLog.committed < m.Commit {
		r.RaftLog.committed = m.Commit
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if !m.Reject {
		if m.Index + 1 > r.Prs[m.From].Next {
			r.Prs[m.From].Next = m.Index + 1
			r.Prs[m.From].Match = m.Index
		}
		r.tryToCommit()
	}
}