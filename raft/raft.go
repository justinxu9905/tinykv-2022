// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeoutBaseline int
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	r := &Raft{
		id: c.ID,
		State: StateFollower,
		Prs: make(map[uint64]*Progress),
		RaftLog: newLog(c.Storage),
		electionTimeoutBaseline: c.ElectionTick,
		electionTimeout: c.ElectionTick + rand.Int() % c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}

	hardState, confState, _ := c.Storage.InitialState()
	r.Term, r.Vote, r.RaftLog.committed = hardState.GetTerm(), hardState.GetVote(), hardState.GetCommit()

	if c.peers == nil {
		c.peers = confState.Nodes
	}
	lastIndex := r.RaftLog.LastIndex()
	for _, peer := range c.peers {
		if peer == r.id {
			r.Prs[peer] = &Progress{Next: lastIndex + 1, Match: lastIndex}
		} else {
			r.Prs[peer] = &Progress{Next: lastIndex + 1}
		}
	}

	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	return r
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed == r.electionTimeout {
			r.electionElapsed = 0
			r.electionTimeout = r.electionTimeoutBaseline + rand.Int() % r.electionTimeoutBaseline
			r.Step(pb.Message{
				From: r.id,
				To: r.id,
				MsgType: pb.MessageType_MsgHup,
			})
		}
		break
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed == r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				From: r.id,
				To: r.id,
				MsgType: pb.MessageType_MsgBeat,
			})
		}
		break
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.State = StateFollower
	r.Lead = lead
	r.votes = nil
	r.electionElapsed = 0
	r.leadTransferee = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.State = StateCandidate
	r.electionElapsed = 0
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.electionElapsed = 0

	lastLogIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		r.Prs[peer].Next = lastLogIndex + 1
		//r.Prs[peer].Match = lastLogIndex
	}

	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term: r.Term,
		Index: r.RaftLog.LastIndex() + 1,
	})
	r.Prs[r.id].Next++
	r.Prs[r.id].Match++

	r.bcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok && m.MsgType == pb.MessageType_MsgTimeoutNow {
		return nil
	}
	switch r.State {
	case StateFollower:
		if m.MsgType == pb.MessageType_MsgHup {
			r.handleHup()
		} else if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.handleHeartbeat(m)
		} else if m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleRequestVote(m)
		} else if m.MsgType == pb.MessageType_MsgAppend {
			r.handleAppendEntries(m)
		} else if m.MsgType == pb.MessageType_MsgSnapshot {
			r.handleSnapshot(m)
		} else if m.MsgType == pb.MessageType_MsgTransferLeader {
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		} else if m.MsgType == pb.MessageType_MsgTimeoutNow {
			r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
		}
		break
	case StateCandidate:
		if m.MsgType == pb.MessageType_MsgHup {
			r.handleHup()
		} else if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.handleHeartbeat(m)
		} else if m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleRequestVote(m)
		} else if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			r.handleRequestVoteResponse(m)
		} else if m.MsgType == pb.MessageType_MsgAppend {
			r.handleAppendEntries(m)
		} else if m.MsgType == pb.MessageType_MsgSnapshot {
			r.handleSnapshot(m)
		} else if m.MsgType == pb.MessageType_MsgTransferLeader {
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		}
		break
	case StateLeader:
		if m.MsgType == pb.MessageType_MsgBeat {
			r.bcastHeartbeat()
		} else if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.handleHeartbeat(m)
		} else if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
			r.sendAppend(m.From)
		} else if m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleRequestVote(m)
		} else if m.MsgType == pb.MessageType_MsgPropose {
			if r.leadTransferee == None {
				r.handlePropose(m)
			}
		} else if m.MsgType == pb.MessageType_MsgAppend {
			r.handleAppendEntries(m)
		} else if m.MsgType == pb.MessageType_MsgAppendResponse {
			r.handleAppendResponse(m)
		} else if m.MsgType == pb.MessageType_MsgSnapshot {
			r.handleSnapshot(m)
		} else if m.MsgType == pb.MessageType_MsgTransferLeader {
			r.handleTransferLeader(m)
		}
		break
	}
	return nil
}

func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgSnapshot,
		From: r.id,
		To: to,
		Term: r.Term,
		Snapshot: &snapshot,
	})
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata
	if meta.Index <= r.RaftLog.committed {
		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To: m.From,
			Reject: false,
			Term: r.Term,
			Index: r.RaftLog.committed,
			MsgType: pb.MessageType_MsgAppendResponse,
		})
		return
	}

	r.becomeFollower(m.Term, m.From)

	if len(r.RaftLog.entries) > 0 {
		r.RaftLog.entries = nil
	}
	r.RaftLog.stabled = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.applied = meta.Index
	r.RaftLog.first = meta.Index + 1
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range meta.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.msgs = append(r.msgs, pb.Message{
		From: r.id,
		To: m.From,
		Reject: false,
		Term: r.Term,
		Index: r.RaftLog.LastIndex(),
		MsgType: pb.MessageType_MsgAppendResponse,
	})
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	if m.From == r.id {
		return
	}
	if r.leadTransferee != None && r.leadTransferee == m.From {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = m.From
	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			From:    r.id,
			To:      m.From,
		}
		r.msgs = append(r.msgs, msg)
	} else {
		r.sendAppend(m.From)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Next: 1}
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		if r.State == StateLeader {
			r.tryCommit()
		}
	}
	r.PendingConfIndex = None
}

func (r *Raft) tryCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	n := match[(len(r.Prs)-1)/2]

	if n > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(n)
		if err != nil {
			panic(err)
		}
		if logTerm == r.Term {
			r.RaftLog.committed = n
			//r.bcastAppend()
		}
	}
}
