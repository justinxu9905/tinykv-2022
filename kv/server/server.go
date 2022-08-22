package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return resp, err
	}
	if lock != nil && lock.Ts <= req.Version {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key: 		 req.Key,
				LockTtl: 	 lock.Ttl,
			},
		}
		return resp, nil
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		return resp, err
	}
	if value == nil {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = value
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, mutation := range req.Mutations {
		write, ts, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			return resp, err
		}
		if write != nil && ts >= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs: write.StartTS,
					ConflictTs: ts,
					Key: mutation.Key,
					Primary: req.PrimaryLock,
				},
			})
			continue
		}

		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return resp, err
		}
		if lock != nil && lock.Ts != req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key: 		 mutation.Key,
					LockTtl: 	 lock.Ttl,
				},
			})
			continue
		}

		newLock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts: req.StartVersion,
			Ttl: req.LockTtl,
		}
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			newLock.Kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			newLock.Kind = mvcc.WriteKindDelete
		case kvrpcpb.Op_Rollback:
			newLock.Kind = mvcc.WriteKindRollback
		}
		txn.PutLock(mutation.Key, newLock)

		txn.PutValue(mutation.Key, mutation.Value)

		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			write, ts, _ := txn.MostRecentWrite(key)
			if ts == req.CommitVersion && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{} // ?
			}
			return resp, nil
		}
		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{}
			return resp, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
