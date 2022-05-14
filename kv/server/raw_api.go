package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	resp.Value = value
	if value == nil {
		resp.NotFound = true
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := &kvrpcpb.RawPutResponse{}
	modifyBatch := []storage.Modify{{
		Data: storage.Put{
			Cf: req.Cf,
			Key:   req.Key,
			Value: req.Value,
		},
	}}
	err := server.storage.Write(req.Context, modifyBatch)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := &kvrpcpb.RawDeleteResponse{}
	modifyBatch := []storage.Modify{{
		Data: storage.Delete{
			Cf: req.Cf,
			Key: req.Key,
		},
	}}
	err := server.storage.Write(req.Context, modifyBatch)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	defer reader.Close()

	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	limit := req.Limit
	for iter.Seek(req.StartKey); iter.Valid() && limit > 0; iter.Next() {
		k := iter.Item().Key()
		v, _ := iter.Item().Value()
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key: k,
			Value: v,
		})
		limit -= 1
	}
	return resp, nil
}
