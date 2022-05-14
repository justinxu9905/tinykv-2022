package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engine 		*engine_util.Engines
	config 		*config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	kvPath := filepath.Join(conf.DBPath, "kv")
	raftPath := filepath.Join(conf.DBPath, "raft")

	kvDb := engine_util.CreateDB(kvPath, false)
	raftDb := engine_util.CreateDB(raftPath, true)

	return &StandAloneStorage{
		engine: engine_util.NewEngines(kvDb, raftDb, kvPath, raftPath),
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.engine.Kv.NewTransaction(false)
	return &StandAloneStorageReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.engine.Kv, modify.Cf(), modify.Key(), modify.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.engine.Kv, modify.Cf(), modify.Key())
			if err != nil {
				return err
			}
		}
	}
	return nil
}


type StandAloneStorageReader struct {
	txn	 	*badger.Txn
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneStorageReader) Close() {
	s.txn.Discard()
}