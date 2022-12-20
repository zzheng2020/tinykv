package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	badgerDB *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		badgerDB: engine_util.CreateDB(conf.DBPath, false),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	// just leave blank in Start()
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).

	// close database
	if err := s.badgerDB.Close(); err != nil {
		return err
	}
	return nil
}

// storage.StorageReader is an interface.
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	txn := s.badgerDB.NewTransaction(false)
	return NewStandAloneReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.badgerDB.NewTransaction(true)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			key := engine_util.KeyWithCF(put.Cf, put.Key)
			value := put.Value
			if err := txn.Set(key, value); err != nil {
				return nil
			}
		case storage.Delete:
			dlt := m.Data.(storage.Delete)
			key := engine_util.KeyWithCF(dlt.Cf, dlt.Key)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
	}

	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

type StandAloneReader struct {
	badgerTxn *badger.Txn
}

func NewStandAloneReader(txn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		badgerTxn: txn,
	}
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	cfValue, err := engine_util.GetCFFromTxn(r.badgerTxn, cf, key)
	// When the key doesn't exist, return nil for the value
	if err != nil {
		return nil, nil
	}

	return cfValue, err
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.badgerTxn)
}

func (r *StandAloneReader) Close() {
	r.badgerTxn.Discard()
}
