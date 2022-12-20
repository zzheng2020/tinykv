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
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}

	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}

	response := &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: false,
	}

	// True if the requested key doesn't exist; another error will not be signalled.
	if value == nil {
		response.NotFound = true
	}

	return response, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}

	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}

	return &kvrpcpb.RawPutResponse{}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}

	err := server.storage.Write(req.Context, batch)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			Error: err.Error(),
		}, err
	}

	return &kvrpcpb.RawDeleteResponse{}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{
			Error: err.Error(),
		}, err
	}

	dbIter := reader.IterCF(req.Cf)

	// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
	// greater than provided.
	dbIter.Seek(req.StartKey)

	kvPair := []*kvrpcpb.KvPair{}

	// Limit: The maximum number of values read.
	if req.Limit == 0 {
		return &kvrpcpb.RawScanResponse{
			Kvs: kvPair,
		}, err
	}

	limit := req.Limit
	for ; dbIter.Valid(); dbIter.Next() {
		if limit == 0 {
			break
		}
		item := dbIter.Item()

		value, _ := item.Value()
		kvPair = append(kvPair, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
		limit--
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvPair,
	}, err
}
