package localstore

import (
	"encoding/hex"
	"errors"

	"github.com/ethersphere/bee/pkg/shed"
	"github.com/syndtr/goleveldb/leveldb"
)

func (db *DB) RebuildIndices() {
	db.batchMu.Lock()
	defer db.batchMu.Unlock()

	batch := new(leveldb.Batch)
	db.pullIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		if err = db.pullIndex.DeleteInBatch(item, batch); err != nil {
			return true, err
		}
		return false, nil
	}, nil)

	err = db.shed.WriteBatch(batch)
	if err != nil {
		return err
	}

	batch = new(leveldb.Batch)
	db.gcIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		if err = db.gcIndex.DeleteInBatch(item, batch); err != nil {
			return true, err
		}
		return false, nil
	}, nil)
	err = db.shed.WriteBatch(batch)
	if err != nil {
		return err
	}

	db.gcSize.Put(0)

	// rebuild gc index
	batch = new(leveldb.Batch)
	gcChange := uint64(0)
	i, err := db.retrievalAccessIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		i2, err := db.retrievalDataIndex.Get(item)
		if err != nil {
			db.logger.Warningf("access index item %s not found in data index. removing entry", hex.EncodeToString(item.Address))
			if err := db.retrievalAccessIndex.DeleteInBatch(item, batch); err != nil {
				return true, err
			}
			return false, nil
		}

		i2.AccessTimestamp = item.AccessTimestamp
		panic("check not in push index or in pin index")
		if err := db.gcIndex.PutInBatch(i2, batch); err != nil {
			return true, err
		}
		gcChange++
		return false, nil
	}, nil)

	// need to get access timestamp here as it is not
	// provided by the access function, and it is not
	// a property of a chunk provided to Accessor.Put.
	i, err := db.retrievalAccessIndex.Get(item)
	switch {
	case err == nil:
		item.AccessTimestamp = i.AccessTimestamp
	case errors.Is(err, leveldb.ErrNotFound):
	default:
		return 0, err
	}
	i, err = db.retrievalDataIndex.Get(item)
	if err != nil {
		return 0, err
	}
	item.StoreTimestamp = i.StoreTimestamp
	item.BinID = i.BinID

	err = db.retrievalDataIndex.DeleteInBatch(batch, item)
	if err != nil {
		return 0, err
	}
	err = db.retrievalAccessIndex.DeleteInBatch(batch, item)
	if err != nil {
		return 0, err
	}
	err = db.pullIndex.DeleteInBatch(batch, item)
	if err != nil {
		return 0, err
	}
	err = db.gcIndex.DeleteInBatch(batch, item)
	if err != nil {
		return 0, err
	}
	// a check is needed for decrementing gcSize
	// as delete is not reporting if the key/value pair
	// is deleted or not
	if _, err := db.gcIndex.Get(item); err == nil {
		gcSizeChange = -1
	}

}
