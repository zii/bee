package localstore

import (
	"encoding/hex"
	"errors"
	"time"

	"github.com/ethersphere/bee/pkg/shed"
	"github.com/syndtr/goleveldb/leveldb"
)

func (db *DB) Rebuild() error {
	db.batchMu.Lock()
	defer db.batchMu.Unlock()
	var lim = 100000
	batch := new(leveldb.Batch)
	count := 0
	db.pullIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		if err = db.pullIndex.DeleteInBatch(batch, item); err != nil {
			return true, err
		}
		count++
		if count%lim == 0 {
			db.logger.Infof("pullsync cleanup writing batch %d", count)
			err := db.shed.WriteBatch(batch)
			if err != nil {
				return true, err
			}
			batch = new(leveldb.Batch)
		}
		return false, nil
	}, nil)
	err := db.shed.WriteBatch(batch)
	if err != nil {
		return err
	}
	count = 0

	batch = new(leveldb.Batch)
	db.gcIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		if err = db.gcIndex.DeleteInBatch(batch, item); err != nil {
			return true, err
		}
		count++
		if count%lim == 0 {
			db.logger.Infof("gc cleanup writing batch %d", count)
			err := db.shed.WriteBatch(batch)
			if err != nil {
				return true, err
			}
			batch = new(leveldb.Batch)
		}
		return false, nil
	}, nil)
	err = db.shed.WriteBatch(batch)
	if err != nil {
		return err
	}

	db.gcSize.Put(0)
	count = 0
	lim = lim / 3
	db.logger.Infof("gc and pullsync cleanup done, sleeping 5 minutes")
	time.Sleep(5 * time.Minute)
	// rebuild gc index
	batch = new(leveldb.Batch)
	gcChange := uint64(0)
	err = db.retrievalAccessIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		i2, err := db.retrievalDataIndex.Get(item)
		if err != nil {
			db.logger.Warningf("access index item %s not found in data index. error %v. removing entry", hex.EncodeToString(item.Address), err)
			if err := db.retrievalAccessIndex.DeleteInBatch(batch, item); err != nil {
				return true, err
			}
			return false, nil
		}

		// todo need to check that item is not in push index or in pin index for this to fly on users machines too
		i2.AccessTimestamp = item.AccessTimestamp
		if err := db.gcIndex.PutInBatch(batch, i2); err != nil {
			return true, err
		}

		if err := db.pullIndex.PutInBatch(batch, i2); err != nil {
			return true, err
		}

		gcChange++

		count++
		if count%lim == 0 {
			db.logger.Infof("accesindex cleanup writing batch %d", count)
			err := db.shed.WriteBatch(batch)
			if err != nil {
				return true, err
			}
			db.gcSize.Put(gcChange)
			batch = new(leveldb.Batch)
		}

		return false, nil
	}, nil)
	if err != nil {
		return err
	}
	err = db.shed.WriteBatch(batch)
	if err != nil {
		return err
	}

	db.gcSize.Put(gcChange)

	db.logger.Infof("accessindex cleanup done, sleeping 5 minutes")
	count = 0
	time.Sleep(5 * time.Minute)

	// force data index into gc
	batch = new(leveldb.Batch)
	err = db.retrievalDataIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		_, err = db.retrievalAccessIndex.Get(item)
		if err != nil {
			if !errors.Is(err, leveldb.ErrNotFound) {
				return true, err
			}
			db.logger.Infof("chunk %s not found in access index, adding to access and gc indexes", hex.EncodeToString(item.Address))

			// item not accessed
			item.AccessTimestamp = now()
			err = db.retrievalAccessIndex.PutInBatch(batch, item)
			if err != nil {
				return true, err
			}

			err = db.gcIndex.PutInBatch(batch, item)
			if err != nil {
				return true, err
			}
			gcChange++

			count++
			if count%lim == 0 {
				db.logger.Infof("force gc cleanup writing batch %d", count)
				err := db.shed.WriteBatch(batch)
				if err != nil {
					return true, err
				}
				db.gcSize.Put(gcChange)
				batch = new(leveldb.Batch)
			}
		}
		return false, nil
	}, nil)
	if err != nil {
		return err
	}

	err = db.shed.WriteBatch(batch)
	if err != nil {
		return err
	}

	db.logger.Infof("all done! calling gc")

	go db.triggerGarbageCollection()
	return nil
}
