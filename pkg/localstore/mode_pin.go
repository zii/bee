// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.package storage

package localstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/syndtr/goleveldb/leveldb"

	"github.com/ethersphere/bee/pkg/shed"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

// Pin updates database indexes for chunks when performing pinning.
func (db *DB) Pin(ctx context.Context, mode storage.ModePin, rootAddr, addr swarm.Address) (err error) {
	db.metrics.ModePin.Inc()
	defer totalTimeMetric(db.metrics.TotalTimePin, time.Now())

	err = db.pin(mode, rootAddr, addr)
	if err != nil {
		db.metrics.ModePinFailure.Inc()
	}
	return err
}

// pin updates database indexes for chunks represented by provided addresses.
// It acquires lockAddr to protect two calls
// of this function for the same address in parallel.
func (db *DB) pin(mode storage.ModePin, rootAddr, addr swarm.Address) (err error) {
	// protect parallel updates
	db.batchMu.Lock()
	defer db.batchMu.Unlock()

	batch := new(leveldb.Batch)

	switch mode {
	case storage.ModePinSingle:
		has, err := db.retrievalDataIndex.Has(addressToItem(addr))
		if err != nil {
			return err
		}

		if !has {
			return storage.ErrNotFound
		}

		err = db.pinSingle(batch, addr)
		if err != nil {
			return err
		}
	case storage.ModePinUnpinSingle:
		err := db.pinUnpinSingle(batch, addr)
		if err != nil {
			return err
		}
	case storage.ModePinStarted:
		has, err := db.retrievalDataIndex.Has(addressToItem(rootAddr))
		if err != nil {
			return err
		}

		if !has {
			return storage.ErrNotFound
		}

		err = db.pinStarted(batch, rootAddr)
		if err != nil {
			return err
		}
	case storage.ModePinCompleted:
		err := db.pinCompleted(batch, rootAddr)
		if err != nil {
			return err
		}
	case storage.ModePinFoundAddress:
		err := db.pinFoundAddress(batch, rootAddr, addr)
		if err != nil {
			return err
		}
	case storage.ModePinAddressesCompleted:
		err := db.pinAddressesCompleted(batch, rootAddr)
		if err != nil {
			return err
		}
	case storage.ModePinUnpinStarted:
		err := db.pinUnpinStarted(batch, rootAddr)
		if err != nil {
			return err
		}
	case storage.ModePinUnpinCompleted:
		err := db.pinUnpinCompleted(batch, rootAddr)
		if err != nil {
			return err
		}
	case storage.ModePinUnpinFoundAddress:
		err := db.pinUnpinFoundAddress(rootAddr, addr)
		if err != nil {
			return err
		}
	default:
		return ErrInvalidMode
	}

	err = db.shed.WriteBatch(batch)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) pinSingle(batch *leveldb.Batch, addr swarm.Address) (err error) {
	item := shed.Item{
		ParentAddress: addr.Bytes(),
	}

	has, err := db.pinningIndex.Has(item)
	if err != nil {
		return err
	}

	if has {
		// already pinned
		return nil
	}

	// item was not pinned previously
	item.PinCounter = 1

	err = db.pinningIndex.PutInBatch(batch, item)
	if err != nil {
		return err
	}

	zeroItem := addressToItem(addr)

	// add in gcExcludeIndex if the chunk is not pinned already
	err = db.gcExcludeIndex.PutInBatch(batch, zeroItem)
	if err != nil {
		return err
	}

	zeroPinnedItem, err := db.pinningIndex.Get(zeroItem)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			zeroItem.PinCounter = 1
		} else {
			return err
		}
	} else {
		zeroItem.PinCounter = zeroPinnedItem.PinCounter + 1
	}

	err = db.pinningIndex.PutInBatch(batch, zeroItem)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) pinUnpinSingle(batch *leveldb.Batch, addr swarm.Address) (err error) {
	item := shed.Item{
		ParentAddress: addr.Bytes(),
	}

	_, err = db.pinningIndex.Get(item)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return storage.ErrNotFound
		}

		return err
	}

	err = db.pinningIndex.DeleteInBatch(batch, item)
	if err != nil {
		return err
	}

	zeroItem := addressToItem(addr)

	zeroPinnedItem, err := db.pinningIndex.Get(zeroItem)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return storage.ErrNotFound
		}

		return err
	}

	if zeroPinnedItem.PinCounter > 1 {
		zeroItem.PinCounter = zeroPinnedItem.PinCounter - 1

		err = db.pinningIndex.PutInBatch(batch, zeroItem)
		if err != nil {
			return err
		}
	} else {
		err = db.pinningIndex.DeleteInBatch(batch, zeroItem)
		if err != nil {
			return err
		}

		// remove from gcExcludeIndex
		err = db.gcExcludeIndex.DeleteInBatch(batch, zeroItem)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) pinStarted(batch *leveldb.Batch, addr swarm.Address) (err error) {
	item := shed.Item{
		Address:       addr.Bytes(),
		ParentAddress: addr.Bytes(),
	}

	var exists bool

	_, err = db.pinningIndex.Get(item)
	if err != nil {
		if !errors.Is(err, leveldb.ErrNotFound) {
			return err
		}
	} else {
		exists = true
	}

	if !exists {
		// item was not pinned previously
		item.PinCounter = 1

		err = db.pinningIndex.PutInBatch(batch, item)
		if err != nil {
			return err
		}

		zeroItem := addressToItem(addr)

		zeroPinnedItem, err := db.pinningIndex.Get(zeroItem)
		if err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
				zeroItem.PinCounter = 1
			} else {
				return err
			}
		} else {
			zeroItem.PinCounter = zeroPinnedItem.PinCounter + 1
		}

		err = db.pinningIndex.PutInBatch(batch, zeroItem)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) pinCompleted(batch *leveldb.Batch, rootAddr swarm.Address) (err error) {
	item := shed.Item{
		Address:       rootAddr.Bytes(),
		ParentAddress: rootAddr.Bytes(),
	}

	// Get the existing pin counter of the chunk
	pinnedItem, err := db.pinningIndex.Get(item)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return storage.ErrNotFound
		}

		return err
	}

	if pinnedItem.PinCounter > 0 {
		return fmt.Errorf("pinning cannot be completed: %s", rootAddr.String())
	}

	item.PinCounter = 1

	err = db.pinningIndex.PutInBatch(batch, item)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) pinFoundAddress(batch *leveldb.Batch, rootAddr, addr swarm.Address) (err error) {
	item := shed.Item{
		Address:       addr.Bytes(),
		ParentAddress: rootAddr.Bytes(),
	}

	existingPinCounter := uint64(0)
	pinnedItem, err := db.pinningIndex.Get(item)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			// if this Address is not present in DB, then it is a new entry
			existingPinCounter = 0
		} else {
			return err
		}
	} else {
		existingPinCounter = pinnedItem.PinCounter
	}

	item.PinCounter = existingPinCounter + 1
	err = db.pinningIndex.PutInBatch(batch, item)
	if err != nil {
		return err
	}

	// add reverse index item
	reverseIndexItem := shed.Item{
		Address:       rootAddr.Bytes(),
		ParentAddress: addr.Bytes(),
	}

	has, err := db.pinningIndex.Has(reverseIndexItem)
	if err != nil {
		return err
	}

	if !has {
		reverseIndexItem.PinCounter = 0

		err = db.pinningIndex.PutInBatch(batch, reverseIndexItem)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) pinAddressesCompleted(batch *leveldb.Batch, rootAddr swarm.Address) (err error) {
	err = db.pinningIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		_, err = db.retrievalAccessIndex.Get(item)
		if err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
				return true, storage.ErrNotFound
			}

			return true, err
		}

		reverseIndexItem := shed.Item{
			Address:       rootAddr.Bytes(),
			ParentAddress: item.Address,
		}

		_, err = db.pinningIndex.Get(reverseIndexItem)
		if err != nil {
			// if missing maybe unpin was called before pin completed
			if errors.Is(err, leveldb.ErrNotFound) {
				return true, storage.ErrNotFound
			}

			return true, err
		}

		reverseIndexItem.PinCounter = item.PinCounter

		err = db.pinningIndex.PutInBatch(batch, reverseIndexItem)
		if err != nil {
			return true, err
		}

		// add to gcExcludeIndex
		err = db.gcExcludeIndex.PutInBatch(batch, item)
		if err != nil {
			return true, err
		}

		return false, nil
	}, &shed.IterateOptions{
		Prefix: rootAddr.Bytes(),
	})

	return err
}

func (db *DB) pinUnpinStarted(batch *leveldb.Batch, rootAddr swarm.Address) (err error) {
	item := shed.Item{
		Address:       rootAddr.Bytes(),
		ParentAddress: rootAddr.Bytes(),
	}

	_, err = db.pinningIndex.Get(item)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return storage.ErrNotFound
		}

		return err
	}

	err = db.pinningIndex.DeleteInBatch(batch, item)
	if err != nil {
		return err
	}

	err = db.gcExcludeIndex.DeleteInBatch(batch, item)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) pinUnpinCompleted(batch *leveldb.Batch, rootAddr swarm.Address) (err error) {
	var exists bool

	_, err = db.pinningIndex.First(rootAddr.Bytes())
	if err != nil {
		if !errors.Is(err, leveldb.ErrNotFound) {
			return err
		}
	} else {
		exists = true
	}

	if exists {
		return fmt.Errorf("unpinning cannot be completed: %s", rootAddr.String())
	}

	zeroItem := addressToItem(rootAddr)

	zeroPinnedItem, err := db.pinningIndex.Get(zeroItem)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return storage.ErrNotFound
		}

		return err
	}

	if zeroPinnedItem.PinCounter > 1 {
		zeroItem.PinCounter = zeroPinnedItem.PinCounter - 1

		err = db.pinningIndex.PutInBatch(batch, zeroItem)
		if err != nil {
			return err
		}
	} else {
		err = db.pinningIndex.DeleteInBatch(batch, zeroItem)
		if err != nil {
			return err
		}

		// remove from gcExcludeIndex
		err = db.gcExcludeIndex.DeleteInBatch(batch, zeroItem)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) pinUnpinFoundAddress(rootAddr, addr swarm.Address) (err error) {
	batch := new(leveldb.Batch)

	// remove from reverse lookup first
	err = db.pinningIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		reverseItem := shed.Item{
			Address:       item.ParentAddress,
			ParentAddress: item.Address,
		}

		err = db.pinningIndex.DeleteInBatch(batch, reverseItem)
		if err != nil {
			return true, err
		}

		return false, nil
	}, &shed.IterateOptions{
		Prefix: rootAddr.Bytes(),
	})
	if err != nil {
		return err
	}

	err = db.shed.WriteBatch(batch)
	if err != nil {
		return err
	}

	batch = new(leveldb.Batch)

	// check found addresses and maybe remove from gcExcludeIndex
	err = db.pinningIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		// check if we need to remove from gcExcludeIndex
		pinnedAddrItem, err := db.pinningIndex.First(item.Address)
		if err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
				has, err := db.gcExcludeIndex.Has(pinnedAddrItem)
				if err != nil {
					return true, err
				}

				if has {
					// remove from gcExcludeIndex
					err = db.gcExcludeIndex.DeleteInBatch(batch, pinnedAddrItem)
					if err != nil {
						return true, err
					}
				}
			} else {
				return true, err
			}
		}

		// chunk pinned directly; not removing from gcExcludeIndex

		return false, nil
	}, &shed.IterateOptions{
		Prefix: rootAddr.Bytes(),
	})
	if err != nil {
		return err
	}

	err = db.shed.WriteBatch(batch)
	if err != nil {
		return err
	}

	batch = new(leveldb.Batch)

	// remove found addresses
	err = db.pinningIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		err = db.pinningIndex.DeleteInBatch(batch, item)
		if err != nil {
			return true, err
		}

		return false, nil
	}, &shed.IterateOptions{
		Prefix: rootAddr.Bytes(),
	})
	if err != nil {
		return err
	}

	err = db.shed.WriteBatch(batch)
	if err != nil {
		return err
	}

	return nil
}
