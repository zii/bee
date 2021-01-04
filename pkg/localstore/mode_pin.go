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
		has, err := db.retrievalDataIndex.Has(addressToItem(addr))
		if err != nil {
			return err
		}

		if !has {
			return storage.ErrNotFound
		}

		err = db.pinFoundAddress(batch, rootAddr, addr)
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
	case storage.ModePinUnpinFoundAddresses:
		err := db.pinUnpinFoundAddresses(rootAddr, addr)
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
	secondaryItem := shed.Item{
		ParentAddress: addr.Bytes(),
	}

	has, err := db.pinSecondaryIndex.Has(secondaryItem)
	if err != nil {
		return err
	}

	if has {
		// already pinned
		return storage.ErrAlreadyPinned
	}

	// single chunk was not pinned previously
	secondaryItem.PinCounter = 1

	err = db.pinSecondaryIndex.PutInBatch(batch, secondaryItem)
	if err != nil {
		return err
	}

	item := addressToItem(addr)

	// add in gcExcludeIndex if the chunk is not pinned already
	err = db.gcExcludeIndex.PutInBatch(batch, item)
	if err != nil {
		return err
	}

	pinnedItem, err := db.pinIndex.Get(item)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			item.PinCounter = 1
		} else {
			return err
		}
	} else {
		item.PinCounter = pinnedItem.PinCounter + 1
	}

	err = db.pinIndex.PutInBatch(batch, item)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) pinUnpinSingle(batch *leveldb.Batch, addr swarm.Address) (err error) {
	secondaryItem := shed.Item{
		ParentAddress: addr.Bytes(),
	}

	_, err = db.pinSecondaryIndex.Get(secondaryItem)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return storage.ErrNotFound
		}

		return err
	}

	err = db.pinSecondaryIndex.DeleteInBatch(batch, secondaryItem)
	if err != nil {
		return err
	}

	item := addressToItem(addr)

	pinnedItem, err := db.pinIndex.Get(item)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return storage.ErrNotFound
		}

		return err
	}

	if pinnedItem.PinCounter > 1 {
		item.PinCounter = pinnedItem.PinCounter - 1

		err = db.pinIndex.PutInBatch(batch, item)
		if err != nil {
			return err
		}
	} else {
		err = db.pinIndex.DeleteInBatch(batch, item)
		if err != nil {
			return err
		}

		// remove from gcExcludeIndex
		err = db.gcExcludeIndex.DeleteInBatch(batch, item)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) pinStarted(batch *leveldb.Batch, rootAddr swarm.Address) (err error) {
	secondaryItem := shed.Item{
		Address:       rootAddr.Bytes(),
		ParentAddress: rootAddr.Bytes(),
	}

	has, err := db.pinSecondaryIndex.Has(secondaryItem)
	if err != nil {
		return err
	}

	if has {
		// Get the existing pin counter of the chunk
		secondaryPinnedItem, err := db.pinSecondaryIndex.Get(secondaryItem)
		if err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
				return storage.ErrNotFound
			}

			return err
		}

		if secondaryPinnedItem.PinCounter == 1 {
			return storage.ErrAlreadyPinned
		}

		return nil
	}

	// item was not pinned previously
	secondaryItem.PinCounter = 0

	err = db.pinSecondaryIndex.PutInBatch(batch, secondaryItem)
	if err != nil {
		return err
	}

	item := addressToItem(rootAddr)

	// add to gcExcludeIndex
	err = db.gcExcludeIndex.PutInBatch(batch, item)
	if err != nil {
		return err
	}

	pinnedItem, err := db.pinIndex.Get(item)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			item.PinCounter = 1
		} else {
			return err
		}
	} else {
		item.PinCounter = pinnedItem.PinCounter + 1
	}

	err = db.pinIndex.PutInBatch(batch, item)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) pinCompleted(batch *leveldb.Batch, rootAddr swarm.Address) (err error) {
	secondaryItem := shed.Item{
		Address:       rootAddr.Bytes(),
		ParentAddress: rootAddr.Bytes(),
	}

	has, err := db.pinSecondaryIndex.Has(secondaryItem)
	if err != nil {
		return err
	}

	if !has {
		return storage.ErrIsUnpinned
	}

	// Get the existing pin counter of the chunk
	secondaryPinnedItem, err := db.pinSecondaryIndex.Get(secondaryItem)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return storage.ErrNotFound
		}

		return err
	}

	if secondaryPinnedItem.PinCounter == 1 {
		// already pinned
		return storage.ErrAlreadyPinned
	}

	// mark root address as pinned (in secondary index)
	secondaryItem.PinCounter = 1

	err = db.pinSecondaryIndex.PutInBatch(batch, secondaryItem)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) pinFoundAddress(batch *leveldb.Batch, rootAddr, addr swarm.Address) (err error) {
	// skipping root address
	if rootAddr.Equal(addr) {
		return nil
	}

	rootSecondaryItem := shed.Item{
		Address:       rootAddr.Bytes(),
		ParentAddress: rootAddr.Bytes(),
	}

	has, err := db.pinSecondaryIndex.Has(rootSecondaryItem)
	if err != nil {
		return err
	}

	if !has {
		return storage.ErrIsUnpinned
	}

	item := shed.Item{
		Address:       addr.Bytes(),
		ParentAddress: rootAddr.Bytes(),
	}

	existingPinCounter := uint64(0)
	pinnedItem, err := db.pinSecondaryIndex.Get(item)
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

	err = db.pinSecondaryIndex.PutInBatch(batch, item)
	if err != nil {
		return err
	}

	// add reverse index item
	reverseIndexItem := shed.Item{
		Address:       rootAddr.Bytes(),
		ParentAddress: addr.Bytes(),
	}

	reverseIndexItem.PinCounter = item.PinCounter

	err = db.pinSecondaryIndex.PutInBatch(batch, reverseIndexItem)
	if err != nil {
		return err
	}

	// add to gcExcludeIndex
	err = db.gcExcludeIndex.PutInBatch(batch, addressToItem(addr))
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) pinUnpinStarted(batch *leveldb.Batch, rootAddr swarm.Address) (err error) {
	secondaryItem := shed.Item{
		Address:       rootAddr.Bytes(),
		ParentAddress: rootAddr.Bytes(),
	}

	has, err := db.pinSecondaryIndex.Has(secondaryItem)
	if err != nil {
		return err
	}

	if has {
		err = db.pinSecondaryIndex.DeleteInBatch(batch, secondaryItem)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) pinUnpinCompleted(batch *leveldb.Batch, rootAddr swarm.Address) (err error) {
	var exists bool

	_, err = db.pinSecondaryIndex.First(rootAddr.Bytes())
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

	item := addressToItem(rootAddr)

	has, err := db.pinIndex.Has(item)
	if err != nil {
		return err
	}

	if has {
		pinnedItem, err := db.pinIndex.Get(item)
		if err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
				return storage.ErrNotFound
			}

			return err
		}

		if pinnedItem.PinCounter > 1 {
			item.PinCounter = pinnedItem.PinCounter - 1

			err = db.pinIndex.PutInBatch(batch, item)
			if err != nil {
				return err
			}
		} else {
			err = db.pinIndex.DeleteInBatch(batch, item)
			if err != nil {
				return err
			}

			// remove from gcExcludeIndex
			err = db.gcExcludeIndex.DeleteInBatch(batch, item)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (db *DB) pinUnpinFoundAddresses(rootAddr, addr swarm.Address) (err error) {
	batch := new(leveldb.Batch)

	// remove from reverse lookup first
	err = db.pinSecondaryIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		reverseItem := shed.Item{
			Address:       item.ParentAddress,
			ParentAddress: item.Address,
		}

		err = db.pinSecondaryIndex.DeleteInBatch(batch, reverseItem)
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
	err = db.pinSecondaryIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		// check if we need to remove from gcExcludeIndex
		pinnedAddrItem, err := db.pinSecondaryIndex.First(item.Address)
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
	err = db.pinSecondaryIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		err = db.pinSecondaryIndex.DeleteInBatch(batch, item)
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
