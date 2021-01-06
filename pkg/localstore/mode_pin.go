// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.package storage

package localstore

import (
	"bytes"
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
func (db *DB) Pin(ctx context.Context, mode storage.ModePin, addr swarm.Address) (err error) {
	db.metrics.ModePin.Inc()
	defer totalTimeMetric(db.metrics.TotalTimePin, time.Now())

	err = db.pin(ctx, mode, addr)
	if err != nil {
		db.metrics.ModePinFailure.Inc()
	}
	return err
}

// pin updates database indexes for chunks represented by provided addresses.
// It acquires lockAddr to protect two calls
// of this function for the same address in parallel.
func (db *DB) pin(ctx context.Context, mode storage.ModePin, addr swarm.Address) (err error) {
	// protect parallel updates
	db.batchMu.Lock()
	defer db.batchMu.Unlock()

	rootAddr, hasRootAddr := ctx.Value(storage.PinRootAddressContextKey{}).(swarm.Address)
	if !hasRootAddr {
		switch mode {
		case storage.ModePinSingle, storage.ModePinUnpinSingle:
			rootAddr = swarm.ZeroAddress
		case storage.ModePinStarted, storage.ModePinCompleted, storage.ModePinFoundAddress:
			fallthrough
		case storage.ModePinUnpinStarted, storage.ModePinUnpinCompleted, storage.ModePinUnpinFoundAddress:
			return fmt.Errorf("root address missing")
		}
	}

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
	case storage.ModePinUploadingStarted:
		err := db.pinUploadingStarted(batch, rootAddr)
		if err != nil {
			return err
		}
	case storage.ModePinUploadingCompleted:
		err := db.pinUploadingCompleted(batch, rootAddr, addr)
		if err != nil {
			return err
		}
	case storage.ModePinUploadingCleanup:
		err := db.pinUploadingCleanup(batch, rootAddr)
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
		err := db.pinUnpinFoundAddress(batch, rootAddr, addr)
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

	// just add to gcExcludeIndex
	err = db.gcExcludeIndex.PutInBatch(batch, item)
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

	// Get the existing pin counter of the chunk
	secondaryPinnedItem, err := db.pinSecondaryIndex.Get(secondaryItem)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return storage.ErrIsUnpinned
		}

		return err
	}

	if secondaryPinnedItem.PinCounter == 1 {
		// already pinned
		return storage.ErrAlreadyPinned
	}

	// update pin count for addresses from root
	err = db.pinSecondaryIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		reverseItem := shed.Item{
			Address:       item.ParentAddress,
			ParentAddress: item.Address,
		}

		reversePinnedItem, err := db.pinSecondaryIndex.Get(reverseItem)
		if err != nil {
			return true, err
		}

		item.PinCounter = reversePinnedItem.PinCounter

		err = db.pinSecondaryIndex.PutInBatch(batch, item)
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

	// mark root address as pinned (in secondary index)
	secondaryItem.PinCounter = 1

	err = db.pinSecondaryIndex.PutInBatch(batch, secondaryItem)
	if err != nil {
		return err
	}

	// all done now
	item := addressToItem(rootAddr)

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

	// update reverse index
	reverseIndexItem := shed.Item{
		Address:       rootAddr.Bytes(),
		ParentAddress: addr.Bytes(),
	}

	reversePinnedItem, err := db.pinSecondaryIndex.Get(reverseIndexItem)
	if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			reverseIndexItem.PinCounter = 1
		} else {
			return err
		}
	} else {
		reverseIndexItem.PinCounter = reversePinnedItem.PinCounter + 1
	}

	err = db.pinSecondaryIndex.PutInBatch(batch, reverseIndexItem)
	if err != nil {
		return err
	}

	// update address chunk count
	item := addressToItem(addr)

	existingPinCounter := uint64(0)
	pinnedItem, err := db.pinIndex.Get(item)
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

	err = db.pinIndex.PutInBatch(batch, item)
	if err != nil {
		return err
	}

	if existingPinCounter == 0 {
		// add to gcExcludeIndex
		err = db.gcExcludeIndex.PutInBatch(batch, addressToItem(addr))
		if err != nil {
			return err
		}
	}

	secondaryItem := shed.Item{
		Address:       addr.Bytes(),
		ParentAddress: rootAddr.Bytes(),
	}

	has, err = db.pinSecondaryIndex.Has(secondaryItem)
	if err != nil {
		return err
	}

	if !has {
		secondaryItem.PinCounter = 0

		err = db.pinSecondaryIndex.PutInBatch(batch, secondaryItem)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) pinUploadingStarted(batch *leveldb.Batch, randomRootAddr swarm.Address) (err error) {
	secondaryItem := shed.Item{
		Address:       randomRootAddr.Bytes(),
		ParentAddress: randomRootAddr.Bytes(),
	}

	has, err := db.pinSecondaryIndex.Has(secondaryItem)
	if err != nil {
		return err
	}

	if has {
		// highly unlikely
		return storage.ErrAlreadyPinned
	}

	// item was not pinned previously
	secondaryItem.PinCounter = 0

	err = db.pinSecondaryIndex.PutInBatch(batch, secondaryItem)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) pinUploadingCompleted(batch *leveldb.Batch, randomRootAddr, rootAddr swarm.Address) (err error) {
	secondaryRandomItem := shed.Item{
		Address:       randomRootAddr.Bytes(),
		ParentAddress: randomRootAddr.Bytes(),
	}

	has, err := db.pinSecondaryIndex.Has(secondaryRandomItem)
	if err != nil {
		return err
	}

	if !has {
		return storage.ErrIsUnpinned
	}

	// migrate from random root hash to actual one
	err = db.pinSecondaryIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		// skipping root address
		if bytes.Equal(item.ParentAddress, item.Address) {
			return false, nil
		}

		reverseRandomItem := shed.Item{
			Address:       item.ParentAddress,
			ParentAddress: item.Address,
		}

		reverseRandomPinnedItem, err := db.pinSecondaryIndex.Get(reverseRandomItem)
		if err != nil {
			return true, err
		}

		secondaryItem := shed.Item{
			Address:       item.Address,
			ParentAddress: rootAddr.Bytes(),
		}
		reverseItem := shed.Item{
			Address:       rootAddr.Bytes(),
			ParentAddress: item.Address,
		}

		secondaryItem.PinCounter = reverseRandomPinnedItem.PinCounter
		reverseItem.PinCounter = reverseRandomPinnedItem.PinCounter

		// only put secondary entries if not already exists

		hasSecondary, err := db.pinSecondaryIndex.Has(secondaryItem)
		if err != nil {
			return true, err
		}

		if !hasSecondary {
			err = db.pinSecondaryIndex.PutInBatch(batch, secondaryItem)
			if err != nil {
				return true, err
			}

			err = db.pinSecondaryIndex.PutInBatch(batch, reverseItem)
			if err != nil {
				return true, err
			}
		} else {
			item := shed.Item{
				Address: item.Address,
			}

			pinnedItem, err := db.pinIndex.Get(item)
			if err != nil {
				if errors.Is(err, leveldb.ErrNotFound) {
					return true, storage.ErrNotFound
				}

				return true, err
			}

			// pin counter must be > 2 in this case

			item.PinCounter = pinnedItem.PinCounter - 1

			err = db.pinIndex.PutInBatch(batch, item)
			if err != nil {
				return true, err
			}
		}

		// remove random root addresses
		err = db.pinSecondaryIndex.DeleteInBatch(batch, item)
		if err != nil {
			return true, err
		}

		err = db.pinSecondaryIndex.DeleteInBatch(batch, reverseRandomItem)
		if err != nil {
			return true, err
		}

		return false, nil
	}, &shed.IterateOptions{
		Prefix: randomRootAddr.Bytes(),
	})
	if err != nil {
		return err
	}

	// remove main random root address
	err = db.pinSecondaryIndex.DeleteInBatch(batch, secondaryRandomItem)
	if err != nil {
		return err
	}

	// create entry for actual root address
	secondaryItem := shed.Item{
		Address:       rootAddr.Bytes(),
		ParentAddress: rootAddr.Bytes(),
	}

	secondaryItem.PinCounter = 1

	err = db.pinSecondaryIndex.PutInBatch(batch, secondaryItem)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) pinUploadingCleanup(batch *leveldb.Batch, randomRootAddr swarm.Address) (err error) {
	secondaryRandomItem := shed.Item{
		Address:       randomRootAddr.Bytes(),
		ParentAddress: randomRootAddr.Bytes(),
	}

	has, err := db.pinSecondaryIndex.Has(secondaryRandomItem)
	if err != nil {
		return err
	}

	if !has {
		return nil
	}

	// remove random root addresses
	err = db.pinSecondaryIndex.Iterate(func(item shed.Item) (stop bool, err error) {
		// skipping root address
		if bytes.Equal(item.ParentAddress, item.Address) {
			return false, nil
		}

		reverseRandomItem := shed.Item{
			Address:       item.ParentAddress,
			ParentAddress: item.Address,
		}

		err = db.pinSecondaryIndex.DeleteInBatch(batch, item)
		if err != nil {
			return true, err
		}

		err = db.pinSecondaryIndex.DeleteInBatch(batch, reverseRandomItem)
		if err != nil {
			return true, err
		}

		return false, nil
	}, &shed.IterateOptions{
		Prefix: randomRootAddr.Bytes(),
	})
	if err != nil {
		return err
	}

	// remove main random root address
	err = db.pinSecondaryIndex.DeleteInBatch(batch, secondaryRandomItem)
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

func (db *DB) pinUnpinFoundAddress(batch *leveldb.Batch, rootAddr, addr swarm.Address) (err error) {
	// skipping root address
	if rootAddr.Equal(addr) {
		return nil
	}

	reverseIndexItem := shed.Item{
		Address:       rootAddr.Bytes(),
		ParentAddress: addr.Bytes(),
		}

	reversePinnedItem, err := db.pinSecondaryIndex.Get(reverseIndexItem)
		if err != nil {
		if errors.Is(err, leveldb.ErrNotFound) {
			return nil
		} else {
			return err
		}
		}

	err = db.pinSecondaryIndex.DeleteInBatch(batch, reverseIndexItem)
		if err != nil {
		return err
	}

	secondaryItem := shed.Item{
		Address:       addr.Bytes(),
		ParentAddress: rootAddr.Bytes(),
	}

	err = db.pinSecondaryIndex.DeleteInBatch(batch, secondaryItem)
	if err != nil {
		return err
		}

		// remove from main pin index
	item := addressToItem(addr)

		pinnedItem, err := db.pinIndex.Get(item)
		if err != nil {
			if errors.Is(err, leveldb.ErrNotFound) {
			return storage.ErrNotFound
			}

		return err
		}

		pinnedItem.PinCounter -= reversePinnedItem.PinCounter

		if pinnedItem.PinCounter > 0 {
			err = db.pinIndex.PutInBatch(batch, pinnedItem)
			if err != nil {
			return err
			}
		} else {
			err = db.pinIndex.DeleteInBatch(batch, pinnedItem)
			if err != nil {
			return err
			}

			// remove from gcExcludeIndex
			err = db.gcExcludeIndex.DeleteInBatch(batch, pinnedItem)
			if err != nil {
		return err
	}
	}

	return nil
}
