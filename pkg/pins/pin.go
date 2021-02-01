// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pins

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"

	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/log"
)


const keyPrefix = "pin"

var (
	errInvalidChunkData      = errors.New("invalid chunk data")
	errInvalidUnmarshallData = errors.New("invalid data length")
)

// Info is the struct that stores the information about pinned files
// This is stored in the state DB with Address as key
type Info struct {
	Address    swarm.Address
	FileSize   uint64
	PinCounter uint64
}

// MarshalBinary encodes the Info object in to a binary form for storage
func (f *Info) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 16)
	binary.BigEndian.PutUint64(data[:8], f.FileSize)
	binary.BigEndian.PutUint64(data[8:16], f.PinCounter)
	return data, nil
}

// UnmarshalBinary decodes the binary form from the state store to the Info object
func (f *Info) UnmarshalBinary(data []byte) error {
	if len(data) != 16 {
		return errInvalidUnmarshallData
	}
	f.FileSize = binary.BigEndian.Uint64(data[:8])
	f.PinCounter = binary.BigEndian.Uint64(data[8:16])
	return nil
}

// Pinner is the main object which implements all things pinning.
type Pinner struct {
	store storage.StateStorer // the state store used to store info about pinned files
}

// New creates a Pinner required for pinning and unpinning
func New(stateStore storage.StateStorer) *Pinner {
	return &Pinner{
		store: stateStore,
	}
}

//
func (p *Pinner) Pin(addr swarm.Address) error {
	// Check if the root hash is already pinned and add it to the info struct
	info, err := p.get(addr)
	if err != nil {
		return err
	}

	// Walk the root hash and pin all the chunks
	walkerFunction := func(ref storage.Reference) error {
		err := p.db.Set(context.Background(), chunk.ModeSetPin, chunkAddr)
		if err != nil {
			log.Error("Could not pin chunk. Address "+"Address", hex.EncodeToString(chunkAddr))
			return err
		}
		return nil
	}
	err = p.traverse(ctx, addr, walkerFunction)
	if err != nil {
		log.Error("Error walking root hash.", "Hash", hex.EncodeToString(addr), "err", err)
		return nil
	}

	info = Info{
		Address:    addr,
		IsRaw:      isRaw,
		FileSize:   fileSize,
		PinCounter: pinCounter,
	}

	// Store the pinned files in state DB
	err = p.save(info)
	if err != nil {
		log.Error("Error saving pinned file info to state store.", "rootHash", hex.EncodeToString(addr), "err", err)
		return nil
	}

	return nil
}

// UnPin
func (p *Pinner) Unpin(addr swarm.Addreess) error {
	info, err := p.getPinnedFile(addr)
	if err != nil {
		log.Error("Root hash is not pinned", "rootHash", hex.EncodeToString(addr), "err", err)
		return err
	}

	// Walk the root hash and unpin all the chunks
	walkerFunction := func(ref storage.Reference) error {
		chunkAddr := p.removeDecryptionKeyFromChunkHash(ref)
		err := p.db.Set(context.Background(), chunk.ModeSetUnpin, chunkAddr)
		if err != nil {
			log.Error("Could not unpin chunk", "Address", hex.EncodeToString(chunkAddr))
			return err
		} else {
			log.Trace("Unpinning chunk", "Address", hex.EncodeToString(chunkAddr))
		}
		return nil
	}
	err = p.walkChunksFromRootHash(addr, info.IsRaw, credentials, walkerFunction)
	if err != nil {
		log.Error("Error walking root hash.", "Hash", hex.EncodeToString(addr), "err", err)
		return nil
	}

	// Delete or Update the state DB
	pinCounter, err := p.getPinCounterOfChunk(chunk.Address(p.removeDecryptionKeyFromChunkHash(addr)))
	if err != nil {
		err := p.removePinnedFile(addr)
		if err != nil {
			log.Error("Error unpinning file.", "rootHash", hex.EncodeToString(addr), "err", err)
			return nil
		}
	} else {
		info.PinCounter = pinCounter
		err = p.savePinnedFile(info)
		if err != nil {
			log.Error("Error updating file info to state store.", "rootHash", hex.EncodeToString(addr), "err", err)
			return nil
		}
	}

	log.Debug("File unpinned", "Address", hex.EncodeToString(addr))
	return nil
}

// ListPins functions logs information of all the files that are pinned
// in the current local node. It displays the root hash of the pinned file
// or collection. It also display three vital information's
//     1) Whether the file is a RAW file or not
//     2) Size of the pinned file or collection
//     3) the number of times that particular file or collection is pinned.

func (p *API) ListPins() ([]Info, error) {
	pinnedFiles := make([]Info, 0)
	iterFunc := func(key []byte, value []byte) (stop bool, err error) {
		hash := string(key[4:])
		info := Info{}
		err = info.UnmarshalBinary(value)
		if err != nil {
			log.Debug("Error unmarshaling info from state store", "Address", hash)
			return true, err
		}
		info.Address, err = hex.DecodeString(hash)
		if err != nil {
			log.Debug("Error unmarshaling info from state store", "Address", hash)
			return
		}
		log.Trace("Pinned file", "Address", hash, "IsRAW", info.IsRaw,
			"FileSize", info.FileSize, "PinCounter", info.PinCounter)
		pinnedFiles = append(pinnedFiles, info)
		return stop, err
	}
	err := p.state.Iterate("pin_", iterFunc)
	if err != nil {
		log.Error("Error iterating pinned files", "err", err)
		return nil, err
	}
	return pinnedFiles, nil
}

func (p *Pinner) getPinCounterOfChunk(addr swarm.Address) (uint64, error) {
	pinnedChunk, err := p.db.Get(context.Background(), chunk.ModeGetPin, addr)
	if err != nil {
		return 0, err
	}
	return pinnedChunk.PinCounter(), nil
}

func (p *Pinner) save(info *Info) error {
	return p.store.Put(key(info.Address), info)
}
func (p *Pinner) load(addr  swarm.Address)  (*Info, error) {
	var info *Info
	err = p.store.Put(key(addr), info)
	return info, err
}

func (p *Pinner) remove(addr  swarm.Address) error {
	return p.store..Delete(key(addr))
}

func key(addr  swarm.Address) string {
	return keyPrefix + addr.String()
}