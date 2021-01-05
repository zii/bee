// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/ethersphere/bee/pkg/jsonhttp"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/gorilla/mux"
)

// pinChunk pin's the already created chunk given its address.
// it fails if the chunk is not present in the local store.
// It also increments a pin counter to keep track of how many pin requests
// are originating for this chunk.
func (s *server) pinChunk(w http.ResponseWriter, r *http.Request) {
	addr, err := swarm.ParseHexAddress(mux.Vars(r)["address"])
	if err != nil {
		s.Logger.Debugf("pin chunk: parse chunk address: %v", err)
		s.Logger.Error("pin chunk: parse address")
		jsonhttp.BadRequest(w, "bad address")
		return
	}

	err = s.Storer.Set(r.Context(), storage.ModeSetPin, addr)
	if err != nil {
		if errors.Is(err, storage.ErrAlreadyPinned) {
			jsonhttp.OK(w, nil)
			return
		}

		if errors.Is(err, storage.ErrNotFound) {
			ch, err := s.Storer.Get(r.Context(), storage.ModeGetRequest, addr)
			if err != nil {
				s.Logger.Debugf("pin chunk: netstore get: %v", err)
				s.Logger.Error("pin chunk: netstore")

				jsonhttp.NotFound(w, nil)
				return
			}

			_, err = s.Storer.Put(r.Context(), storage.ModePutRequestPin, ch)
			if err != nil {
				s.Logger.Debugf("pin chunk: storer put pin: %v", err)
				s.Logger.Error("pin chunk: storer put pin")

				jsonhttp.InternalServerError(w, err)
				return
			}
		} else {
			s.Logger.Debugf("pin chunk: pinning error: %v, addr %s", err, addr)
			s.Logger.Error("pin chunk: cannot pin chunk")

			jsonhttp.InternalServerError(w, "cannot pin chunk")
			return
		}
	}

	jsonhttp.OK(w, nil)
}

// unpinChunk unpin's an already pinned chunk. If the chunk is not present or the
// if the pin counter is zero, it raises error.
func (s *server) unpinChunk(w http.ResponseWriter, r *http.Request) {
	addr, err := swarm.ParseHexAddress(mux.Vars(r)["address"])
	if err != nil {
		s.Logger.Debugf("pin chunk: parse chunk address: %v", err)
		s.Logger.Error("pin chunk: parse address")
		jsonhttp.BadRequest(w, "bad address")
		return
	}

	has, err := s.Storer.Has(r.Context(), addr)
	if err != nil {
		s.Logger.Debugf("pin chunk: localstore has: %v", err)
		s.Logger.Error("pin chunk: store")
		jsonhttp.InternalServerError(w, err)
		return
	}

	if !has {
		jsonhttp.NotFound(w, nil)
		return
	}

	_, err = s.Storer.PinCounter(addr)
	if err != nil {
		s.Logger.Debugf("pin chunk: not pinned: %v", err)
		s.Logger.Error("pin chunk: pin counter")
		jsonhttp.BadRequest(w, "chunk is not yet pinned")
		return
	}

	err = s.Storer.Pin(r.Context(), storage.ModePinUnpinSingle, addr)
	if err != nil {
		if errors.Is(storage.ErrNotFound, err) {
			jsonhttp.NotFound(w, nil)
			return
		}

		s.Logger.Debugf("pin chunk: unpinning error: %v, addr %s", err, addr)
		s.Logger.Error("pin chunk: unpin")
		jsonhttp.InternalServerError(w, "cannot unpin chunk")
		return
	}

	jsonhttp.OK(w, nil)
}

type pinnedChunk struct {
	Address    swarm.Address `json:"address"`
	PinCounter uint64        `json:"pinCounter"`
}

type listPinnedChunksResponse struct {
	Chunks []pinnedChunk `json:"chunks"`
}

// listPinnedChunks lists all the chunk address and pin counters that are currently pinned.
func (s *server) listPinnedChunks(w http.ResponseWriter, r *http.Request) {
	var (
		err           error
		offset, limit = 0, 100 // default offset is 0, default limit 100
	)

	if v := r.URL.Query().Get("offset"); v != "" {
		offset, err = strconv.Atoi(v)
		if err != nil {
			s.Logger.Debugf("list pins: parse offset: %v", err)
			s.Logger.Errorf("list pins: bad offset")
			jsonhttp.BadRequest(w, "bad offset")
		}
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil {
			s.Logger.Debugf("list pins: parse limit: %v", err)
			s.Logger.Errorf("list pins: bad limit")
			jsonhttp.BadRequest(w, "bad limit")
		}
	}

	pinnedChunks, err := s.Storer.PinnedChunks(r.Context(), offset, limit)
	if err != nil {
		s.Logger.Debugf("list pins: list pinned: %v", err)
		s.Logger.Errorf("list pins: list pinned")
		jsonhttp.InternalServerError(w, err)
		return
	}

	chunks := make([]pinnedChunk, len(pinnedChunks))
	for i, c := range pinnedChunks {
		chunks[i] = pinnedChunk(*c)
	}

	jsonhttp.OK(w, listPinnedChunksResponse{
		Chunks: chunks,
	})
}

func (s *server) getPinnedChunk(w http.ResponseWriter, r *http.Request) {
	addr, err := swarm.ParseHexAddress(mux.Vars(r)["address"])
	if err != nil {
		s.Logger.Debugf("pin counter: parse chunk ddress: %v", err)
		s.Logger.Errorf("pin counter: parse address")
		jsonhttp.NotFound(w, nil)
		return
	}

	has, err := s.Storer.Has(r.Context(), addr)
	if err != nil {
		s.Logger.Debugf("pin counter: localstore has: %v", err)
		s.Logger.Errorf("pin counter: store")
		jsonhttp.NotFound(w, nil)
		return
	}

	if !has {
		jsonhttp.NotFound(w, nil)
		return
	}

	pinCounter, err := s.Storer.PinCounter(addr)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			jsonhttp.NotFound(w, nil)
			return
		}
		s.Logger.Debugf("pin counter: get pin counter: %v", err)
		s.Logger.Errorf("pin counter: get pin counter")
		jsonhttp.InternalServerError(w, err)
		return
	}
	jsonhttp.OK(w, pinnedChunk{
		Address:    addr,
		PinCounter: pinCounter,
	})
}

func (s *server) pinTraverseAddressesFn(ctx context.Context, reference swarm.Address) func(address swarm.Address) error {
	return func(address swarm.Address) error {
		// try to fetch chunk
		_, err := s.Storer.Get(ctx, storage.ModeGetRequest, address)
		if err != nil {
			s.Logger.Debugf("pin traversal: storer pin found address: for reference %s, address %s: %w", reference, address, err)
			return err
		}

		err = s.Storer.Pin(ctx, storage.ModePinFoundAddress, address)
		if err != nil {
			s.Logger.Debugf("pin traversal: storer pin found address: for reference %s, address %s: %w", reference, address, err)
			return err
		}

		return nil
	}
}

func (s *server) pinRootAddress(
	ctx context.Context,
	addr swarm.Address,
	traverseFn func(context.Context, swarm.Address, swarm.AddressIterFunc) error,
) error {
	// set root address
	ctx = context.WithValue(ctx, storage.PinRootAddressContextKey{}, addr)

	err := s.Storer.Pin(ctx, storage.ModePinStarted, swarm.ZeroAddress)
	if err != nil {
		s.Logger.Debugf("pin: pinning start error: %v, addr %s", err, addr)

		if errors.Is(err, storage.ErrAlreadyPinned) {
			return nil
		}

		return err
	}

	chunkAddressFn := s.pinTraverseAddressesFn(ctx, addr)

	err = traverseFn(ctx, addr, chunkAddressFn)
	if err != nil {
		s.Logger.Debugf("pin: traverse chunks: %v, addr %s", err, addr)

		if errors.Is(err, storage.ErrIsUnpinned) {
			// call unpin for addresses
			if err := s.Storer.Pin(ctx, storage.ModePinUnpinFoundAddresses, swarm.ZeroAddress); err != nil {
				s.Logger.Debugf("pin: traverse chunks: unpinning found addresses: %v, addr %s", err, addr)
				return err
			}
		}

		return err
	}

	err = s.Storer.Pin(ctx, storage.ModePinCompleted, swarm.ZeroAddress)
	if err != nil {
		s.Logger.Debugf("pin: pinning complete error: %v, addr %s", err, addr)

		if errors.Is(err, storage.ErrAlreadyPinned) {
			return nil
		}

		return err
	}

	return nil
}

func (s *server) unpinRootAddress(ctx context.Context, addr swarm.Address) error {
	// set root address
	ctx = context.WithValue(ctx, storage.PinRootAddressContextKey{}, addr)

	err := s.Storer.Pin(ctx, storage.ModePinUnpinStarted, swarm.ZeroAddress)
	if err != nil {
		s.Logger.Debugf("pin: unpinning start error: %v, addr %s", err, addr)
		return err
	}

	err = s.Storer.Pin(ctx, storage.ModePinUnpinFoundAddresses, swarm.ZeroAddress)
	if err != nil {
		s.Logger.Debugf("pin: unpinning found addresses error: %v, addr %s", err, addr)
		return err
	}

	err = s.Storer.Pin(ctx, storage.ModePinUnpinCompleted, swarm.ZeroAddress)
	if err != nil {
		s.Logger.Debugf("pin: unpinning complete error: %v, addr %s", err, addr)
		return err
	}

	return nil
}
