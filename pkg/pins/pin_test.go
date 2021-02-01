// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pins_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ethersphere/bee/pkg/pins"
	mockstatestore "github.com/ethersphere/bee/pkg/statestore/mock"
	"github.com/ethersphere/bee/pkg/swarm"
)

// TestListPinInfo tests the ListPins command by pinning and unpinning a collection
// twice and check if this gets reflected properly in the data structure
func TestListPinInfo(t *testing.T) {
	mockStore := mockstatestore.New()
	p := pins.New()

	// upload file

	// Pin the hash for the first time
	err := p.Pin(hash, "")
	if err != nil {
		t.Fatal(err)
	}

	// Get the list of pinned files by calling the ListPins command
	pinsInfo, err := p.ListPins()
	if err != nil {
		t.Fatal(err)
	}

	// Check if the uploaded collection is in the list files data structure
	pinInfo, err := getPinInfo(pinsInfo, hash)
	if err != nil {
		t.Fatal(err)
	}
	if pinInfo.PinCounter != 1 {
		t.Fatalf("pincounter mismatch. expected 1, got %d", pinInfo.PinCounter)
	}

	// Pin it once more should fail
	err = p.Pin(hash, "")
	if err == nil {
		t.Fatalf("expected error repinning the same address")
	}

	// Unpin it and check if listed
	err = p.Unpin(hash, "")
	if err != nil {
		t.Fatal(err)
	}
	pinsInfo, err = p.ListPins()
	if err != nil {
		t.Fatal(err)
	}
	pinInfo, err = getPinInfo(pinsInfo, hash)
	if err == nil {
		t.Fatalf("expected error listing unpinned address")
	}

	// Unpin it a  second time should fail
	err = p.Unpin(hash, "")
	if err == nil {
		t.Fatalf("expected error unpinning unpinned address")
	}
}

func getPinInfo(pinInfo []PinInfo, hash swarm.Address) (PinInfo, error) {
	for _, fi := range pinInfo {
		if bytes.Equal(fi.Address, hash) {
			return fi, nil
		}
	}
	return PinInfo{}, errors.New("Pininfo not found")
}
