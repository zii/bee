// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pslice

import (
	"fmt"
	"testing"

	"github.com/ethersphere/bee/pkg/swarm"
	"github.com/ethersphere/bee/pkg/swarm/test"
)

func randomAddresses(base swarm.Address, maxBins, perBin int) []swarm.Address {
	addrs := []swarm.Address{}
	for i := 0; i < maxBins; i++ {
		for j := 0; j < perBin; j++ {
			randAddr := test.RandomAddressAt(base, i)
			addrs = append(addrs, randAddr)
		}
	}
	return addrs
}

func TestBinaryAddressTree(t *testing.T) {
	testCases := []struct {
		maxBins       int
		addressPerBin int
		limit         int
		expected      int
	}{
		{1, 1, 1, 1},
		{1, 10, 1, 2},
		{1, 10, 3, 8},
		{1, 50, 3, 14},
		{4, 1, 1, 2},
		{4, 10, 1, 2},
		{4, 10, 3, 14},
		{4, 50, 3, 14},
		{16, 1, 1, 2},
		{16, 10, 1, 2},
		{16, 10, 3, 14},
		{16, 50, 3, 14},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("bins:%d,perBin:%d,limit:%d", tc.maxBins, tc.addressPerBin, tc.limit)

		base := test.RandomAddress()
		addrs := randomAddresses(base, tc.maxBins, tc.addressPerBin)

		t.Run(name, func(t *testing.T) {
			bat := newBinaryAddressTree(uint8(tc.maxBins), uint8(tc.limit))

			for _, addr := range addrs {
				bat.Insert(addr)
			}

			balancedAddrs := bat.LevelOrderSwitchingTraversalAddresses()

			if len(balancedAddrs) != tc.expected {
				t.Fatalf("expected %d addresses, got %d", tc.expected, len(balancedAddrs))
			}
		})
	}
}

func BenchmarkBinaryAddressTree(b *testing.B) {
	testCases := []struct {
		addressCount int
		index        int
	}{
		{1, 1},
		{4, 1},
		{16, 1},
		{16, 2},
		{64, 2},
		{256, 2},
		{256, 3},
		{4096, 3},
		{16384, 3},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("count:%d,index:%d", tc.addressCount, tc.index)

		base := test.RandomAddress()
		addrs := randomAddresses(base, tc.index, tc.addressCount)

		b.Run(name, func(b *testing.B) {
			bat := newBinaryAddressTree(uint8(tc.index), 5)

			for _, addr := range addrs {
				bat.Insert(addr)
			}

			balancedAddrs := bat.LevelOrderSwitchingTraversalAddresses()
			if len(balancedAddrs) == 0 {
				b.Fatal()
			}
		})
	}
}
