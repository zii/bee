// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mock

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

var _ storage.Storer = (*MockStorer)(nil)

type MockStorer struct {
	store           map[string][]byte
	modePut         map[string]storage.ModePut
	modeSet         map[string]storage.ModeSet
	pinIndex        map[string]uint64
	pinSecondIndex  map[string]uint64
	subpull         []storage.Descriptor
	partialInterval bool
	morePull        chan struct{}
	mtx             sync.Mutex
	quit            chan struct{}
	baseAddress     []byte
	bins            []uint64
}

func WithSubscribePullChunks(chs ...storage.Descriptor) Option {
	return optionFunc(func(m *MockStorer) {
		m.subpull = make([]storage.Descriptor, len(chs))
		for i, v := range chs {
			m.subpull[i] = v
		}
	})
}

func WithBaseAddress(a swarm.Address) Option {
	return optionFunc(func(m *MockStorer) {
		m.baseAddress = a.Bytes()
	})
}

func WithPartialInterval(v bool) Option {
	return optionFunc(func(m *MockStorer) {
		m.partialInterval = v
	})
}

func NewStorer(opts ...Option) *MockStorer {
	s := &MockStorer{
		store:          make(map[string][]byte),
		modePut:        make(map[string]storage.ModePut),
		modeSet:        make(map[string]storage.ModeSet),
		pinIndex:       make(map[string]uint64),
		pinSecondIndex: make(map[string]uint64),
		morePull:       make(chan struct{}),
		quit:           make(chan struct{}),
		bins:           make([]uint64, swarm.MaxBins),
	}

	for _, v := range opts {
		v.apply(s)
	}

	return s
}

func (m *MockStorer) Get(ctx context.Context, mode storage.ModeGet, addr swarm.Address) (ch swarm.Chunk, err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	v, has := m.store[addr.String()]
	if !has {
		return nil, storage.ErrNotFound
	}
	return swarm.NewChunk(addr, v), nil
}

func (m *MockStorer) Put(ctx context.Context, mode storage.ModePut, chs ...swarm.Chunk) (exist []bool, err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	exist = make([]bool, len(chs))
	for i, ch := range chs {
		exist[i], err = m.has(ctx, ch.Address())
		if err != nil {
			return exist, err
		}
		if !exist[i] {
			po := swarm.Proximity(ch.Address().Bytes(), m.baseAddress)
			m.bins[po]++
		}
		m.store[ch.Address().String()] = ch.Data()
		m.modePut[ch.Address().String()] = mode

		// pin chunks if needed
		switch mode {
		case storage.ModePutUploadPin:
			// if mode is set pin, increment the pin counter
			addrString := ch.Address().String()
			if count, ok := m.pinIndex[addrString]; ok {
				m.pinIndex[addrString] = count + 1
			} else {
				m.pinIndex[addrString] = uint64(1)
			}
		default:
		}
	}
	return exist, nil
}

func (m *MockStorer) GetMulti(ctx context.Context, mode storage.ModeGet, addrs ...swarm.Address) (ch []swarm.Chunk, err error) {
	panic("not implemented") // TODO: Implement
}

func (m *MockStorer) has(ctx context.Context, addr swarm.Address) (yes bool, err error) {
	_, has := m.store[addr.String()]
	return has, nil
}

func (m *MockStorer) Has(ctx context.Context, addr swarm.Address) (yes bool, err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.has(ctx, addr)
}

func (m *MockStorer) HasMulti(ctx context.Context, addrs ...swarm.Address) (yes []bool, err error) {
	panic("not implemented") // TODO: Implement
}

func (m *MockStorer) Set(ctx context.Context, mode storage.ModeSet, addrs ...swarm.Address) (err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for _, addr := range addrs {
		m.modeSet[addr.String()] = mode
		switch mode {
		case storage.ModeSetPin:
			// check if chunk exists
			has, err := m.has(ctx, addr)
			if err != nil {
				return err
			}

			if !has {
				return storage.ErrNotFound
			}

			err = m.pinSingle(addr)
			if err != nil {
				return err
			}
		case storage.ModeSetUnpin:
			err = m.pinUnpinSingle(addr)
			if err != nil {
				return err
			}
		case storage.ModeSetRemove:
			delete(m.store, addr.String())
		default:
		}
	}
	return nil
}
func (m *MockStorer) GetModePut(addr swarm.Address) (mode storage.ModePut) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if mode, ok := m.modePut[addr.String()]; ok {
		return mode
	}
	return mode
}

func (m *MockStorer) GetModeSet(addr swarm.Address) (mode storage.ModeSet) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if mode, ok := m.modeSet[addr.String()]; ok {
		return mode
	}
	return mode
}

func (m *MockStorer) LastPullSubscriptionBinID(bin uint8) (id uint64, err error) {
	return m.bins[bin], nil
}

func (m *MockStorer) SubscribePull(ctx context.Context, bin uint8, since, until uint64) (<-chan storage.Descriptor, <-chan struct{}, func()) {
	c := make(chan storage.Descriptor)
	done := make(chan struct{})
	stop := func() {
		close(done)
	}
	go func() {
		defer close(c)
		m.mtx.Lock()
		for _, ch := range m.subpull {
			select {
			case c <- ch:
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-m.quit:
				return
			}
		}
		m.mtx.Unlock()

		if m.partialInterval {
			// block since we're at the top of the bin and waiting for new chunks
			select {
			case <-done:
				return
			case <-m.quit:
				return
			case <-ctx.Done():
				return
			case <-m.morePull:

			}
		}

		m.mtx.Lock()
		defer m.mtx.Unlock()

		// iterate on what we have in the iterator
		for _, ch := range m.subpull {
			select {
			case c <- ch:
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-m.quit:
				return
			}
		}

	}()
	return c, m.quit, stop
}

func (m *MockStorer) MorePull(d ...storage.Descriptor) {
	// clear out what we already have in subpull
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.subpull = make([]storage.Descriptor, len(d))
	for i, v := range d {
		m.subpull[i] = v
	}
	close(m.morePull)
}

func (m *MockStorer) SubscribePush(ctx context.Context) (c <-chan swarm.Chunk, stop func()) {
	panic("not implemented") // TODO: Implement
}

func (m *MockStorer) Pin(ctx context.Context, mode storage.ModePin, addr swarm.Address) (err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

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

	switch mode {
	case storage.ModePinSingle:
		err = m.pinSingle(addr)
		if err != nil {
			return err
		}

	case storage.ModePinUnpinSingle:
		err = m.pinUnpinSingle(addr)
		if err != nil {
			return err
		}

	case storage.ModePinStarted:
		secondaryKey := make([]byte, len(rootAddr.Bytes())*2)
		copy(secondaryKey[:len(rootAddr.Bytes())], rootAddr.Bytes())
		copy(secondaryKey[len(rootAddr.Bytes()):], rootAddr.Bytes())

		secondaryAddrString := swarm.NewAddress(secondaryKey).String()

		if count, ok := m.pinSecondIndex[secondaryAddrString]; ok {
			if count == 1 {
				return storage.ErrAlreadyPinned
			}

			return nil
		}

		m.pinSecondIndex[secondaryAddrString] = 0

	case storage.ModePinCompleted:
		rootAddrString := rootAddr.String()

		// check root secondary value
		secondaryKey := make([]byte, len(rootAddr.Bytes())*2)
		copy(secondaryKey[:len(rootAddr.Bytes())], rootAddr.Bytes())
		copy(secondaryKey[len(rootAddr.Bytes()):], rootAddr.Bytes())

		secondaryAddrString := swarm.NewAddress(secondaryKey).String()

		count, ok := m.pinSecondIndex[secondaryAddrString]
		if !ok {
			return storage.ErrIsUnpinned
		}

		if count == 1 {
			return storage.ErrAlreadyPinned
		}

		// update pin count for addresses from root
		for addrString := range m.pinSecondIndex {
			if !strings.HasPrefix(addrString, rootAddrString) {
				continue
			}

			keyLen := len(addrString) / 2
			parent := addrString[:keyLen]
			actual := addrString[keyLen:]

			reverseAddrString := actual + parent

			count, ok := m.pinSecondIndex[reverseAddrString]
			if !ok {
				return storage.ErrNotFound
			}

			m.pinSecondIndex[addrString] = count
		}

		// update root secondary value
		m.pinSecondIndex[secondaryAddrString] = 1

		// all done now
		if count, ok := m.pinIndex[rootAddrString]; ok {
			m.pinIndex[rootAddrString] = count + 1
		} else {
			m.pinIndex[rootAddrString] = 1
		}

	case storage.ModePinFoundAddress:
		if rootAddr.Equal(addr) {
			return nil
		}

		rootSecondaryKey := make([]byte, len(rootAddr.Bytes())*2)
		copy(rootSecondaryKey[:len(rootAddr.Bytes())], rootAddr.Bytes())
		copy(rootSecondaryKey[len(rootAddr.Bytes()):], rootAddr.Bytes())

		rootSecondaryAddrString := swarm.NewAddress(rootSecondaryKey).String()

		if _, ok := m.pinSecondIndex[rootSecondaryAddrString]; !ok {
			return storage.ErrIsUnpinned
		}

		// update reverse index
		reverseSecondaryKey := make([]byte, len(rootAddr.Bytes())*2)
		copy(reverseSecondaryKey[:len(rootAddr.Bytes())], addr.Bytes())
		copy(reverseSecondaryKey[len(rootAddr.Bytes()):], rootAddr.Bytes())

		reverseSecondaryAddrString := swarm.NewAddress(reverseSecondaryKey).String()

		if count, ok := m.pinSecondIndex[reverseSecondaryAddrString]; ok {
			m.pinSecondIndex[reverseSecondaryAddrString] = count + 1
		} else {
			m.pinSecondIndex[reverseSecondaryAddrString] = 1
		}

		// update address chunk count
		addrString := addr.String()
		if count, ok := m.pinIndex[addrString]; ok {
			m.pinIndex[addrString] = count + 1
		} else {
			m.pinIndex[addrString] = 1
		}

		secondaryKey := make([]byte, len(rootAddr.Bytes())*2)
		copy(secondaryKey[:len(rootAddr.Bytes())], rootAddr.Bytes())
		copy(secondaryKey[len(rootAddr.Bytes()):], addr.Bytes())

		secondaryAddrString := swarm.NewAddress(secondaryKey).String()

		if _, ok := m.pinSecondIndex[secondaryAddrString]; !ok {
			m.pinSecondIndex[secondaryAddrString] = 0
		}

	case storage.ModePinUploadingStarted:
		secondaryKey := make([]byte, len(rootAddr.Bytes())*2)
		copy(secondaryKey[:len(rootAddr.Bytes())], rootAddr.Bytes())
		copy(secondaryKey[len(rootAddr.Bytes()):], rootAddr.Bytes())

		secondaryAddrString := swarm.NewAddress(secondaryKey).String()

		if _, ok := m.pinSecondIndex[secondaryAddrString]; ok {
			return storage.ErrAlreadyPinned
		}

		m.pinSecondIndex[secondaryAddrString] = 0

	case storage.ModePinUploadingCompleted:
		rootAddrString := rootAddr.String()
		calculatedRootAddrString := addr.String() // this is calculated root chunk after upload

		// migrate from random root hash to actual one
		for addrString := range m.pinSecondIndex {
			if !strings.HasPrefix(addrString, rootAddrString) {
				continue
			}

			keyLen := len(addrString) / 2
			parent := addrString[:keyLen]
			actual := addrString[keyLen:]

			// skipping root address
			if parent == actual {
				continue
			}

			reverseRandomAddrString := actual + parent

			count, ok := m.pinSecondIndex[reverseRandomAddrString]
			if !ok {
				return storage.ErrNotFound
			}

			secondaryAddrString := calculatedRootAddrString + actual
			reverseAddrString := actual + calculatedRootAddrString

			// only put secondary entries if not already exists

			if _, ok := m.pinSecondIndex[secondaryAddrString]; !ok {
				m.pinSecondIndex[secondaryAddrString] = count
				m.pinSecondIndex[reverseAddrString] = count
			} else {
				if pinCount, ok := m.pinIndex[calculatedRootAddrString]; ok {
					m.pinIndex[calculatedRootAddrString] = pinCount - 1
				} else {
					m.pinIndex[calculatedRootAddrString] = 1
				}
			}
		}

		// remove random root addresses
		removeAddr := make([]string, 0)

		for addrString := range m.pinSecondIndex {
			if !strings.HasPrefix(addrString, rootAddrString) {
				continue
			}

			keyLen := len(addrString) / 2
			parent := addrString[:keyLen]
			actual := addrString[keyLen:]

			// skipping root address
			if parent == actual {
				continue
			}

			reverseAddrString := actual + parent

			removeAddr = append(removeAddr, addrString)
			removeAddr = append(removeAddr, reverseAddrString)
		}

		// remove main random root address
		removeAddr = append(removeAddr, rootAddrString+rootAddrString)

		for _, addrString := range removeAddr {
			delete(m.pinSecondIndex, addrString)
		}

		// create entry for actual root address
		secondaryKey := make([]byte, len(addr.Bytes())*2)
		copy(secondaryKey[:len(addr.Bytes())], addr.Bytes())
		copy(secondaryKey[len(addr.Bytes()):], addr.Bytes())

		secondaryAddrString := swarm.NewAddress(secondaryKey).String()

		m.pinSecondIndex[secondaryAddrString] = 1

	case storage.ModePinUploadingCleanup:
		rootAddrString := rootAddr.String()

		// remove random root addresses
		removeAddr := make([]string, 0)

		for addrString := range m.pinSecondIndex {
			if !strings.HasPrefix(addrString, rootAddrString) {
				continue
			}

			keyLen := len(addrString) / 2
			parent := addrString[:keyLen]
			actual := addrString[keyLen:]

			// skipping root address
			if parent == actual {
				continue
			}

			reverseAddrString := actual + parent

			removeAddr = append(removeAddr, addrString)
			removeAddr = append(removeAddr, reverseAddrString)
		}

		// remove main random root address
		removeAddr = append(removeAddr, rootAddrString+rootAddrString)

		for _, addrString := range removeAddr {
			delete(m.pinSecondIndex, addrString)
		}

	case storage.ModePinUnpinStarted:
		// remove root secondary key
		secondaryKey := make([]byte, len(rootAddr.Bytes())*2)
		copy(secondaryKey[:len(rootAddr.Bytes())], rootAddr.Bytes())
		copy(secondaryKey[len(rootAddr.Bytes()):], rootAddr.Bytes())

		secondaryAddrString := swarm.NewAddress(secondaryKey).String()

		delete(m.pinSecondIndex, secondaryAddrString)

	case storage.ModePinUnpinCompleted:
		rootAddrString := rootAddr.String()

		var rootHasRelatedAddresses bool

		for addrString := range m.pinSecondIndex {
			if strings.HasPrefix(addrString, rootAddrString) {
				rootHasRelatedAddresses = true
				break
			}
		}

		if rootHasRelatedAddresses {
			return fmt.Errorf("unpinning cannot be completed: %s", rootAddr.String())
		}

		if count, ok := m.pinIndex[rootAddrString]; ok {
			updatedCount := count - 1

			if updatedCount == 0 {
				delete(m.pinIndex, rootAddrString)
			} else {
				m.pinIndex[rootAddrString] = updatedCount
			}
		}

	case storage.ModePinUnpinFoundAddress:
		if rootAddr.Equal(addr) {
			return nil
		}

		rootAddrString := rootAddr.String()
		addrString := addr.String()

		reverseSecondaryAddrString := addrString + rootAddrString
		secondaryAddrString := rootAddrString + addrString

		count, ok := m.pinSecondIndex[reverseSecondaryAddrString]
		if !ok {
			return nil
		}

		delete(m.pinSecondIndex, reverseSecondaryAddrString)
		delete(m.pinSecondIndex, secondaryAddrString)

		// remove from main pin index
		if pinCount, ok := m.pinIndex[addrString]; ok {
			if count > pinCount {
				return fmt.Errorf("reverse address pin count: %d more than expected: %d", count, pinCount)
			}

			updatedCount := pinCount - count

			if updatedCount == 0 {
				delete(m.pinIndex, addrString)
			} else {
				m.pinIndex[addrString] = updatedCount
			}
		}

	default:
	}

	return nil
}

func (m *MockStorer) pinSingle(addr swarm.Address) error {
	secondaryKey := make([]byte, len(addr.Bytes())*2)
	copy(secondaryKey[:len(addr.Bytes())], addr.Bytes())

	secondaryAddrString := swarm.NewAddress(secondaryKey).String()

	if _, ok := m.pinSecondIndex[secondaryAddrString]; ok {
		return storage.ErrAlreadyPinned
	}

	m.pinSecondIndex[secondaryAddrString] = 1

	addrString := addr.String()
	if count, ok := m.pinIndex[addrString]; ok {
		m.pinIndex[addrString] = count + 1
	} else {
		m.pinIndex[addrString] = 1
	}

	return nil
}

func (m *MockStorer) pinUnpinSingle(addr swarm.Address) error {
	secondaryKey := make([]byte, len(addr.Bytes())*2)
	copy(secondaryKey[:len(addr.Bytes())], addr.Bytes())

	secondaryAddrString := swarm.NewAddress(secondaryKey).String()

	if _, ok := m.pinSecondIndex[secondaryAddrString]; !ok {
		return storage.ErrNotFound
	}

	delete(m.pinSecondIndex, secondaryAddrString)

	addrString := addr.String()
	if count, ok := m.pinIndex[addrString]; ok {
		updatedCount := count - 1

		if updatedCount == 0 {
			delete(m.pinIndex, addrString)
		} else {
			m.pinIndex[addrString] = updatedCount
		}
	}

	return nil
}

func (m *MockStorer) PinnedChunks(ctx context.Context, offset, cursor int) (pinnedChunks []*storage.Pinner, err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if len(m.pinIndex) == 0 {
		return pinnedChunks, nil
	}

	for addrString, count := range m.pinIndex {
		addrBytes, _ := hex.DecodeString(addrString)
		pi := &storage.Pinner{
			Address:    swarm.NewAddress(addrBytes),
			PinCounter: count,
		}
		pinnedChunks = append(pinnedChunks, pi)
	}

	if pinnedChunks == nil {
		return pinnedChunks, errors.New("pin chunks: leveldb: not found")
	}

	return pinnedChunks, nil
}

func (m *MockStorer) PinCounter(address swarm.Address) (uint64, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if count, ok := m.pinIndex[address.String()]; ok {
		return count, nil
	}

	return 0, storage.ErrNotFound
}

func (m *MockStorer) Close() error {
	close(m.quit)
	return nil
}

type Option interface {
	apply(*MockStorer)
}
type optionFunc func(*MockStorer)

func (f optionFunc) apply(r *MockStorer) { f(r) }
