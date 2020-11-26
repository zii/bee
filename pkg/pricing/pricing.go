// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pricing

import (
	"context"
	"fmt"
	"time"

	"github.com/ethersphere/bee/pkg/logging"
	"github.com/ethersphere/bee/pkg/p2p"
	"github.com/ethersphere/bee/pkg/p2p/protobuf"
	"github.com/ethersphere/bee/pkg/pricing/pb"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

const (
	protocolName    = "pricing"
	protocolVersion = "1.0.0"
	streamName      = "pricing"
)

var _ Interface = (*Service)(nil)

// Interface is the main interface of the pricing protocol
type Interface interface {
	AnnouncePaymentThreshold(ctx context.Context, peer swarm.Address, paymentThreshold uint64) error
}

// PaymentThresholdObserver is used for being notified of payment threshold updates
type PaymentThresholdObserver interface {
	NotifyPaymentThreshold(peer swarm.Address, paymentThreshold uint64) error
}

// PriceTableObserver is used for being notified of price table updates
type PriceTableObserver interface {
	NotifyPriceTable(peer swarm.Address, priceTable []uint64) error
}

type Service struct {
	streamer                 p2p.Streamer
	logger                   logging.Logger
	paymentThreshold         uint64
	priceTable               []uint64
	paymentThresholdObserver PaymentThresholdObserver
	priceTableObserver       PriceTableObserver
	store                    storage.StateStorer
}

func New(streamer p2p.Streamer, logger logging.Logger, paymentThreshold uint64, store storage.StateStorer) *Service {

	var priceTable []uint64
	err := store.Get(accounting.PriceTableKey(), &priceTable)
	if err != nil {
		priceTable = DefaultPriceTable()
	}

	return &Service{
		streamer:         streamer,
		logger:           logger,
		paymentThreshold: paymentThreshold,
		priceTable:       priceTable,
		store:            store,
	}
}

func (s *Service) Protocol() p2p.ProtocolSpec {
	return p2p.ProtocolSpec{
		Name:    protocolName,
		Version: protocolVersion,
		StreamSpecs: []p2p.StreamSpec{
			{
				Name:    streamName,
				Handler: s.handler,
			},
		},
		ConnectIn:  s.init,
		ConnectOut: s.init,
	}
}

func (s *Service) PriceTable() []uint64 {

	return
}

func (s *Service) handler(ctx context.Context, p p2p.Peer, stream p2p.Stream) (err error) {
	r := protobuf.NewReader(stream)
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			_ = stream.FullClose()
		}
	}()

	var req pb.AnnouncePaymentThreshold
	if err := r.ReadMsgWithContext(ctx, &req); err != nil {
		s.logger.Debugf("error receiving payment threshold and/or price table announcement from peer %v", p.Address)
		return fmt.Errorf("read request from peer %v: %w", p.Address, err)
	}
	s.logger.Tracef("received payment threshold and/or price table announcement from peer %v of %d", p.Address, req.PaymentThreshold)

	if req.PriceTable != nil {
		err = s.priceTableObserver.NotifyPriceTable(p.Address, req.PriceTable)
		if err != nil {
			s.logger.Debugf("error receiving pricetable from peer %v: %w", p.Address, err)
			s.logger.Errorf("error receiving pricetable from peer %v: %w", p.Address, err)
		}
	}

	if req.PaymentThreshold == 0 {
		return err
	}

	return s.paymentThresholdObserver.NotifyPaymentThreshold(p.Address, req.PaymentThreshold)
}

func (s *Service) init(ctx context.Context, p p2p.Peer) error {
	err := s.AnnouncePaymentThresholdAndPriceTable(ctx, p.Address, s.paymentThreshold, s.priceTable)
	if err != nil {
		s.logger.Warningf("error sending payment threshold announcement to peer %v", p.Address)
	}
	return err
}

// AnnouncePaymentThreshold announces the payment threshold to per
func (s *Service) AnnouncePaymentThresholdAndPriceTable(ctx context.Context, peer swarm.Address, paymentThreshold uint64, priceTable []uint64) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	stream, err := s.streamer.NewStream(ctx, peer, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	s.logger.Tracef("sending payment threshold announcement to peer %v of %d", peer, paymentThreshold)
	w := protobuf.NewWriter(stream)
	err = w.WriteMsgWithContext(ctx, &pb.AnnouncePaymentThreshold{
		PaymentThreshold: paymentThreshold,
		PriceTable:       priceTable,
	})

	return err
}

// AnnouncePaymentThreshold announces the payment threshold to per
func (s *Service) AnnouncePaymentThreshold(ctx context.Context, peer swarm.Address, paymentThreshold uint64) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	stream, err := s.streamer.NewStream(ctx, peer, nil, protocolName, protocolVersion, streamName)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = stream.Reset()
		} else {
			go stream.FullClose()
		}
	}()

	s.logger.Tracef("sending payment threshold announcement to peer %v of %d", peer, paymentThreshold)
	w := protobuf.NewWriter(stream)
	err = w.WriteMsgWithContext(ctx, &pb.AnnouncePaymentThreshold{
		PaymentThreshold: paymentThreshold,
	})

	return err
}

// SetPaymentThresholdObserver sets the PaymentThresholdObserver to be used when receiving a new payment threshold
func (s *Service) SetPaymentThresholdObserver(observer PaymentThresholdObserver) {
	s.paymentThresholdObserver = observer
}

// SetPaymentThresholdObserver sets the PaymentThresholdObserver to be used when receiving a new payment threshold
func (s *Service) SetPriceTableObserver(observer PriceTableObserver) {
	s.priceTableObserver = observer
}
