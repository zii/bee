// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package push

import (
	"context"
	"errors"

	"github.com/ethersphere/bee/pkg/file/pipeline"
	"github.com/ethersphere/bee/pkg/pushsync"
	"github.com/ethersphere/bee/pkg/swarm"
	"golang.org/x/sync/errgroup"
)

var errInvalidData = errors.New("store: invalid data")

const concurrentPushes = 10

type pushWriter struct {
	ctx  context.Context
	next pipeline.ChainWriter
	p    pushsync.PushSyncer

	sem chan struct{}
	eg  errgroup.Group
}

// NewPushSyncWriter returns a pushWriter. It writes the given data to the network
// using the PushSyncer.
func NewPushSyncWriter(ctx context.Context, p pushsync.PushSyncer, next pipeline.ChainWriter) pipeline.ChainWriter {
	return &pushWriter{
		ctx:  ctx,
		p:    p,
		sem:  make(chan struct{}, concurrentPushes),
		next: next,
	}
}

func (w *pushWriter) ChainWrite(p *pipeline.PipeWriteArgs) error {
	if p.Ref == nil || p.Data == nil {
		return errInvalidData
	}
	var err error
	c := swarm.NewChunk(swarm.NewAddress(p.Ref), p.Data)
	<-w.sem

	func(c swarm.Chunk) {
		w.eg.Go(func() {
			defer func() {
				w.sem <- struct{}{}
				w.wg.Done()
			}()
		PUSH:
			_, err = w.p.PushChunkToClosest(w.ctx, c)
			if err != nil {
				goto PUSH
			}
		})
	}(c)
	if w.next == nil {
		return nil
	}

	return w.next.ChainWrite(p)
}

func (w *pushWriter) Sum() ([]byte, error) {
	w.wg.Wait()
	return w.next.Sum()
}
