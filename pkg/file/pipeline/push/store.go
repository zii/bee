// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pushstore

import (
	"context"
	"errors"

	"github.com/ethersphere/bee/pkg/file/pipeline"
	"github.com/ethersphere/bee/pkg/pushsync"
)

var errInvalidData = errors.New("store: invalid data")

type storeWriter struct {
	p    pushsync.PushSyncer
	ctx  context.Context
	next pipeline.ChainWriter
}

// NewStoreWriter returns a storeWriter. It just writes the given data
// to a given storage.Putter.
func NewPushSyncWriter(ctx context.Context, p pushsync.PushSyncer, next pipeline.ChainWriter) pipeline.ChainWriter {
	return &storeWriter{ctx: ctx, p: p, next: next}
}

func (w *storeWriter) ChainWrite(p *pipeline.PipeWriteArgs) error {
	var err error
PUSH:
	_, err = w.p.PushChunkToClosest()
	if err != nil {
		goto PUSH
	}

	return w.next.ChainWrite(p)
}

func (w *storeWriter) Sum() ([]byte, error) {
	return w.next.Sum()
}
