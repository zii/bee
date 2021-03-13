package pushsync

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/bee/pkg/swarm"
)

type put struct {
	ch   swarm.Chunk
	mode storage.ModePut
}

type batch struct {
	c    *sync.Cond
	err  error
	feed chan put
}

type binder struct {
	active *batch
	in     chan put
	mtx    sync.RWMutex
	putter storage.Putter

	quit chan struct{}
}

func NewPutBinder(p storage.Putter) *binder {
	b := &binder{
		in: make(chan put),
		active: &batch{
			c:    sync.NewCond(new(sync.RWMutex)),
			feed: make(chan put, writeBuffer),
		},
		putter: p,
		quit:   make(chan struct{}),
	}

	go b.run()
	return b
}

var (
	collectionTime = 20 * time.Millisecond
	writeBuffer    = 50 // how many chunks to buffer
)

func (b *binder) run() {
	csw := make(chan chan put)
	go func(cr chan put) {
		for {
			select {
			case p := <-b.in:
				cr <- p
			case newchan := <-csw:
				close(cr)
				cr = newchan
			}
		}
	}(b.active.feed)

	buffer := []swarm.Chunk{}
	var t <-chan time.Time
	for {
		select {
		case p := <-b.active.feed:
			buffer = append(buffer, p.ch)
			if t == nil {
				t = time.After(collectionTime)
			}
		case <-t:
			t = nil
			b.mtx.Lock()
			cur := b.active

			b.active = &batch{
				c:    sync.NewCond(new(sync.RWMutex)),
				feed: make(chan put),
			}
			csw <- b.active.feed
			// other writes can already grab the new batch
			// and write into the channel buffer while this batch
			// is still flushing.
			b.mtx.Unlock()

			// drain the channel
			for p := range cur.feed {
				buffer = append(buffer, p.ch)
			}

			// do the write
			err := b.putAll()

			// broadcast the result to all waiting goroutines
			cur.c.L.Lock()
			cur.err = err
			cur.c.L.Unlock()
			cur.c.Broadcast()
		case <-b.quit:
			b.active.c.L.Lock()
			b.active.err = errors.New("shutting down...")
			b.active.c.L.Unlock()
			b.active.c.Broadcast()
			return
		}
	}
}

// external individual writes get routed here
func (b *binder) Put(ctx context.Context, mode storage.ModePut, chs ...swarm.Chunk) ([]bool, error) {
	b.mtx.RLock()
	a := b.active
	b.mtx.RUnlock()
	for _, ch := range chs {
		b.in <- put{ch: ch, mode: mode}
	}
	errc := make(chan error)
	go func() {
		a.c.L.Lock()
		a.c.Wait()
		errc <- a.err
		a.c.L.Unlock()
	}()

	select {
	case e := <-errc:
		return []bool{}, e
	case <-ctx.Done():
		return []bool{}, ctx.Err()
	}
}

// this writes the entire batch to leveldb by calling localstore actual Put logic here
func (b *binder) putAll(chs ...swarm.Chunk) error {
	_, err := b.putter.Put(context.Background(), storage.ModePutSync, chs...)
	return err
}

func (b *binder) Close() error {
	close(b.quit)
	return nil
}
