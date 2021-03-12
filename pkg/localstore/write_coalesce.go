package localstore

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

	quit chan struct{}
}

func NewPutBinder() *binder {
	return &binder{
		in: make(chan put),
		active: &batch{
			c:    sync.NewCond(new(sync.RWMutex)),
			feed: make(chan swarm.Chunk, writeBuffer),
		},
		quit: make(chan struct{}),
	}
}

var (
	collectionTime = 50 * time.Millisecond
	writeBuffer    = 50 // how many chunks to buffer
)

func (b *binder) run() {
	csw := make(chan chan put)
	go func(cr chan put) {
		cr := cr
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

	buffer := make(map[string][]swarm.Chunk)
	var t <-chan time.Time
	for {
		select {
		case p := <-b.active.feed:
			chs := buffer[p.mode.String()]
			buffer[p.mode.String()] = append(chs, p.ch)
			if t == nil {
				t = time.After(collectionTime)
			}
		case <-t:
			t = nil
			b.mtx.Lock()
			cur := b.active

			b.active = &batch{
				c:    sync.NewCond(new(sync.RWMutex)),
				feed: make(chan swarm.Chunk),
			}
			csw <- b.feed
			// other writes can already grab the new batch
			// and write into the channel buffer while this batch
			// is still flushing.
			b.mtx.Unlock()

			// drain the channel
			for p := range cur.feed {
				chs := buffer[p.mode.String()]
				buffer[p.mode.String()] = append(chs, p.ch)
			}

			// do the write
			err := b.putAll()

			// broadcast the result to all waiting goroutines
			cur.c.Lock()
			cur.err = err
			cur.c.Unlock()
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
func (b *binder) Put(ctx context.Context, mode storage.ModePut, ch swarm.Chunk) (bool, error) {
	b.mtx.RLock()
	a := b.active
	b.mtx.RUnlock()

	a.feed <- put{ch: ch, mode: mode}
	errc := make(chan error)
	go func() {
		a.L.RLock()
		a.Wait()
		errc <- a.err
		a.L.RUnlock()
	}()

	select {
	case e := <-errc:
		return false, e
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// this writes the entire batch to leveldb by calling localstore actual Put logic here
func (b *binder) putAll(chs ...swarm.Chunk) {

}

func (b *binder) Close() error {
	close(b.quit)
}
