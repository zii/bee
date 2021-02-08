// Copyright 2021 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pslice

import (
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/ethersphere/bee/pkg/swarm"
)

type binaryAddressTree struct {
	root *binaryAddressTreeNode
}

type binaryAddressTreeNode struct {
	index      uint8
	indexLimit uint8

	b0 *binaryAddressTreeNode
	b1 *binaryAddressTreeNode

	addr     swarm.Address
	unsorted []swarm.Address
}

func newBinaryAddressTree(index, limit uint8) *binaryAddressTree {
	var indexLimit uint8
	if index > math.MaxUint8-limit {
		indexLimit = math.MaxUint8
	} else {
		indexLimit = index + limit
	}
	node := &binaryAddressTreeNode{index: index, indexLimit: indexLimit}
	return &binaryAddressTree{root: node}
}

func (t *binaryAddressTree) Insert(addr swarm.Address) *binaryAddressTree {
	if t.root == nil {
		t.root = &binaryAddressTreeNode{addr: addr, index: 0, indexLimit: math.MaxUint8}
	} else {
		t.root.Insert(addr, t.root.index+1)
	}
	return t
}

func (t *binaryAddressTree) Print(w io.Writer, bitsLimit int) {
	printBinaryAddressTree(w, bitsLimit, t.root, 0, 'R')
}

func printBinaryAddressTree(w io.Writer, bitsLimit int, node *binaryAddressTreeNode, ns int, ch rune) {
	if node == nil {
		return
	}

	for i := 0; i < ns; i++ {
		fmt.Fprint(w, " ")
	}
	if !node.Address().IsZero() {
		fmt.Fprintf(w, "%c: %v\t[%s]\n", ch, printBits(node.Address().Bytes(), bitsLimit), node.Address().String())
	}
	printBinaryAddressTree(w, bitsLimit, node.B0(), ns+2, '0')
	printBinaryAddressTree(w, bitsLimit, node.B1(), ns+2, '1')
}

func (t *binaryAddressTree) LevelOrderTraversalAddresses() (addrs []swarm.Address) {
	if t.root == nil {
		return
	}

	queue := []*binaryAddressTreeNode{}
	queue = append(queue, t.root)

	for {
		if len(queue) == 0 {
			break
		}

		node := queue[0]
		if !node.addr.IsZero() {
			addrs = append(addrs, node.addr)
		}
		queue = append(queue[:0], queue[1:]...)

		if node.b0 != nil {
			queue = append(queue, node.b0)
		}

		if node.b1 != nil {
			queue = append(queue, node.b1)
		}
	}

	return
}

func (t *binaryAddressTree) LevelOrderSwitchingTraversalAddresses() (addrs []swarm.Address) {
	if t.root == nil {
		return
	}

	queue := []*binaryAddressTreeNode{}
	queue = append(queue, t.root)

	queues := make([][]*binaryAddressTreeNode, 2)

	for {
		if len(queue) == 0 {

			if len(queues[0]) > 0 {
				queue = append(queue, queues[0]...)
				queues[0] = queues[0][:0]
			}

			if len(queues[1]) > 0 {
				queue = append(queue, queues[1]...)
				queues[1] = queues[1][:0]
			}

			if len(queue) > 0 {
				continue
			}

			break
		}

		node := queue[0]
		if !node.addr.IsZero() {
			addrs = append(addrs, node.addr)
		}
		// pop
		queue = append(queue[:0], queue[1:]...)

		if node.b0 != nil {
			queues[0] = append(queues[0], node.b0)
		}

		if node.b1 != nil {
			queues[1] = append(queues[1], node.b1)
		}
	}

	return
}

func (n *binaryAddressTree) Root() *binaryAddressTreeNode {
	return n.root
}

func (n *binaryAddressTreeNode) Address() swarm.Address {
	return n.addr
}

func (n *binaryAddressTreeNode) B0() *binaryAddressTreeNode {
	return n.b0
}

func (n *binaryAddressTreeNode) B1() *binaryAddressTreeNode {
	return n.b1
}

func (n *binaryAddressTreeNode) Insert(addr swarm.Address, index uint8) {
	if n == nil {
		return
	}

	b := getBitAtIndexUint8(addr.Bytes(), n.index)

	if !b {
		if n.b0 == nil {
			n.b0 = &binaryAddressTreeNode{addr: addr, index: index, indexLimit: n.indexLimit}
		} else {
			if n.b0.index < n.indexLimit {
				n.b0.Insert(addr, n.b0.index+1)
			} else {
				n.b0.unsorted = append(n.b0.unsorted, addr)
			}
		}
	} else {
		if n.b1 == nil {
			n.b1 = &binaryAddressTreeNode{addr: addr, index: index, indexLimit: n.indexLimit}
		} else {
			if n.b1.index < n.indexLimit {
				n.b1.Insert(addr, n.b1.index+1)
			} else {
				n.b1.unsorted = append(n.b1.unsorted, addr)
			}
		}
	}
}

func getIndexAndRemainder(k uint8) (uint8, uint8) {
	return k / 8, k % 8
}

func getBitAtIndexUint8(bytes []byte, k uint8) bool {
	i, pos := getIndexAndRemainder(k)
	return bytes[i]&bits.Reverse8(0x1<<pos) != 0
}

func printBits(bytes []byte, limit int) string {
	s := ""
	for _, b := range bytes {
		s += fmt.Sprintf("%0.8b ", b)
		if len(s) >= limit {
			break
		}
	}
	return s[:len(s)-1]
}
