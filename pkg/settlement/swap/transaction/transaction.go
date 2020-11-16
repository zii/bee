// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package transaction

import (
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/pkg/crypto"
	"github.com/ethersphere/bee/pkg/logging"
	"golang.org/x/net/context"
)

const (
	RpcErrorTransactionNotFound = "not found"
)

var (
	ErrTransactionReverted = errors.New("transaction reverted")
)

// TxRequest describes a request for a transaction that can be executed.
type TxRequest struct {
	To       common.Address // recipient of the transaction
	Data     []byte         // transaction data
	GasPrice *big.Int       // gas price or nil if suggested gas price should be used
	GasLimit uint64         // gas limit or 0 if it should be estimated
	Value    *big.Int       // amount of wei to send
}

// Service is the service to send transactions. It takes care of gas price, gas limit and nonce management.
type Service interface {
	// Send creates a transaction based on the request and sends it.
	Send(ctx context.Context, request *TxRequest) (txHash common.Hash, err error)
	// WaitForReceipt waits until either the transaction with the given hash has been mined or the context is cancelled.
	WaitForReceipt(ctx context.Context, txHash common.Hash) (receipt *types.Receipt, err error)
	// WatchForReceipt watches the transaction in the background until the context is cancelled or there is an unexpected backend error
	WatchForReceipt(ctx context.Context, txHash common.Hash) (chan *types.Receipt, chan error)
}

type transactionService struct {
	logger  logging.Logger
	backend Backend
	signer  crypto.Signer
	sender  common.Address
}

// NewService creates a new transaction service.
func NewService(logger logging.Logger, backend Backend, signer crypto.Signer) (Service, error) {
	senderAddress, err := signer.EthereumAddress()
	if err != nil {
		return nil, err
	}
	return &transactionService{
		logger:  logger,
		backend: backend,
		signer:  signer,
		sender:  senderAddress,
	}, nil
}

// Send creates and signs a transaction based on the request and sends it.
func (t *transactionService) Send(ctx context.Context, request *TxRequest) (txHash common.Hash, err error) {
	tx, err := prepareTransaction(ctx, request, t.sender, t.backend)
	if err != nil {
		return common.Hash{}, err
	}

	signedTx, err := t.signer.SignTx(tx)
	if err != nil {
		return common.Hash{}, err
	}

	err = t.backend.SendTransaction(ctx, signedTx)
	if err != nil {
		return common.Hash{}, err
	}

	return signedTx.Hash(), nil
}

// tryGetReceipt tries to get the receipt from the backend
// treats "not found" errors as pending transactions and every other error as an actual one
func (t *transactionService) tryGetReceipt(ctx context.Context, txHash common.Hash) (receipt *types.Receipt, err error) {
	_, pending, err := t.backend.TransactionByHash(ctx, txHash)
	if err != nil {
		// some clients report "not found" as error, others just return a nil receipt
		// other errors should not be ignored which is why we match here explicitly
		if err.Error() == RpcErrorTransactionNotFound {
			t.logger.Tracef("could not find transaction %x on backend", txHash)
			return nil, nil
		} else {
			return nil, fmt.Errorf("could not get transaction from backend: %w", err)
		}
	}

	if pending {
		t.logger.Tracef("waiting for transaction %x to be mined", txHash)
		return nil, nil
	}

	receipt, err = t.backend.TransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, err
	}
	if receipt == nil {
		return nil, errors.New("did not get receipt")
	}
	return receipt, nil
}

// WaitForReceipt waits until either the transaction with the given hash has been mined or the context is cancelled.
func (t *transactionService) WaitForReceipt(ctx context.Context, txHash common.Hash) (receipt *types.Receipt, err error) {
	for {
		receipt, err := t.tryGetReceipt(ctx, txHash)
		if err != nil {
			return nil, err
		}
		if receipt != nil {
			return receipt, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
}

// prepareTransaction creates a signable transaction based on a request.
func prepareTransaction(ctx context.Context, request *TxRequest, from common.Address, backend Backend) (tx *types.Transaction, err error) {
	var gasLimit uint64
	if request.GasLimit == 0 {
		gasLimit, err = backend.EstimateGas(ctx, ethereum.CallMsg{
			From: from,
			To:   &request.To,
			Data: request.Data,
		})
		if err != nil {
			return nil, err
		}
	} else {
		gasLimit = request.GasLimit
	}

	var gasPrice *big.Int
	if request.GasPrice == nil {
		gasPrice, err = backend.SuggestGasPrice(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		gasPrice = request.GasPrice
	}

	nonce, err := backend.PendingNonceAt(ctx, from)
	if err != nil {
		return nil, err
	}

	return types.NewTransaction(
		nonce,
		request.To,
		request.Value,
		gasLimit,
		gasPrice,
		request.Data,
	), nil
}

// WatchForReceipt watches the transaction in the background until the context is cancelled or there is an unexpected backend error
func (t *transactionService) WatchForReceipt(ctx context.Context, txHash common.Hash) (chan *types.Receipt, chan error) {
	receiptC := make(chan *types.Receipt)
	errC := make(chan error)
	go func() {
		defer close(receiptC)
		defer close(errC)

		for {
			// try to get the receipt and in case of error abort immediately
			receipt, err := t.tryGetReceipt(ctx, txHash)
			if err != nil {
				errC <- err
				return
			}

			if receipt != nil {
				receiptC <- receipt
				return
			}

			// wait some time until next check and abort if context is cancelled
			select {
			case <-ctx.Done():
				errC <- ctx.Err()
				return
			case <-time.After(1 * time.Second):
			}
		}
	}()
	return receiptC, errC
}
