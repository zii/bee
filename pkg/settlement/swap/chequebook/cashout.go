// Copyright 2020 The Swarm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package chequebook

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethersphere/bee/pkg/logging"
	"github.com/ethersphere/bee/pkg/settlement/swap/transaction"
	"github.com/ethersphere/bee/pkg/storage"
	"github.com/ethersphere/sw3-bindings/v2/simpleswapfactory"
)

var (
	// ErrNoCashout is the error if there has not been any cashout action for the chequebook
	ErrNoCashout           = errors.New("no prior cashout")
	ErrCashoutStillPending = errors.New("previous cashout still pending")
	ErrNothingToCash       = errors.New("nothing to cash")
)

// CashoutService is the service responsible for managing cashout actions
type CashoutService interface {
	io.Closer
	// Start starts monitoring past transactions
	Start() error
	// SetNotifyBouncedFunc sets the notify function for bouncing chequebooks
	SetNotifyBouncedFunc(f NotifyBouncedFunc)
	// CashCheque sends a cashing transaction for the last cheque of the chequebook
	CashCheque(ctx context.Context, chequebook common.Address, recipient common.Address) (common.Hash, error)
	// CashoutStatus gets the status of the latest cashout transaction for the chequebook
	CashoutStatus(ctx context.Context, chequebookAddress common.Address) (*CashoutStatus, error)
}

type cashoutService struct {
	lock                  sync.Mutex
	logger                logging.Logger
	store                 storage.StateStorer
	simpleSwapBindingFunc SimpleSwapBindingFunc
	backend               transaction.Backend
	transactionService    transaction.Service
	chequebookABI         abi.ABI
	chequeStore           ChequeStore
	notifyBouncedFunc     NotifyBouncedFunc
	monitorCtx            context.Context
	monitorCtxCancel      context.CancelFunc
	wg                    sync.WaitGroup
}

// CashoutStatus is the action plus its result
type CashoutStatus struct {
	PendingTxHash common.Hash
	PendingCheque SignedCheque
	TxHash        common.Hash
	Cheque        SignedCheque // the cheque that was used to cashout which may be different from the latest cheque
	Result        *CashChequeResult
	Reverted      bool
}

// CashChequeResult summarizes the result of a CashCheque or CashChequeBeneficiary call
type CashChequeResult struct {
	Beneficiary      common.Address // beneficiary of the cheque
	Recipient        common.Address // address which received the funds
	Caller           common.Address // caller of cashCheque
	TotalPayout      *big.Int       // total amount that was paid out in this call
	CumulativePayout *big.Int       // cumulative payout of the cheque that was cashed
	CallerPayout     *big.Int       // payout for the caller of cashCheque
	Bounced          bool           // indicates wether parts of the cheque bounced
}

// cashoutAction is the data we store for a cashout
type cashoutAction struct {
	TxHash common.Hash
	Cheque SignedCheque // the cheque that was used to cashout which may be different from the latest cheque
}

// NotifyBouncedFunc is used to notify something about bounced chequebooks
type NotifyBouncedFunc = func(chequebook common.Address) error

// NewCashoutService creates a new CashoutService
func NewCashoutService(
	logger logging.Logger,
	store storage.StateStorer,
	simpleSwapBindingFunc SimpleSwapBindingFunc,
	backend transaction.Backend,
	transactionService transaction.Service,
	chequeStore ChequeStore,
) (CashoutService, error) {
	chequebookABI, err := abi.JSON(strings.NewReader(simpleswapfactory.ERC20SimpleSwapABI))
	if err != nil {
		return nil, err
	}

	monitorCtx, monitorCtxCancel := context.WithCancel(context.Background())

	return &cashoutService{
		logger:                logger,
		store:                 store,
		simpleSwapBindingFunc: simpleSwapBindingFunc,
		backend:               backend,
		transactionService:    transactionService,
		chequebookABI:         chequebookABI,
		chequeStore:           chequeStore,
		monitorCtx:            monitorCtx,
		monitorCtxCancel:      monitorCtxCancel,
	}, nil
}

func (s *cashoutService) SetNotifyBouncedFunc(f NotifyBouncedFunc) {
	s.notifyBouncedFunc = f
}

// cashoutActionKey computes the store key for the last cashout action for the chequebook
func cashoutActionKey(chequebook common.Address) string {
	return fmt.Sprintf("cashout_%x", chequebook)
}

// cashoutActionKey computes the store key for the last cashout action for the chequebook
func pendingCashoutActionKey(chequebook common.Address) string {
	return fmt.Sprintf("pending_cashout_%x", chequebook)
}

// Start starts monitoring past transactions
func (s *cashoutService) Start() error {
	return s.store.Iterate("pending_cashout_", func(key, value []byte) (stop bool, err error) {
		var cashoutAction cashoutAction
		err = s.store.Get(string(key), &cashoutAction)
		if err != nil {
			return false, err
		}
		s.monitorCashChequeBeneficiaryTransaction(cashoutAction.Cheque.Chequebook, cashoutAction.TxHash)
		return false, nil
	})
}

// CashCheque sends a cashout transaction for the last cheque of the chequebook
func (s *cashoutService) CashCheque(ctx context.Context, chequebook common.Address, recipient common.Address) (common.Hash, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// check if there is already a pending action for this chequebook
	_, found, err := s.loadCashoutAction(chequebook, true)
	if err != nil {
		return common.Hash{}, err
	}
	if found {
		return common.Hash{}, ErrCashoutStillPending
	}

	cheque, err := s.chequeStore.LastCheque(chequebook)
	if err != nil {
		return common.Hash{}, err
	}

	// check if there actually was a cheque since the last successful action
	lastAction, found, err := s.loadCashoutAction(chequebook, false)
	if err != nil {
		return common.Hash{}, err
	}

	if found {
		if cheque.CumulativePayout.Cmp(lastAction.Cheque.CumulativePayout) <= 0 {
			return common.Hash{}, ErrNothingToCash
		}
	}

	// prepare and send the transaction
	callData, err := s.chequebookABI.Pack("cashChequeBeneficiary", recipient, cheque.CumulativePayout, cheque.Signature)
	if err != nil {
		return common.Hash{}, err
	}

	request := &transaction.TxRequest{
		To:       chequebook,
		Data:     callData,
		GasPrice: nil,
		GasLimit: 0,
		Value:    big.NewInt(0),
	}

	txHash, err := s.transactionService.Send(ctx, request)
	if err != nil {
		return common.Hash{}, err
	}

	// save the pending transaction and start monitoring
	err = s.store.Put(pendingCashoutActionKey(chequebook), &cashoutAction{
		TxHash: txHash,
		Cheque: *cheque,
	})
	if err != nil {
		return common.Hash{}, err
	}

	s.monitorCashChequeBeneficiaryTransaction(chequebook, txHash)

	return txHash, nil
}

func (s *cashoutService) monitorCashChequeBeneficiaryTransaction(chequebook common.Address, txHash common.Hash) {
	timeoutCtx, cancel := context.WithTimeout(s.monitorCtx, 1*time.Hour)
	receiptC, errC := s.transactionService.WatchForReceipt(timeoutCtx, txHash)

	s.wg.Add(1)
	go func() {
		defer cancel()
		defer s.wg.Done()

		select {
		case err := <-errC:
			if err == nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				// if the context expired, delete the info about the pending transaction
				// it is still possible that the pending transaction will still confirm eventually
				s.logger.Error("giving up on transaction %x", txHash)
				err = s.store.Delete(pendingCashoutActionKey(chequebook))
				if err != nil {
					s.logger.Error("could not timeout action: %v", err)
				}
				return
			} else if errors.Is(err, context.Canceled) {
				// if the context was cancelled just abort monitoring
				// as it is still the pending cashout monitoring will continue on next startup
				return
			}
			s.logger.Errorf("failed to monitor transaction %x: %v", txHash, err)
		case receipt := <-receiptC:
			if receipt == nil {
				return
			}
			err := s.processCashChequeBeneficiaryReceipt(chequebook, receipt)
			if err != nil {
				s.logger.Errorf("could not process cashout receipt: %v", err)
			}
		case <-s.monitorCtx.Done():
			return
		}
	}()
}

// load the last or pending cashout action from the store
func (s *cashoutService) loadCashoutAction(chequebook common.Address, pending bool) (action *cashoutAction, found bool, err error) {
	if pending {
		err = s.store.Get(pendingCashoutActionKey(chequebook), &action)
	} else {
		err = s.store.Get(cashoutActionKey(chequebook), &action)
	}
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return action, true, nil
}

// process the result of a cashout receipt
func (s *cashoutService) processCashChequeBeneficiaryReceipt(chequebook common.Address, receipt *types.Receipt) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	action, found, err := s.loadCashoutAction(chequebook, true)
	if err != nil {
		return err
	}
	if !found {
		return ErrNoCashout
	}

	// this should never happen
	// we remove it as pending and leave the old action in place (to avoid losing info about the last cashout)
	if receipt.Status == types.ReceiptStatusFailed {
		s.logger.Errorf("cashout transaction reverted: %x", action.TxHash)
		return s.store.Delete(pendingCashoutActionKey(chequebook))
	}

	result, err := s.parseCashChequeBeneficiaryReceipt(chequebook, receipt)
	if err != nil {
		return fmt.Errorf("could not parse cashout receipt: %w", err)
	}

	// save the pending action as the last one
	err = s.store.Put(cashoutActionKey(chequebook), &cashoutAction{
		TxHash: action.TxHash,
		Cheque: action.Cheque,
	})
	if err != nil {
		return err
	}

	// remove the pending action
	err = s.store.Delete(pendingCashoutActionKey(chequebook))
	if err != nil {
		return err
	}

	// notify if bounced
	if result.Bounced {
		s.logger.Infof("cashout bounced: %x", receipt.TxHash)
		if err = s.notifyBouncedFunc(chequebook); err != nil {
			return fmt.Errorf("notify bounced: %w", err)
		}
	}

	s.logger.Tracef("cashout confirmed: %x", receipt.TxHash)

	return nil
}

// CashoutStatus gets the status of the latest cashout transaction for the chequebook
func (s *cashoutService) CashoutStatus(ctx context.Context, chequebookAddress common.Address) (*CashoutStatus, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// status struct to be filled
	var cashoutStatus CashoutStatus

	// set the pending fields if there is one
	pendingAction, found, err := s.loadCashoutAction(chequebookAddress, true)
	if err != nil {
		return nil, err
	}

	if found {
		cashoutStatus.PendingTxHash = pendingAction.TxHash
		cashoutStatus.PendingCheque = pendingAction.Cheque
	}

	// load the last action
	action, found, err := s.loadCashoutAction(chequebookAddress, false)
	if err != nil {
		return nil, err
	}

	// if there is no last action we return with just the pending information (or all empty if nothing pending)
	if !found {
		return &cashoutStatus, nil
	}

	cashoutStatus.TxHash = action.TxHash
	cashoutStatus.Cheque = action.Cheque

	receipt, err := s.backend.TransactionReceipt(ctx, action.TxHash)
	if err != nil {
		return nil, err
	}

	// in the unlikely event of reversion (only happens in rare cases were the action was seen as successful and then reverted after a chain reorg)
	// set the reverted flag and return
	if receipt.Status == types.ReceiptStatusFailed {
		cashoutStatus.Reverted = true
		return &cashoutStatus, nil
	}

	// if not reverted we can parse the result and complete the status struct
	result, err := s.parseCashChequeBeneficiaryReceipt(chequebookAddress, receipt)
	if err != nil {
		return nil, err
	}

	cashoutStatus.Result = result
	return &cashoutStatus, nil
}

// parseCashChequeBeneficiaryReceipt processes the receipt from a CashChequeBeneficiary transaction
func (s *cashoutService) parseCashChequeBeneficiaryReceipt(chequebookAddress common.Address, receipt *types.Receipt) (*CashChequeResult, error) {
	result := &CashChequeResult{
		Bounced: false,
	}

	binding, err := s.simpleSwapBindingFunc(chequebookAddress, s.backend)
	if err != nil {
		return nil, err
	}

	for _, log := range receipt.Logs {
		if log.Address != chequebookAddress {
			continue
		}
		if event, err := binding.ParseChequeCashed(*log); err == nil {
			result.Beneficiary = event.Beneficiary
			result.Caller = event.Caller
			result.CallerPayout = event.CallerPayout
			result.TotalPayout = event.TotalPayout
			result.CumulativePayout = event.CumulativePayout
			result.Recipient = event.Recipient
		} else if _, err := binding.ParseChequeBounced(*log); err == nil {
			result.Bounced = true
		}
	}

	return result, nil
}

// Close stops the monitoring of all actions and waits for the monitoring routines to terminate
func (s *cashoutService) Close() error {
	s.monitorCtxCancel()
	s.wg.Wait()
	return nil
}

// Equal compares to CashChequeResults
func (r *CashChequeResult) Equal(o *CashChequeResult) bool {
	if r.Beneficiary != o.Beneficiary {
		return false
	}
	if r.Bounced != o.Bounced {
		return false
	}
	if r.Caller != o.Caller {
		return false
	}
	if r.CallerPayout.Cmp(o.CallerPayout) != 0 {
		return false
	}
	if r.CumulativePayout.Cmp(o.CumulativePayout) != 0 {
		return false
	}
	if r.Recipient != o.Recipient {
		return false
	}
	if r.TotalPayout.Cmp(o.TotalPayout) != 0 {
		return false
	}
	return true
}
