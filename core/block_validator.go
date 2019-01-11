// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"fmt"

	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/horae/replica"
	"github.com/ethereum/go-ethereum/params"
)

// BlockValidator is responsible for validating block headers, uncles and
// processed state.
//
// BlockValidator implements Validator.
type BlockValidator struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for validating
	signer types.Signer
}

// NewBlockValidator returns a new block validator which is safe for re-use
func NewBlockValidator(config *params.ChainConfig, blockchain *BlockChain, engine consensus.Engine) *BlockValidator {
	validator := &BlockValidator{
		config: config,
		engine: engine,
		bc:     blockchain,
		signer: types.NewEIP155Signer(config.ChainID),
	}
	return validator
}

// ValidateBody validates the given block's uncles and verifies the the block
// header's transaction and uncle roots. The headers are assumed to be already
// validated at this point.
func (v *BlockValidator) ValidateBody(block *types.Block) error {
	// Check whether the block's known, and if not, that it's linkable
	if v.bc.HasBlockAndState(block.Hash(), block.NumberU64()) {
		return ErrKnownBlock
	}
	if !v.bc.HasBlockAndState(block.ParentHash(), block.NumberU64()-1) {
		if !v.bc.HasBlock(block.ParentHash(), block.NumberU64()-1) {
			return consensus.ErrUnknownAncestor
		}
		return consensus.ErrPrunedAncestor
	}
	// Header validity is known at this point, check the uncles and transactions
	header := block.Header()
	if err := v.engine.VerifyUncles(v.bc, block); err != nil {
		return err
	}
	if hash := types.CalcUncleHash(block.Uncles()); hash != header.UncleHash {
		return fmt.Errorf("uncle root hash mismatch: have %x, want %x", hash, header.UncleHash)
	}
	if hash := types.DeriveSha(block.Transactions()); hash != header.TxHash {
		return fmt.Errorf("transaction root hash mismatch: have %x, want %x", hash, header.TxHash)
	}
	if err := v.ValidateTransaction(block); err != nil {
		return fmt.Errorf("invalid transactions: err : %s", err.Error())
	}
	return nil
}

func (v *BlockValidator) ValidateTransaction(block *types.Block) error {
	if v.bc.replicaDb == nil {
		return nil
	}
	replicas := make(map[uint64]*replica.Replica, 0)
	groups := make(map[uint64]*replica.Group, 0)
	preparedGroup := make(map[uint64]*replica.Group, 0)
	for _, tx := range block.Transactions() {
		if !v.bc.CheckEpoch(block.NumberU64()) && tx.Type()>>7 == 1 {
			continue
		}
		switch tx.Type() {
		case types.TRANSACTION_TYPE_JOINED_NODE:
			r := replica.DeserializeReplica(tx.Data())
			if r == nil {
				return fmt.Errorf("invalid join node tranaction, txHash: %x", tx.Hash())
			}
			if replicas[r.Id] != nil {
				return fmt.Errorf("duplicate replica id of the join node txs, id: %d", r.Id)
			}
			replicas[r.Id] = r
		case types.TRANSACTION_TYPE_NEW_NODE:
			from, _ := types.Sender(v.signer, tx)
			r := replica.DeserializeReplica(tx.Data())
			if r == nil || r.Address != from {
				return fmt.Errorf("invalid new node transaction, from : %s", from.Hex())
			}
		case types.TRANSACTION_TYPE_BUILD_GROUP:
			group := replica.DeserializeGroup(tx.Data())
			if group == nil {
				return fmt.Errorf("invalid build group tx, txHash :%x", tx.Hash())
			}
			if groups[group.Index] != nil {
				return fmt.Errorf("duplicate group id of the build group txs, id: %d", group.Index)
			}
			groups[group.Index] = group
		case types.TRANSACTION_TYPE_PREPARED_GROUP:
			group := replica.DeserializeGroup(tx.Data())
			if group == nil {
				return fmt.Errorf("invalid build group tx, txHash :%x", tx.Hash())
			}
			if preparedGroup[group.Index] != nil && preparedGroup[group.Index].Hash() != group.Hash() {
				return fmt.Errorf("duplicate group id of the build group txs, id: %d", group.Index)
			}
			preparedGroup[group.Index] = group
		case types.TRANSACTION_TYPE_GROUP_SHARE_REPORT:
			groupShareReport := replica.DeserializeGroupShareReport(tx.Data())
			if groupShareReport == nil {
				return fmt.Errorf("invalid group share report tx, txHash :%x", tx.Hash())
			}
			from, _ := types.Sender(v.signer, tx)
			if from != groupShareReport.From || groupShareReport.Epoch != block.Epoch() {
				return fmt.Errorf("invalid group share report tx")
			}
		}
	}
	if err := v.validateReplicas(replicas, block); err != nil {
		return fmt.Errorf("invalid join node transaction, err: %s", err.Error())
	}
	if err := v.validateGroups(groups, block, replicas); err != nil {
		return fmt.Errorf("invalid build group transaction, err: %s", err.Error())
	}
	if err := v.validatePreparedGroups(preparedGroup, block); err != nil {
		return fmt.Errorf("invalid prepared group transaction, err: %s", err.Error())
	}
	return nil
}

func (v *BlockValidator) validateReplicas(replicas map[uint64]*replica.Replica, block *types.Block) error {
	parent := v.bc.GetBlockByHash(block.ParentHash())
	if uint64(len(replicas)) != block.ReplicaNum()-parent.ReplicaNum() {
		return fmt.Errorf("new replicas size mismatch. have: %d, want: %d", len(replicas), block.ReplicaNum()-parent.ReplicaNum())
	}
	if v.bc.CheckEpoch(block.NumberU64()) {
		newNodes := v.bc.GetUnconfirmedReplica(parent.Hash())
		for i := parent.ReplicaNum() + 1; i < block.ReplicaNum(); i++ {
			if replicas[i] == nil || replicas[i].HashWithoutId() != newNodes[replicas[i].Address].HashWithoutId() {
				return fmt.Errorf("invalid replica. id: %d", i)
			}
		}
	}
	return nil
}

func (v *BlockValidator) validateGroups(groups map[uint64]*replica.Group, block *types.Block, newReplicas map[uint64]*replica.Replica) error {
	if !v.bc.CheckEpoch(block.NumberU64()) {
		return nil
	}
	block.Header().Hash()
	parent := v.bc.GetBlockByHash(block.ParentHash())
	groupsInfo := v.bc.GenerateGroupsInfo(block.Header(), parent.ReplicaNum(), newReplicas)
	for _, groupInfo := range groupsInfo {
		if groupInfo.HashWithoutKey() != groups[groupInfo.Index].HashWithoutKey() {
			return fmt.Errorf("invalid group members of group %d", groupInfo.Index)
		}
	}
	return nil
}

func (v *BlockValidator) validatePreparedGroups(groups map[uint64]*replica.Group, block *types.Block) error {
	if !v.bc.CheckEpoch(block.NumberU64()) {
		return nil
	}
	groupShareReports := v.bc.GetUnconfirmedGroupShareReport(block.ParentHash())
	for index, group := range groups {
		expectGroup := v.bc.BuildGroup(index, block.Epoch()-1, groupShareReports[index])
		if group.Hash() != expectGroup.Hash() {
			return fmt.Errorf("invalid prepared group. groupIndex: %d", group.Index)
		}
	}
	return nil
}

// ValidateState validates the various changes that happen after a state
// transition, such as amount of used gas, the receipt roots and the state root
// itself. ValidateState returns a database batch if the validation was a success
// otherwise nil and an error is returned.
func (v *BlockValidator) ValidateState(block, parent *types.Block, statedb *state.StateDB, receipts types.Receipts, usedGas uint64) error {
	header := block.Header()
	if block.GasUsed() != usedGas {
		return fmt.Errorf("invalid gas used (remote: %d local: %d)", block.GasUsed(), usedGas)
	}
	// Validate the received block's bloom with the one derived from the generated receipts.
	// For valid blocks this should always validate to true.
	rbloom := types.CreateBloom(receipts)
	if rbloom != header.Bloom {
		return fmt.Errorf("invalid bloom (remote: %x  local: %x)", header.Bloom, rbloom)
	}
	// Tre receipt Trie's root (R = (Tr [[H1, R1], ... [Hn, R1]]))
	receiptSha := types.DeriveSha(receipts)
	if receiptSha != header.ReceiptHash {
		return fmt.Errorf("invalid receipt root hash (remote: %x local: %x)", header.ReceiptHash, receiptSha)
	}
	// Validate the state root against the received state root and throw
	// an error if they don't match.
	if root := statedb.IntermediateRoot(v.config.IsEIP158(header.Number)); header.Root != root {
		return fmt.Errorf("invalid merkle root (remote: %x local: %x)", header.Root, root)
	}
	return nil
}

// CalcGasLimit computes the gas limit of the next block after parent.
// This is miner strategy, not consensus protocol.
func CalcGasLimit(parent *types.Block) uint64 {
	// contrib = (parentGasUsed * 3 / 2) / 1024
	contrib := (parent.GasUsed() + parent.GasUsed()/2) / params.GasLimitBoundDivisor

	// decay = parentGasLimit / 1024 -1
	decay := parent.GasLimit()/params.GasLimitBoundDivisor - 1

	/*
		strategy: gasLimit of block-to-mine is set based on parent's
		gasUsed value.  if parentGasUsed > parentGasLimit * (2/3) then we
		increase it, otherwise lower it (or leave it unchanged if it's right
		at that usage) the amount increased/decreased depends on how far away
		from parentGasLimit * (2/3) parentGasUsed is.
	*/
	limit := parent.GasLimit() - decay + contrib
	if limit < params.MinGasLimit {
		limit = params.MinGasLimit
	}
	// however, if we're now below the target (TargetGasLimit) we increase the
	// limit as much as we can (parentGasLimit / 1024 -1)
	if limit < params.TargetGasLimit {
		limit = parent.GasLimit() + decay
		if limit > params.TargetGasLimit {
			limit = params.TargetGasLimit
		}
	}
	return limit
}
