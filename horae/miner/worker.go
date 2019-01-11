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

package miner

import (
	"bytes"
	"fmt"
	"github.com/hashicorp/golang-lru"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"crypto/ecdsa"
	"github.com/deckarep/golang-set"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/horae/consensus"
	"github.com/ethereum/go-ethereum/horae/horaetypes"
	"github.com/ethereum/go-ethereum/horae/random-beacon/bls"
	"github.com/ethereum/go-ethereum/horae/replica"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/pkg/errors"
)

const (
	resultQueueSize  = 10
	miningLogAtDepth = 5

	// txChanSize is the size of channel listening to NewTxsEvent.
	// The number is referenced from the size of tx pool.
	txChanSize = 4096
	// chainHeadChanSize is the size of channel listening to ChainHeadEvent.
	chainHeadChanSize = 10

	dfinityEventChanSize = 4096

	signatureChanSize = 5

	blockTime = 2 * time.Second

	epochLength = 16
)

// Agent can register themselves with the worker
type Agent interface {
	AssignTask(*Package)
	DeliverTo(chan<- *Package)
	Start()
	Stop()
}

// Env is the workers current environment and holds all of the current state information.
type Env struct {
	config *params.ChainConfig
	signer types.Signer

	state     *state.StateDB // apply state changes here
	ancestors mapset.Set     // ancestor set (used for checking uncle parent validity)
	family    mapset.Set     // family set (used for checking uncle invalidity)
	uncles    mapset.Set     // uncle set
	tcount    int            // tx count in cycle
	gasPool   *core.GasPool  // available gas used to pack transactions

	header   *types.Header
	txs      []*types.Transaction
	receipts []*types.Receipt

	createdAt time.Time
}

// Package contains all information for consensus engine sealing and result submitting.
type Package struct {
	Receipts []*types.Receipt
	State    *state.StateDB
	Block    *types.Block
}

// worker is the main object which takes care of applying messages to the new state
type worker struct {
	config *params.ChainConfig
	engine consensus.Engine

	mu sync.Mutex

	// update loop
	mux          *event.TypeMux
	txsCh        chan core.NewTxsEvent
	txsSub       event.Subscription
	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription

	dfinityCh  chan interface{}
	dfinitySub event.Subscription
	sigCh      chan *replica.SigResponse

	rbCh  chan interface{}
	rbSub event.Subscription

	agents map[Agent]struct{}
	recv   chan *Package

	eth           Backend
	chain         *core.BlockChain
	proc          core.Validator
	chainDb       ethdb.Database
	replicaDb     ethdb.Database
	isBootReplica bool
	height        uint64

	coinbase common.Address
	extra    []byte

	currentMu sync.Mutex
	current   *Env

	snapshotMu    sync.RWMutex
	snapshotBlock *types.Block
	snapshotState *state.StateDB

	// atomic status counters
	running int32 // The indicator whether the consensus engine is running or not.

	priKey *ecdsa.PrivateKey
	rand   bls.Rand
	secKey bls.Seckey
	pubKey bls.Pubkey
	nonce  uint64

	groupShareChannel map[uint64]map[common.Address]chan *replica.GroupShareEvent
	groupShareMu      sync.Mutex
	groupShareMap     map[common.Address]map[common.Address]*bls.Seckey
	secKeyCache       *lru.Cache

	groupThreshold  int
	lastMinedHeight uint64
	signingHeight   uint64
}

func newWorker(config *params.ChainConfig, engine consensus.Engine, eth Backend, mux *event.TypeMux) *worker {
	worker := &worker{
		config:            config,
		engine:            engine,
		eth:               eth,
		mux:               mux,
		txsCh:             make(chan core.NewTxsEvent, txChanSize),
		chainHeadCh:       make(chan core.ChainHeadEvent, chainHeadChanSize),
		dfinityCh:         make(chan interface{}, dfinityEventChanSize),
		rbCh:              make(chan interface{}, dfinityEventChanSize),
		sigCh:             make(chan *replica.SigResponse, signatureChanSize),
		chainDb:           eth.ChainDb(),
		replicaDb:         eth.ReplicaDb(),
		isBootReplica:     false,
		recv:              make(chan *Package, resultQueueSize),
		chain:             eth.BlockChain(),
		proc:              eth.BlockChain().Validator(),
		agents:            make(map[Agent]struct{}),
		groupShareChannel: make(map[uint64]map[common.Address]chan *replica.GroupShareEvent),
		groupShareMap:     make(map[common.Address]map[common.Address]*bls.Seckey),
		//TODO: horae - restore group info from replicaDB
		height:         0,
		groupThreshold: int(config.GroupThreshold.Uint64()),
	}
	// Subscribe NewTxsEvent for tx pool
	worker.txsSub = eth.TxPool().SubscribeNewTxsEvent(worker.txsCh)
	// Subscribe events for blockchain
	worker.chainHeadSub = eth.BlockChain().SubscribeChainHeadEvent(worker.chainHeadCh)
	worker.dfinitySub = eth.BlockChain().SubscribeDfinityEvent(worker.dfinityCh)
	worker.rbSub = eth.BlockChain().SubscribeRbEvent(worker.rbCh)
	if groupSeckeyCache, err := lru.New(int(config.FixedGroupNum.Uint64())); err == nil {
		worker.secKeyCache = groupSeckeyCache
	}
	go worker.update()
	go worker.dfinityEventLoop()
	worker.chain.PostRbEvent(worker.chain.GetCurrentRandomBeacon())

	go worker.wait()
	//worker.commitNewWork(nil)

	return worker
}

func (self *worker) setEtherbase(addr common.Address) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.coinbase = addr
	self.chain.SetCoinbase(addr)
	self.isBootReplica = replica.ReadBootReplica(self.replicaDb).Address == addr
}

func (self *worker) setKey(priKey *ecdsa.PrivateKey) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.priKey = priKey
	rand := bls.RandFromBytes(crypto.FromECDSA(priKey))
	self.rand = rand
	self.secKey = bls.SeckeyFromRand(rand)
	self.pubKey = bls.PubkeyFromSeckey(self.secKey)
}

func (self *worker) setExtra(extra []byte) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.extra = extra
}

func (self *worker) pending() (*types.Block, *state.StateDB) {
	// return a snapshot to avoid contention on currentMu mutex
	self.snapshotMu.RLock()
	defer self.snapshotMu.RUnlock()
	return self.snapshotBlock, self.snapshotState.Copy()
}

func (self *worker) pendingBlock() *types.Block {
	// return a snapshot to avoid contention on currentMu mutex
	self.snapshotMu.RLock()
	defer self.snapshotMu.RUnlock()
	return self.snapshotBlock
}

func (self *worker) start() {
	self.mu.Lock()
	defer self.mu.Unlock()
	atomic.StoreInt32(&self.running, 1)
	for agent := range self.agents {
		agent.Start()
	}
	go self.handleNewRb(self.eth.BlockChain().GetCurrentRandomBeacon())
	go self.rbEventLoop()
}

func (self *worker) stop() {
	self.mu.Lock()
	defer self.mu.Unlock()

	atomic.StoreInt32(&self.running, 0)
	for agent := range self.agents {
		agent.Stop()
	}
}

func (self *worker) isRunning() bool {
	return atomic.LoadInt32(&self.running) == 1
}

func (self *worker) register(agent Agent) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.agents[agent] = struct{}{}
	agent.DeliverTo(self.recv)
	if self.isRunning() {
		agent.Start()
	}
}

func (self *worker) unregister(agent Agent) {
	self.mu.Lock()
	defer self.mu.Unlock()
	delete(self.agents, agent)
	agent.Stop()
}

func (self *worker) update() {
	defer self.txsSub.Unsubscribe()
	defer self.chainHeadSub.Unsubscribe()

	for {
		// A real event arrived, process interesting content
		select {
		// Handle ChainHeadEvent
		case <-self.chainHeadCh:
			//self.commitNewWork(chainHeadEvent.Block)
			return
		case <-self.txsSub.Err():
			return
		case <-self.chainHeadSub.Err():
			return
		}
	}
}

func (self *worker) dfinityEventLoop() {
	defer self.dfinitySub.Unsubscribe()

	for {
		// A real event arrived, process interesting content
		select {

		case ev := <-self.dfinityCh:
			switch dfinityEvent := ev.(type) {
			case replica.SigRequest:
				log.Info("[HORAE EVENT] Try to process signature request", "groupId", dfinityEvent.GroupIndex, "blockNum", dfinityEvent.BlockNumber, "requestFrom", dfinityEvent.RequestFrom.Hex())
				if dfinityEvent.Signer == self.coinbase {
					self.Sign(&dfinityEvent)
				}
			case replica.SigResponse:
				log.Info("[HORAE EVENT] Try to process signature response", "signer", dfinityEvent.Signer.Hex(), "blockNum", dfinityEvent.BlockNumber)
				if dfinityEvent.RequestFrom == self.coinbase {
					self.sigCh <- &dfinityEvent
				}
			case replica.BuildGroupEvent:
				groupInfo := dfinityEvent.Body
				log.Info("[HORAE EVENT] Try to process build group event", "groupIndex", groupInfo.Index, "memSize", len(groupInfo.Members), "groupAddress", groupInfo.Address)
				go func() {
					//Send out
					groupSeed := self.rand.DerivedRand(groupInfo.Address.Bytes())
					msec := make([]bls.Seckey, self.groupThreshold)
					for i := 0; i < self.groupThreshold; i++ {
						msec[i] = bls.SeckeyFromRand(groupSeed.Deri(i))
					}
					self.groupShareMu.Lock()
					defer self.groupShareMu.Unlock()
					if self.groupShareChannel[groupInfo.Epoch] == nil {
						delete(self.groupShareChannel, groupInfo.Epoch-1)
						self.groupShareChannel[groupInfo.Epoch] = make(map[common.Address]chan *replica.GroupShareEvent)
					}
					if self.groupShareChannel[groupInfo.Epoch][groupInfo.Address] == nil {
						self.groupShareChannel[groupInfo.Epoch][groupInfo.Address] = make(chan *replica.GroupShareEvent, self.config.GroupSize.Uint64())
					}
					sharePubKeyList := make([][]byte, 0)
					for _, v := range groupInfo.Members {
						shareSec := bls.ShareSeckeyByAddr(msec, v)
						shareVerify := bls.PubkeyFromSeckey(shareSec)
						shareEvent := &replica.GroupShareEvent{
							From:   self.coinbase,
							To:     v,
							Group:  groupInfo.Address,
							Key:    shareSec.Bytes(),
							Verify: shareVerify.Bytes(),
							Epoch:  groupInfo.Epoch,
						}
						sharePubKeyList = append(sharePubKeyList, shareVerify.Bytes())
						if v.String() == self.coinbase.String() {
							self.groupShareChannel[groupInfo.Epoch][groupInfo.Address] <- shareEvent
							continue
						}
						self.chain.PostDfinityEvent(replica.DfinityEvent{Data: shareEvent, To: shareEvent.To, From: self.coinbase, Typ: 38})
					}

					go func() {

						time.Sleep(epochLength >> 2 * time.Duration(self.config.BlockTime.Int64()) * time.Second)

						self.groupShareMu.Lock()
						defer self.groupShareMu.Unlock()

						//Build transaction
						shareEvents := make(map[common.Address]*replica.GroupShareEvent)
						invalidPeers := make([]common.Address, 0, self.config.GroupSize.Uint64())

						shareLength := len(self.groupShareChannel[groupInfo.Epoch][groupInfo.Address])
						for i := 0; i < shareLength; i++ {
							shareEvent := <-self.groupShareChannel[groupInfo.Epoch][groupInfo.Address]
							shareEvents[shareEvent.From] = shareEvent
						}
						close(self.groupShareChannel[groupInfo.Epoch][groupInfo.Address])
						delete(self.groupShareChannel[groupInfo.Epoch], groupInfo.Address)

						self.groupShareMap[groupInfo.Address] = make(map[common.Address]*bls.Seckey)
						secMap := self.groupShareMap[groupInfo.Address]
						pubList := make([][]byte, 0)
						sigList := make([][]byte, 0)
						for _, v := range groupInfo.Members {
							var secKey bls.Seckey
							var signature bls.Signature
							if shareEvents[v] != nil && len(shareEvents[v].Key) > 0 {
								secKey = bls.BytesToSeckey(shareEvents[v].Key)
								signature = bls.Sign(secKey, []byte("HORAE"))
							}
							if shareEvents[v] == nil || len(shareEvents[v].Key) == 0 || len(shareEvents[v].Verify) == 0 || !bls.VerifySignature(shareEvents[v].Verify, []byte("HORAE"), signature.Bytes()) {
								pubList = append(pubList, nil)
								invalidPeers = append(invalidPeers, v)
								sigList = append(sigList, nil)
								continue
							}

							//Store sec keys
							secMap[v] = &secKey
							pubList = append(pubList, shareEvents[v].Verify)
							sigList = append(sigList, signature.Bytes())
						}

						self.addTx(replica.GroupShareReport{
							Group:           groupInfo.Address,
							GroupIndex:      groupInfo.Index,
							From:            self.coinbase,
							InvalidMembers:  invalidPeers,
							ReceivedPubKeys: pubList,
							SharePubKeys:    sharePubKeyList,
							Signatures:      sigList,
							GroupPubKey:     bls.PubkeyFromSeckey(msec[0]).Bytes(),
							Epoch:           groupInfo.Epoch,
						}, types.TRANSACTION_TYPE_GROUP_SHARE_REPORT)
						log.Info(fmt.Sprint("Invalid nodes...", invalidPeers, groupInfo.Index))
					}()
				}()
			case replica.GroupShareEvent:
				log.Info("[HORAE EVENT] Try to process group share event.", "from", dfinityEvent.From.Hex(), "to", dfinityEvent.To.Hex())
				go func() {
					self.groupShareMu.Lock()
					defer self.groupShareMu.Unlock()
					if dfinityEvent.To != self.coinbase {
						return
					}
					if self.groupShareChannel[dfinityEvent.Epoch] == nil {
						self.groupShareChannel[dfinityEvent.Epoch] = make(map[common.Address]chan *replica.GroupShareEvent)
					}
					if self.groupShareChannel[dfinityEvent.Epoch][dfinityEvent.Group] == nil {
						log.Info("Get a group share event, create responding channel first.")
						self.groupShareChannel[dfinityEvent.Epoch][dfinityEvent.Group] = make(chan *replica.GroupShareEvent, self.config.GroupSize.Uint64())
					}
					self.groupShareChannel[dfinityEvent.Epoch][dfinityEvent.Group] <- &dfinityEvent
				}()
			case replica.PreparedGroupEvent:
				log.Info("[HORAE EVENT] Try to process prepared group event", "groupId", dfinityEvent.Body.Index, "groupAddress", dfinityEvent.Body.Address.Hex())
				go func() {
					self.groupShareMu.Lock()
					defer self.groupShareMu.Unlock()
					group := dfinityEvent.Body
					secKeyMap := self.groupShareMap[group.Address]
					secList := make([]bls.Seckey, 0, len(group.Members))
					for _, v := range group.Members {
						if secKey := secKeyMap[v]; secKey != nil {
							secList = append(secList, *secKey)
						}
					}
					delete(self.groupShareMap, group.Address)
					groupSecKey := bls.AggregateSeckeys(secList)
					replica.WriteGroupSecKey(self.replicaDb, group.Address, &groupSecKey, group.Epoch)
					log.Info(fmt.Sprint("Finish build group sec key with members length: ", len(secList)))
				}()
			}
		case <-self.dfinitySub.Err():
			return
		}
	}
}

func (self *worker) addTx(payload interface{}, txType uint64) error {
	state := self.eth.TxPool().State()

	data, rlpErr := rlp.EncodeToBytes(payload)
	if rlpErr != nil {
		return fmt.Errorf("Failed to serialize. ")
	}

	tx := types.NewTx(state.GetNonce(self.coinbase), new(big.Int), 2100000, new(big.Int), data, txType)

	var (
		transaction *types.Transaction
		err         error
	)

	chainId := self.eth.ChainID()
	// Depending on the presence of the chain ID, sign with EIP155 or homestead
	if chainId != nil {
		transaction, err = types.SignTx(tx, types.NewEIP155Signer(chainId), self.priKey)
	} else {
		transaction, err = types.SignTx(tx, types.HomesteadSigner{}, self.priKey)
	}

	if err != nil {
		return fmt.Errorf("failed to sign tx : %v", err)
	}
	return self.eth.TxPool().AddLocal(transaction)
}

func (self *worker) applyEpochTx(parentHash common.Hash, header *types.Header) {
	log.Info("************ APPLY epoch tx *******", "block Num", header.Number)
	replicaNum := header.ReplicaNum

	newReplicas := self.chain.GetUnconfirmedReplica(parentHash)
	replicaMap := make(map[uint64]*replica.Replica)
	log.Info("Get unconfirmed replica", "size", len(newReplicas))
	for _, r := range newReplicas {
		r.Id = header.ReplicaNum + 1
		err := self.addTx(r, types.TRANSACTION_TYPE_JOINED_NODE)
		if err != nil {
			log.Info("Failed to add joined node transaction", "replicaAddr", r.Address.Hex(), "id", replicaNum)
		} else {
			header.ReplicaNum++
			replicaMap[r.Id] = r
			log.Info("Success to add joined node transaction", "replicaAddr", r.Address.Hex(), "id", replicaNum)
		}
	}
	self.buildGroup(header, replicaNum, replicaMap)
}

func (self *worker) buildGroup(header *types.Header, oldReplicaNum uint64, newReplicas map[uint64]*replica.Replica) {

	groups := self.chain.GenerateGroupsInfo(header, oldReplicaNum, newReplicas)
	for _, group := range groups {
		err := self.addTx(group, types.TRANSACTION_TYPE_BUILD_GROUP)
		if err != nil {
			log.Info(fmt.Sprint("Failed to add group tx: ", err.Error()))
		} else {
			log.Info("Success add group build tx", "num", header.Number.Uint64(), "groupIndex", group.Index)
		}
	}

	pendingShareReport := self.chain.GetUnconfirmedGroupShareReport(header.ParentHash)
	if len(pendingShareReport) == 0 {
		return
	}
	for index, reports := range pendingShareReport {
		groupResult := self.chain.BuildGroup(index, header.Epoch-1, reports)
		if groupResult == nil {
			continue
		}
		err := self.addTx(groupResult, types.TRANSACTION_TYPE_PREPARED_GROUP)
		if err != nil {
			log.Info(fmt.Sprint("Failed to add group tx: ", err.Error()))
		}
	}
}

func (self *worker) Sign(request *replica.SigRequest) {
	var err error
	var signature []byte
	r := replica.ReadReplicaFromAddress(self.replicaDb, request.RequestFrom)
	if r == nil || !bls.VerifySignature(r.PublicKey, request.Context.Bytes(), request.Identify) {
		err = errors.New("unknown replica where the request is from")
	} else {
		signature, err = self.sign(request)
	}
	response := &replica.SigResponse{
		Signer:      self.coinbase,
		RequestFrom: request.RequestFrom,
		Signature:   signature,
		BlockNumber: request.BlockNumber,
		Context:     request.Context,
		Epoch:       request.Epoch,
	}
	if err != nil {
		response.ErrMsg = []byte(err.Error())
	}
	self.chain.PostDfinityEvent(replica.DfinityEvent{Data: response, To: request.RequestFrom, From: self.coinbase, Typ: 37})
}

func (self *worker) sign(request *replica.SigRequest) ([]byte, error) {
	if request.BlockNumber != self.eth.BlockChain().GetCurrentRandomBeacon().Round {
		return nil, errors.New("Out dated block, give up notarization.")
	}
	if request.GroupIndex == 0 {
		return bls.Sign(self.secKey, request.Context.Bytes()).Bytes(), nil
	}
	group := replica.ReadGroupWithIndex(self.replicaDb, request.GroupIndex, request.Epoch)
	if group == nil {
		return nil, errors.New("unknown group index")
	}
	return self.signWithGroupSeckey(request.Context.Bytes(), group.Address, group.Epoch)
}

func (self *worker) Notary(header *types.Header) ([]byte, error) {
	lastBeacon := horaetypes.RandomBeacon{
		Round: header.Number.Uint64() - 1,
		Data:  replica.ReadRandomBeacon(self.replicaDb, header.Number.Uint64()-1),
	}
	group := self.chain.GetGroupWithBeacon(lastBeacon)
	if header.Number.Uint64() != self.eth.BlockChain().GetCurrentRandomBeacon().Round {
		return nil, errors.New("Out dated block, give up notarization.")
	}
	if group == nil {
		return self.NotaryByBootReplica(header)
	}
	return self.NotaryByGroup(header, group)
}

func (self *worker) NotaryByGroup(header *types.Header, group *replica.Group) ([]byte, error) {
	sigMap := make(map[common.Address]bls.Signature, self.groupThreshold)
	for _, member := range group.Members {
		if member == self.coinbase {
			signature, err := self.signWithGroupSeckey(header.HashNoSignature().Bytes(), group.Address, group.Epoch)
			if err == nil {
				sigMap[member] = bls.SignatureFromBytes(signature)
				continue
			}
			log.Error("failed to sign with my group seckey", "err", err.Error(), "blockNum", header.Number.Uint64())
		} else {
			request := &replica.SigRequest{
				Context:     header.HashNoSignature(),
				Signer:      member,
				RequestFrom: self.coinbase,
				BlockNumber: header.Number.Uint64(),
				GroupIndex:  group.Index,
				Identify:    bls.Sign(self.secKey, header.HashNoSignature().Bytes()).Bytes(),
				Epoch:       group.Epoch,
			}
			self.chain.PostDfinityEvent(replica.DfinityEvent{Data: request, To: member, From: self.coinbase, Typ: 36})
		}
	}

	start := header.Time.Int64()
	limit := time.Duration(self.config.BlockTime.Int64()) * time.Second >> 1
loop:
	for {
		rest := start + limit.Nanoseconds() - time.Now().UnixNano()
		if rest <= 0 {
			break
		}
		select {
		case response := <-self.sigCh:
			if response.Context == header.HashNoSignature() {
				if len(response.ErrMsg) > 0 {
					log.Info("Failed to get signature from signer", "signer", response.Signer.Hex(), "err", string(response.ErrMsg))
					continue
				}
				pubKey := replica.ReadGroupPubKey(self.replicaDb, response.Signer, group.Index, group.Epoch)
				if pubKey != nil && bls.VerifySignature(pubKey.Bytes(), header.HashNoSignature().Bytes(), response.Signature) {
					sigMap[response.Signer] = bls.SignatureFromBytes(response.Signature)
				} else {
					log.Info("received invalid signature", "signer", response.Signer.Hex())
				}
			}
			if len(sigMap) >= self.groupThreshold {
				break loop
			}
		case <-time.After(time.Duration(rest)):
			break loop
		}
	}
	if len(sigMap) < self.groupThreshold {
		return nil, errors.New("failed to get enough signature")
	}
	groupSig := bls.RecoverSignatureByMap(sigMap, self.groupThreshold)
	return groupSig.Bytes(), nil
}

func (self *worker) NotaryByBootReplica(header *types.Header) ([]byte, error) {
	if self.isBootReplica {
		return bls.Sign(self.secKey, header.HashNoSignature().Bytes()).Bytes(), nil
	}
	if signer := replica.ReadReplicaFromAddress(self.replicaDb, replica.ReadBootReplica(self.replicaDb).Address); signer != nil {
		request := &replica.SigRequest{
			Context:     header.HashNoSignature(),
			Signer:      signer.Address,
			RequestFrom: self.coinbase,
			BlockNumber: header.Number.Uint64(),
			GroupIndex:  0,
			Identify:    bls.Sign(self.secKey, header.HashNoSignature().Bytes()).Bytes(),
			Epoch:       1,
		}
		self.chain.PostDfinityEvent(replica.DfinityEvent{Data: request, To: signer.Address, From: self.coinbase, Typ: 36})
		for {
			select {
			case response := <-self.sigCh:
				if response.Context == request.Context {
					if len(response.ErrMsg) > 0 {
						log.Info("Failed to get signature from signer", "signer", response.Signer.Hex(), "err", string(response.ErrMsg))
						return nil, errors.New(string(response.ErrMsg))
					}
					return response.Signature, nil
				}
			case <-time.After(time.Duration(self.config.BlockTime.Int64()) * time.Second >> 1):
				return nil, errors.New("time out for signature")
			}
		}
	}
	return nil, errors.New("Failed to get boot replica.")
}

func (self *worker) wait() {
	for {
		for result := range self.recv {

			if result == nil {
				continue
			}
			block := result.Block
			if block.NumberU64() <= self.lastMinedHeight {
				continue
			}
			if signature, err := self.Notary(block.Header()); err != nil {
				log.Error("Failed to notary.", "blockNumber", block.NumberU64(), "err", err.Error())
				continue
			} else {
				block.WithSignature(signature)
			}

			// Update the block hash in all logs since it is now available and not when the
			// receipt/log of individual transactions were created.
			for _, r := range result.Receipts {
				for _, l := range r.Logs {
					l.BlockHash = block.Hash()
				}
			}
			for _, log := range result.State.Logs() {
				log.BlockHash = block.Hash()
			}

			self.currentMu.Lock()
			stat, err := self.chain.WriteBlockWithState(block, result.Receipts, result.State)
			self.lastMinedHeight = block.NumberU64()
			self.currentMu.Unlock()
			if err != nil {
				log.Error("Failed writing block to chain", "err", err)
				continue
			}
			log.Info("🔨 mined potential block", "number", block.NumberU64(), "hash", block.Hash(), "td", block.Difficulty(), "rating", block.Rating())
			// Broadcast the block and announce chain insertion event
			self.mux.Post(core.NewMinedBlockEvent{Block: block})
			var (
				events []interface{}
				logs   = result.State.Logs()
			)
			events = append(events, core.ChainEvent{Block: block, Hash: block.Hash(), Logs: logs})
			if stat == core.CanonStatTy {
				events = append(events, core.ChainHeadEvent{Block: block})
			}
			self.chain.PostChainEvents(events, logs)

			// Insert the block into the set of pending ones to wait for confirmations
		}
	}
}

func (self *worker) signWithGroupSeckey(content []byte, groupAddr common.Address, epoch uint64) ([]byte, error) {
	secKey := self.getMyGroupSeckey(groupAddr, epoch)
	if secKey != nil {
		return bls.Sign(*secKey, content).Bytes(), nil
	}
	return nil, errors.New("failed to get group seckey from group address.")
}

func (self *worker) getMyGroupSeckey(groupAddr common.Address, epoch uint64) *bls.Seckey {
	if data, bl := self.secKeyCache.Get(fmt.Sprintf("%v%v", epoch, groupAddr.Hex())); bl {
		return data.(*bls.Seckey)
	}
	secKey := replica.ReadGroupSecKey(self.replicaDb, groupAddr, epoch)
	if secKey != nil {
		self.secKeyCache.Add(fmt.Sprintf("%v%v", epoch, groupAddr.Hex()), secKey)
	}
	return secKey
}

func (self *worker) VerifySignature(header *types.Header) error {
	if header.GroupSignature == nil {
		return dfinity.ErrEmptySignature
	}
	beacon := horaetypes.RandomBeacon{
		Round: header.Number.Uint64() - 1,
		Data:  replica.ReadRandomBeacon(self.replicaDb, header.Number.Uint64()-1),
	}
	group := self.chain.GetGroupWithBeacon(beacon)
	var publicKey []byte
	if group != nil {
		publicKey = group.GroupPubKey
	} else {
		if bootreplica := replica.ReadBootReplica(self.replicaDb); bootreplica != nil {
			publicKey = bootreplica.PublicKey
		}
	}
	if publicKey == nil {
		return dfinity.ErrNoPubKey
	}
	if !bls.VerifySignature(publicKey, header.HashNoSignature().Bytes(), header.GroupSignature) {
		return dfinity.ErrInvalidSignature
	}
	return nil
}

// push sends a new work task to currently live miner agents.
func (self *worker) push(p *Package) {
	for agent := range self.agents {
		agent.AssignTask(p)
	}
}

// makeCurrent creates a new environment for the current cycle.
func (self *worker) makeCurrent(parent *types.Block, header *types.Header) error {
	state, err := self.chain.StateAt(parent.Root())
	if err != nil {
		return err
	}
	env := &Env{
		config:    self.config,
		signer:    types.NewEIP155Signer(self.config.ChainID),
		state:     state,
		ancestors: mapset.NewSet(),
		family:    mapset.NewSet(),
		uncles:    mapset.NewSet(),
		header:    header,
		createdAt: time.Now(),
	}

	// when 08 is processed ancestors contain 07 (quick block)
	for _, ancestor := range self.chain.GetBlocksFromHash(parent.Hash(), 7) {
		for _, uncle := range ancestor.Uncles() {
			env.family.Add(uncle.Hash())
		}
		env.family.Add(ancestor.Hash())
		env.ancestors.Add(ancestor.Hash())
	}

	// Keep track of transactions which return errors so they can be removed
	env.tcount = 0
	self.current = env
	return nil
}

func (self *worker) commitNewWork(parent *types.Block) {
	tstart := time.Now()
	if parent == nil {
		return
	}

	rating := self.eth.BlockChain().CalcReplicaRating(parent.NumberU64()+1, self.coinbase)

	if rating == 0 {
		log.Info("It's not my turn....", "number", parent.NumberU64()+1, "parentNum", parent.NumberU64(), "parentHash", parent.Hash().Hex())
		return
	}

	self.mu.Lock()
	defer self.mu.Unlock()
	self.currentMu.Lock()
	defer self.currentMu.Unlock()

	log.Info("Assign mining task ", "num", parent.NumberU64()+1, "account", self.coinbase.Hex(), "parent", parent.Hash(), "replicaNum", parent.ReplicaNum())

	tstamp := tstart.UnixNano()
	if parent.Time().Cmp(new(big.Int).SetInt64(tstamp)) >= 0 {
		tstamp = parent.Time().Int64() + 1
	}

	num := parent.Number()

	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     num.Add(num, common.Big1),
		GasLimit:   core.CalcGasLimit(parent),
		Extra:      self.extra,
		Time:       big.NewInt(tstamp),
		ReplicaNum: parent.ReplicaNum(),
		Rating:     rating,
		Epoch:      parent.Epoch(),
	}

	if self.chain.CheckEpoch(header.Number.Uint64()) {
		header.Epoch++
		self.applyEpochTx(parent.Hash(), header)
	}

	// Only set the coinbase if our consensus engine is running (avoid spurious block rewards)
	if self.isRunning() {
		if self.coinbase == (common.Address{}) {
			log.Error("Refusing to mine withouterbase")
			return
		}
		header.Coinbase = self.coinbase
	}
	if err := self.engine.Prepare(self.chain, header); err != nil {
		log.Error("Failed to prepare header for mining", "err", err)
		return
	}
	// If we are care about TheDAO hard-fork check whether to override the extra-data or not
	if daoBlock := self.config.DAOForkBlock; daoBlock != nil {
		// Check whether the block is among the fork extra-override range
		limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
		if header.Number.Cmp(daoBlock) >= 0 && header.Number.Cmp(limit) < 0 {
			// Depending whether we support or oppose the fork, override differently
			if self.config.DAOForkSupport {
				header.Extra = common.CopyBytes(params.DAOForkBlockExtra)
			} else if bytes.Equal(header.Extra, params.DAOForkBlockExtra) {
				header.Extra = []byte{} // If miner opposes, don't let it use the reserved extra-data
			}
		}
	}
	// Could potentially happen if starting to mine in an odd state.
	err := self.makeCurrent(parent, header)
	if err != nil {
		log.Error("Failed to create mining context", "err", err)
		return
	}
	// Create the current work task and check any fork transitions needed
	env := self.current
	if self.config.DAOForkSupport && self.config.DAOForkBlock != nil && self.config.DAOForkBlock.Cmp(header.Number) == 0 {
		misc.ApplyDAOHardFork(env.state)
	}

	var (
		fullBlock *types.Block
	)

	// Fill the block with all available pending transactions.
	pending, err := self.eth.TxPool().Pending()
	if err != nil {
		log.Error("Failed to fetch pending transactions", "err", err)
		return
	}
	txs := types.NewTransactionsByPriceAndNonce(self.current.signer, pending)
	env.commitTransactions(self.mux, txs, self.chain, self.coinbase)

	// Create the full block to seal with the consensus engine
	if fullBlock, err = self.engine.Finalize(self.chain, header, env.state, env.txs, nil, env.receipts); err != nil {
		log.Error("Failed to finalize block for sealing", "err", err)
		return
	}
	// We only care about logging if we're actually mining.
	if self.isRunning() {
		log.Info("Commit new full mining work", "number", fullBlock.Number(), "txs", env.tcount, "uncles", 0, "elapsed", common.PrettyDuration(time.Since(tstart)))
		self.push(&Package{env.receipts, env.state, fullBlock})
	}
	self.updateSnapshot()
	self.height = header.Number.Uint64()
}

func (self *worker) updateSnapshot() {
	self.snapshotMu.Lock()
	defer self.snapshotMu.Unlock()

	var uncles []*types.Header

	self.snapshotBlock = types.NewBlock(
		self.current.header,
		self.current.txs,
		uncles,
		self.current.receipts,
	)
	self.snapshotState = self.current.state.Copy()
}

func (env *Env) commitTransactions(mux *event.TypeMux, txs *types.TransactionsByPriceAndNonce, bc *core.BlockChain, coinbase common.Address) {
	if env.gasPool == nil {
		env.gasPool = new(core.GasPool).AddGas(env.header.GasLimit)
	}

	var coalescedLogs []*types.Log

	start := time.Now()
	limit := time.Duration(env.config.BlockTime.Int64()) * time.Second
	txsLimit := int(env.config.TxsLimit.Int64())
	for {
		// Retrieve the next transaction and abort if all done
		tx := txs.Peek()
		if tx == nil || (time.Since(start)) > limit || env.tcount > txsLimit {
			break
		}
		// Error may be ignored here. The error has already been checked
		// during transaction acceptance is the transaction pool.
		//
		// We use the eip155 signer regardless of the current hf.
		from, _ := types.Sender(env.signer, tx)
		// Check whether the tx is replay protected. If we're not in the EIP155 hf
		// phase, start ignoring the sender until we do.
		if tx.Protected() && !env.config.IsEIP155(env.header.Number) {
			log.Trace("Ignoring reply protected transaction", "hash", tx.Hash(), "eip155", env.config.EIP155Block)

			txs.Pop()
			continue
		}
		// Start executing the transaction
		env.state.Prepare(tx.Hash(), common.Hash{}, env.tcount)

		err, logs := env.commitTransaction(tx, bc, coinbase, env.gasPool)
		switch err {
		case core.ErrGasLimitReached:
			// Pop the current out-of-gas transaction without shifting in the next from the account
			log.Trace("Gas limit exceeded for current block", "sender", from)
			txs.Pop()

		case core.ErrNonceTooLow:
			// New head notification data race between the transaction pool and miner, shift
			log.Trace("Skipping transaction with low nonce", "sender", from, "nonce", tx.Nonce())
			txs.Shift()

		case core.ErrNonceTooHigh:
			// Reorg notification data race between the transaction pool and miner, skip account =
			log.Trace("Skipping account with hight nonce", "sender", from, "nonce", tx.Nonce())
			txs.Pop()

		case nil:
			// Everything ok, collect the logs and shift in the next transaction from the same account
			coalescedLogs = append(coalescedLogs, logs...)
			env.tcount++
			txs.Shift()

		default:
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			log.Debug("Transaction failed, account skipped", "hash", tx.Hash(), "err", err)
			txs.Shift()
		}
	}

	if len(coalescedLogs) > 0 || env.tcount > 0 {
		// make a copy, the state caches the logs and these logs get "upgraded" from pending to mined
		// logs by filling in the block hash when the block was mined by the local miner. This can
		// cause a race condition if a log was "upgraded" before the PendingLogsEvent is processed.
		cpy := make([]*types.Log, len(coalescedLogs))
		for i, l := range coalescedLogs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		go func(logs []*types.Log, tcount int) {
			if len(logs) > 0 {
				mux.Post(core.PendingLogsEvent{Logs: logs})
			}
			if tcount > 0 {
				mux.Post(core.PendingStateEvent{})
			}
		}(cpy, env.tcount)
	}
}

func (env *Env) commitTransaction(tx *types.Transaction, bc *core.BlockChain, coinbase common.Address, gp *core.GasPool) (error, []*types.Log) {
	snap := env.state.Snapshot()

	receipt, _, err := core.ApplyTransaction(env.config, bc, &coinbase, gp, env.state, env.header, tx, &env.header.GasUsed, vm.Config{})
	if err != nil {
		env.state.RevertToSnapshot(snap)
		return err, nil
	}
	env.txs = append(env.txs, tx)
	env.receipts = append(env.receipts, receipt)

	return nil, receipt.Logs
}

func (self *worker) rbEventLoop() {
	defer self.rbSub.Unsubscribe()
	for {
		// A real event arrived, process interesting content
		select {
		// Handle ChainHeadEvent
		case ev := <-self.rbCh:
			switch rbEvent := ev.(type) {
			case horaetypes.RandomBeacon:
				self.handleNewRb(rbEvent)
			}
		case <-self.rbSub.Err():
			return
		}
	}
}
func (self *worker) handleNewRb(beacon horaetypes.RandomBeacon) {

	if beacon.Round < self.chain.GetCurrentRandomBeacon().Round {
		log.Info("Get an out dated beacon, skip it.")
		return
	}

	log.Info("[Horae] Important - Get a new random beacon. ", "number", beacon.Round)

	block := self.chain.GetBlockByNumber(beacon.Round - 1)
	if block != nil && beacon.Round > self.lastMinedHeight {
		go self.commitNewWork(block)
	}

	group := self.eth.BlockChain().GetGroupWithBeacon(beacon)

	if !self.isMyGroup(group) || (beacon.Round <= self.signingHeight && self.signingHeight != 0) {
		return
	}

	self.signingHeight = beacon.Round
	go func() {
		time.Sleep(time.Duration(self.config.BlockTime.Int64()) * time.Second)
		logged := false
		for self.eth.BlockChain().GetBlockByNumber(beacon.Round) == nil {
			if !logged {
				log.Warn("Waiting for next round, no blocks found.", "round", beacon.Round)
				logged = true
			}
			time.Sleep(100 * time.Millisecond)
		}
		log.Info("Block found for current round.", "round", beacon.Round)
		if group == nil {
			sig := bls.Sign(self.secKey, beacon.Data)
			newBeacon := &horaetypes.RandomBeacon{
				Round: beacon.Round + 1,
				Data:  sig.Bytes(),
			}
			self.eth.BlockChain().AddRandomBeacon(newBeacon)
			return
		}

		secKey := self.getMyGroupSeckey(group.Address, group.Epoch)
		if secKey == nil {
			log.Error("Can not get sec key.")
			return
		}
		sig := bls.Sign(*secKey, beacon.Data)
		newPartialBeacon := &horaetypes.PartialRandomBeacon{
			Round: beacon.Round + 1,
			Data:  sig.Bytes(),
			From:  self.coinbase,
		}
		self.eth.BlockChain().AddPartialRandomBeacon(newPartialBeacon)
		return
	}()
}

func (self *worker) isMyGroup(group *replica.Group) bool {
	if group == nil {
		return self.isBootReplica
	}
	for _, address := range group.Members {
		if address == self.coinbase {
			return true
		}
	}
	return false
}
