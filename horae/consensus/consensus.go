package dfinity

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"math/big"

	"errors"
	"github.com/ethereum/go-ethereum/horae/horaetypes"
	"github.com/ethereum/go-ethereum/horae/random-beacon/bls"
	"github.com/ethereum/go-ethereum/horae/replica"
	"github.com/ethereum/go-ethereum/log"
	"runtime"
)

var (
	ErrNoPubKey              = errors.New("failed to get public key")
	ErrEmptySignature        = errors.New("empty signature")
	ErrInvalidSignature      = errors.New("invalid signature")
	ErrInvalidSigGroupNumber = errors.New("invalid signature group number")
)

type Dfinity struct {
	//db ethdb.Database
	//bc *core.BlockChain
}

type API struct {
	dfinity *Dfinity // Make sure the mode of ethash is normal.
}

func New() *Dfinity {
	return &Dfinity{
		//db: bc.ReplicaDb(),
		//bc: bc,
	}
}

func (self *Dfinity) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

func (self *Dfinity) VerifyHeader(chain consensus.ChainReader, header *types.Header, seal bool) error {
	parent := chain.GetBlock(header.ParentHash, header.Number.Uint64()-1)

	if parent == nil {
		return consensus.ErrUnknownAncestor
	}

	expectRating := chain.CalcReplicaRating(header.Number.Uint64(), header.Coinbase)
	if expectRating == 0 {
		log.Info("[HORAE VERIFY] invalid replica to seal the block.", "num", header.Number.Uint64(), "hash", header.Hash().Hex(), "from", header.Coinbase.Hex())
		return errors.New("invalid replica to seal the block")
	}
	if expectRating != header.Rating {
		log.Error("Invalid rating", "rating", expectRating, "headerRating", header.Rating)
		return errors.New("invalid rating for the coinbase to seal the block")
	}
	if header.Difficulty.Uint64() != (1 << (header.Rating - 1)) {
		return errors.New("invalid difficulty")
	}
	return self.VerifySignature(chain, header)
}

func (self *Dfinity) VerifySignature(chain consensus.ChainReader, header *types.Header) error {
	if header.GroupSignature == nil {
		return ErrEmptySignature
	}
	beacon := horaetypes.RandomBeacon{
		Round: header.Number.Uint64() - 1,
		Data:  replica.ReadRandomBeacon(chain.ReplicaDb(), header.Number.Uint64()-1),
	}
	group := chain.GetGroupWithBeacon(beacon)
	var publicKey []byte
	if group != nil {
		publicKey = group.GroupPubKey
	} else {
		if bootreplica := replica.ReadBootReplica(chain.ReplicaDb()); bootreplica != nil {
			publicKey = bootreplica.PublicKey
		}
	}
	if publicKey == nil {
		return ErrNoPubKey
	}
	if !bls.VerifySignature(publicKey, header.HashNoSignature().Bytes(), header.GroupSignature) {
		return ErrInvalidSignature
	}
	return nil
}

func (self *Dfinity) VerifyHeaders(chain consensus.ChainReader, headers []*types.Header, seals []bool) (chan<- struct{}, <-chan error) {
	workers := runtime.GOMAXPROCS(0)
	if len(headers) < workers {
		workers = len(headers)
	}

	// Create a task channel and spawn the verifiers
	var (
		inputs = make(chan int)
		done   = make(chan int, workers)
		errors = make([]error, len(headers))
		abort  = make(chan struct{})
	)
	for i := 0; i < workers; i++ {
		go func() {
			for index := range inputs {
				errors[index] = self.verifyHeaderWorker(chain, headers, seals, index)
				done <- index
			}
		}()
	}

	errorsOut := make(chan error, len(headers))
	go func() {
		defer close(inputs)
		var (
			in, out = 0, 0
			checked = make([]bool, len(headers))
			inputs  = inputs
		)
		for {
			select {
			case inputs <- in:
				if in++; in == len(headers) {
					// Reached end of headers. Stop sending to workers.
					inputs = nil
				}
			case index := <-done:
				for checked[index] = true; checked[out]; out++ {
					errorsOut <- errors[out]
					if out == len(headers)-1 {
						return
					}
				}
			case <-abort:
				return
			}
		}
	}()
	return abort, errorsOut
}

func (self *Dfinity) verifyHeaderWorker(chain consensus.ChainReader, headers []*types.Header, seals []bool, index int) error {
	var parent *types.Header
	if index == 0 {
		parent = chain.GetHeader(headers[0].ParentHash, headers[0].Number.Uint64()-1)
	} else if headers[index-1].Hash() == headers[index].ParentHash {
		parent = headers[index-1]
	}
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}
	if chain.GetHeader(headers[index].Hash(), headers[index].Number.Uint64()) != nil {
		return nil // known block
	}
	return nil
}

func (self *Dfinity) VerifyUncles(chain consensus.ChainReader, block *types.Block) error {
	// Always passed for compatibility
	return nil
}

func (self *Dfinity) VerifySeal(chain consensus.ChainReader, header *types.Header) error {
	return nil
}

// Calculate the rating and determine if we need to generate a block.
func (self *Dfinity) Prepare(chain consensus.ChainReader, header *types.Header) error {
	header.Difficulty = big.NewInt(1 << (header.Rating - 1))
	return nil
}

// Calculate rewards and package block.
func (self *Dfinity) Finalize(chain consensus.ChainReader, header *types.Header, state *state.StateDB, txs []*types.Transaction,
	uncles []*types.Header, receipts []*types.Receipt) (*types.Block, error) {
	//Always ignore uncles
	//accumulateRewards(chain.Config(), state, header, uncles)
	header.Root = state.IntermediateRoot(chain.Config().IsEIP158(header.Number))
	// Header seems complete, assemble into a block and return

	return types.NewBlock(header, txs, nil, receipts), nil
}

// Notary and set group signatures.
func (self *Dfinity) Seal(chain consensus.ChainReader, block *types.Block, stop <-chan struct{}) (*types.Block, error) {
	/*if block.Transactions().Len() == 0 {
		return nil, errors.New("Horae - Do not mine for empty block.")
	}*/

	return block, nil

}

func (self *Dfinity) CalcDifficulty(chain consensus.ChainReader, time uint64, parent *types.Header) *big.Int {
	// Always return 1
	return common.Big1
}

func (self *Dfinity) APIs(chain consensus.ChainReader) []rpc.API {
	return []rpc.API{}
}

func (self *Dfinity) Close() error {
	return nil
}
