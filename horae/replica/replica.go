package replica

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/horae/horaetypes"
	"github.com/ethereum/go-ethereum/horae/random-beacon/bls"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"math/big"
)

//go:generate gencodec -type Replica -field-override replicaMarshaling -out gen_replica.go

type Replica struct {
	Id        uint64         `json:"id"`
	Address   common.Address `json:"address"`
	PublicKey []byte         `json:"publicKey"`
	NodeId    string         `json:"nodeId"`
	Enode     string         `json:"enode"`
}

//go:generate gencodec -type Group -out gen_group.go
type Group struct {
	Index       uint64
	Epoch       uint64
	Address     common.Address
	GroupPubKey []byte
	Members     []common.Address
	MemPubKeys  [][]byte
	Failed      bool
}

//go:generate gencodec -type BootReplica -out gen_bootreplica.go
type BootReplica struct {
	Address   common.Address `json:"address"`
	PublicKey string         `json:"publicKey"`
}

type replicaMarshaling struct {
	Id math.HexOrDecimal64
}

func (r *Replica) SetId(id uint64) {
	r.Id = id
}

func (self BootReplica) ToReplica() *Replica {
	publicKey, _ := hex.DecodeString(self.PublicKey)
	return &Replica{
		Address:   self.Address,
		PublicKey: publicKey,
	}
}

// ReadReplica retrieves a replica corresponding to the number.
func ReadReplicaFromAddress(db rawdb.DatabaseReader, account common.Address) *Replica {
	key := replicaAddressKey(account)
	data, _ := db.Get(key)
	if len(data) == 0 {
		return nil
	}
	replica := new(Replica)
	if err := rlp.Decode(bytes.NewReader(data), replica); err != nil {
		log.Error("Invalid replica RLP", "address", account, "err", err)
		return nil
	}
	return replica
}

func ReadConfirmHeight(db rawdb.DatabaseReader) uint64 {
	data, _ := db.Get(confirmHeightPrefix)
	if len(data) == 0 {
		return 0
	}
	height := new(uint64)
	if err := rlp.Decode(bytes.NewReader(data), height); err != nil {
		log.Error("Invalid confirm height", "err", err)
		return 0
	}
	return *height
}

func ReadReplicaFromId(db rawdb.DatabaseReader, id uint64) *Replica {
	data, _ := db.Get(replicaIdKey(id))
	if len(data) == 0 {
		return nil
	}
	replica := new(Replica)
	if err := rlp.Decode(bytes.NewReader(data), replica); err != nil {
		log.Error("Invalid replica RLP", "number", id, "err", err)
		return nil
	}
	return replica
}

func ReadBootReplica(db rawdb.DatabaseReader) *Replica {
	data, _ := db.Get(bootReplicaPrefix)
	if len(data) == 0 {
		return nil
	}
	replica := new(Replica)
	if err := rlp.Decode(bytes.NewReader(data), replica); err != nil {
		log.Error("Invalid replica RLP", "err", err)
		return nil
	}
	return replica
}

func ReadGroupWithIndex(db rawdb.DatabaseReader, index uint64, epoch uint64) *Group {
	data, _ := db.Get(groupIndexKey(index, epoch))
	if len(data) == 0 {
		return nil
	}
	group := new(Group)
	if err := rlp.Decode(bytes.NewReader(data), group); err != nil {
		log.Error("Invalid group RLP", "number", index, "err", err)
		return nil
	}
	return group
}

func ReadGroupInfoWithIndex(db rawdb.DatabaseReader, index uint64, epoch uint64) *Group {
	data, _ := db.Get(groupInfoKey(index, epoch))
	if len(data) == 0 {
		return nil
	}
	groupinfo := new(Group)
	if err := rlp.Decode(bytes.NewReader(data), groupinfo); err != nil {
		log.Error("Invalid group info RLP", "number", index, "err", err)
		return nil
	}
	return groupinfo
}

func DeserializeReplica(data []byte) *Replica {
	if len(data) == 0 {
		return nil
	}
	replica := new(Replica)
	if err := rlp.Decode(bytes.NewReader(data), replica); err != nil {
		log.Error("Invalid replica RLP", "err", err)
		return nil
	}
	return replica
}

func DeserializeGroup(data []byte) *Group {
	if len(data) == 0 {
		return nil
	}
	group := new(Group)
	if err := rlp.Decode(bytes.NewReader(data), group); err != nil {
		log.Error("Invalid group RLP", "err", err)
		return nil
	}
	return group
}

func DeserializeGroupShareReport(data []byte) *GroupShareReport {
	if len(data) == 0 {
		return nil
	}
	report := new(GroupShareReport)
	if err := rlp.Decode(bytes.NewReader(data), report); err != nil {
		log.Error("Invalid group RLP", "err", err)
		return nil
	}
	return report
}

func WriteReplica(db rawdb.DatabaseWriter, replica *Replica) {
	WriteReplicaId(db, replica)
	WriteReplicaAddress(db, replica)
}

// WriteTd stores the total difficulty of a block into the database.
func WriteReplicaAddress(db rawdb.DatabaseWriter, replica *Replica) {
	data, err := rlp.EncodeToBytes(replica)
	if err != nil {
		log.Crit("Failed to RLP encode replica", "err", err)
	}
	key := replicaAddressKey(replica.Address)
	if err := db.Put(key, data); err != nil {
		log.Crit("Failed to store replica", "err", err)
	}
	log.Info("[HORAE VERIFY] Write replica with Address", "address", replica.Address.Hex(), "id", replica.Id)
}

func WriteReplicaId(db rawdb.DatabaseWriter, replica *Replica) {
	data, err := rlp.EncodeToBytes(replica)
	if err != nil {
		log.Crit("Failed to RLP encode replica", "err", err)
	}
	if err := db.Put(replicaIdKey(replica.Id), data); err != nil {
		log.Crit("Failed to store replica", "err", err)
	}
	log.Info("[HORAE VERIFY] Write replica with id", "address", replica.Address.Hex(), "id", replica.Id)
}

func WriteGroupWithId(db rawdb.DatabaseWriter, group *Group) {
	data, err := rlp.EncodeToBytes(group)
	if err != nil {
		log.Crit("Failed to RLP encode group", "err", err)
	}
	if err := db.Put(groupIndexKey(group.Index, group.Epoch), data); err != nil {
		log.Crit("Failed to store group", "err", err)
	}
	log.Info("[HORAE VERIFY] Write group with id", "index", group.Index, "groupAddress", group.Address.Hex(), "epoch", group.Epoch)
}

func WriteGroupInfoWithId(db rawdb.DatabaseWriter, groupinfo *Group) {
	data, err := rlp.EncodeToBytes(groupinfo)
	if err != nil {
		log.Crit("Failed to RLP encode group", "err", err)
	}
	if err := db.Put(groupInfoKey(groupinfo.Index, groupinfo.Epoch), data); err != nil {
		log.Crit("Failed to store group info", "err", err)
	}
	log.Info("[HORAE VERIFY] Write group info with id", "index", groupinfo.Index, "groupAddress", groupinfo.Address.Hex())
}

func WriteConfirmHeight(db rawdb.DatabaseWriter, height uint64) {
	data, err := rlp.EncodeToBytes(height)
	if err != nil {
		log.Crit("Failed to RLP encode height", "err", err)
	}
	if err := db.Put(confirmHeightPrefix, data); err != nil {
		log.Crit("Failed to store confirm height", "err", err)
	}
}

func WriteBootReplica(db rawdb.DatabaseWriter, replica *Replica) {
	data, err := rlp.EncodeToBytes(replica)
	if err != nil {
		log.Crit("Failed to RLP encode boot replica", "err", err)
	}
	if err := db.Put(bootReplicaPrefix, data); err != nil {
		log.Crit("Failed to store boot replica", "err", err)
	}
}

func WriteRandomBeacon(db rawdb.DatabaseWriter, height uint64, data []byte) {
	heightBytes, _ := rlp.EncodeToBytes(height)
	if err := db.Put(randomBeaconKey(heightBytes), data); err != nil {
		log.Crit("Failed to store random beacon", "height", height, "data", data, "err", err)
	}

	//TODO: only update greater height
	if err := db.Put(randomBeaconRound, heightBytes); err != nil {
		log.Crit("Failed to store random beacon", "height", height, "data", data, "err", err)
	}
}

func ReadRandomBeacon(db rawdb.DatabaseReader, height uint64) []byte {
	heightBytes, _ := rlp.EncodeToBytes(height)
	data, _ := db.Get(randomBeaconKey(heightBytes))
	return data
}

func ReadBatchRandomBeacon(db rawdb.DatabaseReader, from uint64, to uint64) []horaetypes.RandomBeacon {
	result := make([]horaetypes.RandomBeacon, 0)
	for i := from + 1; i <= to; i++ {
		result = append(result, horaetypes.RandomBeacon{
			Round: i,
			Data:  ReadRandomBeacon(db, i),
		})
	}
	return result
}

func ReadLatestRandomBeaconRound(db rawdb.DatabaseReader) uint64 {
	data, _ := db.Get(randomBeaconRound)
	if len(data) == 0 {
		return 0
	}
	height := new(uint64)
	rlp.DecodeBytes(data, height)
	return *height
}

func WriteGroupSecKey(db rawdb.DatabaseWriter, group common.Address, seckey *bls.Seckey, epoch uint64) {
	data, err := rlp.EncodeToBytes(seckey.BigInt())
	if err != nil {
		log.Crit("Failed to RLP encode group seckey", "err", err)
	}
	if err := db.Put(groupSecKey(group, epoch), data); err != nil {
		log.Crit("Failed to store group seckey", "err", err)
	}
}

func ReadGroupSecKey(db rawdb.DatabaseReader, group common.Address, epoch uint64) *bls.Seckey {
	data, _ := db.Get(groupSecKey(group, epoch))
	if len(data) == 0 {
		return nil
	}
	bigint := &big.Int{}
	if err := rlp.DecodeBytes(data, bigint); err != nil {
		log.Error("Invalid group seckey RLP", "groupAddr", group.Hex(), "err", err)
		return nil
	}
	seckey := bls.SeckeyFromBigInt(bigint)
	return &seckey
}

func ReadGroupPubKey(db rawdb.DatabaseReader, member common.Address, groupIndex uint64, epoch uint64) *bls.Pubkey {
	group := ReadGroupWithIndex(db, groupIndex, epoch)
	if group == nil {
		return nil
	}
	for i, addr := range group.Members {
		if addr == member {
			key := bls.BytesToPubkey(group.MemPubKeys[i])
			return &key
		}
	}
	return nil
}

var bootReplicaPrefix = []byte("BR")
var confirmHeightPrefix = []byte("CH")
var groupInfoPrefix = []byte("GF")
var groupIndexPrefix = []byte("GI")
var groupSeckeyPrefix = []byte("GS")
var replicaAddressPrefix = []byte("RA")
var randomBeaconPrefix = []byte("RB")
var replicaIdPrefix = []byte("RI")
var randomBeaconRound = []byte("RR")

func replicaAddressKey(address common.Address) []byte {
	return append(replicaAddressPrefix, address.Bytes()...)
}

func replicaIdKey(id uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, id)
	return append(replicaIdPrefix, enc...)
}

func groupIndexKey(id uint64, epoch uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, id)
	epo := make([]byte, 8)
	binary.BigEndian.PutUint64(epo, epoch)
	return append(groupIndexPrefix, append(epo, enc...)...)
}

func groupInfoKey(id uint64, epoch uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, id)
	epo := make([]byte, 8)
	binary.BigEndian.PutUint64(epo, epoch)
	return append(groupInfoPrefix, append(epo, enc...)...)
}

func groupSecKey(group common.Address, epoch uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, epoch)
	return append(groupSeckeyPrefix, append(enc, group.Bytes()...)...)
}

func randomBeaconKey(heightBytes []byte) []byte {
	return append(randomBeaconPrefix, heightBytes...)
}

type BuildGroupEvent struct {
	Body *Group
}

type PreparedGroupEvent struct {
	Body *Group
}

type GroupShareEvent struct {
	Group  common.Address
	From   common.Address
	To     common.Address
	Key    []byte
	Verify []byte
	Epoch  uint64
}

type GroupShareReport struct {
	Group           common.Address
	GroupIndex      uint64
	From            common.Address
	InvalidMembers  []common.Address
	ReceivedPubKeys [][]byte
	SharePubKeys    [][]byte
	Signatures      [][]byte
	GroupPubKey     []byte
	Epoch           uint64
}

type TempPeerRequest struct {
	To    string
	MsgRW chan interface{}
	Err   chan error
}

type SigRequest struct {
	Context     common.Hash
	Signer      common.Address
	RequestFrom common.Address
	BlockNumber uint64
	GroupIndex  uint64
	Identify    []byte
	Epoch       uint64
}

type SigResponse struct {
	Signature   []byte
	Signer      common.Address
	RequestFrom common.Address
	BlockNumber uint64
	Context     common.Hash
	Epoch       uint64
	ErrMsg      []byte
}

type DfinityEvent struct {
	From common.Address
	To   common.Address
	Typ  uint64
	Data interface{}
}

func (r *Replica) Hash() (h common.Hash) {
	return rlpHash(r)
}

func (group *Group) Hash() (h common.Hash) {
	return rlpHash(group)
}

func (r *Replica) HashWithoutId() (h common.Hash) {
	return rlpHash([]interface{}{
		r.Address,
		r.Enode,
		r.NodeId,
		r.PublicKey,
	})
}

func (group *Group) HashWithoutKey() (h common.Hash) {
	return rlpHash([]interface{}{
		group.Index,
		group.Members,
		group.Epoch,
	})
}

func rlpHash(x interface{}) (h common.Hash) {
	hw := sha3.NewKeccak256()
	rlp.Encode(hw, x)
	hw.Sum(h[:0])
	return h
}
