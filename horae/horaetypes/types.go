package horaetypes

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
)

type RandomBeacon struct {
	Data  []byte
	Round uint64
}

type PartialRandomBeacon struct {
	Round uint64
	Data  []byte
	From  common.Address
}

type StuckRandomBeacon struct {
	Data  []byte
	Round uint64
}

func (prb *PartialRandomBeacon) String() string {
	return fmt.Sprintf("%020d", prb.Round) + prb.From.String()
}
