package consensus

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	acrypto "github.com/filecoin-project/go-state-types/crypto"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/venus/pkg/state"
	"github.com/filecoin-project/venus/pkg/types"
)

// RequireNewTipSet instantiates and returns a new tipset of the given blocks
// and requires that the setup validation succeed.
func RequireNewTipSet(require *require.Assertions, blks ...*types.BlockHeader) *types.TipSet {
	ts, err := types.NewTipSet(blks...)
	require.NoError(err)
	return ts
}

// FakeConsensusStateViewer is a fake power state viewer.
type FakeConsensusStateViewer struct {
	Views map[cid.Cid]*state.FakeStateView
}

// PowerStateView returns the state view for a root.
func (f *FakeConsensusStateViewer) PowerStateView(root cid.Cid) state.PowerStateView {
	return f.Views[root]
}

// FaultStateView returns the state view for a root.
func (f *FakeConsensusStateViewer) FaultStateView(root cid.Cid) state.FaultStateView {
	return f.Views[root]
}

// FakeMessageValidator is a validator that doesn't validate to simplify message creation in tests.
type FakeMessageValidator struct{}

func (mv *FakeMessageValidator) ValidateSignedMessageSyntax(ctx context.Context, smsg *types.SignedMessage) error {
	return nil
}

func (mv *FakeMessageValidator) ValidateUnsignedMessageSyntax(ctx context.Context, msg *types.UnsignedMessage) error {
	return nil
}

// FakeTicketMachine generates fake tickets and verifies all tickets
type FakeTicketMachine struct{}

// MakeTicket returns a fake ticket
func (ftm *FakeTicketMachine) MakeTicket(ctx context.Context, base types.TipSetKey, epoch abi.ChainEpoch, miner address.Address, entry *types.BeaconEntry, newPeriod bool, worker address.Address, signer types.Signer) (types.Ticket, error) {
	return MakeFakeTicketForTest(), nil
}

// IsValidTicket always returns true
func (ftm *FakeTicketMachine) IsValidTicket(ctx context.Context, base types.TipSetKey, entry *types.BeaconEntry, newPeriod bool,
	epoch abi.ChainEpoch, miner address.Address, workerSigner address.Address, ticket types.Ticket) error {
	return nil
}

// FailingTicketValidator marks all tickets as invalid
type FailingTicketValidator struct{}

// IsValidTicket always returns false
func (ftv *FailingTicketValidator) IsValidTicket(ctx context.Context, base types.TipSetKey, entry *types.BeaconEntry, newPeriod bool,
	epoch abi.ChainEpoch, miner address.Address, workerSigner address.Address, ticket types.Ticket) error {
	return fmt.Errorf("invalid ticket")
}

// MakeFakeTicketForTest creates a fake ticket
func MakeFakeTicketForTest() types.Ticket {
	val := make([]byte, 65)
	val[0] = 200
	return types.Ticket{
		VRFProof: types.VRFPi(val[:]),
	}
}

// MakeFakeVRFProofForTest creates a fake election proof
func MakeFakeVRFProofForTest() []byte {
	proof := make([]byte, 65)
	proof[0] = 42
	return proof
}

// MakeFakePoStForTest creates a fake post
//func MakeFakePoStsForTest() []block.PoStProof {
//	return []block.PoStProof{{
//		RegisteredProof: constants.DevRegisteredWinningPoStProof,
//		ProofBytes:      []byte{0xe},
//	}}
//}
//
//// NFakeSectorInfos returns numSectors fake sector infos
//func RequireFakeSectorInfos(t *testing.T, numSectors uint64) []abi.SectorInfo {
//	var infos []abi.SectorInfo
//	for i := uint64(0); i < numSectors; i++ {
//		infos = append(infos, abi.SectorInfo{
//			RegisteredProof: constants.DevRegisteredSealProof,
//			SectorNumber:    abi.SectorNumber(i),
//			SealedCID:       types.CidFromString(t, fmt.Sprintf("fake-sector-%d", i)),
//		})
//	}
//
//	return infos
//}

///// Sampler /////

// FakeChainRandomness generates deterministic values that are a function of a seed and the provided
// tag, epoch, and entropy (but *not* the Chain Head key).
type FakeChainRandomness struct {
	Seed uint
}

func (s *FakeChainRandomness) GetChainRandomness(ctx context.Context, tsk types.TipSetKey, pers acrypto.DomainSeparationTag, round abi.ChainEpoch, entropy []byte, _ bool) ([]byte, error) {
	return []byte(fmt.Sprintf("s=%d,e=%d,t=%d,p=%s", s.Seed, round, pers, string(entropy))), nil
}

func (s *FakeChainRandomness) GetBeaconRandomness(ctx context.Context, tsk types.TipSetKey, personalization acrypto.DomainSeparationTag, randEpoch abi.ChainEpoch, entropy []byte, _ bool) (abi.Randomness, error) {
	return []byte(""), nil
}

type FakeSampler struct {
	Seed uint
}

func (s *FakeSampler) SampleTicket(_ context.Context, _ types.TipSetKey, epoch abi.ChainEpoch, _ bool) (types.Ticket, error) {
	return types.Ticket{
		VRFProof: []byte(fmt.Sprintf("s=%d,e=%d", s.Seed, epoch)),
	}, nil
}
