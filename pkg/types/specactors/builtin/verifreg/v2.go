package verifreg

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/venus/pkg/types/specactors"
	"github.com/filecoin-project/venus/pkg/types/specactors/adt"

	verifreg2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/verifreg"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

var _ State = (*state2)(nil)

func load2(store adt.Store, root cid.Cid) (State, error) {
	out := state2{store: store}
	err := store.Get(store.Context(), root, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func make2(store adt.Store, rootKeyAddress address.Address) (State, error) {
	out := state2{store: store}

	em, err := adt2.MakeEmptyMap(store).Root()
	if err != nil {
		return nil, err
	}

	out.State = *verifreg2.ConstructState(em, rootKeyAddress)

	return &out, nil
}

type state2 struct {
	verifreg2.State
	store adt.Store
}

func (s *state2) RootKey() (address.Address, error) {
	return s.State.RootKey, nil
}

func (s *state2) VerifiedClientDataCap(addr address.Address) (bool, abi.StoragePower, error) {
	return getDataCap(s.store, specactors.Version2, s.verifiedClients, addr)
}

func (s *state2) VerifierDataCap(addr address.Address) (bool, abi.StoragePower, error) {
	return getDataCap(s.store, specactors.Version2, s.verifiers, addr)
}

func (s *state2) ForEachVerifier(cb func(addr address.Address, dcap abi.StoragePower) error) error {
	return forEachCap(s.store, specactors.Version2, s.verifiers, cb)
}

func (s *state2) ForEachClient(cb func(addr address.Address, dcap abi.StoragePower) error) error {
	return forEachCap(s.store, specactors.Version2, s.verifiedClients, cb)
}

func (s *state2) verifiedClients() (adt.Map, error) {
	return adt2.AsMap(s.store, s.VerifiedClients)
}

func (s *state2) verifiers() (adt.Map, error) {
	return adt2.AsMap(s.store, s.Verifiers)
}

func (s *state2) GetState() interface{} {
	return &s.State
}
