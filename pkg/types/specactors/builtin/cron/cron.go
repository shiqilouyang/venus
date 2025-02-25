package cron

import (
	"github.com/filecoin-project/venus/pkg/types/specactors"
	"github.com/filecoin-project/venus/pkg/types/specactors/adt"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	builtin0 "github.com/filecoin-project/specs-actors/actors/builtin"

	builtin2 "github.com/filecoin-project/specs-actors/v2/actors/builtin"

	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"

	builtin4 "github.com/filecoin-project/specs-actors/v4/actors/builtin"

	builtin5 "github.com/filecoin-project/specs-actors/v5/actors/builtin"

	builtin6 "github.com/filecoin-project/specs-actors/v6/actors/builtin"
)

func MakeState(store adt.Store, av specactors.Version) (State, error) {
	switch av {

	case specactors.Version0:
		return make0(store)

	case specactors.Version2:
		return make2(store)

	case specactors.Version3:
		return make3(store)

	case specactors.Version4:
		return make4(store)

	case specactors.Version5:
		return make5(store)

	case specactors.Version6:
		return make6(store)

	}
	return nil, xerrors.Errorf("unknown actor version %d", av)
}

func GetActorCodeID(av specactors.Version) (cid.Cid, error) {
	switch av {

	case specactors.Version0:
		return builtin0.CronActorCodeID, nil

	case specactors.Version2:
		return builtin2.CronActorCodeID, nil

	case specactors.Version3:
		return builtin3.CronActorCodeID, nil

	case specactors.Version4:
		return builtin4.CronActorCodeID, nil

	case specactors.Version5:
		return builtin5.CronActorCodeID, nil

	case specactors.Version6:
		return builtin6.CronActorCodeID, nil

	}

	return cid.Undef, xerrors.Errorf("unknown actor version %d", av)
}

var (
	Address = builtin6.CronActorAddr
	Methods = builtin6.MethodsCron
)

type State interface {
	GetState() interface{}
}
