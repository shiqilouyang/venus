package messagepool

import (
	"context"
	"github.com/filecoin-project/go-address"
	tbig "github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/venus/pkg/chain"
	"github.com/filecoin-project/venus/pkg/config"
	"github.com/filecoin-project/venus/pkg/statemanger"
	"github.com/filecoin-project/venus/pkg/types"
	"github.com/filecoin-project/venus/pkg/types/specactors/policy"
	"github.com/ipfs/go-cid"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"golang.org/x/xerrors"
	"time"
)

var (
	HeadChangeCoalesceMinDelay      = 2 * time.Second
	HeadChangeCoalesceMaxDelay      = 6 * time.Second
	HeadChangeCoalesceMergeInterval = time.Second
)

type Provider interface {
	ChainHead() (*types.TipSet, error)
	ChainTipSet(types.TipSetKey) (*types.TipSet, error)
	SubscribeHeadChanges(func(rev, app []*types.TipSet) error) *types.TipSet
	PutMessage(m types.ChainMsg) (cid.Cid, error)
	PubSubPublish(string, []byte) error
	GetActorAfter(address.Address, *types.TipSet) (*types.Actor, error)
	StateAccountKeyAtFinality(context.Context, address.Address, *types.TipSet) (address.Address, error)
	StateAccountKey(context.Context, address.Address, *types.TipSet) (address.Address, error)
	MessagesForBlock(block2 *types.BlockHeader) ([]*types.UnsignedMessage, []*types.SignedMessage, error)
	MessagesForTipset(*types.TipSet) ([]types.ChainMsg, error)
	LoadTipSet(tsk types.TipSetKey) (*types.TipSet, error)
	ChainComputeBaseFee(ctx context.Context, ts *types.TipSet) (tbig.Int, error)
	IsLite() bool
}

type mpoolProvider struct {
	sm     *statemanger.Stmgr
	cs     *chain.Store
	cms    *chain.MessageStore
	config *config.NetworkParamsConfig
	ps     *pubsub.PubSub

	lite MpoolNonceAPI
}

var _ Provider = (*mpoolProvider)(nil)

func NewProvider(sm *statemanger.Stmgr, cs *chain.Store, cms *chain.MessageStore, cfg *config.NetworkParamsConfig, ps *pubsub.PubSub) Provider {
	return &mpoolProvider{
		sm:     sm,
		cs:     cs,
		cms:    cms,
		config: cfg,
		ps:     ps,
	}
}

func NewProviderLite(sm *chain.Store, ps *pubsub.PubSub, noncer MpoolNonceAPI) Provider {
	return &mpoolProvider{cs: sm, ps: ps, lite: noncer}
}

func (mpp *mpoolProvider) IsLite() bool {
	return mpp.lite != nil
}

func (mpp *mpoolProvider) SubscribeHeadChanges(cb func(rev, app []*types.TipSet) error) *types.TipSet {
	mpp.cs.SubscribeHeadChanges(
		chain.WrapHeadChangeCoalescer(
			cb,
			HeadChangeCoalesceMinDelay,
			HeadChangeCoalesceMaxDelay,
			HeadChangeCoalesceMergeInterval,
		))
	return mpp.cs.GetHead()
}

func (mpp *mpoolProvider) ChainHead() (*types.TipSet, error) {
	return mpp.cs.GetHead(), nil
}

func (mpp *mpoolProvider) ChainTipSet(key types.TipSetKey) (*types.TipSet, error) {
	return mpp.cs.GetTipSet(key)
}

func (mpp *mpoolProvider) PutMessage(m types.ChainMsg) (cid.Cid, error) {
	return mpp.cs.PutMessage(m)
}

func (mpp *mpoolProvider) PubSubPublish(k string, v []byte) error {
	return mpp.ps.Publish(k, v) // nolint
}

func (mpp *mpoolProvider) GetActorAfter(addr address.Address, ts *types.TipSet) (*types.Actor, error) {
	if mpp.IsLite() {
		n, err := mpp.lite.GetNonce(context.TODO(), addr, ts.Key())
		if err != nil {
			return nil, xerrors.Errorf("getting nonce over lite: %w", err)
		}
		a, err := mpp.lite.GetActor(context.TODO(), addr, ts.Key())
		if err != nil {
			return nil, xerrors.Errorf("getting actor over lite: %w", err)
		}
		a.Nonce = n
		return a, nil
	}

	st, err := mpp.cs.GetTipSetState(context.TODO(), ts)
	if err != nil {
		return nil, xerrors.Errorf("computing tipset state for GetActor: %v", err)
	}

	act, found, err := st.GetActor(context.TODO(), addr)
	if !found {
		err = xerrors.New("actor not found")
	}

	return act, err
}

func (mpp *mpoolProvider) StateAccountKeyAtFinality(ctx context.Context, addr address.Address, ts *types.TipSet) (address.Address, error) {
	var err error
	if ts.Height() > policy.ChainFinality {
		ts, err = mpp.cs.GetTipSetByHeight(ctx, ts, ts.Height()-policy.ChainFinality, true)
		if err != nil {
			return address.Undef, xerrors.Errorf("failed to load lookback tipset: %w", err)
		}
	}
	return mpp.sm.ResolveToKeyAddress(ctx, addr, ts)
}

func (mpp *mpoolProvider) StateAccountKey(ctx context.Context, addr address.Address, ts *types.TipSet) (address.Address, error) {
	return mpp.sm.ResolveToKeyAddress(ctx, addr, ts)
}

func (mpp *mpoolProvider) MessagesForBlock(h *types.BlockHeader) ([]*types.UnsignedMessage, []*types.SignedMessage, error) {
	secpMsgs, blsMsgs, err := mpp.cms.LoadMetaMessages(context.TODO(), h.Messages)
	return blsMsgs, secpMsgs, err
}

func (mpp *mpoolProvider) MessagesForTipset(ts *types.TipSet) ([]types.ChainMsg, error) {
	return mpp.cms.MessagesForTipset(ts)
}

func (mpp *mpoolProvider) LoadTipSet(tsk types.TipSetKey) (*types.TipSet, error) {
	return mpp.cs.GetTipSet(tsk)
}

func (mpp *mpoolProvider) ChainComputeBaseFee(ctx context.Context, ts *types.TipSet) (tbig.Int, error) {
	baseFee, err := mpp.cms.ComputeBaseFee(ctx, ts, mpp.config.ForkUpgradeParam)
	if err != nil {
		return tbig.NewInt(0), xerrors.Errorf("computing base fee at %s: %v", ts, err)
	}
	return baseFee, nil
}
