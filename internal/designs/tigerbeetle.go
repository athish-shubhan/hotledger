package designs

import (
	"context"
	"fmt"

	tb "github.com/tigerbeetle/tigerbeetle-go"

	"hotledger/internal/model"
)

// ---------------------------------------------------------------------------
// Design 6: TigerBeetle, the reference point. A purpose-built ledger: one
// replica here (no replication), Direct IO, a single core applying batches
// in order, accounts with `debits_must_not_exceed_credits` for every balance
// that must not go negative (viewers and creators, SKUs).
//
// Mapping: account id = our id; transfer id = our idempotency key (TigerBeetle
// rejects a second transfer with the same id: `exists` if it was created,
// `id_already_failed` if it failed with a transient error such as
// `exceeds_credits`, so a retried rejection stays rejected). Initial balances
// are funded from a mint account (id NumAccounts+1, no balance constraint)
// with transfer code 2; workload transfers use code 1.
//
// TigerBeetle stores no record of failed transfers that can be listed, so the
// checker's record has applied transfers only (queried by code), and I7/I11
// use what clients were told.

const (
	tbLedger       = 1
	tbCodeTransfer = 1
	tbCodeFund     = 2
)

type TigerBeetle struct {
	c tb.Client
}

func NewTigerBeetle(addrs []string) (*TigerBeetle, error) {
	c, err := tb.NewClient(tb.ToUint128(0), addrs)
	if err != nil {
		return nil, err
	}
	return &TigerBeetle{c: c}, nil
}

func (d *TigerBeetle) Close() error { d.c.Close(); return nil }

func (d *TigerBeetle) Transfer(ctx context.Context, t model.Transfer) (model.Outcome, error) {
	type res struct {
		r   []tb.CreateTransferResult
		err error
	}
	ch := make(chan res, 1)
	go func() {
		r, err := d.c.CreateTransfers([]tb.Transfer{{
			ID: tb.ToUint128(uint64(t.ID)), DebitAccountID: tb.ToUint128(uint64(t.Src)), CreditAccountID: tb.ToUint128(uint64(t.Dst)),
			Amount: tb.ToUint128(uint64(t.Amount)), Ledger: tbLedger, Code: tbCodeTransfer,
		}})
		ch <- res{r, err}
	}()
	var r res
	select {
	case r = <-ch:
	case <-ctx.Done():
		// The request may still complete; the client retries with the same id
		// and TigerBeetle answers `exists` or `id_already_failed`.
		return model.Unknown, ctx.Err()
	}
	if r.err != nil {
		return model.Unknown, r.err
	}
	if len(r.r) != 1 {
		return model.Unknown, fmt.Errorf("tigerbeetle: %d results for one transfer", len(r.r))
	}
	switch r.r[0].Status {
	case tb.TransferCreated, tb.TransferExists:
		return model.Applied, nil
	case tb.TransferExceedsCredits, tb.TransferIDAlreadyFailed:
		return model.Rejected, nil
	}
	return model.Unknown, fmt.Errorf("tigerbeetle: transfer %d: %s", t.ID, r.r[0].Status)
}

// SeedTigerBeetle creates the accounts and funds their initial balances.
func SeedTigerBeetle(addrs []string, l model.Layout) error {
	c, err := tb.NewClient(tb.ToUint128(0), addrs)
	if err != nil {
		return err
	}
	defer c.Close()
	mint := uint64(l.NumAccounts() + 1)
	const batch = 8000
	var accts []tb.Account
	flushA := func() error {
		if len(accts) == 0 {
			return nil
		}
		rs, err := c.CreateAccounts(accts)
		if err != nil {
			return err
		}
		for i, r := range rs {
			if r.Status != tb.AccountCreated {
				return fmt.Errorf("create account %d: %s", i, r.Status)
			}
		}
		accts = accts[:0]
		return nil
	}
	accts = append(accts, tb.Account{ID: tb.ToUint128(mint), Ledger: tbLedger, Code: 1})
	for id := int64(1); id <= l.NumAccounts(); id++ {
		flags := uint16(0)
		if l.Inventory() {
			if id > l.Viewers && id <= l.Viewers+l.Creators {
				flags = tb.AccountFlags{DebitsMustNotExceedCredits: true}.ToUint16() // SKU stock
			}
		} else if id <= l.Viewers+l.Creators {
			flags = tb.AccountFlags{DebitsMustNotExceedCredits: true}.ToUint16() // viewers and creators
		}
		accts = append(accts, tb.Account{ID: tb.ToUint128(uint64(id)), Ledger: tbLedger, Code: 1, Flags: flags})
		if len(accts) == batch {
			if err := flushA(); err != nil {
				return err
			}
		}
	}
	if err := flushA(); err != nil {
		return err
	}
	var ts []tb.Transfer
	flushT := func() error {
		if len(ts) == 0 {
			return nil
		}
		rs, err := c.CreateTransfers(ts)
		if err != nil {
			return err
		}
		for i, r := range rs {
			if r.Status != tb.TransferCreated {
				return fmt.Errorf("fund %d: %s", i, r.Status)
			}
		}
		ts = ts[:0]
		return nil
	}
	for id := int64(1); id <= l.NumAccounts(); id++ {
		b := l.InitialBalance(id)
		if b == 0 {
			continue
		}
		// Funding ids live far above any workload id (client ids << 40).
		ts = append(ts, tb.Transfer{ID: tb.ToUint128(uint64(1)<<62 | uint64(id)), DebitAccountID: tb.ToUint128(mint), CreditAccountID: tb.ToUint128(uint64(id)),
			Amount: tb.ToUint128(uint64(b)), Ledger: tbLedger, Code: tbCodeFund})
		if len(ts) == batch {
			if err := flushT(); err != nil {
				return err
			}
		}
	}
	return flushT()
}

func u128(v tb.Uint128) int64 { return v.BigInt().Int64() }

// SnapshotTigerBeetle reads every account balance and every workload transfer.
func SnapshotTigerBeetle(addrs []string, l model.Layout) (model.State, error) {
	st := model.State{Balances: map[int64]int64{}, Extra: map[string]string{}}
	c, err := tb.NewClient(tb.ToUint128(0), addrs)
	if err != nil {
		return st, err
	}
	defer c.Close()
	const batch = 8000
	for lo := int64(1); lo <= l.NumAccounts(); lo += batch {
		var ids []tb.Uint128
		for id := lo; id < lo+batch && id <= l.NumAccounts(); id++ {
			ids = append(ids, tb.ToUint128(uint64(id)))
		}
		as, err := c.LookupAccounts(ids)
		if err != nil {
			return st, err
		}
		for _, a := range as {
			id := u128(a.ID)
			b := u128(a.CreditsPosted) - u128(a.DebitsPosted)
			st.Balances[id] = b
			if b < 0 {
				st.Negative = append(st.Negative, fmt.Sprintf("account %d balance %d", id, b))
			}
		}
	}
	// Page through every workload transfer by timestamp.
	var after uint64
	for {
		ts, err := c.QueryTransfers(tb.QueryFilter{Ledger: tbLedger, Code: tbCodeTransfer, TimestampMin: after + 1, Limit: batch})
		if err != nil {
			return st, err
		}
		for _, t := range ts {
			st.Transfers = append(st.Transfers, model.AppliedTransfer{ID: u128(t.ID), Src: u128(t.DebitAccountID), Dst: u128(t.CreditAccountID),
				Amount: u128(t.Amount), Applied: true})
			after = t.Timestamp
		}
		if len(ts) < batch {
			break
		}
	}
	return st, nil
}
