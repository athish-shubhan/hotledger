package designs

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"hotledger/internal/model"
	"hotledger/internal/store"
)

// ---------------------------------------------------------------------------
// Design 5 for auctions: single writer per auction over Kafka, with replies.
//
// A bidder needs to know at once whether the bid stands, so unlike the
// wallet ("enqueued") this client waits. The API tier stamps the receive
// time, produces the bid to `bids` keyed by auction (one writer applies every
// bid of an auction, in log order), and waits for the writer's decision on
// `bid-replies`. The writer replies after its Postgres commit; if it dies in
// between, the client times out and resends the same bid id, the writer finds
// the id already decided and replies with the recorded decision. Reads go to
// Postgres, where the writer's commits are.

const (
	BidTopic   = "bids"
	ReplyTopic = "bid-replies"
	bidRecSize = 48
	replySize  = 26
)

func EncodeBid(b model.Bid, tsUs, enqNs int64) (key, val []byte) {
	key = binary.BigEndian.AppendUint64(nil, uint64(b.Auction))
	val = make([]byte, bidRecSize)
	binary.BigEndian.PutUint64(val[0:], uint64(b.ID))
	binary.BigEndian.PutUint64(val[8:], uint64(b.Auction))
	binary.BigEndian.PutUint64(val[16:], uint64(b.Bidder))
	binary.BigEndian.PutUint64(val[24:], uint64(b.Amount))
	binary.BigEndian.PutUint64(val[32:], uint64(tsUs))
	binary.BigEndian.PutUint64(val[40:], uint64(enqNs))
	return key, val
}

func DecodeBid(val []byte) (b model.Bid, tsUs, enqNs int64, err error) {
	if len(val) != bidRecSize {
		return b, 0, 0, errors.New("bad bid record size")
	}
	b = model.Bid{
		ID:      int64(binary.BigEndian.Uint64(val[0:])),
		Auction: int64(binary.BigEndian.Uint64(val[8:])),
		Bidder:  int64(binary.BigEndian.Uint64(val[16:])),
		Amount:  int64(binary.BigEndian.Uint64(val[24:])),
	}
	return b, int64(binary.BigEndian.Uint64(val[32:])), int64(binary.BigEndian.Uint64(val[40:])), nil
}

func reasonCode(r string) byte {
	switch r {
	case model.ReasonLow:
		return 1
	case model.ReasonClosed:
		return 2
	}
	return 0
}

func EncodeReply(id int64, r model.BidResult) []byte {
	v := make([]byte, replySize)
	binary.BigEndian.PutUint64(v[0:], uint64(id))
	if r.Accepted {
		v[8] = 1
	}
	v[9] = reasonCode(r.Reason)
	binary.BigEndian.PutUint64(v[10:], uint64(r.TsUs))
	binary.BigEndian.PutUint64(v[18:], uint64(r.High))
	return v
}

func DecodeReply(v []byte) (int64, model.BidResult, error) {
	if len(v) != replySize {
		return 0, model.BidResult{}, errors.New("bad reply size")
	}
	r := model.BidResult{Accepted: v[8] == 1, TsUs: int64(binary.BigEndian.Uint64(v[10:])), High: int64(binary.BigEndian.Uint64(v[18:]))}
	switch v[9] {
	case 1:
		r.Reason = model.ReasonLow
	case 2:
		r.Reason = model.ReasonClosed
	}
	return int64(binary.BigEndian.Uint64(v[0:])), r, nil
}

type AuctionKafka struct {
	prod, cons *kgo.Client
	db         *sql.DB
	mu         sync.Mutex
	waiters    map[int64][]chan model.BidResult
	stop       context.CancelFunc
}

func NewAuctionKafka(brokers []string, db *sql.DB) (*AuctionKafka, error) {
	prod, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(BidTopic),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(0),
		kgo.RecordDeliveryTimeout(20*time.Second),
	)
	if err != nil {
		return nil, err
	}
	cons, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(ReplyTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(10*time.Millisecond),
	)
	if err != nil {
		prod.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &AuctionKafka{prod: prod, cons: cons, db: db, waiters: map[int64][]chan model.BidResult{}, stop: cancel}
	go d.consume(ctx)
	return d, nil
}

func (d *AuctionKafka) consume(ctx context.Context) {
	for ctx.Err() == nil {
		fs := d.cons.PollFetches(ctx)
		if fs.IsClientClosed() {
			return
		}
		fs.EachRecord(func(r *kgo.Record) {
			id, res, err := DecodeReply(r.Value)
			if err != nil {
				return
			}
			d.mu.Lock()
			ws := d.waiters[id]
			delete(d.waiters, id)
			d.mu.Unlock()
			for _, w := range ws {
				w <- res
			}
		})
	}
}

func (d *AuctionKafka) Close() error {
	d.stop()
	d.cons.Close()
	d.prod.Close()
	return d.db.Close()
}

func (d *AuctionKafka) Read(ctx context.Context, a int64) (model.AuctionState, error) {
	return store.ReadAuction(ctx, d.db, a)
}

func (d *AuctionKafka) Bid(ctx context.Context, b model.Bid) (model.BidResult, error) {
	now := time.Now()
	ch := make(chan model.BidResult, 1)
	d.mu.Lock()
	d.waiters[b.ID] = append(d.waiters[b.ID], ch)
	d.mu.Unlock()
	unregister := func() {
		d.mu.Lock()
		ws := d.waiters[b.ID]
		for i, w := range ws {
			if w == ch {
				ws = append(ws[:i], ws[i+1:]...)
				break
			}
		}
		if len(ws) == 0 {
			delete(d.waiters, b.ID)
		} else {
			d.waiters[b.ID] = ws
		}
		d.mu.Unlock()
	}
	k, v := EncodeBid(b, now.UnixMicro(), now.UnixNano())
	if err := d.prod.ProduceSync(ctx, &kgo.Record{Key: k, Value: v}).FirstErr(); err != nil {
		unregister()
		return model.BidResult{}, err
	}
	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		unregister()
		return model.BidResult{}, ctx.Err()
	}
}
