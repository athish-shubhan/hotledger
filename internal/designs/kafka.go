package designs

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"hotledger/internal/model"
)

// ---------------------------------------------------------------------------
// Design 5 (client side): single writer per partition over Kafka.
//
// Clients append transfer commands to a topic keyed by the debit account, so
// every debit of one account is applied by exactly one writer in log order.
// The franz-go producer is idempotent (producer id + epoch + per-partition
// sequence numbers), so internal produce retries never duplicate a record.
// Client-level retries of the same idempotency key can still put the command
// in the log twice; the writer dedupes those by transfer id.
//
// The client is answered "enqueued" once the broker acks the record. The
// result (applied or rejected) is decided by the writer; see internal/writer.

const RecordSize = 41

func EncodeTransfer(t model.Transfer, enqNs int64) (key, val []byte) {
	key = binary.BigEndian.AppendUint64(nil, uint64(t.Src))
	val = make([]byte, RecordSize)
	binary.BigEndian.PutUint64(val[0:], uint64(t.ID))
	val[8] = byte(t.Kind)
	binary.BigEndian.PutUint64(val[9:], uint64(t.Src))
	binary.BigEndian.PutUint64(val[17:], uint64(t.Dst))
	binary.BigEndian.PutUint64(val[25:], uint64(t.Amount))
	binary.BigEndian.PutUint64(val[33:], uint64(enqNs))
	return key, val
}

func DecodeTransfer(val []byte) (model.Transfer, int64, error) {
	if len(val) != RecordSize {
		return model.Transfer{}, 0, errors.New("bad record size")
	}
	t := model.Transfer{
		ID:     int64(binary.BigEndian.Uint64(val[0:])),
		Kind:   model.Kind(val[8]),
		Src:    int64(binary.BigEndian.Uint64(val[9:])),
		Dst:    int64(binary.BigEndian.Uint64(val[17:])),
		Amount: int64(binary.BigEndian.Uint64(val[25:])),
	}
	return t, int64(binary.BigEndian.Uint64(val[33:])), nil
}

type KafkaSW struct{ cl *kgo.Client }

func NewKafkaSW(brokers []string, topic string) (*KafkaSW, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), // murmur2 on the key, like the Java client
		kgo.RequiredAcks(kgo.AllISRAcks()),                   // required for idempotence
		kgo.ProducerLinger(time.Millisecond),
		kgo.RecordDeliveryTimeout(20*time.Second),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaSW{cl: cl}, nil
}

func (d *KafkaSW) Close() error { d.cl.Close(); return nil }

func (d *KafkaSW) Transfer(ctx context.Context, t model.Transfer) (model.Outcome, error) {
	k, v := EncodeTransfer(t, time.Now().UnixNano())
	if err := d.cl.ProduceSync(ctx, &kgo.Record{Key: k, Value: v}).FirstErr(); err != nil {
		return model.Unknown, err
	}
	return model.Enqueued, nil
}
