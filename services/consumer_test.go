package svc

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

type fakeClaim struct {
	msgCh chan *sarama.ConsumerMessage
}

func (f *fakeClaim) Messages() <-chan *sarama.ConsumerMessage {
	return f.msgCh
}

func (f *fakeClaim) InitialOffset() int64       { return 0 }
func (f *fakeClaim) Topic() string              { return "test-topic" }
func (f *fakeClaim) Partition() int32           { return 0 }
func (f *fakeClaim) HighWaterMarkOffset() int64 { return 0 }

type fakeSession struct{}

func (f *fakeSession) MarkMessage(_ *sarama.ConsumerMessage, _ string) {}
func (f *fakeSession) Commit()                                         {}
func (f *fakeSession) ResetOffset(string, int32, int64, string)        {}
func (f *fakeSession) Claims() map[string][]int32                      { return nil }
func (f *fakeSession) MemberID() string                                { return "" }
func (f *fakeSession) GenerationID() int32                             { return 0 }
func (f *fakeSession) MarkOffset(string, int32, int64, string)         {}
func (f *fakeSession) Context() context.Context                        { return context.Background() }

func TestConsumerHandler_ConsumeClaim(t *testing.T) {
	// given
	type testValue struct {
		ID   string `json:"id"`
		Num  int    `json:"num"`
		Name string `json:"name"`
	}

	var (
		capturedKey   string
		capturedValue testValue
	)
	onMessage := func(ctx context.Context, key string, value testValue) {
		capturedKey = key
		capturedValue = value
	}

	consumer := &ConsumerHandler[testValue]{OnMessage: onMessage}

	payload := testValue{
		ID:   "abc",
		Num:  42,
		Name: "hello",
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)

	msg := &sarama.ConsumerMessage{
		Key:   []byte("key"),
		Value: raw,
	}
	claim := &fakeClaim{
		msgCh: make(chan *sarama.ConsumerMessage, 1),
	}
	session := &fakeSession{}

	// when
	claim.msgCh <- msg
	close(claim.msgCh)

	err = consumer.ConsumeClaim(session, claim)
	require.NoError(t, err)

	// then
	require.Equal(t, "key", capturedKey)
	require.Equal(t, payload.ID, capturedValue.ID)
	require.Equal(t, payload.Num, capturedValue.Num)
	require.Equal(t, payload.Name, capturedValue.Name)
}
