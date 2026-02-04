package svc

import (
	"context"
	"encoding/json"
	"errors"
	cfg "filogger/config"
	"filogger/testutil"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func testConsumer[JSON any](ctx context.Context, t *testing.T, reader *kafka.Reader, count int) []any {
	t.Logf("starting test consumer on topic %s", reader.Config().Topic)

	var values []any
	for range count {
		m, err := reader.ReadMessage(ctx) // respects deadline.
		if err != nil {
			values = append(values, err)
		} else {
			var val JSON
			if err := json.Unmarshal(m.Value, &val); err != nil {
				values = append(values, err)
			} else {
				values = append(values, val)
			}
		}
	}
	return values
}

func TestProducePutErrors(t *testing.T) {
	// given
	cfg.InitLoggers(nil)
	kafkaClient := cfg.MakeKafka(testutil.SetupITest(t, testutil.Kafka).KafkaConfig)
	app := &App{KafkaClient: kafkaClient}

	cnctEnrichment := ExtnCnctEnrichment{
		PrtyId:       "p1",
		PrtyIdTypeCd: "Y",
		ExtnCnctId:   "c1",
		BusDt:        "2025-06-12",
		VendorName:   "BankOfAmerica",
	}
	putErrs := []PutResult{
		{
			Key:    "p1/Y/c1/2025-06-12",
			Origin: cnctEnrichment,
			Err:    errors.New("test error 1"),
		},
	}
	wantPutCount := len(putErrs)

	cnctEnrichmentChan := make(chan any)

	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), "trace", t.Name()), time.Second*30)
	defer cancel()

	go func() {
		cnctEnrichmentChan <- testConsumer[ExtnCnctEnrichment](ctx, t, app.CnctEnrichmentConsumer, wantPutCount)
	}()

	// when
	app.ProducePutErrors(ctx, app.CnctEnrichmentTopic, putErrs)

	// then
	cnctEnrichments := <-cnctEnrichmentChan

	t.Logf("received connection enrichments from consumer: %+v", cnctEnrichments)
	assert.ElementsMatch(t, []any{cnctEnrichment}, cnctEnrichments)
}

func TestProduceDeleteErrors(t *testing.T) {
	// given
	kafkaClient := cfg.MakeKafka(testutil.SetupITest(t, testutil.Kafka).KafkaConfig)
	app := &App{KafkaClient: kafkaClient}

	deleteErrs := []DeleteResult{
		{
			Bucket: "bucket",
			Keys:   []string{"key1", "key2"},
			Err:    errors.New("err1"),
		},
		{
			Bucket: "bucket",
			Prefix: "prefix",
			Err:    errors.New("err1"),
		},
	}
	wantDeleteCount := len(deleteErrs)

	deleteResultsChan := make(chan any)

	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), "trace", t.Name()), time.Second*30)
	defer cancel()
	//ctx := context.WithValue(t.Context(), "trace", t.Name())

	go func() {
		deleteResultsChan <- testConsumer[DeleteRetry](ctx, t, app.DeleteRecoveryConsumer, wantDeleteCount)
	}()

	// when
	app.ProduceDeleteErrors(ctx, deleteErrs)

	// then
	deleteResults := <-deleteResultsChan

	t.Logf("received delete results from consumer: %+v", deleteResults)

	wantDeleteResults := []any{
		DeleteRetry{
			Kind:   DeleteKind,
			Bucket: "bucket",
			Keys:   []string{"key1", "key2"},
		},
		DeleteRetry{
			Kind:   ListKind,
			Bucket: "bucket",
			Prefix: "prefix",
		},
	}
	assert.ElementsMatch(t, wantDeleteResults, deleteResults)
}
