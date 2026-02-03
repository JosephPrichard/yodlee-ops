package svc

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func testConsumer[JSON any](ctx context.Context, reader *kafka.Reader, count int) []any {
	var values []any
	for range count {
		m, err := reader.ReadMessage(ctx) // respects deadline.
		if err != nil {
			values = append(values, err)
		} else {
			var errorLog JSON
			_ = json.Unmarshal(m.Value, &errorLog)
			values = append(values, errorLog)
		}
	}
	return values
}

func TestProducePutErrors(t *testing.T) {
	app := SetupAppTest(t)

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

	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), "trace", t.Name()), time.Second*5)
	defer cancel()

	go func() {
		cnctEnrichmentChan <- testConsumer[ExtnCnctEnrichment](ctx, app.CnctEnrichmentConsumer, wantPutCount)
	}()

	app.ProducePutErrors(ctx, app.CnctEnrichmentTopic, putErrs)

	cnctEnrichments := <-cnctEnrichmentChan

	t.Logf("received connection enrichments from consumer: %+v", cnctEnrichments)
	assert.Equal(t, []any{cnctEnrichment}, cnctEnrichments)
}

func TestProduceDeleteErrors(t *testing.T) {
	app := SetupAppTest(t)

	deleteErrs := []DeleteResult{
		{
			Bucket: "bucket",
			Keys: []string{"key1", "key2"},
			Err:   errors.New("err1"),
		},
		{
			Bucket: "bucket",
			Prefix: "prefix",
			Err:   errors.New("err1"),
		},
	}
	wantDeleteCount := len(deleteErrs)

	deleteResultsChan := make(chan any)

	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), "trace", t.Name()), time.Second*5)
	defer cancel()

	go func() {
		deleteResultsChan <- testConsumer[DeleteRetry](ctx, app.DeleteRcoveryConsumer, wantDeleteCount)
	}()

	app.ProduceDeleteErrors(ctx, deleteErrs)

	deleteResults := <-deleteResultsChan

	t.Logf("received delete results from consumer: %+v", deleteResults)
	
	wantDeleteResults := []any{
		DeleteRetry{
			Kind: DeleteKind,
			Bucket: "bucket",
			Keys: []string{"key1", "key2"},
		},
		DeleteRetry{
			Kind: ListKind,
			Bucket: "bucket",
			Prefix: "prefix",
		},
	}
	assert.Equal(t, wantDeleteResults, deleteResults)
}