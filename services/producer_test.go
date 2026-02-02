package svc

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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
	putErrs := []PutError[ExtnCnctEnrichment]{
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

	ProducePutErrors(ctx, app, app.CnctEnrichmentTopic, putErrs)

	cnctEnrichments := <-cnctEnrichmentChan
	assert.Equal(t, []any{cnctEnrichment}, cnctEnrichments)
}
