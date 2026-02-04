package svc

import (
	"context"
	"encoding/json"
	"filogger/testutil"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFiMessageConsumers(t *testing.T) {
	// given
	testCfg := testutil.SetupITest(t, testutil.Aws, testutil.Kafka)
	app := MakeApp(testCfg)
	testutil.SeedS3Buckets(t, &app.AwsClient)

	var produceMsgs []kafka.Message

	for _, input := range []struct {
		topic string
		value any
	}{
		{topic: app.CnctRefreshTopic, value: []ExtnCnctRefresh{
			{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnCnctId: "c1", BusDt: "2025-06-12", VendorName: "BankOfAmerica"},
			{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c10", BusDt: "2025-06-13", VendorName: "GoldmanSachs"},
		}},
		{topic: app.AcctRefreshTopic, value: []ExtnAcctRefresh{
			{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnCnctId: "c1", ExtnAcctId: "a1", BusDt: "2025-06-12", AcctName: "Checking Account"},
			{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c100", ExtnAcctId: "a200", BusDt: "2025-06-13", AcctName: "Savings Account"},
		}},
		{topic: app.HoldRefreshTopic, value: []ExtnHoldRefresh{
			{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnAcctId: "a1", ExtnHoldId: "h1", BusDt: "2025-06-12", HoldName: "Security"},
			{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnHoldId: "h100", BusDt: "2025-06-13", HoldName: "Stock"},
		}},
		{topic: app.TxnRefreshTopic, value: []ExtnTxnRefresh{
			{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnAcctId: "a1", ExtnTxnId: "t1", BusDt: "2025-06-11", TxnDt: "2025-06-11T07:06:18Z", TxnAmt: 4523},
			{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnTxnId: "t200", BusDt: "2025-06-13", TxnDt: "2025-06-13T07:06:18Z", TxnAmt: -1299},
		}},
		{topic: app.CnctEnrichmentTopic, value: []ExtnCnctEnrichment{
			{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnCnctId: "c1", BusDt: "2025-06-12", VendorName: "BankOfAmerica"},
			{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c10", BusDt: "2025-06-13", VendorName: "GoldmanSachs"},
		}},
		{topic: app.AcctEnrichmentTopic, value: []ExtnAcctEnrichment{
			{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnCnctId: "c1", ExtnAcctId: "a1", BusDt: "2025-06-12", AcctName: "Checking Account"},
			{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c100", ExtnAcctId: "a200", BusDt: "2025-06-13", AcctName: "Savings Account"},
		}},
		{topic: app.HoldEnrichmentTopic, value: []ExtnHoldEnrichment{
			{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnAcctId: "a1", ExtnHoldId: "h1", BusDt: "2025-06-12", HoldName: "Security"},
			{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnHoldId: "h100", BusDt: "2025-06-13", HoldName: "Stock"},
		}},
		{topic: app.TxnEnrichmentTopic, value: []ExtnTxnEnrichment{
			{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnAcctId: "a1", ExtnTxnId: "t1", BusDt: "2025-06-11", TxnDt: "2025-06-11T07:06:18Z", TxnAmt: 4523},
			{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnTxnId: "t200", BusDt: "2025-06-13", TxnDt: "2025-06-13T07:06:18Z", TxnAmt: -1299},
		}},
		{topic: app.DeleteRecoveryTopic, value: []DeleteRetry{
			{
				Kind:   ListKind,
				Bucket: app.AcctBucket,
				Prefix: "p2/Y/c3",
			},
			{
				Kind:   DeleteKind,
				Bucket: app.CnctBucket,
				Keys:   []string{"p1/Y/c3/2025-06-15"},
			},
		}},
	} {
		inputBytes, err := json.Marshal(input.value)
		require.NoError(t, err)

		produceMsgs = append(produceMsgs, kafka.Message{
			Topic: input.topic,
			Value: inputBytes,
		})
	}
	wantConsumeCount := len(produceMsgs) // 1:1 produce -> consume

	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), "trace", t.Name()), time.Second*60) // consumers respect timeout. timeout must be long due to poor kafka latency.
	defer cancel()

	mChan := make(chan bool)

	// when
	app.StartConsumers(ConsumersConfig{
		Context:     ctx,
		ConsumeChan: mChan, // used to let us deterministically wait on N messages to be consumed.
		Concurrency: 2,
	})

	for _, msg := range produceMsgs {
		require.NoError(t, app.Producer.WriteMessages(ctx, msg))
	}

	// then
	// asserts that we consume the expected number of messages and that the events perform the expected state changes to the keyspace

	for range wantConsumeCount {
		<-mChan
	}
	t.Logf("all %d messages consumed", wantConsumeCount)

	// removed keys are commented.
	wantKeys := []string{
		// Connections
		"p1/Y/c1/2025-06-12",
		"p1/Y/c1/2025-06-13",
		"p1/Y/c2/2025-06-14",
		// "p1/Y/c3/2025-06-15",
		"p300/Y/c10/2025-06-13",

		// Accounts
		"p1/Y/c1/a1/2025-06-12",
		"p1/Y/c1/a1/2025-06-13",
		"p2/Y/c2/a2/2025-06-14",
		// "p2/Y/c3/a3/2025-06-15",
		"p300/Y/c100/a200/2025-06-13",

		// Holds
		"p1/Y/a1/h1/2025-06-12",
		"p1/Y/a1/h1/2025-06-13",
		"p2/Y/a1/h1/2025-06-14",
		"p2/Y/a2/h2/2025-06-15",
		"p300/Y/a200/h100/2025-06-13",

		// Transactions
		"p1/Y/a1/t1/2025-06-12T00:14:37Z",
		"p1/Y/a1/t1/2025-06-12T02:48:09Z",
		"p2/Y/a1/t1/2025-06-13T02:48:09Z",
		"p2/Y/a2/t2/2025-06-14T07:06:18Z",
		"p1/Y/a1/t1/2025-06-11T07:06:18Z",
		"p300/Y/a200/t200/2025-06-13T07:06:18Z",
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, &app.AwsClient))
}
