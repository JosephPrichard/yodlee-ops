package main

import (
	"context"
	"encoding/json"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"log"
	"log/slog"
	"math/rand"
	"strconv"
	"time"
	"yodleeops/infra"
	"yodleeops/internal/yodlee"
)

// generates random ids with a tight finite range so we can get a random spread of data

func makePartyId() []byte {
	return []byte("p" + strconv.Itoa(rand.Intn(5)+1))
}

func makeProviderAccountId() int64 {
	return int64(rand.Intn(1000) + 1)
}

func makeAccountId() int64 {
	return int64(rand.Intn(10000) + 1)
}

func makeHoldingId() int64 {
	return int64(rand.Intn(100000) + 1)
}

func makeTransactionId() int64 {
	return int64(rand.Intn(1000000) + 1)
}

func makeAccountName() string {
	return gofakeit.RandomString([]string{
		"checking",
		"savings",
		"credit_card",
		"loan",
		"mortgage",
		"investment",
		"retirement",
		"brokerage",
	})
}

func makeHoldingName() string {
	return gofakeit.RandomString([]string{
		"Stock",
		"Mutual Fund",
		"Bond",
		"ETF",
		"Cash",
	})
}

func makeAmount() float64 {
	return float64(1000 + rand.Intn(10000))
}

func makeBusDt() string {
	return time.Now().UTC().Format("2006-01-02")
}

func makeTxnDt() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func makeCnctRefreshes(n int) []yodlee.DataExtractsProviderAccount {
	arr := make([]yodlee.DataExtractsProviderAccount, 0, n)

	for range n {
		arr = append(arr, yodlee.DataExtractsProviderAccount{
			Id:          makeProviderAccountId(),
			LastUpdated: makeBusDt(),
			RequestId:   gofakeit.UUID(),
		})
	}

	return arr
}

func makeAcctRefreshes(n int) []yodlee.DataExtractsAccount {
	arr := make([]yodlee.DataExtractsAccount, 0, n)

	for range n {
		providerId := makeProviderAccountId()

		arr = append(arr, yodlee.DataExtractsAccount{
			ProviderAccountId: providerId,
			Id:                makeAccountId(),
			LastUpdated:       makeBusDt(),
			AccountName:       makeAccountName(),
		})
	}

	return arr
}

func makeTxnRefreshes(n int) []yodlee.DataExtractsTransaction {
	arr := make([]yodlee.DataExtractsTransaction, 0, n)

	for range n {
		accountId := makeAccountId()

		arr = append(arr, yodlee.DataExtractsTransaction{
			AccountId: accountId,
			Id:        makeTransactionId(),
			Date:      makeTxnDt(),
			Quantity:  makeAmount(),
		})
	}

	return arr
}

func makeHoldRefreshes(n int) []yodlee.DataExtractsHolding {
	arr := make([]yodlee.DataExtractsHolding, 0, n)

	for range n {
		accountId := makeAccountId()

		arr = append(arr, yodlee.DataExtractsHolding{
			AccountId:   accountId,
			Id:          makeHoldingId(),
			LastUpdated: makeBusDt(),
			HoldingType: makeHoldingName(),
		})
	}

	return arr
}

func makeCnctResponses(n int) yodlee.ProviderAccountResponse {
	arr := make([]yodlee.ProviderAccount, 0, n)

	for range n {
		arr = append(arr, yodlee.ProviderAccount{
			Id:          makeProviderAccountId(),
			LastUpdated: makeBusDt(),
			RequestId:   gofakeit.UUID(),
		})
	}

	return yodlee.ProviderAccountResponse{ProviderAccount: arr}
}

func makeAcctResponses(n int) yodlee.AccountResponse {
	arr := make([]yodlee.Account, 0, n)

	for range n {
		providerId := makeProviderAccountId()

		arr = append(arr, yodlee.Account{
			ProviderAccountId: providerId,
			Id:                makeAccountId(),
			LastUpdated:       makeBusDt(),
			AccountName:       makeAccountName(),
		})
	}

	return yodlee.AccountResponse{Account: arr}
}

func makeTxnResponses(n int) yodlee.TransactionResponse {
	arr := make([]yodlee.TransactionWithDateTime, 0, n)

	for range n {
		accountId := makeAccountId()

		arr = append(arr, yodlee.TransactionWithDateTime{
			AccountId: accountId,
			Id:        makeTransactionId(),
			Date:      makeTxnDt(),
			Quantity:  makeAmount(),
		})
	}

	return yodlee.TransactionResponse{Transaction: arr}
}

func makeHoldResponses(n int) yodlee.HoldingResponse {
	arr := make([]yodlee.Holding, 0, n)

	for range n {
		accountId := makeAccountId()

		arr = append(arr, yodlee.Holding{
			AccountId:   accountId,
			Id:          makeHoldingId(),
			LastUpdated: makeBusDt(),
			HoldingType: makeHoldingName(),
		})
	}

	return yodlee.HoldingResponse{Holding: arr}
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("failed to load .env file: %v", err)
	}

	infra.InitLoggers(nil)
	config := infra.MakeConfig()
	ctx := context.Background()

	slog.Info("starting test producer", "config", config)
	kafkaClient := infra.MakeKafkaProducers(config)

	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		topicKind := rand.Intn(8)

		var topic infra.Topic
		var value any

		n := rand.Intn(10)

		switch topicKind {
		case 0:
			topic = infra.CnctRefreshTopic
			value = makeCnctRefreshes(n)
		case 1:
			topic = infra.AcctRefreshTopic
			value = makeAcctRefreshes(n)
		case 2:
			topic = infra.TxnRefreshTopic
			value = makeTxnRefreshes(n)
		case 3:
			topic = infra.HoldRefreshTopic
			value = makeHoldRefreshes(n)
		case 4:
			topic = infra.CnctResponseTopic
			value = makeCnctResponses(n)
		case 5:
			topic = infra.AcctResponseTopic
			value = makeAcctResponses(n)
		case 6:
			topic = infra.TxnResponseTopic
			value = makeTxnResponses(n)
		case 7:
			topic = infra.HoldResponseTopic
			value = makeHoldResponses(n)
		default:
			continue
		}

		slog.Info("producing message", "topic", topic, "value", value)

		v, err := json.Marshal(value)
		if err != nil {
			slog.Error("failed to marshal produce message", "err", err)
			continue
		}

		if err := kafkaClient.Producer.WriteMessages(ctx, kafka.Message{Topic: string(topic), Key: makePartyId(), Value: v}); err != nil {
			slog.Error("failed to produce message", "err", err)
		}
	}
}
