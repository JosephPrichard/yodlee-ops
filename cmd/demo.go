package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/brianvoe/gofakeit/v6"
	"log/slog"
	"math/rand"
	"strconv"
	"time"
	"yodleeops/model"
	"yodleeops/yodlee"
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

func makeDate() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func makeCnctRefreshes(n int) []yodlee.DataExtractsProviderAccount {
	arr := make([]yodlee.DataExtractsProviderAccount, 0, n)

	for range n {
		arr = append(arr, yodlee.DataExtractsProviderAccount{
			Id:          makeProviderAccountId(),
			LastUpdated: makeDate(),
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
			LastUpdated:       makeDate(),
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
			Date:      makeDate(),
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
			LastUpdated: makeDate(),
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
			LastUpdated: makeDate(),
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
			LastUpdated:       makeDate(),
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
			Date:      makeDate(),
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
			LastUpdated: makeDate(),
			HoldingType: makeHoldingName(),
		})
	}

	return yodlee.HoldingResponse{Holding: arr}
}

func ExecuteDemoProducer(serverConfig model.Config, kafkaConfig *sarama.Config) {
	producer := model.MakeSaramaProducer(serverConfig.KafkaBrokers, kafkaConfig)

	slog.Info("starting test producer", "serverConfig", serverConfig, "kafkaConfig", fmt.Sprintf("%+v", kafkaConfig))

	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		topicKind := rand.Intn(8)

		var topic model.Topic
		var value any

		n := rand.Intn(10)

		switch topicKind {
		case 0:
			topic = model.CnctRefreshTopic
			value = makeCnctRefreshes(n)
		case 1:
			topic = model.AcctRefreshTopic
			value = makeAcctRefreshes(n)
		case 2:
			topic = model.TxnRefreshTopic
			value = makeTxnRefreshes(n)
		case 3:
			topic = model.HoldRefreshTopic
			value = makeHoldRefreshes(n)
		case 4:
			topic = model.CnctResponseTopic
			value = makeCnctResponses(n)
		case 5:
			topic = model.AcctResponseTopic
			value = makeAcctResponses(n)
		case 6:
			topic = model.TxnResponseTopic
			value = makeTxnResponses(n)
		case 7:
			topic = model.HoldResponseTopic
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

		producer.Input() <- &sarama.ProducerMessage{
			Topic: string(topic),
			Key:   sarama.StringEncoder(makePartyId()),
			Value: sarama.ByteEncoder(v),
		}
	}
}
