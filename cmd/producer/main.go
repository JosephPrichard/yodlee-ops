package main

import (
	"context"
	"encoding/json"
	cfg "filogger/config"
	svc "filogger/services"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"log"
	"log/slog"
	"math/rand"
	"strconv"
	"time"
)

// generates random ids with a tight finite range so we can get a random spread of data

func makePartyId() string {
	return "p" + strconv.Itoa(rand.Intn(5)+1)
}

func makeCnctId() string {
	return "c" + strconv.Itoa(rand.Intn(10)+1)
}

func makeAcctId() string {
	return "a" + strconv.Itoa(rand.Intn(10)+1)
}

func makeHoldId() string {
	return "h" + strconv.Itoa(rand.Intn(100)+1)
}

func makeTxnId() string {
	return "t" + strconv.Itoa(rand.Intn(10000)+1)
}

func makePartyTypeCd() string {
	num := rand.Intn(2)
	if num == 0 {
		return "Y"
	} else {
		return "M"
	}
}

func makeAccountName() string {
	return gofakeit.RandomString([]string{
		"checking",
		"savings",
		"credit_card",
		"loan",
		"mortgage",
		"investment",
		"property",
		"retirement",
		"brokerage",
	})
}

func makeHoldingName() string {
	return gofakeit.RandomString([]string{
		"Stock",
		"Mutual Fund",
		"Fixed Income",
		"Bond",
		"ETF",
		"Cash",
	})
}

func makeAmount() int64 {
	return int64(1000 + rand.Intn(10000))
}

func makeBusDt() string {
	return time.Now().Format("2006-01-02")
}

func makeTxnDt() string {
	return time.Now().Format(time.RFC3339)
}

func makeCnctRefreshes(n int) []svc.ExtnCnctRefresh {
	arr := make([]svc.ExtnCnctRefresh, 0, n)
	for range n {
		arr = append(arr, svc.ExtnCnctRefresh{
			IsDeleted:    false,
			PrtyId:       makePartyId(),
			PrtyIdTypeCd: makePartyTypeCd(),
			ExtnCnctId:   makeCnctId(),
			BusDt:        makeBusDt(),
			VendorName:   gofakeit.Company(),
		})
	}
	return arr
}

func makeAcctRefreshes(n int) []svc.ExtnAcctRefresh {
	arr := make([]svc.ExtnAcctRefresh, 0, n)
	for range n {
		arr = append(arr, svc.ExtnAcctRefresh{
			IsDeleted:    false,
			PrtyId:       makePartyId(),
			PrtyIdTypeCd: makePartyTypeCd(),
			ExtnCnctId:   makeCnctId(),
			ExtnAcctId:   makeAcctId(),
			BusDt:        makeBusDt(),
			AcctName:     makeAccountName(),
		})
	}
	return arr
}

func makeTxnRefreshes(n int) []svc.ExtnTxnRefresh {
	arr := make([]svc.ExtnTxnRefresh, 0, n)
	for range n {
		arr = append(arr, svc.ExtnTxnRefresh{
			IsDeleted:    false,
			PrtyId:       makePartyId(),
			PrtyIdTypeCd: makePartyTypeCd(),
			ExtnAcctId:   makeAcctId(),
			ExtnTxnId:    makeTxnId(),
			BusDt:        makeBusDt(),
			TxnDt:        makeTxnDt(),
			TxnAmt:       makeAmount(),
		})
	}
	return arr
}

func makeHoldRefreshes(n int) []svc.ExtnHoldRefresh {
	arr := make([]svc.ExtnHoldRefresh, 0, n)
	for range n {
		arr = append(arr, svc.ExtnHoldRefresh{
			IsDeleted:    false,
			PrtyId:       makePartyId(),
			PrtyIdTypeCd: makePartyTypeCd(),
			ExtnAcctId:   makeAcctId(),
			ExtnHoldId:   makeHoldId(),
			BusDt:        makeBusDt(),
			HoldName:     makeHoldingName(),
		})
	}
	return arr
}

func makeCnctEnrichments(n int) []svc.ExtnCnctEnrichment {
	arr := make([]svc.ExtnCnctEnrichment, 0, n)
	for range n {
		arr = append(arr, svc.ExtnCnctEnrichment{
			PrtyId:       makePartyId(),
			PrtyIdTypeCd: makePartyTypeCd(),
			ExtnCnctId:   makeCnctId(),
			BusDt:        makeBusDt(),
			VendorName:   gofakeit.Company(),
		})
	}
	return arr
}

func makeAcctEnrichments(n int) []svc.ExtnAcctEnrichment {
	arr := make([]svc.ExtnAcctEnrichment, 0, n)
	for range n {
		arr = append(arr, svc.ExtnAcctEnrichment{
			PrtyId:       makePartyId(),
			PrtyIdTypeCd: makePartyTypeCd(),
			ExtnCnctId:   makeCnctId(),
			ExtnAcctId:   makeAcctId(),
			BusDt:        makeBusDt(),
			AcctName:     makeAccountName(),
		})
	}
	return arr
}

func makeTxnEnrichments(n int) []svc.ExtnTxnEnrichment {
	arr := make([]svc.ExtnTxnEnrichment, 0, n)
	for range n {
		arr = append(arr, svc.ExtnTxnEnrichment{
			PrtyId:       makePartyId(),
			PrtyIdTypeCd: makePartyTypeCd(),
			ExtnAcctId:   makeAcctId(),
			ExtnTxnId:    makeTxnId(),
			BusDt:        makeBusDt(),
			TxnDt:        makeTxnDt(),
			TxnAmt:       makeAmount(),
		})
	}
	return arr
}

func makeHoldEnrichments(n int) []svc.ExtnHoldEnrichment {
	arr := make([]svc.ExtnHoldEnrichment, 0, n)
	for range n {
		arr = append(arr, svc.ExtnHoldEnrichment{
			PrtyId:       makePartyId(),
			PrtyIdTypeCd: makePartyTypeCd(),
			ExtnAcctId:   makeAcctId(),
			ExtnHoldId:   makeHoldId(),
			BusDt:        makeBusDt(),
			HoldName:     makeHoldingName(),
		})
	}
	return arr
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("failed to load .env file: %v", err)
	}

	config := cfg.MakeConfig(cfg.ReadEnv())
	ctx := context.Background()

	slog.Info("starting test producer", "config", config)
	app := svc.MakeApp(config)

	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		topicKind := rand.Intn(8)

		var topic string
		var value any

		n := rand.Intn(10)

		switch topicKind {
		case 0:
			topic = app.CnctRefreshTopic
			value = makeCnctRefreshes(n)
		case 1:
			topic = app.AcctRefreshTopic
			value = makeAcctRefreshes(n)
		case 2:
			topic = app.TxnRefreshTopic
			value = makeTxnRefreshes(n)
		case 3:
			topic = app.HoldRefreshTopic
			value = makeHoldRefreshes(n)
		case 4:
			topic = app.CnctEnrichmentTopic
			value = makeCnctEnrichments(n)
		case 5:
			topic = app.AcctEnrichmentTopic
			value = makeAcctEnrichments(n)
		case 6:
			topic = app.TxnEnrichmentTopic
			value = makeTxnEnrichments(n)
		case 7:
			topic = app.HoldEnrichmentTopic
			value = makeHoldEnrichments(n)
		default:
			continue
		}

		slog.Info("producing message", "topic", topic, "value", value)

		v, err := json.Marshal(value)
		if err != nil {
			slog.Error("failed to marshal produce message", "err", err)
			continue
		}

		if err := app.Producer.WriteMessages(ctx, kafka.Message{Topic: topic, Value: v}); err != nil {
			slog.Error("failed to produce message", "err", err)
		}
	}
}
