package svc

import (
	"time"
	"yodleeops/internal/yodlee"
)

type YodleeWrapper[YodleeInput any] interface {
	Inner() YodleeInput
	ProfileID() string
}

type YodleeInput interface {
	yodlee.DataExtractsProviderAccount | yodlee.DataExtractsAccount | yodlee.DataExtractsHolding | yodlee.DataExtractsTransaction
	yodlee.ProviderAccount | yodlee.Account | yodlee.Holding | yodlee.TransactionWithDateTime
}

// Ops data types are wrappers for yodlee data types that add some additional context to the data type.

type OpsFiMessage struct {
	ProfileId   string    `json:"profileId"`
	Timestamp   time.Time `json:"timestamp"`
	OriginTopic string    `json:"originTopic"`
}

type OpsProviderAccountRefresh struct {
	OpsFiMessage
	Data yodlee.DataExtractsProviderAccount `json:"data"`
}

type OpsAccountRefresh struct {
	OpsFiMessage
	Data yodlee.DataExtractsAccount `json:"data"`
}

type OpsHoldingRefresh struct {
	OpsFiMessage
	Data yodlee.DataExtractsHolding `json:"data"`
}

type OpsTransactionRefresh struct {
	OpsFiMessage
	Data yodlee.DataExtractsTransaction `json:"data"`
}

type OpsProviderAccount struct {
	OpsFiMessage
	Data yodlee.ProviderAccount `json:"data"`
}

type OpsAccount struct {
	OpsFiMessage
	Data yodlee.Account `json:"data"`
}

type OpsHolding struct {
	OpsFiMessage
	Data yodlee.Holding `json:"data"`
}

type OpsTransaction struct {
	OpsFiMessage
	Data yodlee.TransactionWithDateTime `json:"data"`
}

func (r OpsProviderAccountRefresh) Inner() yodlee.DataExtractsProviderAccount {
	return r.Data
}
func (r OpsAccountRefresh) Inner() yodlee.DataExtractsAccount { return r.Data }
func (r OpsHoldingRefresh) Inner() yodlee.DataExtractsHolding { return r.Data }
func (r OpsTransactionRefresh) Inner() yodlee.DataExtractsTransaction {
	return r.Data
}

func (r OpsProviderAccount) Inner() yodlee.ProviderAccount     { return r.Data }
func (r OpsAccount) Inner() yodlee.Account                     { return r.Data }
func (r OpsHolding) Inner() yodlee.Holding                     { return r.Data }
func (r OpsTransaction) Inner() yodlee.TransactionWithDateTime { return r.Data }

func (r OpsProviderAccountRefresh) ProfileID() string { return r.ProfileId }
func (r OpsAccountRefresh) ProfileID() string         { return r.ProfileId }
func (r OpsHoldingRefresh) ProfileID() string         { return r.ProfileId }
func (r OpsTransactionRefresh) ProfileID() string     { return r.ProfileId }

func (r OpsProviderAccount) ProfileID() string { return r.ProfileId }
func (r OpsAccount) ProfileID() string         { return r.ProfileId }
func (r OpsHolding) ProfileID() string         { return r.ProfileId }
func (r OpsTransaction) ProfileID() string     { return r.ProfileId }

const (
	DeleteKind = "delete"
	ListKind   = "list"
)

type DeleteRetry struct {
	Kind   string   `json:"kind"` // either 'list' or 'delete'
	Bucket string   `json:"Bucket"`
	Prefix string   `json:"prefix"`
	Keys   []string `json:"keys"`
}
