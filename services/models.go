package svc

import (
	"yodleeops/internal/yodlee"
)

type YodleeWrapper[YodleeInput any] interface {
	Inner() YodleeInput
}

// Ops data types are wrappers for yodlee data types that add some additional context to the data type.

type OpsProviderAccountRefresh struct {
	ProfileId                   string                             `json:"profileId"`
	DataExtractsProviderAccount yodlee.DataExtractsProviderAccount `json:"dataExtractsProviderAccount"`
}

type OpsAccountRefresh struct {
	ProfileId           string                     `json:"profileId"`
	DataExtractsAccount yodlee.DataExtractsAccount `json:"dataExtractsAccount"`
}

type OpsHoldingRefresh struct {
	ProfileId           string                     `json:"profileId"`
	DataExtractsHolding yodlee.DataExtractsHolding `json:"dataExtractsHolding"`
}

type OpsTransactionRefresh struct {
	ProfileId               string                         `json:"profileId"`
	DataExtractsTransaction yodlee.DataExtractsTransaction `json:"dataExtractsTransaction"`
}

type OpsProviderAccount struct {
	ProfileId       string                 `json:"profileId"`
	ProviderAccount yodlee.ProviderAccount `json:"providerAccount"`
}

type OpsAccount struct {
	ProfileId string         `json:"profileId"`
	Account   yodlee.Account `json:"account"`
}

type OpsHolding struct {
	ProfileId string         `json:"profileId"`
	Holding   yodlee.Holding `json:"holding"`
}

type OpsTransaction struct {
	ProfileId               string                         `json:"profileId"`
	TransactionWithDateTime yodlee.TransactionWithDateTime `json:"transactionWithDateTime"`
}

func (r OpsProviderAccountRefresh) Inner() yodlee.DataExtractsProviderAccount {
	return r.DataExtractsProviderAccount
}
func (r OpsAccountRefresh) Inner() yodlee.DataExtractsAccount { return r.DataExtractsAccount }
func (r OpsHoldingRefresh) Inner() yodlee.DataExtractsHolding { return r.DataExtractsHolding }
func (r OpsTransactionRefresh) Inner() yodlee.DataExtractsTransaction {
	return r.DataExtractsTransaction
}

func (r OpsProviderAccount) Inner() yodlee.ProviderAccount     { return r.ProviderAccount }
func (r OpsAccount) Inner() yodlee.Account                     { return r.Account }
func (r OpsHolding) Inner() yodlee.Holding                     { return r.Holding }
func (r OpsTransaction) Inner() yodlee.TransactionWithDateTime { return r.TransactionWithDateTime }

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
