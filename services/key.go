package svc

import (
	"fmt"
	"strings"
)

type CnctKey struct {
	PrtyId       string
	PrtyIdTypeCd string
	CnctID       string
	BusDt        string
}

func (k CnctKey) String() string {
	return fmt.Sprintf("%s/%s/%s/%s", k.PrtyId, k.PrtyIdTypeCd, k.CnctID, k.BusDt)
}

type CnctPrefix struct {
	PrtyId       string
	PrtyIdTypeCd string
	CnctID       string
}

func (k CnctPrefix) String() string {
	return fmt.Sprintf("%s/%s/%s", k.PrtyId, k.PrtyIdTypeCd, k.CnctID)
}

type AcctKey struct {
	PrtyId       string
	PrtyIdTypeCd string
	CnctID       string
	AcctID       string
	BusDt        string
}

func (k AcctKey) String() string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", k.PrtyId, k.PrtyIdTypeCd, k.CnctID, k.AcctID, k.BusDt)
}

func ParseAcctKey(acctKeyStr string) (AcctKey, error) {
	tokens := strings.Split(acctKeyStr, "/")
	if len(tokens) != 5 {
		return AcctKey{}, fmt.Errorf("invalid AcctKey %s: expected 5 tokens, got %d", acctKeyStr, len(tokens))
	}

	return AcctKey{
		PrtyId:       tokens[0],
		PrtyIdTypeCd: tokens[1],
		CnctID:       tokens[2],
		AcctID:       tokens[3],
		BusDt:        tokens[4],
	}, nil
}

// AcctMemberPrefix is a prefix key to delete txns or holdings.
type AcctMemberPrefix struct {
	PrtyId       string
	PrtyIdTypeCd string
	AcctID       string
}

func (k AcctMemberPrefix) String() string {
	return fmt.Sprintf("%s/%s/%s", k.PrtyId, k.PrtyIdTypeCd, k.AcctID)
}

type AcctPrefix struct {
	PrtyId       string
	PrtyIdTypeCd string
	CnctID       string
	AcctID       string
}

func (k AcctPrefix) String() string {
	return fmt.Sprintf("%s/%s/%s/%s", k.PrtyId, k.PrtyIdTypeCd, k.CnctID, k.AcctID)
}

type TxnKey struct {
	PrtyId       string
	PrtyIdTypeCd string
	AcctID       string
	TxnID        string
	TxnDt        string
}

func (k TxnKey) String() string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", k.PrtyId, k.PrtyIdTypeCd, k.AcctID, k.TxnID, k.TxnDt)
}

type HoldKey struct {
	PrtyId       string
	PrtyIdTypeCd string
	AcctID       string
	HoldID       string
	BusDt        string
}

func (k HoldKey) String() string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", k.PrtyId, k.PrtyIdTypeCd, k.AcctID, k.HoldID, k.BusDt)
}

// AcctChildPrefix is a prefix key for either a txn or a holding.
type AcctChildPrefix struct {
	PrtyId       string
	PrtyIdTypeCd string
	AcctID       string
	ChildID      string
}

func (k AcctChildPrefix) String() string {
	return fmt.Sprintf("%s/%s/%s/%s", k.PrtyId, k.PrtyIdTypeCd, k.AcctID, k.ChildID)
}
