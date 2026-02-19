package svc

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"yodleeops/infra"
)

type CnctKey struct {
	ProfileId string
	CnctID    int64
	UpdtTs    string
}

func (k CnctKey) String() string {
	return fmt.Sprintf("%s/1/%d/%s", k.ProfileId, k.CnctID, k.UpdtTs)
}

type CnctPrefix struct {
	ProfileId string
	CnctID    int64
}

func (k CnctPrefix) String() string {
	return fmt.Sprintf("%s/1/%d", k.ProfileId, k.CnctID)
}

type AcctKey struct {
	ProfileId string
	CnctID    int64
	AcctID    int64
	UpdtTs    string
}

func (k AcctKey) String() string {
	return fmt.Sprintf("%s/1/%d/%d/%s", k.ProfileId, k.CnctID, k.AcctID, k.UpdtTs)
}

func ParseAcctKey(acctKeyStr string) (AcctKey, error) {
	tokens := strings.Split(acctKeyStr, "/")
	if len(tokens) != 5 {
		return AcctKey{}, fmt.Errorf("invalid AcctKey %s: expected 5 tokens, got %d", acctKeyStr, len(tokens))
	}

	cnctIdStr := tokens[2]
	cnctId, err := strconv.ParseInt(cnctIdStr, 10, 64)
	if err != nil {
		return AcctKey{}, fmt.Errorf("invalid AcctKey %s: invalid cnctId %s: %w", acctKeyStr, cnctIdStr, err)
	}

	acctIdStr := tokens[3]
	acctId, err := strconv.ParseInt(acctIdStr, 10, 64)
	if err != nil {
		return AcctKey{}, fmt.Errorf("invalid AcctKey %s: invalid acctId %s: %w", acctKeyStr, acctIdStr, err)
	}

	return AcctKey{
		ProfileId: tokens[0],
		CnctID:    cnctId,
		AcctID:    acctId,
		UpdtTs:    tokens[4],
	}, nil
}

// AcctMemberPrefix is a prefix Key to delete txns or holdings.
type AcctMemberPrefix struct {
	ProfileId string
	AcctID    int64
}

func (k AcctMemberPrefix) String() string {
	return fmt.Sprintf("%s/1/%d", k.ProfileId, k.AcctID)
}

type AcctPrefix struct {
	ProfileId string
	CnctID    int64
	AcctID    int64
}

func (k AcctPrefix) String() string {
	return fmt.Sprintf("%s/1/%d/%d", k.ProfileId, k.CnctID, k.AcctID)
}

type TxnKey struct {
	ProfileId string
	AcctID    int64
	TxnID     int64
	TxnDt     string
}

func (k TxnKey) String() string {
	return fmt.Sprintf("%s/1/%d/%d/%s", k.ProfileId, k.AcctID, k.TxnID, k.TxnDt)
}

type HoldKey struct {
	ProfileId string
	AcctID    int64
	HoldID    int64
	UpdtTs    string
}

func (k HoldKey) String() string {
	return fmt.Sprintf("%s/1/%d/%d/%s", k.ProfileId, k.AcctID, k.HoldID, k.UpdtTs)
}

// AcctChildPrefix is a prefix Key for either a txn or a holding.
type AcctChildPrefix struct {
	ProfileId string
	AcctID    int64
	ChildID   int64
}

func (k AcctChildPrefix) String() string {
	return fmt.Sprintf("%s/1/%d/%d", k.ProfileId, k.AcctID, k.ChildID)
}

type OpsFiMetadata struct {
	Key          string
	LastModified time.Time
	// parsed key data - any unused fields will be zeroed out.
	ProfileID         string
	ProviderAccountID string
	PartyIDTypeCd     string
	AccountID         string
	HoldingID         string
	TransactionID     string
	LastUpdated       time.Time
}

type ParseOpsFiMetadataError struct {
	Key              string
	Bucket           string
	WantTokenCount   int
	ActualTokenCount int
}

func (e ParseOpsFiMetadataError) Error() string {
	return fmt.Sprintf("invalid key for bucket %s, %s: expected %d tokens, got %d", e.Bucket, e.Key, e.WantTokenCount, e.ActualTokenCount)
}

func TimeParseLax(dateString string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339,
		time.RFC3339Nano,
		time.DateOnly,
	} {
		if t, err := time.Parse(layout, dateString); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("parse time with any known layout: %s", dateString)
}

func (o *OpsFiMetadata) ParseOpsFiMetadata(buckets infra.S3Buckets, bucket string, key string) error {
	tokens := strings.Split(key, "/")

	switch bucket {
	case buckets.CnctBucket:
		wantTokenCount := 4
		if len(tokens) != wantTokenCount {
			return ParseOpsFiMetadataError{Key: key, Bucket: bucket, WantTokenCount: wantTokenCount, ActualTokenCount: len(tokens)}
		}
		o.ProfileID = tokens[0]
		o.ProviderAccountID = tokens[1]
		o.PartyIDTypeCd = tokens[2]
	case buckets.AcctBucket:
		wantTokenCount := 5
		if len(tokens) != wantTokenCount {
			return ParseOpsFiMetadataError{Key: key, Bucket: bucket, WantTokenCount: wantTokenCount, ActualTokenCount: len(tokens)}
		}
		o.ProfileID = tokens[0]
		o.ProviderAccountID = tokens[1]
		o.PartyIDTypeCd = tokens[2]
		o.AccountID = tokens[3]
	case buckets.HoldBucket:
		wantTokenCount := 5
		if len(tokens) != wantTokenCount {
			return ParseOpsFiMetadataError{Key: key, Bucket: bucket, WantTokenCount: wantTokenCount, ActualTokenCount: len(tokens)}
		}
		o.ProfileID = tokens[0]
		o.PartyIDTypeCd = tokens[1]
		o.AccountID = tokens[2]
		o.HoldingID = tokens[3]
	case buckets.TxnBucket:
		wantTokenCount := 5
		if len(tokens) != wantTokenCount {
			return ParseOpsFiMetadataError{Key: key, Bucket: bucket, WantTokenCount: wantTokenCount, ActualTokenCount: len(tokens)}
		}
		o.ProfileID = tokens[0]
		o.PartyIDTypeCd = tokens[1]
		o.AccountID = tokens[2]
		o.TransactionID = tokens[3]
	default:
		return fmt.Errorf("invalid bucket %s", bucket)
	}

	lastUpdated, err := TimeParseLax(tokens[len(tokens)-1])
	if err != nil {
		return err
	}
	o.LastUpdated = lastUpdated

	return nil
}
