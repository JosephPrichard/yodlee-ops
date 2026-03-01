package svc

import (
	"testing"

	"yodleeops/infra"

	"github.com/stretchr/testify/assert"
)

func TestParseOpsFiMetadata(t *testing.T) {
	buckets := infra.Buckets{
		Connections:  infra.CnctBucket,
		Accounts:     infra.AcctBucket,
		Holdings:     infra.HoldBucket,
		Transactions: infra.TxnBucket,
	}
	for _, test := range []struct {
		name        string
		bucket      infra.Bucket
		key         string
		expectError bool
		validateFn  func(t *testing.T, o *OpsFiMetadata)
	}{
		{
			name:   "Connections key valid",
			bucket: infra.CnctBucket,
			key:    "profile1/provider1/party1/2024-01-01T00:00:00Z",
			validateFn: func(t *testing.T, metadata *OpsFiMetadata) {
				assert.Equal(t, "profile1", metadata.ProfileID)
				assert.Equal(t, "provider1", metadata.ProviderAccountID)
				assert.Equal(t, "party1", metadata.PartyIDTypeCd)
				assert.NotZero(t, metadata.LastUpdated)
			},
		},
		{
			name:        "Connections invalid token count",
			bucket:      buckets.Connections,
			key:         "too/few",
			expectError: true,
		},
		{
			name:   "Accounts key valid",
			bucket: buckets.Accounts,
			key:    "profile1/provider1/party1/account1/2024-01-01T00:00:00Z",
			validateFn: func(t *testing.T, metadata *OpsFiMetadata) {
				assert.Equal(t, "profile1", metadata.ProfileID)
				assert.Equal(t, "provider1", metadata.ProviderAccountID)
				assert.Equal(t, "party1", metadata.PartyIDTypeCd)
				assert.Equal(t, "account1", metadata.AccountID)
			},
		},
		{
			name:        "Invalid bucket",
			bucket:      "invalid",
			key:         "anything",
			expectError: true,
		},
		{
			name:        "Invalid timestamp",
			bucket:      buckets.Connections,
			key:         "profile/provider/party/bad-timestamp",
			expectError: true,
		},
		{
			name:   "Holdings key valid",
			bucket: buckets.Holdings,
			key:    "profile1/party1/account1/element1/2024-01-01T00:00:00Z",
			validateFn: func(t *testing.T, metadata *OpsFiMetadata) {
				assert.Equal(t, "profile1", metadata.ProfileID)
				assert.Equal(t, "party1", metadata.PartyIDTypeCd)
				assert.Equal(t, "account1", metadata.AccountID)
				assert.Equal(t, "element1", metadata.ElementID)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			meta := &OpsFiMetadata{}
			err := meta.ParseOpsFiMetadata(buckets, test.bucket, test.key)

			if test.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			if test.validateFn != nil {
				test.validateFn(t, meta)
			}
		})
	}
}
