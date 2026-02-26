package svc

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"yodleeops/internal/infra"
)

func TestParseOpsFiMetadata(t *testing.T) {
	buckets := infra.S3Buckets{
		CnctBucket: infra.CnctBucket,
		AcctBucket: infra.AcctBucket,
		HoldBucket: infra.HoldBucket,
		TxnBucket:  infra.TxnBucket,
	}
	for _, test := range []struct {
		name        string
		bucket      infra.Bucket
		key         string
		expectError bool
		validateFn  func(t *testing.T, o *OpsFiMetadata)
	}{
		{
			name:   "CnctBucket valid",
			bucket: infra.CnctBucket,
			key:    "profile1/provider1/party1/2024-01-01T00:00:00Z",
			validateFn: func(t *testing.T, o *OpsFiMetadata) {
				assert.Equal(t, "profile1", o.ProfileID)
				assert.Equal(t, "provider1", o.ProviderAccountID)
				assert.Equal(t, "party1", o.PartyIDTypeCd)
				assert.NotZero(t, o.LastUpdated)
			},
		},
		{
			name:        "CnctBucket invalid token count",
			bucket:      buckets.CnctBucket,
			key:         "too/few",
			expectError: true,
		},
		{
			name:   "AcctBucket valid",
			bucket: buckets.AcctBucket,
			key:    "profile1/provider1/party1/account1/2024-01-01T00:00:00Z",
			validateFn: func(t *testing.T, o *OpsFiMetadata) {
				assert.Equal(t, "profile1", o.ProfileID)
				assert.Equal(t, "provider1", o.ProviderAccountID)
				assert.Equal(t, "party1", o.PartyIDTypeCd)
				assert.Equal(t, "account1", o.AccountID)
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
			bucket:      buckets.CnctBucket,
			key:         "profile/provider/party/bad-timestamp",
			expectError: true,
		},
		{
			name:   "HoldBucket valid",
			bucket: buckets.HoldBucket,
			key:    "profile1/party1/account1/element1/2024-01-01T00:00:00Z",
			validateFn: func(t *testing.T, o *OpsFiMetadata) {
				assert.Equal(t, "profile1", o.ProfileID)
				assert.Equal(t, "party1", o.PartyIDTypeCd)
				assert.Equal(t, "account1", o.AccountID)
				assert.Equal(t, "element1", o.ElementID)
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
