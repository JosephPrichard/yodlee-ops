package svc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"sync"
	"time"
)

func IngestErrorLog(ctx context.Context, errorLog ErrorLog) error {
	errorLogEntity := ErrorLogEntity{
		ErrMsg: errorLog.ErrMsg,
	}

	key := time.Now().Format(time.RFC3339)

	value, err := json.Marshal(errorLogEntity)
	if err != nil {
		// log and fail silently, since we want to consumer to commit since we *know* this error means this error log is malformed and should be removed from the queue
		slog.ErrorContext(ctx, "failed to marshal error log entity", "err", err)
		return nil
	}

	if _, err = app.S3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(app.ErrorLogBucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(value),
	}); err != nil {
		return fmt.Errorf("failed to put error log entity to s3: %w", err)
	}

	return nil
}

func ProducePutErrors[Input any](ctx context.Context, originTopic string, putErrors []IngestError[Input]) {
	var msgs []kafka.Message
	for _, putError := range putErrors {
		errorLogBytes, loggingErr := json.Marshal(ErrorLog{ErrMsg: putError.Err.Error()})
		inputBytes, inputErr := json.Marshal(putError.Origin)

		if err := errors.Join(loggingErr, inputErr); err != nil {
			slog.ErrorContext(ctx, "failed to marshal messages for put errors", "err", err)
			continue
		}

		msgs = append(msgs, kafka.Message{
			Topic: app.ErrorLogTopic,
			Value: errorLogBytes,
		}, kafka.Message{
			Topic: originTopic,
			Value: inputBytes,
		})
	}
	if len(msgs) > 0 {
		if err := app.Producer.WriteMessages(ctx, msgs...); err != nil {
			// if we aren't able to put these messages, we need to drop them. this is already a "last line" of defense for errors that shouldn't have happened
			slog.ErrorContext(ctx, "failed to write put errors to kafka", "err", err)
		}
	}
}

// ingest enrichments. for each message type, we serialize the records directly into s3. we skip messages that cannot be serialized
// if we fail to upload any messages to s3, we will naively retry the entire enrichment message

func IngestCnctEnrichments(ctx context.Context, cncts []ExtnCnctEnrichment) []IngestError[ExtnCnctEnrichment] {
	type Input = ExtnCnctEnrichment
	var putList []PutInput[Input]

	for _, cnct := range cncts {
		body, err := MarshalExtnCnctEnrichment(cnct)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal cnct enrichment", "err", err)
			continue
		}
		key := CnctKey{PrtyId: cnct.PrtyId, PrtyIdTypeCd: cnct.PrtyIdTypeCd, CnctID: cnct.ExtnCnctId, BusDt: cnct.BusDt}
		putList = append(putList, PutInput[Input]{Key: key.String(), Input: cnct, Body: body})
	}

	return PutObjects(ctx, app.CnctBucket, putList)()
}

func IngestAcctEnrichments(ctx context.Context, accts []ExtnAcctEnrichment) []IngestError[ExtnAcctEnrichment] {
	type Input = ExtnAcctEnrichment
	var putList []PutInput[Input]

	for _, acct := range accts {
		body, err := MarshalExtnAcctEnrichment(acct)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal acct enrichment", "err", err)
			continue
		}
		key := AcctKey{PrtyId: acct.PrtyId, PrtyIdTypeCd: acct.PrtyIdTypeCd, CnctID: acct.ExtnCnctId, AcctID: acct.ExtnAcctId, BusDt: acct.BusDt}
		putList = append(putList, PutInput[Input]{Key: key.String(), Input: acct, Body: body})
	}

	return PutObjects(ctx, app.CnctBucket, putList)()
}

func IngestHoldEnrichments(ctx context.Context, holds []ExtnHoldEnrichment) []IngestError[ExtnHoldEnrichment] {
	type Input = ExtnHoldEnrichment
	var putList []PutInput[Input]

	for _, hold := range holds {
		body, err := MarshalExtnHoldEnrichment(hold)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal hold enrichment", "err", err)
			continue
		}
		key := HoldKey{PrtyId: hold.PrtyId, PrtyIdTypeCd: hold.PrtyIdTypeCd, AcctID: hold.ExtnAcctId, HoldID: hold.ExtnHoldId, BusDt: hold.BusDt}
		putList = append(putList, PutInput[Input]{Key: key.String(), Input: hold, Body: body})
	}

	return PutObjects(ctx, app.CnctBucket, putList)()
}

func IngestTxnEnrichments(ctx context.Context, txns []ExtnTxnEnrichment) []IngestError[ExtnTxnEnrichment] {
	type Input = ExtnTxnEnrichment
	var putList []PutInput[Input]

	for _, txn := range txns {
		body, err := MarshalExtnTxnEnrichment(txn)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal txn enrichment", "err", err)
			continue
		}
		key := TxnKey{PrtyId: txn.PrtyId, PrtyIdTypeCd: txn.PrtyIdTypeCd, AcctID: txn.ExtnAcctId, TxnID: txn.ExtnTxnId, TxnDt: txn.TxnDt}
		putList = append(putList, PutInput[Input]{Key: key.String(), Input: txn, Body: body})
	}

	return PutObjects(ctx, app.CnctBucket, putList)()
}

// ingest refreshes. for each message type, we serialize the records directly into s3 OR delete if the refresh is marked as deleted
// handles failed uploads in the same way as enrichments

func IngestCnctRefreshes(ctx context.Context, cncts []ExtnCnctRefresh) []IngestError[ExtnCnctRefresh] {
	type Input = ExtnCnctRefresh
	var putList []PutInput[Input]
	var removeCnctKeys []CnctKey

	for _, cnct := range cncts {
		key := CnctKey{PrtyId: cnct.PrtyId, PrtyIdTypeCd: cnct.PrtyIdTypeCd, CnctID: cnct.ExtnCnctId, BusDt: cnct.BusDt}
		if cnct.IsDeleted {
			removeCnctKeys = append(removeCnctKeys, key)
		} else {
			body, err := MarshalExtnCnctRefresh(cnct)
			if err != nil {
				slog.ErrorContext(ctx, "failed to marshal cnct refresh", "err", err)
				continue
			}
			putList = append(putList, PutInput[Input]{Key: key.String(), Body: body})
		}
	}

	getPutCall := PutObjects(ctx, app.CnctBucket, putList) // async
	DeleteCncts(ctx, removeCnctKeys)

	return getPutCall()
}

func IngestAcctsRefreshes(ctx context.Context, accts []ExtnAcctRefresh) []IngestError[ExtnAcctRefresh] {
	type Input = ExtnAcctRefresh
	var putList []PutInput[Input]
	var removeAcctKeys []AcctKey

	for _, acct := range accts {
		key := AcctKey{PrtyId: acct.PrtyId, PrtyIdTypeCd: acct.PrtyIdTypeCd, CnctID: acct.ExtnCnctId, AcctID: acct.ExtnAcctId, BusDt: acct.BusDt}
		if acct.IsDeleted {
			removeAcctKeys = append(removeAcctKeys, key)
		} else {
			body, err := MarshalExtnAcctRefresh(acct)
			if err != nil {
				slog.ErrorContext(ctx, "failed to marshal acct refresh", "err", err)
				continue
			}
			putList = append(putList, PutInput[Input]{Key: key.String(), Body: body})
		}
	}

	getPutCall := PutObjects(ctx, app.AcctBucket, putList) // async
	DeleteAccts(ctx, removeAcctKeys)

	return getPutCall()
}

func IngestHoldRefreshes(ctx context.Context, holds []ExtnHoldRefresh) []IngestError[ExtnHoldRefresh] {
	type Input = ExtnHoldRefresh
	var putList []PutInput[Input]
	holdPrefixes := make(map[string]bool)

	for _, hold := range holds {
		if hold.IsDeleted {
			prefix := AcctChildPrefix{PrtyId: hold.PrtyId, PrtyIdTypeCd: hold.PrtyIdTypeCd, AcctID: hold.ExtnAcctId, ChildID: hold.ExtnHoldId}
			holdPrefixes[prefix.String()] = true
		} else {
			body, err := MarshalExtnHoldRefresh(hold)
			if err != nil {
				slog.ErrorContext(ctx, "failed to marshal holding refresh", "err", err)
				continue
			}
			key := HoldKey{PrtyId: hold.PrtyId, PrtyIdTypeCd: hold.PrtyIdTypeCd, AcctID: hold.ExtnAcctId, HoldID: hold.ExtnHoldId, BusDt: hold.BusDt}
			putList = append(putList, PutInput[Input]{Key: key.String(), Body: body})
		}
	}

	getPutCall := PutObjects(ctx, app.HoldBucket, putList) // async
	DeletePrefixes(ctx, app.HoldBucket, holdPrefixes)

	return getPutCall()
}

func IngestTxnRefreshes(ctx context.Context, txns []ExtnTxnRefresh) []IngestError[ExtnTxnRefresh] {
	type Input = ExtnTxnRefresh
	var putList []PutInput[Input]
	txnPrefixes := make(map[string]bool)

	for _, txn := range txns {
		if txn.IsDeleted {
			prefix := AcctChildPrefix{PrtyId: txn.PrtyId, PrtyIdTypeCd: txn.PrtyIdTypeCd, AcctID: txn.ExtnAcctId, ChildID: txn.ExtnTxnId}
			txnPrefixes[prefix.String()] = true
		} else {
			body, err := MarshalExtnTxnRefresh(txn)
			if err != nil {
				slog.ErrorContext(ctx, "failed to marshal txn refresh", "err", err)
				continue
			}
			key := TxnKey{PrtyId: txn.PrtyId, PrtyIdTypeCd: txn.PrtyIdTypeCd, AcctID: txn.ExtnAcctId, TxnID: txn.ExtnTxnId, TxnDt: txn.TxnDt}
			putList = append(putList, PutInput[Input]{Key: key.String(), Body: body})
		}
	}

	getPutCall := PutObjects(ctx, app.TxnBucket, putList)
	DeletePrefixes(ctx, app.TxnBucket, txnPrefixes)

	return getPutCall()
}

type PutInput[Input any] struct {
	Key   string
	Input Input
	Body  []byte
}

func (o PutInput[Input]) String() string {
	return o.Key
}

type IngestError[Input any] struct {
	Key    string
	Origin Input
	Err    error
}

func (e IngestError[Input]) Error() string {
	return fmt.Sprintf("failed to put object to s3: key=%s, input=%v, err=%v", e.Key, e.Origin, e.Err)
}

// PutObjects uploads objects to a given bucket async and sends any failed uploads through the return channel
func PutObjects[Input any](ctx context.Context, bucket string, inputObjects []PutInput[Input]) func() []IngestError[Input] {
	var errs []IngestError[Input]
	var mu sync.Mutex

	var wg sync.WaitGroup

	for _, object := range inputObjects {
		wg.Add(1)
		go func() {
			defer wg.Done()
			putOutput, err := app.S3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(object.Key),
				Body:   bytes.NewReader(object.Body),
			})
			if err != nil {
				slog.ErrorContext(ctx, "failed to upload object to s3", "key", object.Key, "bucket", bucket, "err", err)
				mu.Lock()
				errs = append(errs, IngestError[Input]{Key: object.Key, Origin: object.Input, Err: err})
				mu.Unlock()
			} else {
				slog.InfoContext(ctx, "uploaded object to s3", "bucket", bucket, "key", object.Key, "output", putOutput)
			}
		}()
	}

	return func() []IngestError[Input] {
		wg.Wait()
		if len(errs) == 0 {
			slog.InfoContext(ctx, "finished put objects call")
		} else {
			slog.ErrorContext(ctx, "finished put objects call", "errs", errs)
		}
		return errs
	}
}

type AcctPrefixTable struct {
	m    map[string]bool
	lock sync.Mutex
}

func (b *AcctPrefixTable) putObjectIDs(ctx context.Context, acctObjectIDs []s3types.ObjectIdentifier) {
	for _, acctObjectID := range acctObjectIDs {
		if acctObjectID.Key == nil {
			continue
		}
		acctKeyStr := *acctObjectID.Key
		acctKey, err := ParseAcctKey(acctKeyStr)
		if err != nil {
			slog.ErrorContext(ctx, "failed to parse acct key from s3 object key", "acctKeyStr", acctKeyStr, "err", err)
			continue
		}
		acctPrefixStr := AcctPrefix{PrtyId: acctKey.PrtyId, PrtyIdTypeCd: acctKey.PrtyIdTypeCd, AcctID: acctKey.AcctID}.String()

		b.lock.Lock()
		b.m[acctPrefixStr] = true
		b.lock.Unlock()
	}
}

// DeleteCncts deletes all records with the following account keys from all parts of s3.
// blocks until all goroutines are complete, while logs any failed delete calls.
func DeleteCncts(ctx context.Context, keys []CnctKey) {
	cnctPrefixes := make(map[string]bool)
	for _, key := range keys {
		prefix := CnctPrefix{PrtyId: key.PrtyId, PrtyIdTypeCd: key.PrtyIdTypeCd, CnctID: key.CnctID}
		cnctPrefixes[prefix.String()] = true
	}
	slog.InfoContext(ctx, "deleting cncts", "cnctPrefixes", cnctPrefixes)

	var deleteWg sync.WaitGroup

	for cnctPrefix := range cnctPrefixes {
		DeletePrefixWorker(ctx, &deleteWg, app.CnctBucket, cnctPrefix)
	}

	// in addition to deleting each cnct by prefix, we need to parse the acctID and create an acctPrefix to delete txns and holdings.
	var listAcctsWg sync.WaitGroup
	var acctPrefixes AcctPrefixTable

	for cnctPrefix := range cnctPrefixes {
		listIDsChan := ListObjectsByPrefix(ctx, app.AcctBucket, cnctPrefix)
		deleteIDsChan := make(chan []s3types.ObjectIdentifier)

		listAcctsWg.Add(1)
		go func() {
			// intercept acct IDs and pipeline the delete operations. we use acct IDs before deletes are finished
			defer listAcctsWg.Done()
			defer close(deleteIDsChan)

			for acctObjectIDs := range listIDsChan {
				acctPrefixes.putObjectIDs(ctx, acctObjectIDs)
				deleteIDsChan <- acctObjectIDs
			}
		}()

		deleteWg.Add(1)
		go func() {
			defer deleteWg.Done()
			for acctObjectIDs := range deleteIDsChan {
				DeleteObjects(ctx, app.AcctBucket, acctObjectIDs)
			}
		}()
	}

	listAcctsWg.Wait()

	for acctPrefix := range acctPrefixes.m {
		for _, bucket := range []string{
			app.HoldBucket,
			app.TxnBucket,
		} {
			DeletePrefixWorker(ctx, &deleteWg, bucket, acctPrefix)
		}
	}

	deleteWg.Wait()
}

// DeleteAccts deletes all records with the following cnct keys from all parts s3.
// blocks until all goroutines are complete, while logs any failed delete callapp.
func DeleteAccts(ctx context.Context, keys []AcctKey) {
	acctPrefixes := make(map[string]bool)
	for _, key := range keys {
		prefix := AcctPrefix{PrtyId: key.PrtyId, PrtyIdTypeCd: key.PrtyIdTypeCd, AcctID: key.AcctID}
		acctPrefixes[prefix.String()] = true
	}
	slog.InfoContext(ctx, "deleting accts", "acctPrefixes", acctPrefixes)

	var deleteWg sync.WaitGroup

	for acctPrefix := range acctPrefixes {
		for _, bucket := range []string{
			app.AcctBucket,
			app.HoldBucket,
			app.TxnBucket,
		} {
			DeletePrefixWorker(ctx, &deleteWg, bucket, acctPrefix)
		}
	}

	deleteWg.Wait()
}

// DeletePrefixes generically deletes from a single bucket by prefixes. used for transactions and holdings.
func DeletePrefixes(ctx context.Context, bucket string, prefixes map[string]bool) {
	slog.InfoContext(ctx, "deleting objects by prefixes", "bucket", bucket, "prefixes", prefixes)

	var wg sync.WaitGroup
	for prefix := range prefixes {
		DeletePrefixWorker(ctx, &wg, bucket, prefix)
	}
	wg.Wait()
}

func DeletePrefixWorker(ctx context.Context, wg *sync.WaitGroup, bucket string, prefix string) {
	slog.InfoContext(ctx, "deleting objects by prefix", "bucket", bucket, "prefix", prefix)

	listIDsChan := ListObjectsByPrefix(ctx, bucket, prefix)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for objectIDs := range listIDsChan {
			DeleteObjects(ctx, bucket, objectIDs)
		}
	}()
}

// ListObjectsByPrefix lists all object keys for a certain prefix in a bucket and streams each page of data through a channel as they come.
// aws s3 API does not support multiple buckets/prefixes per call, so each bucket prefix needs its own api call.
func ListObjectsByPrefix(ctx context.Context, bucket string, prefix string) chan []s3types.ObjectIdentifier {
	keysChan := make(chan []s3types.ObjectIdentifier)

	// paginate through all objects under the given prefix for a bucket and send each page to the channel. closes when all pages have been walked
	go func() {
		defer close(keysChan)
		var continuationToken *string
		var isLastPage bool
		var page int

		for !isLastPage {
			page++

			listObjects, err := app.S3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket:            aws.String(bucket),
				Prefix:            aws.String(prefix),
				MaxKeys:           app.PageLength,
				ContinuationToken: continuationToken,
			})
			if err != nil {
				slog.ErrorContext(ctx, "failed to list objects from s3", "bucket", bucket, "prefix", prefix, "isLastPage", isLastPage, "page", page, "err", err)
				return
			}

			isLastPage = listObjects.IsTruncated == nil || !*listObjects.IsTruncated
			continuationToken = listObjects.NextContinuationToken

			slog.InfoContext(ctx, "list objects", "bucket", bucket, "prefix", prefix, "isLastPage", isLastPage, "page", page, "listObjects", listObjects.Contents)

			var keys []s3types.ObjectIdentifier
			for _, obj := range listObjects.Contents {
				keys = append(keys, s3types.ObjectIdentifier{Key: obj.Key})
			}
			keysChan <- keys
		}
	}()

	return keysChan
}

// DeleteObjects is a helper to delete all keys from a bucket and log
// if a deletion call fails, we should log and continue execution, but we rely on another refresh to come in and actually delete these records
func DeleteObjects(ctx context.Context, bucket string, keys []s3types.ObjectIdentifier) {
	strKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if key.Key == nil {
			continue
		}
		strKeys = append(strKeys, *key.Key)
	}

	_, err := app.S3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3types.Delete{Objects: keys},
	})
	if err != nil {
		slog.ErrorContext(ctx, "failed to delete objects from s3", "bucket", bucket, "keys", strKeys, "err", err)
	} else {
		slog.InfoContext(ctx, "deleted objects from s3", "bucket", bucket, "keys", strKeys)
	}
}
