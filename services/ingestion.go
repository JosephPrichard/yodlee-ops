package svc

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log/slog"
	"sync"
)

// ingest enrichments. for each message type, we serialize the records directly into s3. we skip messages that cannot be serialized
// we return any failed insertions so they can be reinserted back into the queue

func IngestCnctEnrichments(ctx context.Context, app *App, cncts []ExtnCnctEnrichment) []PutError[ExtnCnctEnrichment] {
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

	return PutObjects(ctx, app, app.CnctBucket, putList)()
}

func IngestAcctEnrichments(ctx context.Context, app *App, accts []ExtnAcctEnrichment) []PutError[ExtnAcctEnrichment] {
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

	return PutObjects(ctx, app, app.AcctBucket, putList)()
}

func IngestHoldEnrichments(ctx context.Context, app *App, holds []ExtnHoldEnrichment) []PutError[ExtnHoldEnrichment] {
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

	return PutObjects(ctx, app, app.HoldBucket, putList)()
}

func IngestTxnEnrichments(ctx context.Context, app *App, txns []ExtnTxnEnrichment) []PutError[ExtnTxnEnrichment] {
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

	return PutObjects(ctx, app, app.TxnBucket, putList)()
}

type RefreshResult[Input any] struct {
	PutErrs    []PutError[Input]
	DeleteErrs []DeleteError
}

// ingest refreshes. for each message type, we serialize the records directly into s3 OR delete if the refresh is marked as deleted
// handles failed uploads in the same way as enrichments

func IngestCnctRefreshes(ctx context.Context, app *App, cncts []ExtnCnctRefresh) RefreshResult[ExtnCnctRefresh] {
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

	getPutCall := PutObjects(ctx, app, app.CnctBucket, putList) // async
	deleteErrs := app.DeleteCncts(ctx, removeCnctKeys)

	return RefreshResult[Input]{PutErrs: getPutCall(), DeleteErrs: deleteErrs}
}

func IngestAcctsRefreshes(ctx context.Context, app *App, accts []ExtnAcctRefresh) RefreshResult[ExtnAcctRefresh] {
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

	getPutCall := PutObjects(ctx, app, app.AcctBucket, putList) // async
	deleteErrs := app.DeleteAccts(ctx, removeAcctKeys)

	return RefreshResult[Input]{PutErrs: getPutCall(), DeleteErrs: deleteErrs}
}

func IngestHoldRefreshes(ctx context.Context, app *App, holds []ExtnHoldRefresh) RefreshResult[ExtnHoldRefresh] {
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

	getPutCall := PutObjects(ctx, app, app.HoldBucket, putList) // async
	deleteErrs := app.DeletePrefixes(ctx, app.HoldBucket, holdPrefixes)

	return RefreshResult[Input]{PutErrs: getPutCall(), DeleteErrs: deleteErrs}
}

func IngestTxnRefreshes(ctx context.Context, app *App, txns []ExtnTxnRefresh) RefreshResult[ExtnTxnRefresh] {
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

	getPutCall := PutObjects(ctx, app, app.TxnBucket, putList)
	deleteErrs := app.DeletePrefixes(ctx, app.TxnBucket, txnPrefixes)

	return RefreshResult[Input]{PutErrs: getPutCall(), DeleteErrs: deleteErrs}
}

type PutInput[Input any] struct {
	Key   string
	Input Input
	Body  []byte
}

func (o PutInput[Input]) String() string {
	return o.Key
}

type PutError[Input any] struct {
	Key    string
	Origin Input
	Err    error
}

func (e PutError[Input]) Error() string {
	return fmt.Sprintf("failed to put object to s3: key=%s, input=%v, err=%v", e.Key, e.Origin, e.Err)
}

// PutObjects uploads objects to a given bucket async and sends any failed uploads through the return channel
func PutObjects[Input any](ctx context.Context, app *App, bucket string, inputObjects []PutInput[Input]) func() []PutError[Input] {
	var errs []PutError[Input]
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
				errs = append(errs, PutError[Input]{Key: object.Key, Origin: object.Input, Err: err})
				mu.Unlock()
			} else {
				slog.InfoContext(ctx, "uploaded object to s3", "bucket", bucket, "key", object.Key, "output", putOutput)
			}
		}()
	}

	return func() []PutError[Input] {
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

func MakeAcctPrefixTable() AcctPrefixTable {
	return AcctPrefixTable{m: make(map[string]bool)}
}

func (b *AcctPrefixTable) insertObjectIDs(ctx context.Context, acctObjectIDs []s3types.ObjectIdentifier) {
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
		acctPrefixStr := AcctMemberPrefix{PrtyId: acctKey.PrtyId, PrtyIdTypeCd: acctKey.PrtyIdTypeCd, AcctID: acctKey.AcctID}.String()

		b.lock.Lock()
		b.m[acctPrefixStr] = true
		b.lock.Unlock()
	}
}

// DeleteSupervisor is a block of state to control the goroutines executing concurrent delete jobs
// it stores errors any deletes run into (protected with a lock) and a wait group for coordinating job execution
type DeleteSupervisor struct {
	Context context.Context
	app     *App

	deleteErrs []DeleteError
	lock       sync.Mutex

	wg sync.WaitGroup
}

func MakeDeleteSupervisor(ctx context.Context, app *App) DeleteSupervisor {
	return DeleteSupervisor{Context: ctx, app: app}
}

func (ds *DeleteSupervisor) Execute(bucket string, listIDsChan chan []s3types.ObjectIdentifier) {
	slog.InfoContext(ds.Context, "begin an execute delete worker", "bucket", bucket)

	ds.wg.Add(1)
	go func() {
		defer ds.wg.Done()
		for objectIDs := range listIDsChan {
			if deleteErr := ds.app.DeleteObjects(ds.Context, bucket, objectIDs); deleteErr.Err != nil {
				ds.lock.Lock()
				ds.deleteErrs = append(ds.deleteErrs, deleteErr)
				ds.lock.Unlock()
			}
		}
	}()
}

func (ds *DeleteSupervisor) Wait() []DeleteError {
	ds.wg.Wait()
	if len(ds.deleteErrs) == 0 {
		slog.InfoContext(ds.Context, "finished delete supervisor")
	} else {
		slog.ErrorContext(ds.Context, "finished delete supervisor", "errs", ds.deleteErrs)
	}
	return ds.deleteErrs
}

// DeleteCncts deletes all records with the following account keys from all parts of s3.
// blocks until all goroutines are complete, while logs any failed delete calls.
func (app *App) DeleteCncts(ctx context.Context, keys []CnctKey) []DeleteError {
	if len(keys) == 0 {
		slog.InfoContext(ctx, "no cncts to delete")
		return nil
	}

	cnctPrefixes := make(map[string]bool)
	for _, key := range keys {
		prefix := CnctPrefix{PrtyId: key.PrtyId, PrtyIdTypeCd: key.PrtyIdTypeCd, CnctID: key.CnctID}
		cnctPrefixes[prefix.String()] = true
	}
	slog.InfoContext(ctx, "deleting cncts", "cnctPrefixes", cnctPrefixes)

	deletes := MakeDeleteSupervisor(ctx, app)

	for cnctPrefix := range cnctPrefixes {
		deletes.Execute(app.CnctBucket, app.ListObjectsByPrefix(ctx, app.CnctBucket, cnctPrefix))
	}

	// in addition to deleting each cnct by prefix, we need to parse the acctID and create an acctPrefix to delete txns and holdings.
	var listAcctsWg sync.WaitGroup
	acctPrefixes := MakeAcctPrefixTable()

	for cnctPrefix := range cnctPrefixes {
		listIDsChan := app.ListObjectsByPrefix(ctx, app.AcctBucket, cnctPrefix)
		deleteIDsChan := make(chan []s3types.ObjectIdentifier)

		listAcctsWg.Add(1)
		go func() {
			// intercept acct IDs and pipeline the delete operations. we use acct IDs before deletes are finished
			defer listAcctsWg.Done()
			defer close(deleteIDsChan)

			for acctObjectIDs := range listIDsChan {
				acctPrefixes.insertObjectIDs(ctx, acctObjectIDs)
				deleteIDsChan <- acctObjectIDs
			}
		}()

		deletes.Execute(app.AcctBucket, deleteIDsChan)
	}

	listAcctsWg.Wait()

	for acctPrefix := range acctPrefixes.m {
		for _, bucket := range []string{
			app.HoldBucket,
			app.TxnBucket,
		} {
			deletes.Execute(bucket, app.ListObjectsByPrefix(ctx, bucket, acctPrefix))
		}
	}

	return deletes.Wait()
}

// DeleteAccts deletes all records with the following cnct keys from all parts s3.
// blocks until all goroutines are complete, while logs any failed delete calls.
func (app *App) DeleteAccts(ctx context.Context, keys []AcctKey) []DeleteError {
	if len(keys) == 0 {
		slog.InfoContext(ctx, "no accts to delete")
		return nil
	}

	acctMemPrefixes := make(map[string]bool)
	acctPrefixes := make(map[string]bool)
	for _, key := range keys {
		memPrefix := AcctMemberPrefix{PrtyId: key.PrtyId, PrtyIdTypeCd: key.PrtyIdTypeCd, AcctID: key.AcctID}
		prefix := AcctPrefix{PrtyId: key.PrtyId, PrtyIdTypeCd: key.PrtyIdTypeCd, CnctID: key.CnctID, AcctID: key.AcctID}
		acctMemPrefixes[memPrefix.String()] = true
		acctPrefixes[prefix.String()] = true
	}
	slog.InfoContext(ctx, "deleting accts", "acctMemPrefixes", acctMemPrefixes, "acctPrefixes", acctPrefixes)

	deletes := MakeDeleteSupervisor(ctx, app)

	for acctMemPrefix := range acctMemPrefixes {
		for _, bucket := range []string{
			app.HoldBucket,
			app.TxnBucket,
		} {
			deletes.Execute(bucket, app.ListObjectsByPrefix(ctx, bucket, acctMemPrefix))
		}
	}
	for acctPrefix := range acctPrefixes {
		deletes.Execute(app.AcctBucket, app.ListObjectsByPrefix(ctx, app.AcctBucket, acctPrefix))
	}

	return deletes.Wait()
}

// DeletePrefixes generically deletes from a single bucket by prefixes. used for transactions and holdings.
func (app *App) DeletePrefixes(ctx context.Context, bucket string, prefixes map[string]bool) []DeleteError {
	if len(prefixes) == 0 {
		slog.InfoContext(ctx, "no prefixes to delete")
		return nil
	}

	slog.InfoContext(ctx, "deleting objects by prefixes", "bucket", bucket, "prefixes", prefixes)

	deletes := MakeDeleteSupervisor(ctx, app)

	for prefix := range prefixes {
		deletes.Execute(bucket, app.ListObjectsByPrefix(ctx, bucket, prefix))
	}

	return deletes.Wait()
}

// ListObjectsByPrefix lists all object keys for a certain prefix in a bucket and streams each page of data through a channel as they come.
// aws s3 API does not support multiple buckets/prefixes per call, so each bucket prefix needs its own api call.
func (app *App) ListObjectsByPrefix(ctx context.Context, bucket string, prefix string) chan []s3types.ObjectIdentifier {
	keysChan := make(chan []s3types.ObjectIdentifier)

	// paginate through all objects under the given prefix for a bucket and send each page to the channel. closes when all pages have been walked
	go func() {
		defer close(keysChan)

		paginator := s3.NewListObjectsV2Paginator(app.S3Client, &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(prefix),
			MaxKeys: app.PageLength,
		})

		page := 0

		for paginator.HasMorePages() {
			page++
			listObjects, err := paginator.NextPage(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "failed to list objects from s3", "bucket", bucket, "prefix", prefix, "page", page, "err", err)
				return
			}

			slog.InfoContext(ctx, "list objects", "bucket", bucket, "prefix", prefix, "page", page, "objectCount", len(listObjects.Contents))

			if len(listObjects.Contents) == 0 {
				continue
			}

			keys := make([]s3types.ObjectIdentifier, 0, len(listObjects.Contents))
			for _, obj := range listObjects.Contents {
				keys = append(keys, s3types.ObjectIdentifier{
					Key: obj.Key,
				})
			}
			keysChan <- keys
		}
	}()

	return keysChan
}

type DeleteError struct {
	Keys []string
	Err  error
}

func (e DeleteError) Error() string {
	return fmt.Sprintf("failed to delete object from s3: keys=%+v, err=%v", e.Keys, e.Err)
}

// DeleteObjects is a helper to delete all keys from a bucket and log
// if a deletion call fails, we should log and continue execution, but we rely on another refresh to come in and actually delete these records
func (app *App) DeleteObjects(ctx context.Context, bucket string, keys []s3types.ObjectIdentifier) DeleteError {
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

	return DeleteError{Keys: strKeys, Err: err}
}
