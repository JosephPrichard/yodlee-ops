package svc

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ingest enrichments. for each MChan type, we serialize the records directly into s3. we skip messages that cannot be serialized
// we return any failed insertions so they can be reinserted back into the queue

func (app *App) IngestCnctEnrichments(ctx context.Context, cncts []ExtnCnctEnrichment) []PutResult {
	start := time.Now()

	var putList []PutInput

	for _, cnct := range cncts {
		body, err := MarshalExtnCnctEnrichment(cnct)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal cnct enrichment", "err", err)
			continue
		}
		key := CnctKey{PrtyId: cnct.PrtyId, PrtyIdTypeCd: cnct.PrtyIdTypeCd, CnctID: cnct.ExtnCnctId, BusDt: cnct.BusDt}
		putList = append(putList, PutInput{Key: key.String(), Input: cnct, Body: body})
	}

	putErrors := app.PutObjects(ctx, app.CnctBucket, putList)()

	slog.InfoContext(ctx, "finished ingest cnct enrichments", "elapsed", time.Since(start))
	return putErrors
}

func (app *App) IngestAcctEnrichments(ctx context.Context, accts []ExtnAcctEnrichment) []PutResult {
	start := time.Now()

	var putList []PutInput

	for _, acct := range accts {
		body, err := MarshalExtnAcctEnrichment(acct)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal acct enrichment", "err", err)
			continue
		}
		key := AcctKey{PrtyId: acct.PrtyId, PrtyIdTypeCd: acct.PrtyIdTypeCd, CnctID: acct.ExtnCnctId, AcctID: acct.ExtnAcctId, BusDt: acct.BusDt}
		putList = append(putList, PutInput{Key: key.String(), Input: acct, Body: body})
	}

	putErrors := app.PutObjects(ctx, app.AcctBucket, putList)()

	slog.InfoContext(ctx, "finished ingest acct enrichments", "elapsed", time.Since(start))
	return putErrors
}

func (app *App) IngestHoldEnrichments(ctx context.Context, holds []ExtnHoldEnrichment) []PutResult {
	start := time.Now()

	var putList []PutInput

	for _, hold := range holds {
		body, err := MarshalExtnHoldEnrichment(hold)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal hold enrichment", "err", err)
			continue
		}
		key := HoldKey{PrtyId: hold.PrtyId, PrtyIdTypeCd: hold.PrtyIdTypeCd, AcctID: hold.ExtnAcctId, HoldID: hold.ExtnHoldId, BusDt: hold.BusDt}
		putList = append(putList, PutInput{Key: key.String(), Input: hold, Body: body})
	}

	putErrors := app.PutObjects(ctx, app.HoldBucket, putList)()

	slog.InfoContext(ctx, "finished ingest hold enrichments", "elapsed", time.Since(start))
	return putErrors
}

func (app *App) IngestTxnEnrichments(ctx context.Context, txns []ExtnTxnEnrichment) []PutResult {
	start := time.Now()

	var putList []PutInput

	for _, txn := range txns {
		body, err := MarshalExtnTxnEnrichment(txn)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal txn enrichment", "err", err)
			continue
		}
		key := TxnKey{PrtyId: txn.PrtyId, PrtyIdTypeCd: txn.PrtyIdTypeCd, AcctID: txn.ExtnAcctId, TxnID: txn.ExtnTxnId, TxnDt: txn.TxnDt}
		putList = append(putList, PutInput{Key: key.String(), Input: txn, Body: body})
	}

	putErrors := app.PutObjects(ctx, app.TxnBucket, putList)()

	slog.InfoContext(ctx, "finished ingest txn enrichments", "elapsed", time.Since(start))
	return putErrors
}

type RefreshResult struct {
	PutErrors    []PutResult
	DeleteErrors []DeleteResult
}

// ingest refreshes. for each MChan type, we serialize the records directly into s3 OR delete if the refresh is marked as deleted
// handles failed uploads in the same way as enrichments

func (app *App) IngestCnctRefreshes(ctx context.Context, cncts []ExtnCnctRefresh) RefreshResult {
	start := time.Now()

	var putList []PutInput
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
			putList = append(putList, PutInput{Key: key.String(), Input: cnct, Body: body})
		}
	}

	getPutCall := app.PutObjects(ctx, app.CnctBucket, putList) // async
	deleteErrs := app.DeleteCncts(ctx, removeCnctKeys)

	slog.InfoContext(ctx, "finished ingest cnct refreshes", "elapsed", time.Since(start))

	return RefreshResult{PutErrors: getPutCall(), DeleteErrors: deleteErrs}
}

func (app *App) IngestAcctsRefreshes(ctx context.Context, accts []ExtnAcctRefresh) RefreshResult {
	start := time.Now()

	var putList []PutInput
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
			putList = append(putList, PutInput{Key: key.String(), Input: acct, Body: body})
		}
	}

	getPutCall := app.PutObjects(ctx, app.AcctBucket, putList) // async
	deleteErrs := app.DeleteAccts(ctx, removeAcctKeys)

	slog.InfoContext(ctx, "finished ingest accts refreshes", "elapsed", time.Since(start))

	return RefreshResult{PutErrors: getPutCall(), DeleteErrors: deleteErrs}
}

func (app *App) IngestHoldRefreshes(ctx context.Context, holds []ExtnHoldRefresh) RefreshResult {
	start := time.Now()

	type Input = ExtnHoldRefresh
	var putList []PutInput
	holdPrefixes := make(map[Prefix]bool)

	for _, hold := range holds {
		if hold.IsDeleted {
			prefix := AcctChildPrefix{PrtyId: hold.PrtyId, PrtyIdTypeCd: hold.PrtyIdTypeCd, AcctID: hold.ExtnAcctId, ChildID: hold.ExtnHoldId}
			holdPrefixes[Prefix{Value: prefix.String(), Bucket: app.HoldBucket}] = true
		} else {
			body, err := MarshalExtnHoldRefresh(hold)
			if err != nil {
				slog.ErrorContext(ctx, "failed to marshal holding refresh", "err", err)
				continue
			}
			key := HoldKey{PrtyId: hold.PrtyId, PrtyIdTypeCd: hold.PrtyIdTypeCd, AcctID: hold.ExtnAcctId, HoldID: hold.ExtnHoldId, BusDt: hold.BusDt}
			putList = append(putList, PutInput{Key: key.String(), Input: hold, Body: body})
		}
	}

	getPutCall := app.PutObjects(ctx, app.HoldBucket, putList) // async
	deleteErrs := app.DeletePrefixes(ctx, holdPrefixes)

	slog.InfoContext(ctx, "finished ingest hold refreshes", "elapsed", time.Since(start))

	return RefreshResult{PutErrors: getPutCall(), DeleteErrors: deleteErrs}
}

func (app *App) IngestTxnRefreshes(ctx context.Context, txns []ExtnTxnRefresh) RefreshResult {
	start := time.Now()

	var putList []PutInput
	txnPrefixes := make(map[Prefix]bool)

	for _, txn := range txns {
		if txn.IsDeleted {
			prefix := AcctChildPrefix{PrtyId: txn.PrtyId, PrtyIdTypeCd: txn.PrtyIdTypeCd, AcctID: txn.ExtnAcctId, ChildID: txn.ExtnTxnId}
			txnPrefixes[Prefix{Value: prefix.String(), Bucket: app.TxnBucket}] = true
		} else {
			body, err := MarshalExtnTxnRefresh(txn)
			if err != nil {
				slog.ErrorContext(ctx, "failed to marshal txn refresh", "err", err)
				continue
			}
			key := TxnKey{PrtyId: txn.PrtyId, PrtyIdTypeCd: txn.PrtyIdTypeCd, AcctID: txn.ExtnAcctId, TxnID: txn.ExtnTxnId, TxnDt: txn.TxnDt}
			putList = append(putList, PutInput{Key: key.String(), Input: txn, Body: body})
		}
	}

	getPutCall := app.PutObjects(ctx, app.TxnBucket, putList)
	deleteErrs := app.DeletePrefixes(ctx, txnPrefixes)

	slog.InfoContext(ctx, "finished ingest txn refreshes", "elapsed", time.Since(start))

	return RefreshResult{PutErrors: getPutCall(), DeleteErrors: deleteErrs}
}

func (app *App) IngestDeleteRetries(ctx context.Context, deleteRetries []DeleteRetry) []DeleteResult {
	slog.InfoContext(ctx, "begin ingest delete retries", "deleteRetries", deleteRetries)

	deletes := makeDeleteSupervisor(ctx, app)

	prefixes := make(map[Prefix]bool)

	for _, d := range deleteRetries {
		switch d.Kind {
		case ListKind:
			prefixes[Prefix{Bucket: d.Bucket, Value: d.Prefix}] = true
		case DeleteKind:
			objectIDs := make([]s3types.ObjectIdentifier, 0, len(d.Keys))
			for _, key := range d.Keys {
				objectIDs = append(objectIDs, s3types.ObjectIdentifier{Key: aws.String(key)})
			}
			deletes.deleteIDs(d.Bucket, objectIDs)
		}
	}

	slog.InfoContext(ctx, "deleting prefixes for delete retries", "prefixes", prefixes)

	for prefix := range prefixes {
		deletes.deletePages(prefix.Bucket, app.ListObjectsByPrefix(ctx, prefix.Bucket, prefix.Value))
	}

	return deletes.wait()
}

type PutInput struct {
	Key   string
	Input FiInput
	Body  []byte
}

func (o PutInput) String() string {
	return o.Key
}

type PutResult struct {
	Key    string
	Origin FiInput
	Err    error
}

// PutObjects uploads objects to a given bucket async and returns any failed uploads when the task is joined
func (app *App) PutObjects(ctx context.Context, bucket string, inputObjects []PutInput) func() []PutResult {
	if len(inputObjects) == 0 {
		return func() []PutResult { return nil }
	}

	var errs []PutResult
	var mu sync.Mutex

	var wg sync.WaitGroup

	for _, object := range inputObjects {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := app.S3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(object.Key),
				Body:   bytes.NewReader(object.Body),
			})
			if err != nil {
				slog.ErrorContext(ctx, "failed to upload object to s3", "key", object.Key, "bucket", bucket, "err", err)
				mu.Lock()
				errs = append(errs, PutResult{Key: object.Key, Origin: object.Input, Err: err})
				mu.Unlock()
			} else {
				slog.InfoContext(ctx, "uploaded object to s3", "bucket", bucket, "key", object.Key, "bytes", len(object.Body))
			}
		}()
	}

	return func() []PutResult {
		wg.Wait()
		if len(errs) == 0 {
			slog.InfoContext(ctx, "finished put objects call")
		} else {
			slog.ErrorContext(ctx, "finished put objects call", "errs", errs)
		}
		return errs
	}
}

type ListAcctsSupervisor struct {
	context context.Context
	table   map[string]bool
	lock    sync.Mutex
	wg      sync.WaitGroup
}

func makeListAcctsSupervisor(ctx context.Context) ListAcctsSupervisor {
	return ListAcctsSupervisor{context: ctx, table: make(map[string]bool)}
}

func (ls *ListAcctsSupervisor) interceptListedPrefixes(listIDsChan chan ListResult) chan ListResult {
	pipeIDsChan := make(chan ListResult)
	ls.wg.Add(1)
	go func() {
		defer ls.wg.Done()
		defer close(pipeIDsChan) // nothing more to pipe once there is nothing more to receive.

		for listResult := range listIDsChan {
			if listResult.Err == nil {
				// intercept and store listed keys.
				for _, acctObjectID := range listResult.Keys {
					if acctObjectID.Key == nil {
						continue
					}
					acctKeyStr := *acctObjectID.Key
					acctKey, err := ParseAcctKey(acctKeyStr)
					if err != nil {
						slog.ErrorContext(ls.context, "failed to parse acct key from s3 object key", "acctKeyStr", acctKeyStr, "err", err)
						continue
					}
					acctPrefixStr := AcctMemberPrefix{PrtyId: acctKey.PrtyId, PrtyIdTypeCd: acctKey.PrtyIdTypeCd, AcctID: acctKey.AcctID}.String()

					ls.lock.Lock()
					ls.table[acctPrefixStr] = true
					ls.lock.Unlock()
				}
			}
			// always pipeline regardless of the list acct is an error or not. (we handle errors later)
			pipeIDsChan <- listResult
		}
	}()
	return pipeIDsChan
}

func (ls *ListAcctsSupervisor) wait() map[string]bool {
	ls.wg.Wait()
	slog.InfoContext(ls.context, "finished list accts supervisor", "acctPrefixTable", ls.table)
	return ls.table
}

// DeleteSupervisor is a block of state to control the goroutines executing concurrent delete jobs
// it stores errors any deletes run into (protected with a lock) and a wait group for coordinating job execution
type DeleteSupervisor struct {
	context    context.Context
	app        *App
	deleteErrs []DeleteResult
	lock       sync.Mutex
	wg         sync.WaitGroup
}

func makeDeleteSupervisor(ctx context.Context, app *App) DeleteSupervisor {
	return DeleteSupervisor{context: ctx, app: app}
}

func (ds *DeleteSupervisor) addResult(deleteResult DeleteResult) {
	if deleteResult.Err == nil {
		return
	}
	ds.lock.Lock()
	ds.deleteErrs = append(ds.deleteErrs, deleteResult)
	ds.lock.Unlock()
}

func (ds *DeleteSupervisor) deletePages(bucket string, listIDsChan chan ListResult) {
	slog.InfoContext(ds.context, "begin an execute delete worker", "bucket", bucket)

	ds.wg.Add(1)
	go func() {
		defer ds.wg.Done()
		for listResult := range listIDsChan {
			var deleteResult DeleteResult

			if listResult.Err != nil {
				deleteResult = DeleteResult{Bucket: listResult.Bucket, Prefix: listResult.Prefix, Err: listResult.Err}
			} else {
				deleteResult = ds.app.DeleteObjects(ds.context, bucket, listResult.Keys)
			}

			ds.addResult(deleteResult)
		}
	}()
}

type DeleteChunk struct {
	Bucket string
	Keys   []s3types.ObjectIdentifier
}

func (ds *DeleteSupervisor) deleteIDs(bucket string, keys []s3types.ObjectIdentifier) {
	ds.wg.Add(1)
	go func() {
		defer ds.wg.Done()
		deleteResult := ds.app.DeleteObjects(ds.context, bucket, keys)
		ds.addResult(deleteResult)
	}()
}

func (ds *DeleteSupervisor) wait() []DeleteResult {
	ds.wg.Wait()
	if len(ds.deleteErrs) == 0 {
		slog.InfoContext(ds.context, "finished delete supervisor")
	} else {
		slog.ErrorContext(ds.context, "finished delete supervisor", "errs", ds.deleteErrs)
	}
	return ds.deleteErrs
}

// DeleteCncts deletes all records with the following account keys from all parts of s3.
// blocks until all goroutines are complete, while logs any failed delete calls.
func (app *App) DeleteCncts(ctx context.Context, keys []CnctKey) []DeleteResult {
	if len(keys) == 0 {
		return nil
	}

	cnctPrefixes := make(map[string]bool)
	for _, key := range keys {
		prefix := CnctPrefix{PrtyId: key.PrtyId, PrtyIdTypeCd: key.PrtyIdTypeCd, CnctID: key.CnctID}
		cnctPrefixes[prefix.String()] = true
	}
	slog.InfoContext(ctx, "deleting cncts", "cnctPrefixes", cnctPrefixes)

	deletes := makeDeleteSupervisor(ctx, app)

	for cnctPrefix := range cnctPrefixes {
		deletes.deletePages(app.CnctBucket, app.ListObjectsByPrefix(ctx, app.CnctBucket, cnctPrefix))
	}

	// in addition to deleting each cnct by prefix, we need to parse the acctID and create an acctPrefix to delete txns and holdings.
	listAccts := makeListAcctsSupervisor(ctx)

	for cnctPrefix := range cnctPrefixes {
		listIDsChan := app.ListObjectsByPrefix(ctx, app.AcctBucket, cnctPrefix)
		deleteIDsChan := listAccts.interceptListedPrefixes(listIDsChan)
		deletes.deletePages(app.AcctBucket, deleteIDsChan)
	}

	acctsPrefixTable := listAccts.wait()

	for acctPrefix := range acctsPrefixTable {
		for _, bucket := range []string{
			app.HoldBucket,
			app.TxnBucket,
		} {
			deletes.deletePages(bucket, app.ListObjectsByPrefix(ctx, bucket, acctPrefix))
		}
	}

	return deletes.wait()
}

// DeleteAccts deletes all records with the following cnct keys from all parts s3.
// blocks until all goroutines are complete, while logs any failed delete calls.
func (app *App) DeleteAccts(ctx context.Context, keys []AcctKey) []DeleteResult {
	if len(keys) == 0 {
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

	deletes := makeDeleteSupervisor(ctx, app)

	for acctMemPrefix := range acctMemPrefixes {
		for _, bucket := range []string{
			app.HoldBucket,
			app.TxnBucket,
		} {
			deletes.deletePages(bucket, app.ListObjectsByPrefix(ctx, bucket, acctMemPrefix))
		}
	}
	for acctPrefix := range acctPrefixes {
		deletes.deletePages(app.AcctBucket, app.ListObjectsByPrefix(ctx, app.AcctBucket, acctPrefix))
	}

	return deletes.wait()
}

type Prefix struct {
	Bucket string
	Value  string
}

func (p Prefix) String() string {
	return p.Bucket + ":" + p.Value
}

// DeletePrefixes generically deletes pairs of prefixes in one shot.
func (app *App) DeletePrefixes(ctx context.Context, prefixes map[Prefix]bool) []DeleteResult {
	if len(prefixes) == 0 {
		return nil
	}

	slog.InfoContext(ctx, "deleting objects by prefixes", "prefixes", prefixes)

	deletes := makeDeleteSupervisor(ctx, app)

	for prefix := range prefixes {
		deletes.deletePages(prefix.Bucket, app.ListObjectsByPrefix(ctx, prefix.Bucket, prefix.Value))
	}

	return deletes.wait()
}

type ListResult struct {
	Bucket string
	Prefix string
	Keys   []s3types.ObjectIdentifier
	Err    error
}

// ListObjectsByPrefix lists all object keys for a certain prefix in a bucket and streams each page of data through a channel as they come.
// aws s3 API does not support multiple buckets/prefixes per call, so each bucket prefix needs its own api call.
func (app *App) ListObjectsByPrefix(ctx context.Context, bucket string, prefix string) chan ListResult {
	resultsChan := make(chan ListResult)

	// paginate through all objects under the given prefix for a bucket and send each page to the channel. closes when all pages have been walked
	go func() {
		defer close(resultsChan)

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
				resultsChan <- ListResult{Bucket: bucket, Prefix: prefix, Err: err}
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
			resultsChan <- ListResult{Bucket: bucket, Prefix: prefix, Keys: keys}
		}
	}()

	return resultsChan
}

type DeleteResult struct {
	Bucket string
	Prefix string
	Keys   []string
	Err    error
}

// DeleteObjects is a helper to delete all keys from a bucket and log
// if a deletion call fails, we should log and continue execution, but we rely on another refresh to come in and actually delete these records
func (app *App) DeleteObjects(ctx context.Context, bucket string, keys []s3types.ObjectIdentifier) DeleteResult {
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

	return DeleteResult{Keys: strKeys, Bucket: bucket, Err: err}
}
