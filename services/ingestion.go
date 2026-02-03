package svc

import (
	"bytes"
	"context"
	"log/slog"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ingest enrichments. for each message type, we serialize the records directly into s3. we skip messages that cannot be serialized
// we return any failed insertions so they can be reinserted back into the queue

func (app *App) IngestCnctEnrichments(ctx context.Context, cncts []ExtnCnctEnrichment) []PutResult {
	type Input = ExtnCnctEnrichment
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

	return app.PutObjects(ctx, app.CnctBucket, putList)()
}

func (app *App) IngestAcctEnrichments(ctx context.Context, accts []ExtnAcctEnrichment) []PutResult {
	type Input = ExtnAcctEnrichment
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

	return app.PutObjects(ctx, app.AcctBucket, putList)()
}

func (app *App) IngestHoldEnrichments(ctx context.Context, holds []ExtnHoldEnrichment) []PutResult {
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

	return app.PutObjects(ctx, app.HoldBucket, putList)()
}

func (app *App) IngestTxnEnrichments(ctx context.Context, txns []ExtnTxnEnrichment) []PutResult {
	type Input = ExtnTxnEnrichment
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

	return app.PutObjects(ctx, app.TxnBucket, putList)()
}

type RefreshResult[Input any] struct {
	PutErrs    []PutResult
	DeleteErrs []DeleteResult
}

// ingest refreshes. for each message type, we serialize the records directly into s3 OR delete if the refresh is marked as deleted
// handles failed uploads in the same way as enrichments

func (app *App) IngestCnctRefreshes(ctx context.Context, cncts []ExtnCnctRefresh) RefreshResult[ExtnCnctRefresh] {
	type Input = ExtnCnctRefresh
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

	return RefreshResult[Input]{PutErrs: getPutCall(), DeleteErrs: deleteErrs}
}

func (app *App) IngestAcctsRefreshes(ctx context.Context, accts []ExtnAcctRefresh) RefreshResult[ExtnAcctRefresh] {
	type Input = ExtnAcctRefresh
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

	return RefreshResult[Input]{PutErrs: getPutCall(), DeleteErrs: deleteErrs}
}

func (app *App) IngestHoldRefreshes(ctx context.Context, holds []ExtnHoldRefresh) RefreshResult[ExtnHoldRefresh] {
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

	return RefreshResult[Input]{PutErrs: getPutCall(), DeleteErrs: deleteErrs}
}

func (app *App) IngestTxnRefreshes(ctx context.Context, txns []ExtnTxnRefresh) RefreshResult[ExtnTxnRefresh] {
	type Input = ExtnTxnRefresh
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

	return RefreshResult[Input]{PutErrs: getPutCall(), DeleteErrs: deleteErrs}
}

func (app *App) IngestDeleteRetries(ctx context.Context, deletes []DeleteRetry) []DeleteResult {
	prefixMap := make(map[string]bool)

	for _, delete := range deletes {
		switch delete.Kind {
		case ListKind:
			prefixMap[delete.Prefix] = true
		case DeleteKind:
			objectIDs := make([]s3types.ObjectIdentifier, 0)
			for _, key := range delete.Keys {
				objectIDs = append(objectIDs, s3types.ObjectIdentifier{Key: aws.String(key)})
			}
			result := app.DeleteObjects(ctx, delete.Bucket, objectIDs)
			deleteResults = append(deleteResults, result)
		}
	}

	var deleteResults []DeleteResult

	return deleteResults
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

func (ls *ListAcctsSupervisor) execute(listIDsChan chan ListResult) chan ListResult {
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

func (ds *DeleteSupervisor) execute(bucket string, listIDsChan chan ListResult) {
	slog.InfoContext(ds.context, "begin an execute delete worker", "bucket", bucket)

	ds.wg.Add(1)
	go func() {
		defer ds.wg.Done()
		// ok. get all listed ids from the channel
		for listResult := range listIDsChan {
			var deleteResult DeleteResult
			if listResult.Err == nil {
				// list didn't error - try to delete the ids
				deleteResult = ds.app.DeleteObjects(ds.context, bucket, listResult.Keys)
			} else {
				// list erroring is a special case of a delete erroring
				deleteResult = DeleteResult{Bucket: listResult.Bucket, Prefix: listResult.Prefix, Err: listResult.Err}
			}
			// keep track of all failed deletes
			if deleteResult.Err != nil {
				ds.lock.Lock()
				ds.deleteErrs = append(ds.deleteErrs, deleteResult)
				ds.lock.Unlock()
			}
		}
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
		slog.InfoContext(ctx, "no cncts to delete")
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
		deletes.execute(app.CnctBucket, app.ListObjectsByPrefix(ctx, app.CnctBucket, cnctPrefix))
	}

	// in addition to deleting each cnct by prefix, we need to parse the acctID and create an acctPrefix to delete txns and holdings.
	listAccts := makeListAcctsSupervisor(ctx)

	for cnctPrefix := range cnctPrefixes {
		listIDsChan := app.ListObjectsByPrefix(ctx, app.AcctBucket, cnctPrefix)
		deleteIDsChan := listAccts.execute(listIDsChan) // intercepts records coming from the listIDsChan and sends them to the deleteIDsChan
		deletes.execute(app.AcctBucket, deleteIDsChan)
	}

	acctsPreixTable := listAccts.wait()

	for acctPrefix := range acctsPreixTable {
		for _, bucket := range []string{
			app.HoldBucket,
			app.TxnBucket,
		} {
			deletes.execute(bucket, app.ListObjectsByPrefix(ctx, bucket, acctPrefix))
		}
	}

	return deletes.wait()
}

// DeleteAccts deletes all records with the following cnct keys from all parts s3.
// blocks until all goroutines are complete, while logs any failed delete calls.
func (app *App) DeleteAccts(ctx context.Context, keys []AcctKey) []DeleteResult {
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

	deletes := makeDeleteSupervisor(ctx, app)

	for acctMemPrefix := range acctMemPrefixes {
		for _, bucket := range []string{
			app.HoldBucket,
			app.TxnBucket,
		} {
			deletes.execute(bucket, app.ListObjectsByPrefix(ctx, bucket, acctMemPrefix))
		}
	}
	for acctPrefix := range acctPrefixes {
		deletes.execute(app.AcctBucket, app.ListObjectsByPrefix(ctx, app.AcctBucket, acctPrefix))
	}

	return deletes.wait()
}

type Prefix struct {
	Bucket string
	Value string
}

func (p Prefix) String() string {
	return p.Bucket + ":" + p.Value
}

// DeletePrefixes generically deletes pairs of prefixes in one shot.
func (app *App) DeletePrefixes(ctx context.Context, prefixes map[Prefix]bool) []DeleteResult {
	if len(prefixes) == 0 {
		slog.InfoContext(ctx, "no prefixes to delete")
		return nil
	}

	slog.InfoContext(ctx, "deleting objects by prefixes", "prefixes", prefixes)

	deletes := makeDeleteSupervisor(ctx, app)

	for prefix := range prefixes {
		deletes.execute(prefix.Bucket, app.ListObjectsByPrefix(ctx, prefix.Bucket, prefix.Value))
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
