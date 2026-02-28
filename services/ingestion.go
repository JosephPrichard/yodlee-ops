package svc

import (
	"bytes"
	"log/slog"
	"sync"
	"time"
	"yodleeops/infra"

	"yodleeops/yodlee"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type PutResult[YodleeInput any] struct {
	Key   string
	Input YodleeInput
	Err   error
}

type PutCnctResult = PutResult[OpsProviderAccount]
type PutAcctResult = PutResult[OpsAccount]
type PutHoldResult = PutResult[OpsHolding]
type PutTxnResult = PutResult[OpsTransaction]

func IngestCnctResponses(ctx Context, profileId string, response yodlee.ProviderAccountResponse) []PutCnctResult {
	start := time.Now()

	var putList []PutInput[OpsProviderAccount]

	for _, cnct := range response.ProviderAccount {
		key := CnctKey{
			ProfileId: profileId,
			CnctID:    cnct.Id,
			UpdtTs:    cnct.LastUpdated,
		}
		cnct := OpsProviderAccount{
			Data:         cnct,
			OpsFiMessage: OpsFiMessage{ProfileId: profileId, Timestamp: time.Now(), OriginTopic: infra.CnctResponseTopic},
		}
		putList = append(putList, PutInput[OpsProviderAccount]{Key: key.String(), Input: cnct})
	}

	joinPuts := PutObjects(ctx, ctx.AWS.Buckets.Connections, putList)

	slog.InfoContext(ctx, "finished ingest cnct responses", "elapsed", time.Since(start))
	return joinPuts()
}

func IngestAcctResponses(ctx Context, profileId string, response yodlee.AccountResponse) []PutAcctResult {
	start := time.Now()

	var putList []PutInput[OpsAccount]

	for _, acct := range response.Account {
		key := AcctKey{
			ProfileId: profileId,
			CnctID:    acct.ProviderAccountId,
			AcctID:    acct.Id,
			UpdtTs:    acct.LastUpdated,
		}
		acct := OpsAccount{
			Data:         acct,
			OpsFiMessage: OpsFiMessage{ProfileId: profileId, Timestamp: time.Now(), OriginTopic: infra.AcctResponseTopic},
		}
		putList = append(putList, PutInput[OpsAccount]{Key: key.String(), Input: acct})
	}

	joinPuts := PutObjects(ctx, ctx.AWS.Buckets.Accounts, putList)

	slog.InfoContext(ctx, "finished ingest acct responses", "elapsed", time.Since(start))
	return joinPuts()
}

func IngestHoldResponses(ctx Context, profileId string, response yodlee.HoldingResponse) []PutHoldResult {
	start := time.Now()

	var putList []PutInput[OpsHolding]

	for _, hold := range response.Holding {
		key := HoldKey{
			ProfileId: profileId,
			AcctID:    hold.AccountId,
			HoldID:    hold.Id,
			UpdtTs:    hold.LastUpdated,
		}
		hold := OpsHolding{
			Data:         hold,
			OpsFiMessage: OpsFiMessage{ProfileId: profileId, Timestamp: time.Now(), OriginTopic: infra.HoldResponseTopic},
		}
		putList = append(putList, PutInput[OpsHolding]{Key: key.String(), Input: hold})
	}

	joinPuts := PutObjects(ctx, ctx.AWS.Buckets.Holdings, putList)

	slog.InfoContext(ctx, "finished ingest hold responses", "elapsed", time.Since(start))
	return joinPuts()
}

func IngestTxnResponses(ctx Context, profileId string, response yodlee.TransactionResponse) []PutTxnResult {
	start := time.Now()

	var putList []PutInput[OpsTransaction]

	for _, txn := range response.Transaction {
		key := TxnKey{
			ProfileId: profileId,
			AcctID:    txn.AccountId,
			TxnID:     txn.Id,
			TxnDt:     txn.Date,
		}
		txn := OpsTransaction{
			Data:         txn,
			OpsFiMessage: OpsFiMessage{ProfileId: profileId, Timestamp: time.Now(), OriginTopic: infra.TxnResponseTopic},
		}
		putList = append(putList, PutInput[OpsTransaction]{Key: key.String(), Input: txn})
	}

	joinPuts := PutObjects(ctx, ctx.AWS.Buckets.Transactions, putList)

	slog.InfoContext(ctx, "finished ingest txn responses", "elapsed", time.Since(start))
	return joinPuts()
}

type RefreshResult[Input any] struct {
	PutResults   []PutResult[Input]
	DeleteErrors []DeleteResult
}

type CnctRefreshResult = RefreshResult[OpsProviderAccountRefresh]
type AcctRefreshResult = RefreshResult[OpsAccountRefresh]
type HoldRefreshResult = RefreshResult[OpsHoldingRefresh]
type TxnRefreshResult = RefreshResult[OpsTransactionRefresh]

func IngestCnctRefreshes(ctx Context, profileId string, cncts []yodlee.DataExtractsProviderAccount) CnctRefreshResult {
	start := time.Now()

	var putList []PutInput[OpsProviderAccountRefresh]
	var removeCnctKeys []CnctKey

	for i := range cncts {
		cnct := &cncts[i]
		key := CnctKey{
			ProfileId: profileId,
			CnctID:    cnct.Id,
			UpdtTs:    cnct.LastUpdated,
		}
		if cnct.IsDeleted {
			removeCnctKeys = append(removeCnctKeys, key)
		} else {
			cnct := OpsProviderAccountRefresh{
				Data:         *cnct,
				OpsFiMessage: OpsFiMessage{ProfileId: profileId, Timestamp: time.Now(), OriginTopic: infra.CnctRefreshTopic},
			}
			putList = append(putList, PutInput[OpsProviderAccountRefresh]{Key: key.String(), Input: cnct})
		}
	}

	joinPuts := PutObjects(ctx, ctx.AWS.Buckets.Connections, putList)
	deleteErrs := DeleteCncts(ctx, removeCnctKeys)

	slog.InfoContext(ctx, "finished ingest cnct refreshes", "elapsed", time.Since(start))

	return CnctRefreshResult{PutResults: joinPuts(), DeleteErrors: deleteErrs}
}

func IngestAcctsRefreshes(ctx Context, profileId string, accts []yodlee.DataExtractsAccount) AcctRefreshResult {
	start := time.Now()

	var putList []PutInput[OpsAccountRefresh]
	var removeAcctKeys []AcctKey

	for i := range accts {
		acct := &accts[i]
		key := AcctKey{
			ProfileId: profileId,
			CnctID:    acct.ProviderAccountId,
			AcctID:    acct.Id,
			UpdtTs:    acct.LastUpdated,
		}
		if acct.IsDeleted {
			removeAcctKeys = append(removeAcctKeys, key)
		} else {
			acct := OpsAccountRefresh{
				Data:         *acct,
				OpsFiMessage: OpsFiMessage{ProfileId: profileId, Timestamp: time.Now(), OriginTopic: infra.AcctRefreshTopic},
			}
			putList = append(putList, PutInput[OpsAccountRefresh]{Key: key.String(), Input: acct})
		}
	}

	joinPuts := PutObjects(ctx, ctx.AWS.Buckets.Accounts, putList)
	deleteErrs := DeleteAccts(ctx, removeAcctKeys)

	slog.InfoContext(ctx, "finished ingest accts refreshes", "elapsed", time.Since(start))

	return AcctRefreshResult{PutResults: joinPuts(), DeleteErrors: deleteErrs}
}

func IngestHoldRefreshes(ctx Context, profileId string, holds []yodlee.DataExtractsHolding) HoldRefreshResult {
	start := time.Now()

	var putList []PutInput[OpsHoldingRefresh]

	for _, hold := range holds {
		key := HoldKey{
			ProfileId: profileId,
			AcctID:    hold.AccountId,
			HoldID:    hold.Id,
			UpdtTs:    hold.LastUpdated,
		}
		hold := OpsHoldingRefresh{
			Data:         hold,
			OpsFiMessage: OpsFiMessage{ProfileId: profileId, Timestamp: time.Now(), OriginTopic: infra.HoldRefreshTopic},
		}
		putList = append(putList, PutInput[OpsHoldingRefresh]{Key: key.String(), Input: hold})
	}

	joinPuts := PutObjects(ctx, ctx.AWS.Buckets.Holdings, putList)

	slog.InfoContext(ctx, "finished ingest hold refreshes", "elapsed", time.Since(start))

	return HoldRefreshResult{PutResults: joinPuts()}
}

func IngestTxnRefreshes(ctx Context, profileId string, txns []yodlee.DataExtractsTransaction) TxnRefreshResult {
	start := time.Now()

	var putList []PutInput[OpsTransactionRefresh]
	txnPrefixes := make(map[Prefix]bool)

	for i := range txns {
		txn := &txns[i]
		if txn.IsDeleted {
			prefix := AcctChildPrefix{
				ProfileId: profileId,
				AcctID:    txn.AccountId,
				ChildID:   txn.Id,
			}
			txnPrefixes[Prefix{Value: prefix.String(), Bucket: ctx.AWS.Buckets.Transactions}] = true
		} else {
			key := TxnKey{
				ProfileId: profileId,
				AcctID:    txn.AccountId,
				TxnID:     txn.Id,
				TxnDt:     txn.Date,
			}
			txn := OpsTransactionRefresh{
				Data:         *txn,
				OpsFiMessage: OpsFiMessage{ProfileId: profileId, Timestamp: time.Now(), OriginTopic: infra.TxnRefreshTopic},
			}
			putList = append(putList, PutInput[OpsTransactionRefresh]{Key: key.String(), Input: txn})
		}
	}

	joinPuts := PutObjects(ctx, ctx.AWS.Buckets.Transactions, putList)
	deleteErrs := DeletePrefixes(ctx, txnPrefixes)

	slog.InfoContext(ctx, "finished ingest txn refreshes", "elapsed", time.Since(start))

	return TxnRefreshResult{PutResults: joinPuts(), DeleteErrors: deleteErrs}
}

func IngestDeleteRetries(ctx Context, deleteRetries []DeleteRetry) []DeleteResult {
	slog.InfoContext(ctx, "begin ingest delete retries", "deleteRetries", deleteRetries)

	deletes := makeDeleteSupervisor(ctx)

	prefixes := make(map[Prefix]bool)

	for _, d := range deleteRetries {
		switch d.Kind {
		case ListKind:
			prefixes[Prefix{Bucket: d.Bucket, Value: d.Prefix}] = true
		case DeleteKind:
			objectIDs := make([]string, 0, len(d.Keys))
			for _, key := range d.Keys {
				objectIDs = append(objectIDs, key)
			}
			deletes.deleteIDs(d.Bucket, objectIDs)
		}
	}

	slog.InfoContext(ctx, "deleting prefixes for delete retries", "prefixes", prefixes)

	for prefix := range prefixes {
		deletes.DeleteList(prefix.Bucket, ListObjectsByPrefix(ctx, prefix.Bucket, prefix.Value))
	}

	return deletes.wait()
}

type PutInput[YodleeInput any] struct {
	Key   string
	Input YodleeInput
}

func (o PutInput[T]) String() string {
	return o.Key
}

// PutObjects uploads objects to a given Bucket async and returns any failed uploads when the task is joined
func PutObjects[Input any](ctx Context, bucket infra.Bucket, inputObjects []PutInput[Input]) func() []PutResult[Input] {
	results := make([]PutResult[Input], len(inputObjects))

	var wg sync.WaitGroup

	for i, object := range inputObjects {
		body, ok := EncodeGzipJSON(ctx, object.Input)
		if !ok {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()

			_, err := ctx.AWS.S3.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(string(bucket)),
				Key:    aws.String(object.Key),
				Body:   bytes.NewReader(body),
			})
			if err != nil {
				slog.ErrorContext(ctx, "failed to upload object to s3", "Key", object.Key, "Bucket", bucket, "err", err)
			} else {
				slog.InfoContext(ctx, "uploaded object to s3", "Bucket", bucket, "Key", object.Key, "bytes", len(body))
			}

			results[i] = PutResult[Input]{Key: object.Key, Input: object.Input, Err: err}
		}()
	}

	return func() []PutResult[Input] {
		wg.Wait()

		var putCount int64
		var errs []PutResult[Input]
		for _, result := range results {
			if result.Err != nil {
				errs = append(errs, result)
			} else {
				putCount++
			}
		}

		if len(errs) == 0 {
			slog.InfoContext(ctx, "finished put objects call", "count", putCount)
		} else {
			slog.ErrorContext(ctx, "finished put objects call", "errs", errs)
		}
		return results
	}
}

type Supervisor struct {
	context Context
	wg      sync.WaitGroup
}

func (s *Supervisor) Go(cb func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		cb()
	}()
}

type ListAcctsTable struct {
	Supervisor
	table map[string]bool
	lock  sync.Mutex
}

func makeListAcctsSupervisor(ctx Context) ListAcctsTable {
	return ListAcctsTable{
		Supervisor: Supervisor{context: ctx},
		table:      make(map[string]bool),
	}
}

func (ls *ListAcctsTable) InterceptListedPrefixes(listIDsChan chan ListResult) chan ListResult {
	pipeIDsChan := make(chan ListResult)
	ls.Go(func() {
		defer close(pipeIDsChan) // nothing more to pipe once there is nothing more to receive.

		for listResult := range listIDsChan {
			for _, acctObjectID := range listResult.Keys {
				acctKey, err := ParseAcctKey(acctObjectID)
				if err != nil {
					slog.ErrorContext(ls.context, "failed to parse acct Key from s3 object Key", "acctKeyStr", acctObjectID, "err", err)
					continue
				}
				acctPrefixStr := AcctMemberPrefix{ProfileId: acctKey.ProfileId, AcctID: acctKey.AcctID}.String()

				ls.lock.Lock()
				ls.table[acctPrefixStr] = true
				ls.lock.Unlock()
			}
			// always pipeline regardless of the list acct is an error or not. (we handle errors later)
			pipeIDsChan <- listResult
		}
	})
	return pipeIDsChan
}

func (ls *ListAcctsTable) Wait() map[string]bool {
	ls.wg.Wait()
	slog.InfoContext(ls.context, "finished list accts supervisor", "acctPrefixTable", ls.table)
	return ls.table
}

// DeleteSupervisor is a block of state to control the goroutines executing concurrent delete jobs
// it stores errors any deletes run into (protected with a lock) and a wait group for coordinating job execution
type DeleteSupervisor struct {
	Supervisor
	deleteErrs []DeleteResult
	lock       sync.Mutex
}

func makeDeleteSupervisor(ctx Context) DeleteSupervisor {
	return DeleteSupervisor{
		Supervisor: Supervisor{context: ctx},
	}
}

func (ds *DeleteSupervisor) AddResult(deleteResult DeleteResult) {
	if deleteResult.Err == nil {
		return
	}
	ds.lock.Lock()
	ds.deleteErrs = append(ds.deleteErrs, deleteResult)
	ds.lock.Unlock()
}

func (ds *DeleteSupervisor) DeleteList(bucket infra.Bucket, listIDsChan chan ListResult) {
	ds.Go(func() {
		for listResult := range listIDsChan {
			slog.InfoContext(ds.context, "deleting listed ids", "Bucket", bucket, "listResult", listResult)

			var deleteResult DeleteResult

			if listResult.Err != nil {
				deleteResult = DeleteResult{Bucket: listResult.Bucket, Prefix: listResult.Prefix, Err: listResult.Err}
			} else {
				deleteResult = DeleteObjects(ds.context, bucket, listResult.Keys)
			}

			ds.AddResult(deleteResult)
		}
	})
}

type DeleteChunk struct {
	Bucket string
	Keys   []s3types.ObjectIdentifier
}

func (ds *DeleteSupervisor) deleteIDs(bucket infra.Bucket, keys []string) {
	ds.Go(func() {
		deleteResult := DeleteObjects(ds.context, bucket, keys)
		ds.AddResult(deleteResult)
	})
}

func (ds *DeleteSupervisor) wait() []DeleteResult {
	ds.wg.Wait()
	if len(ds.deleteErrs) == 0 {
		slog.InfoContext(ds.context, "finished delete objects supervisor")
	} else {
		slog.ErrorContext(ds.context, "finished delete objects supervisor", "errs", ds.deleteErrs)
	}
	return ds.deleteErrs
}

// DeleteCncts deletes all records with the following account keys from all parts of s3.
// blocks until all goroutines are complete, while logs any failed delete calls.
func DeleteCncts(ctx Context, keys []CnctKey) []DeleteResult {
	if len(keys) == 0 {
		return nil
	}

	cnctPrefixes := make(map[string]bool)
	for _, key := range keys {
		prefix := CnctPrefix{ProfileId: key.ProfileId, CnctID: key.CnctID}
		cnctPrefixes[prefix.String()] = true
	}
	slog.InfoContext(ctx, "deleting cncts", "cnctPrefixes", cnctPrefixes)

	deletes := makeDeleteSupervisor(ctx)

	for cnctPrefix := range cnctPrefixes {
		deletes.DeleteList(ctx.AWS.Buckets.Connections, ListObjectsByPrefix(ctx, ctx.AWS.Buckets.Connections, cnctPrefix))
	}

	// in addition to deleting each cnct by prefix, we need to parse the acctID and create an acctPrefix to delete txns and holdings.
	listAccts := makeListAcctsSupervisor(ctx)

	for cnctPrefix := range cnctPrefixes {
		listIDsChan := ListObjectsByPrefix(ctx, ctx.AWS.Buckets.Accounts, cnctPrefix)
		deleteIDsChan := listAccts.InterceptListedPrefixes(listIDsChan)
		deletes.DeleteList(ctx.AWS.Buckets.Accounts, deleteIDsChan)
	}

	acctsPrefixTable := listAccts.Wait()

	for acctPrefix := range acctsPrefixTable {
		for _, bucket := range []infra.Bucket{
			ctx.AWS.Buckets.Holdings,
			ctx.AWS.Buckets.Transactions,
		} {
			deletes.DeleteList(bucket, ListObjectsByPrefix(ctx, bucket, acctPrefix))
		}
	}

	return deletes.wait()
}

// DeleteAccts deletes all records with the following cnct keys from all parts s3.
// blocks until all goroutines are complete, while logs any failed delete calls.
func DeleteAccts(ctx Context, keys []AcctKey) []DeleteResult {
	if len(keys) == 0 {
		return nil
	}

	acctMembPrefixes := make(map[string]bool)
	acctPrefixes := make(map[string]bool)

	for _, key := range keys {
		memPrefix := AcctMemberPrefix{ProfileId: key.ProfileId, AcctID: key.AcctID}
		prefix := AcctPrefix{ProfileId: key.ProfileId, CnctID: key.CnctID, AcctID: key.AcctID}

		acctMembPrefixes[memPrefix.String()] = true
		acctPrefixes[prefix.String()] = true
	}

	slog.InfoContext(ctx, "deleting accts", "acctMembPrefixes", acctMembPrefixes, "acctPrefixes", acctPrefixes)

	deletes := makeDeleteSupervisor(ctx)

	for acctMemPrefix := range acctMembPrefixes {
		for _, bucket := range []infra.Bucket{
			ctx.AWS.Buckets.Holdings,
			ctx.AWS.Buckets.Transactions,
		} {
			deletes.DeleteList(bucket, ListObjectsByPrefix(ctx, bucket, acctMemPrefix))
		}
	}
	for acctPrefix := range acctPrefixes {
		deletes.DeleteList(ctx.AWS.Buckets.Accounts, ListObjectsByPrefix(ctx, ctx.AWS.Buckets.Accounts, acctPrefix))
	}

	return deletes.wait()
}

type Prefix struct {
	Bucket infra.Bucket
	Value  string
}

func (p Prefix) String() string {
	return string(p.Bucket) + ":" + p.Value
}

// DeletePrefixes generically deletes pairs of prefixes in one shot.
func DeletePrefixes(ctx Context, prefixes map[Prefix]bool) []DeleteResult {
	if len(prefixes) == 0 {
		return nil
	}

	slog.InfoContext(ctx, "deleting objects by prefixes", "prefixes", prefixes)

	deletes := makeDeleteSupervisor(ctx)

	for prefix := range prefixes {
		deletes.DeleteList(prefix.Bucket, ListObjectsByPrefix(ctx, prefix.Bucket, prefix.Value))
	}

	return deletes.wait()
}

type ListResult struct {
	Bucket infra.Bucket
	Prefix string
	Keys   []string
	Err    error
}

// ListObjectsByPrefix lists all object keys for a certain prefix in a Bucket and streams each page of data through a channel as they come.
// aws s3 API does not support multiple buckets/prefixes per call, so each Bucket prefix needs its own api call.
func ListObjectsByPrefix(ctx Context, bucket infra.Bucket, prefix string) chan ListResult {
	resultsChan := make(chan ListResult)

	// paginate through all objects under the given prefix for a Bucket and send each page to the channel. closes when all pages have been walked
	go func() {
		defer close(resultsChan)

		paginator := s3.NewListObjectsV2Paginator(ctx.AWS.S3, &s3.ListObjectsV2Input{
			Bucket:  aws.String(string(bucket)),
			Prefix:  aws.String(prefix),
			MaxKeys: ctx.AWS.PaginationLen,
		})
		page := 0

		slog.InfoContext(ctx, "begin list objects by prefix pagination", "Bucket", bucket, "prefix", prefix)

		for paginator.HasMorePages() {
			page++
			listObjects, err := paginator.NextPage(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "failed to list objects from s3", "Bucket", bucket, "prefix", prefix, "page", page, "err", err)
				resultsChan <- ListResult{Bucket: bucket, Prefix: prefix, Err: err}
				return
			}

			if len(listObjects.Contents) == 0 {
				slog.WarnContext(ctx, "no objects found for prefix", "Bucket", bucket, "prefix", prefix, "page", page)
				continue
			}

			keys := make([]string, 0, len(listObjects.Contents))
			for _, obj := range listObjects.Contents {
				if obj.Key == nil {
					continue
				}
				keys = append(keys, *obj.Key)
			}

			slog.InfoContext(ctx, "list objects", "Bucket", bucket, "prefix", prefix, "page", page, "objectCount", len(listObjects.Contents), "objects", keys)
			resultsChan <- ListResult{Bucket: bucket, Prefix: prefix, Keys: keys}
		}

		slog.InfoContext(ctx, "finished list objects by prefix pagination", "Bucket", bucket, "prefix", prefix, "totalPages", page)
	}()

	return resultsChan
}

type DeleteResult struct {
	Bucket infra.Bucket
	Prefix string
	Keys   []string
	Err    error
}

func DeleteObjects(ctx Context, bucket infra.Bucket, keys []string) DeleteResult {
	objectIDs := make([]s3types.ObjectIdentifier, 0, len(keys))
	for _, key := range keys {
		objectIDs = append(objectIDs, s3types.ObjectIdentifier{Key: aws.String(key)})
	}

	_, err := ctx.AWS.S3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(string(bucket)),
		Delete: &s3types.Delete{Objects: objectIDs},
	})
	if err != nil {
		slog.ErrorContext(ctx, "failed to delete objects from s3", "Bucket", bucket, "keys", keys, "err", err)
	} else {
		slog.InfoContext(ctx, "deleted objects from s3", "Bucket", bucket, "keys", keys)
	}

	return DeleteResult{Keys: keys, Bucket: bucket, Err: err}
}
