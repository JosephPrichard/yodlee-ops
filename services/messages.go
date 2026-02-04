package svc

type ErrorLog struct {
	ErrMsg    string `json:"errMsg"`
	Timestamp string `json:"timestamp"`
}

type ErrorLogEntity struct {
	ErrMsg    string `json:"errMsg"`
	Timestamp string `json:"timestamp"`
}

type ExtnCnctRefresh struct {
	// key
	PrtyId       string `json:"prtyId"`
	PrtyIdTypeCd string `json:"prtyIdTypeCd"`
	ExtnCnctId   string `json:"extnCnctId"`
	BusDt        string `json:"busDt"`
	// value
	VendorName string `json:"vendorName"`
	// meta
	IsDeleted bool `json:"isDeleted"`
}

type ExtnAcctRefresh struct {
	// key
	PrtyId       string `json:"prtyId"`
	PrtyIdTypeCd string `json:"prtyIdTypeCd"`
	ExtnCnctId   string `json:"extnCnctId"`
	ExtnAcctId   string `json:"extnAcctId"`
	BusDt        string `json:"busDt"`
	// value
	AcctName string `json:"acctName"`
	// todo: add the rest of the fields
	// meta
	IsDeleted bool `json:"isDeleted"`
}

type ExtnHoldRefresh struct {
	// key
	PrtyId       string `json:"prtyId"`
	PrtyIdTypeCd string `json:"prtyIdTypeCd"`
	ExtnAcctId   string `json:"extnAcctId"`
	ExtnHoldId   string `json:"extnHoldId"`
	BusDt        string `json:"busDt"`
	// value
	HoldName string `json:"holdName"`
	// todo: add the rest of the fields
	// meta
	IsDeleted bool `json:"isDeleted"`
}

type ExtnTxnRefresh struct {
	// key
	PrtyId       string `json:"prtyId"`
	PrtyIdTypeCd string `json:"prtyIdTypeCd"`
	ExtnAcctId   string `json:"extnAcctId"`
	ExtnTxnId    string `json:"extnTxnId"`
	TxnDt        string `json:"txnDt"`
	BusDt        string `json:"busDt"`
	// value
	TxnAmt int64 `json:"txnAmt"`
	// todo: add the rest of the fields
	// meta
	IsDeleted bool `json:"isDeleted"`
}

type ExtnCnctEnrichment struct {
	// key
	PrtyId       string `json:"prtyId"`
	PrtyIdTypeCd string `json:"prtyIdTypeCd"`
	ExtnCnctId   string `json:"extnCnctId"`
	BusDt        string `json:"busDt"`
	// value
	VendorName string `json:"vendorName"`
	// todo: add the rest of the fields
}

type ExtnAcctEnrichment struct {
	// key
	PrtyId       string `json:"prtyId"`
	PrtyIdTypeCd string `json:"prtyIdTypeCd"`
	ExtnCnctId   string `json:"extnCnctId"`
	ExtnAcctId   string `json:"extnAcctId"`
	BusDt        string `json:"busDt"`
	// value
	AcctName string `json:"acctName"`
	// todo: add the rest of the fields
}

type ExtnHoldEnrichment struct {
	// key
	PrtyId       string `json:"prtyId"`
	PrtyIdTypeCd string `json:"prtyIdTypeCd"`
	ExtnAcctId   string `json:"extnAcctId"`
	ExtnHoldId   string `json:"extnHoldId"`
	BusDt        string `json:"busDt"`
	// value
	HoldName string `json:"holdName"`
	// todo: add the rest of the fields
}

type ExtnTxnEnrichment struct {
	// key
	PrtyId       string `json:"prtyId"`
	PrtyIdTypeCd string `json:"prtyIdTypeCd"`
	ExtnAcctId   string `json:"extnAcctId"`
	ExtnTxnId    string `json:"extnTxnId"`
	TxnDt        string `json:"txnDt"`
	BusDt        string `json:"busDt"`
	// value
	TxnAmt int64 `json:"txnAmt"`
	// todo: add the rest of the fields
}

const (
	DeleteKind = "delete"
	ListKind   = "list"
)

type DeleteRetry struct {
	Kind   string   `json:"kind"` // either 'list' or 'delete'
	Bucket string   `json:"bucket"`
	Prefix string   `json:"prefix"`
	Keys   []string `json:"keys"`
}

type FiInput interface {
	isFiInput()
}

func (_ ExtnCnctRefresh) isFiInput()    {}
func (_ ExtnAcctRefresh) isFiInput()    {}
func (_ ExtnHoldRefresh) isFiInput()    {}
func (_ ExtnTxnRefresh) isFiInput()     {}
func (_ ExtnCnctEnrichment) isFiInput() {}
func (_ ExtnAcctEnrichment) isFiInput() {}
func (_ ExtnHoldEnrichment) isFiInput() {}
func (_ ExtnTxnEnrichment) isFiInput()  {}
