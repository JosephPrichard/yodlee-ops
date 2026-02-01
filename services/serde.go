package svc

import (
	"filogger/pb"
	"google.golang.org/protobuf/proto"
)

func MarshalExtnCnctRefresh(v ExtnCnctRefresh) ([]byte, error) {
	return proto.Marshal(&pb.ExtnCnctEntity{
		PrtyId:       v.PrtyId,
		PrtyIdTypeCd: v.PrtyIdTypeCd,
		ExtnCnctId:   v.ExtnCnctId,
		BusDt:        v.BusDt,
		VendorName:   v.VendorName,
	})
}

func MarshalExtnAcctRefresh(v ExtnAcctRefresh) ([]byte, error) {
	return proto.Marshal(&pb.ExtnAcctEntity{
		PrtyId:       v.PrtyId,
		PrtyIdTypeCd: v.PrtyIdTypeCd,
		ExtnCnctId:   v.ExtnCnctId,
		ExtnAcctId:   v.ExtnAcctId,
		BusDt:        v.BusDt,
		AcctName:     v.AcctName,
	})
}

func MarshalExtnHoldRefresh(v ExtnHoldRefresh) ([]byte, error) {
	return proto.Marshal(&pb.ExtnHoldEntity{
		PrtyId:       v.PrtyId,
		PrtyIdTypeCd: v.PrtyIdTypeCd,
		ExtnAcctId:   v.ExtnAcctId,
		ExtnHoldId:   v.ExtnHoldId,
		BusDt:        v.BusDt,
		HoldName:     v.HoldName,
	})
}

func MarshalExtnTxnRefresh(v ExtnTxnRefresh) ([]byte, error) {
	return proto.Marshal(&pb.ExtnTxnEntity{
		PrtyId:       v.PrtyId,
		PrtyIdTypeCd: v.PrtyIdTypeCd,
		ExtnAcctId:   v.ExtnAcctId,
		ExtnTxnId:    v.ExtnTxnId,
		TxnDt:        v.TxnDt,
		BusDt:        v.BusDt,
		TxnAmt:       v.TxnAmt,
	})
}

func MarshalExtnCnctEnrichment(v ExtnCnctEnrichment) ([]byte, error) {
	return proto.Marshal(&pb.ExtnCnctEntity{
		PrtyId:       v.PrtyId,
		PrtyIdTypeCd: v.PrtyIdTypeCd,
		ExtnCnctId:   v.ExtnCnctId,
		BusDt:        v.BusDt,
		VendorName:   v.VendorName,
	})
}

func MarshalExtnAcctEnrichment(v ExtnAcctEnrichment) ([]byte, error) {
	return proto.Marshal(&pb.ExtnAcctEntity{
		PrtyId:       v.PrtyId,
		PrtyIdTypeCd: v.PrtyIdTypeCd,
		ExtnCnctId:   v.ExtnCnctId,
		ExtnAcctId:   v.ExtnAcctId,
		BusDt:        v.BusDt,
		AcctName:     v.AcctName,
	})
}

func MarshalExtnHoldEnrichment(v ExtnHoldEnrichment) ([]byte, error) {
	return proto.Marshal(&pb.ExtnHoldEntity{
		PrtyId:       v.PrtyId,
		PrtyIdTypeCd: v.PrtyIdTypeCd,
		ExtnAcctId:   v.ExtnAcctId,
		ExtnHoldId:   v.ExtnHoldId,
		BusDt:        v.BusDt,
		HoldName:     v.HoldName,
	})
}

func MarshalExtnTxnEnrichment(v ExtnTxnEnrichment) ([]byte, error) {
	return proto.Marshal(&pb.ExtnTxnEntity{
		PrtyId:       v.PrtyId,
		PrtyIdTypeCd: v.PrtyIdTypeCd,
		ExtnAcctId:   v.ExtnAcctId,
		ExtnTxnId:    v.ExtnTxnId,
		TxnDt:        v.TxnDt,
		BusDt:        v.BusDt,
		TxnAmt:       v.TxnAmt,
	})
}
