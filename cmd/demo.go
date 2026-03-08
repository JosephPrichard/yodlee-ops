package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/brianvoe/gofakeit/v6"
	"log/slog"
	"math/rand"
	"strconv"
	"time"
	"yodleeops/model"
	"yodleeops/yodlee"
)

// generates random ids with a tight finite range so we can get a random spread of data

func makePartyId() []byte {
	return []byte("p" + strconv.Itoa(rand.Intn(5)+1))
}

func makeProviderAccountId() int64 {
	return int64(rand.Intn(1000) + 1)
}

func makeAccountId() int64 {
	return int64(rand.Intn(10000) + 1)
}

func makeHoldingId() int64 {
	return int64(rand.Intn(100000) + 1)
}

func makeTransactionId() int64 {
	return int64(rand.Intn(1000000) + 1)
}

func makeAccountName() string {
	return gofakeit.RandomString([]string{
		"checking",
		"savings",
		"credit_card",
		"loan",
		"mortgage",
		"investment",
		"retirement",
		"brokerage",
	})
}

func makeHoldingName() string {
	return gofakeit.RandomString([]string{
		"Stock",
		"Mutual Fund",
		"Bond",
		"ETF",
		"Cash",
	})
}

func makeAmount() float64 {
	return float64(1000 + rand.Intn(10000))
}

func makeDate() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func makeMoney() yodlee.Money {
	return yodlee.Money{
		Amount:            makeAmount(),
		ConvertedAmount:   makeAmount(),
		Currency:          gofakeit.CurrencyShort(),
		ConvertedCurrency: gofakeit.CurrencyShort(),
	}
}

func makeAccountAddress() yodlee.AccountAddress {
	return yodlee.AccountAddress{
		Zip:        gofakeit.Zip(),
		Country:    gofakeit.Country(),
		Address1:   gofakeit.StreetNumber() + " " + gofakeit.StreetName(),
		Address2:   gofakeit.AppName(),
		Address3:   "",
		City:       gofakeit.City(),
		State:      gofakeit.State(),
		Street:     gofakeit.StreetName(),
		SourceType: gofakeit.RandomString([]string{"SYSTEM", "AGGREGATED"}),
		Type:       gofakeit.RandomString([]string{"HOME", "WORK", "MAILING"}),
	}
}

func makeAccountDataset() yodlee.AccountDataset {
	return yodlee.AccountDataset{
		LastUpdated:               makeDate(),
		UpdateEligibility:         gofakeit.RandomString([]string{"ALLOW_UPDATE", "DISALLOW_UPDATE"}),
		AdditionalStatus:          gofakeit.RandomString([]string{"AVAILABLE_DATA_RETRIEVED", "PARTIAL_DATA_RETRIEVED"}),
		NextUpdateScheduled:       makeDate(),
		Name:                      gofakeit.RandomString([]string{"BASIC_AGG_DATA", "ADVANCE_AGG_DATA", "ACCT_PROFILE"}),
		LastUpdateAttempt:         makeDate(),
		AdditionalStatusErrorCode: "",
	}
}

func makeBusinessInformation() yodlee.BusinessInformation {
	return yodlee.BusinessInformation{
		LegalName:    gofakeit.Company(),
		BusinessName: gofakeit.Company(),
		Abn:          gofakeit.Numerify("##########"),
		Acn:          gofakeit.Numerify("#########"),
	}
}

func makeRewardBalance() yodlee.RewardBalance {
	return yodlee.RewardBalance{
		ExpiryDate:      makeDate(),
		BalanceToReward: strconv.Itoa(rand.Intn(5000)),
		BalanceType:     gofakeit.RandomString([]string{"MILES", "POINTS", "CASH_BACK"}),
		Balance:         makeAmount(),
		Description:     gofakeit.Sentence(5),
		BalanceToLevel:  strconv.Itoa(rand.Intn(1000)),
		Units:           gofakeit.RandomString([]string{"MILES", "POINTS", "DOLLARS"}),
	}
}

func makeBankTransferCode() yodlee.BankTransferCode {
	return yodlee.BankTransferCode{
		Id:   gofakeit.Numerify("#########"),
		Type: gofakeit.RandomString([]string{"BSB", "IFSC", "ROUTING_NUMBER", "SORT_CODE"}),
	}
}

func makeCoverage() yodlee.Coverage {
	return yodlee.Coverage{
		Amount: []yodlee.CoverageAmount{
			{
				Cover:     makeMoney(),
				UnitType:  gofakeit.RandomString([]string{"AMOUNT", "PERCENTAGE"}),
				Type:      gofakeit.RandomString([]string{"DEDUCTIBLE", "OUT_OF_POCKET", "COPAY"}),
				LimitType: gofakeit.RandomString([]string{"ANNUAL", "LIFETIME"}),
				Met:       makeMoney(),
			},
		},
		PlanType:  gofakeit.RandomString([]string{"PPO", "HMO", "EPO"}),
		EndDate:   makeDate(),
		Type:      gofakeit.RandomString([]string{"MEDICAL", "DENTAL", "VISION"}),
		StartDate: makeDate(),
	}
}

func makeLoanPayoffDetails() yodlee.LoanPayoffDetails {
	return yodlee.LoanPayoffDetails{
		PayByDate:          makeDate(),
		PayoffAmount:       makeMoney(),
		OutstandingBalance: makeMoney(),
	}
}

func makeAccountHolder() yodlee.AccountHolder {
	return yodlee.AccountHolder{
		Identifier: []yodlee.Identifier{
			{Type: "SSN", Value: gofakeit.SSN()},
		},
		Gender:    gofakeit.RandomString([]string{"MALE", "FEMALE"}),
		Ownership: gofakeit.RandomString([]string{"PRIMARY", "SECONDARY", "JOINT"}),
		Name: yodlee.Name{
			First:    gofakeit.FirstName(),
			Last:     gofakeit.LastName(),
			Middle:   gofakeit.MiddleName(),
			FullName: gofakeit.Name(),
		},
	}
}

func makeAccountProfile() yodlee.AccountProfile {
	return yodlee.AccountProfile{
		Identifier: []yodlee.Identifier{
			{Type: "EMAIL", Value: gofakeit.Email()},
		},
		Address: []yodlee.AccountAddress{makeAccountAddress()},
		PhoneNumber: []yodlee.PhoneNumber{
			{Type: "HOME", Value: gofakeit.Phone()},
			{Type: "MOBILE", Value: gofakeit.Phone()},
		},
		Email: []yodlee.Email{
			{Type: "PRIMARY", Value: gofakeit.Email()},
		},
	}
}

func makePaymentProfile() yodlee.PaymentProfile {
	return yodlee.PaymentProfile{
		Identifier: yodlee.PaymentIdentifier{
			Type:  gofakeit.RandomString([]string{"IBAN", "BSB", "ROUTING_NUMBER"}),
			Value: gofakeit.Numerify("####################"),
		},
		Address: []yodlee.AccountAddress{makeAccountAddress()},
		PaymentBankTransferCode: yodlee.PaymentBankTransferCode{
			Id:   gofakeit.Numerify("#########"),
			Type: gofakeit.RandomString([]string{"BSB", "IFSC", "ROUTING_NUMBER"}),
		},
	}
}

func makeAutoRefresh() yodlee.AutoRefresh {
	return yodlee.AutoRefresh{
		AdditionalStatus: gofakeit.RandomString([]string{"SCHEDULED", "TEMP_ERROR"}),
		AsOfDate:         makeDate(),
		Status:           gofakeit.RandomString([]string{"ENABLED", "DISABLED"}),
	}
}

func makeMerchant() yodlee.Merchant {
	return yodlee.Merchant{
		Website: gofakeit.URL(),
		Address: makeAccountAddress(),
		LogoURLs: []yodlee.LogoURLs{
			{Type: "SQUARE", Url: gofakeit.URL()},
			{Type: "ORIGINAL", Url: gofakeit.URL()},
		},
		Contact: yodlee.Contact{
			Phone: gofakeit.Phone(),
			Email: gofakeit.Email(),
		},
		CategoryLabel: []string{gofakeit.BeerName(), gofakeit.BeerName()},
		Coordinates: yodlee.Coordinates{
			Latitude:  gofakeit.Latitude(),
			Longitude: gofakeit.Longitude(),
		},
		Name:    gofakeit.Company(),
		Id:      gofakeit.UUID(),
		Source:  gofakeit.RandomString([]string{"YODLEE", "FACTUAL"}),
		LogoURL: gofakeit.URL(),
	}
}

func makeDescription() yodlee.Description {
	return yodlee.Description{
		Security: gofakeit.Word(),
		Original: gofakeit.Sentence(6),
		Simple:   gofakeit.Sentence(3),
		Consumer: gofakeit.Company(),
	}
}

func makeBusinessCategory() yodlee.BusinessCategory {
	return yodlee.BusinessCategory{
		CategoryName: gofakeit.RandomString([]string{"Food", "Travel", "Shopping", "Entertainment", "Utilities"}),
		CategoryId:   int64(rand.Intn(100) + 1),
	}
}

func makeAssetClassification() yodlee.AssetClassification {
	return yodlee.AssetClassification{
		Allocation:          float64(rand.Intn(100)),
		ClassificationType:  gofakeit.RandomString([]string{"ASSET_CLASS", "SECTOR", "GEOGRAPHY"}),
		ClassificationValue: gofakeit.RandomString([]string{"EQUITY", "FIXED_INCOME", "CASH", "REAL_ESTATE"}),
	}
}

func makeCnctRefreshes(n int) []yodlee.DataExtractsProviderAccount {
	arr := make([]yodlee.DataExtractsProviderAccount, 0, n)
	for range n {
		arr = append(arr, yodlee.DataExtractsProviderAccount{
			Id:                           makeProviderAccountId(),
			DestinationProviderAccountId: makeProviderAccountId(),
			OauthMigrationStatus:         gofakeit.RandomString([]string{"IN_PROGRESS", "TO_BE_MIGRATED", "COMPLETED"}),
			IsManual:                     gofakeit.Bool(),
			LastUpdated:                  makeDate(),
			CreatedDate:                  makeDate(),
			AggregationSource:            gofakeit.RandomString([]string{"SYSTEM", "USER"}),
			IsDeleted:                    gofakeit.Bool(),
			ProviderId:                   makeProviderAccountId(),
			RequestId:                    gofakeit.UUID(),
			SourceProviderAccountIds:     []int64{makeProviderAccountId(), makeProviderAccountId()},
			AuthType:                     gofakeit.RandomString([]string{"OAUTH", "CREDENTIALS", "MFA_CREDENTIALS"}),
			Dataset:                      []yodlee.AccountDataset{makeAccountDataset()},
			Status:                       gofakeit.RandomString([]string{"LOGIN_IN_PROGRESS", "USER_INPUT_REQUIRED", "IN_PROGRESS", "PARTIAL_SUCCESS", "SUCCESS", "FAILED"}),
		})
	}
	return arr
}

func makeAcctRefreshes(n int) []yodlee.DataExtractsAccount {
	arr := make([]yodlee.DataExtractsAccount, 0, n)
	for range n {
		arr = append(arr, yodlee.DataExtractsAccount{
			ProviderAccountId:              makeProviderAccountId(),
			Id:                             makeAccountId(),
			LastUpdated:                    makeDate(),
			AccountName:                    makeAccountName(),
			AvailableCash:                  makeMoney(),
			IncludeInNetWorth:              gofakeit.Bool(),
			MoneyMarketBalance:             makeMoney(),
			EnrollmentDate:                 makeDate(),
			EstimatedDate:                  makeDate(),
			Memo:                           gofakeit.Sentence(4),
			Guarantor:                      gofakeit.Name(),
			InterestPaidLastYear:           makeMoney(),
			Balance:                        makeMoney(),
			HomeInsuranceType:              gofakeit.RandomString([]string{"HOME_OWNER", "RENTER", "FLOOD"}),
			Cash:                           makeMoney(),
			TotalCreditLine:                makeMoney(),
			ProviderName:                   gofakeit.Company(),
			ValuationType:                  gofakeit.RandomString([]string{"SYSTEM", "MANUAL"}),
			MarginBalance:                  makeMoney(),
			Apr:                            float64(rand.Intn(30)) / 10.0,
			AvailableCredit:                makeMoney(),
			SourceProductName:              gofakeit.AppName(),
			CurrentBalance:                 makeMoney(),
			IsManual:                       gofakeit.Bool(),
			EscrowBalance:                  makeMoney(),
			NextLevel:                      gofakeit.Word(),
			Classification:                 gofakeit.RandomString([]string{"CORPORATE", "SMALL_BUSINESS", "PERSONAL", "OTHER"}),
			LoanPayoffAmount:               makeMoney(),
			InterestRateType:               gofakeit.RandomString([]string{"FIXED", "VARIABLE", "ADJUSTABLE"}),
			LoanPayByDate:                  makeDate(),
			FaceAmount:                     makeMoney(),
			PolicyFromDate:                 makeDate(),
			SourceLoanOffsetEnabled:        gofakeit.Bool(),
			PremiumPaymentTerm:             gofakeit.RandomString([]string{"MONTHLY", "ANNUAL", "QUARTERLY"}),
			PolicyTerm:                     gofakeit.RandomString([]string{"1_YEAR", "5_YEAR", "10_YEAR"}),
			RepaymentPlanType:              gofakeit.RandomString([]string{"STANDARD", "GRADUATED", "INCOME_BASED"}),
			AggregatedAccountType:          gofakeit.RandomString([]string{"BANK", "INVESTMENT", "INSURANCE", "LOAN"}),
			AvailableBalance:               makeMoney(),
			AccountStatus:                  gofakeit.RandomString([]string{"ACTIVE", "INACTIVE", "CLOSED"}),
			LifeInsuranceType:              gofakeit.RandomString([]string{"TERM_LIFE_INSURANCE", "UNIVERSAL_LIFE_INSURANCE"}),
			Premium:                        makeMoney(),
			AggregationSource:              gofakeit.RandomString([]string{"SYSTEM", "USER"}),
			IsDeleted:                      gofakeit.Bool(),
			OverDraftLimit:                 makeMoney(),
			Nickname:                       gofakeit.Username(),
			Term:                           gofakeit.RandomString([]string{"6_MONTHS", "1_YEAR", "30_YEAR"}),
			InterestRate:                   float64(rand.Intn(20)) / 10.0,
			DeathBenefit:                   makeMoney(),
			Address:                        makeAccountAddress(),
			CashValue:                      makeMoney(),
			HomeValue:                      makeMoney(),
			AccountNumber:                  gofakeit.Numerify("############"),
			CreatedDate:                    makeDate(),
			InterestPaidYTD:                makeMoney(),
			BusinessInformation:            makeBusinessInformation(),
			MaxRedraw:                      makeMoney(),
			Collateral:                     gofakeit.Word(),
			Dataset:                        []yodlee.AccountDataset{makeAccountDataset()},
			RunningBalance:                 makeMoney(),
			SourceId:                       gofakeit.UUID(),
			DueDate:                        makeDate(),
			Frequency:                      gofakeit.RandomString([]string{"DAILY", "WEEKLY", "BIWEEKLY", "MONTHLY", "ANNUALLY"}),
			SourceAccountOwnership:         gofakeit.RandomString([]string{"INDIVIDUAL", "JOINT"}),
			MaturityAmount:                 makeMoney(),
			AssociatedProviderAccountId:    []int64{makeProviderAccountId()},
			IsAsset:                        gofakeit.Bool(),
			PrincipalBalance:               makeMoney(),
			TotalCashLimit:                 makeMoney(),
			MaturityDate:                   makeDate(),
			MinimumAmountDue:               makeMoney(),
			AnnualPercentageYield:          float64(rand.Intn(50)) / 10.0,
			AccountType:                    gofakeit.RandomString([]string{"CHECKING", "SAVINGS", "CD", "BROKERAGE"}),
			OriginationDate:                makeDate(),
			TotalVestedBalance:             makeMoney(),
			RewardBalance:                  []yodlee.RewardBalance{makeRewardBalance()},
			SourceAccountStatus:            gofakeit.RandomString([]string{"ACTIVE", "CLOSED"}),
			LinkedAccountIds:               []int64{makeAccountId(), makeAccountId()},
			DerivedApr:                     float64(rand.Intn(30)) / 10.0,
			PolicyEffectiveDate:            makeDate(),
			TotalUnvestedBalance:           makeMoney(),
			AnnuityBalance:                 makeMoney(),
			AccountCategory:                gofakeit.RandomString([]string{"INVESTMENT", "RETIREMENT", "OTHER"}),
			TotalCreditLimit:               makeMoney(),
			PolicyStatus:                   gofakeit.RandomString([]string{"ACTIVE", "INACTIVE"}),
			ShortBalance:                   makeMoney(),
			Lender:                         gofakeit.Company(),
			LastEmployeeContributionAmount: makeMoney(),
			ProviderId:                     strconv.Itoa(int(makeProviderAccountId())),
			LastPaymentDate:                makeDate(),
			PrimaryRewardUnit:              gofakeit.RandomString([]string{"MILES", "POINTS", "CASH_BACK"}),
			LastPaymentAmount:              makeMoney(),
			RemainingBalance:               makeMoney(),
			UserClassification:             gofakeit.RandomString([]string{"BUSINESS", "PERSONAL"}),
			BankTransferCode:               []yodlee.BankTransferCode{makeBankTransferCode()},
			ExpirationDate:                 makeDate(),
			Coverage:                       []yodlee.Coverage{makeCoverage()},
			CashApr:                        float64(rand.Intn(30)) / 10.0,
			OauthMigrationStatus:           gofakeit.RandomString([]string{"IN_PROGRESS", "COMPLETED"}),
			DisplayedName:                  gofakeit.Name(),
			SourceProviderAccountId:        makeProviderAccountId(),
			AmountDue:                      makeMoney(),
			CurrentLevel:                   gofakeit.Word(),
			OriginalLoanAmount:             makeMoney(),
			PolicyToDate:                   makeDate(),
			LoanPayoffDetails:              makeLoanPayoffDetails(),
			Container:                      gofakeit.RandomString([]string{"bank", "creditCard", "investment", "insurance", "loan", "reward", "realEstate"}),
			IsOwnedAtSource:                gofakeit.Bool(),
			LastEmployeeContributionDate:   makeDate(),
			LastPayment:                    makeMoney(),
			RecurringPayment:               makeMoney(),
			MinRedraw:                      makeMoney(),
		})
	}
	return arr
}

func makeTxnRefreshes(n int) []yodlee.DataExtractsTransaction {
	arr := make([]yodlee.DataExtractsTransaction, 0, n)
	for range n {
		arr = append(arr, yodlee.DataExtractsTransaction{
			AccountId:                  makeAccountId(),
			Id:                         makeTransactionId(),
			Date:                       makeDate(),
			Quantity:                   makeAmount(),
			SourceId:                   gofakeit.UUID(),
			Symbol:                     gofakeit.RandomString([]string{"AAPL", "GOOG", "AMZN", "TSLA", "MSFT"}),
			CusipNumber:                gofakeit.Numerify("#########"),
			SourceApcaNumber:           gofakeit.Numerify("######"),
			HighLevelCategoryId:        int64(rand.Intn(100) + 1),
			Memo:                       gofakeit.Sentence(4),
			Type:                       gofakeit.RandomString([]string{"BUY", "SELL", "DIVIDEND", "INTEREST"}),
			Intermediary:               []string{gofakeit.Company()},
			Frequency:                  gofakeit.RandomString([]string{"DAILY", "WEEKLY", "MONTHLY"}),
			LastUpdated:                makeDate(),
			Price:                      makeMoney(),
			SourceMerchantCategoryCode: gofakeit.Numerify("####"),
			CheckNumber:                gofakeit.Numerify("######"),
			TransactionDateTime:        makeDate(),
			TypeAtSource:               gofakeit.Word(),
			Valoren:                    gofakeit.Numerify("#######"),
			IsManual:                   gofakeit.Bool(),
			SourceBillerName:           gofakeit.Company(),
			Merchant:                   makeMerchant(),
			Sedol:                      gofakeit.Lexify("???????"),
			CategoryType:               gofakeit.RandomString([]string{"TRANSFER", "DEFERRED_COMPENSATION", "UNCATEGORIZE", "INCOME", "EXPENSE"}),
			SourceType:                 gofakeit.RandomString([]string{"AGGREGATED", "MANUAL"}),
			SubType:                    gofakeit.RandomString([]string{"REWARD", "ATM_DEPOSIT", "ATM_WITHDRAWAL", "TAX", "REFUND"}),
			HoldingDescription:         gofakeit.Sentence(3),
			Status:                     gofakeit.RandomString([]string{"POSTED", "PENDING", "SCHEDULED", "FAILED", "CLEARED"}),
			DetailCategoryId:           int64(rand.Intn(1000) + 1),
			Description:                makeDescription(),
			SettleDate:                 makeDate(),
			PostDateTime:               makeDate(),
			BaseType:                   gofakeit.RandomString([]string{"CREDIT", "DEBIT"}),
			CategorySource:             gofakeit.RandomString([]string{"SYSTEM", "USER"}),
			Principal:                  makeMoney(),
			SourceBillerCode:           gofakeit.Numerify("######"),
			IsDeleted:                  gofakeit.Bool(),
			Interest:                   makeMoney(),
			Commission:                 makeMoney(),
			MerchantType:               gofakeit.RandomString([]string{"GOODS", "SERVICES", "OTHERS"}),
			Amount:                     makeMoney(),
			IsPhysical:                 gofakeit.Bool(),
			IsRecurring:                gofakeit.RandomString([]string{"true", "false"}),
			TransactionDate:            makeDate(),
			CreatedDate:                makeDate(),
			CONTAINER:                  gofakeit.RandomString([]string{"bank", "creditCard", "investment", "insurance", "loan"}),
			BusinessCategory:           makeBusinessCategory(),
			PostDate:                   makeDate(),
			ParentCategoryId:           int64(rand.Intn(100) + 1),
			Category:                   gofakeit.RandomString([]string{"Food", "Travel", "Shopping", "Entertainment", "Utilities", "Income"}),
			RunningBalance:             makeMoney(),
			CategoryId:                 int64(rand.Intn(1000) + 1),
			Isin:                       gofakeit.Lexify("??##########"),
		})
	}
	return arr
}

func makeHoldRefreshes(n int) []yodlee.DataExtractsHolding {
	arr := make([]yodlee.DataExtractsHolding, 0, n)
	for range n {
		arr = append(arr, yodlee.DataExtractsHolding{
			AccountId:               makeAccountId(),
			Id:                      makeHoldingId(),
			LastUpdated:             makeDate(),
			HoldingType:             makeHoldingName(),
			Symbol:                  gofakeit.RandomString([]string{"AAPL", "GOOG", "AMZN", "TSLA", "MSFT", "SPY", "QQQ"}),
			ExercisedQuantity:       makeAmount(),
			CusipNumber:             gofakeit.Numerify("#########"),
			VestedQuantity:          makeAmount(),
			Description:             gofakeit.Sentence(5),
			UnvestedValue:           makeMoney(),
			SecurityStyle:           gofakeit.RandomString([]string{"Growth", "Value", "Blend"}),
			VestedValue:             makeMoney(),
			OptionType:              gofakeit.RandomString([]string{"CALL", "PUT"}),
			MatchStatus:             gofakeit.RandomString([]string{"UNCLASSIFIED", "CLASSIFIED"}),
			MaturityDate:            makeDate(),
			Price:                   makeMoney(),
			Term:                    gofakeit.RandomString([]string{"SHORT", "LONG"}),
			ContractQuantity:        makeAmount(),
			IsShort:                 gofakeit.Bool(),
			Value:                   makeMoney(),
			ExpirationDate:          makeDate(),
			InterestRate:            float64(rand.Intn(20)) / 10.0,
			AccruedInterest:         makeMoney(),
			GrantDate:               makeDate(),
			Sedol:                   gofakeit.Lexify("???????"),
			VestedSharesExercisable: makeAmount(),
			Spread:                  makeMoney(),
			ProviderAccountId:       makeProviderAccountId(),
			EnrichedDescription:     gofakeit.Sentence(4),
			CouponRate:              float64(rand.Intn(100)) / 10.0,
			CreatedDate:             makeDate(),
			AccruedIncome:           makeMoney(),
			SecurityType:            gofakeit.RandomString([]string{"STOCK", "MUTUAL_FUND", "ETF", "BOND", "CD"}),
			UnvestedQuantity:        makeAmount(),
			CostBasis:               makeMoney(),
			VestingDate:             makeDate(),
			Isin:                    gofakeit.Lexify("??##########"),
			StrikePrice:             makeMoney(),
		})
	}
	return arr
}

func makeCnctResponses(n int) yodlee.ProviderAccountResponse {
	arr := make([]yodlee.ProviderAccount, 0, n)
	for range n {
		arr = append(arr, yodlee.ProviderAccount{
			Id:                   makeProviderAccountId(),
			LastUpdated:          makeDate(),
			RequestId:            gofakeit.UUID(),
			OauthMigrationStatus: gofakeit.RandomString([]string{"IN_PROGRESS", "TO_BE_MIGRATED", "COMPLETED"}),
			IsManual:             gofakeit.Bool(),
			IsRealTimeMFA:        gofakeit.Bool(),
			CreatedDate:          makeDate(),
			AggregationSource:    gofakeit.RandomString([]string{"SYSTEM", "USER"}),
			ProviderId:           makeProviderAccountId(),
			ConsentId:            int64(rand.Intn(10000) + 1),
			AuthType:             gofakeit.RandomString([]string{"OAUTH", "CREDENTIALS", "MFA_CREDENTIALS"}),
			Dataset:              []yodlee.AccountDataset{makeAccountDataset()},
			Status:               gofakeit.RandomString([]string{"LOGIN_IN_PROGRESS", "USER_INPUT_REQUIRED", "IN_PROGRESS", "PARTIAL_SUCCESS", "SUCCESS", "FAILED"}),
			Preferences: yodlee.ProviderAccountPreferences{
				IsDataExtractsEnabled:   gofakeit.Bool(),
				LinkedProviderAccountId: makeProviderAccountId(),
				IsAutoRefreshEnabled:    gofakeit.Bool(),
			},
		})
	}
	return yodlee.ProviderAccountResponse{ProviderAccount: arr}
}

func makeAcctResponses(n int) yodlee.AccountResponse {
	arr := make([]yodlee.Account, 0, n)
	for range n {
		arr = append(arr, yodlee.Account{
			ProviderAccountId:              makeProviderAccountId(),
			Id:                             makeAccountId(),
			LastUpdated:                    makeDate(),
			AccountName:                    makeAccountName(),
			AvailableCash:                  makeMoney(),
			IncludeInNetWorth:              gofakeit.Bool(),
			MoneyMarketBalance:             makeMoney(),
			EnrollmentDate:                 makeDate(),
			EstimatedDate:                  makeDate(),
			Memo:                           gofakeit.Sentence(4),
			Guarantor:                      gofakeit.Name(),
			InterestPaidLastYear:           makeMoney(),
			Balance:                        makeMoney(),
			HomeInsuranceType:              gofakeit.RandomString([]string{"HOME_OWNER", "RENTER", "FLOOD"}),
			Cash:                           makeMoney(),
			TotalCreditLine:                makeMoney(),
			ProviderName:                   gofakeit.Company(),
			ValuationType:                  gofakeit.RandomString([]string{"SYSTEM", "MANUAL"}),
			MarginBalance:                  makeMoney(),
			Apr:                            float64(rand.Intn(30)) / 10.0,
			AvailableCredit:                makeMoney(),
			SourceProductName:              gofakeit.AppName(),
			CurrentBalance:                 makeMoney(),
			IsManual:                       gofakeit.Bool(),
			Profile:                        makeAccountProfile(),
			EscrowBalance:                  makeMoney(),
			NextLevel:                      gofakeit.Word(),
			Classification:                 gofakeit.RandomString([]string{"CORPORATE", "SMALL_BUSINESS", "PERSONAL", "OTHER"}),
			LoanPayoffAmount:               makeMoney(),
			InterestRateType:               gofakeit.RandomString([]string{"FIXED", "VARIABLE", "ADJUSTABLE"}),
			LoanPayByDate:                  makeDate(),
			FaceAmount:                     makeMoney(),
			PolicyFromDate:                 makeDate(),
			SourceLoanOffsetEnabled:        gofakeit.Bool(),
			PremiumPaymentTerm:             gofakeit.RandomString([]string{"MONTHLY", "ANNUAL", "QUARTERLY"}),
			PolicyTerm:                     gofakeit.RandomString([]string{"1_YEAR", "5_YEAR", "10_YEAR"}),
			RepaymentPlanType:              gofakeit.RandomString([]string{"STANDARD", "GRADUATED", "INCOME_BASED"}),
			AggregatedAccountType:          gofakeit.RandomString([]string{"BANK", "INVESTMENT", "INSURANCE", "LOAN"}),
			AvailableBalance:               makeMoney(),
			AccountStatus:                  gofakeit.RandomString([]string{"ACTIVE", "INACTIVE", "CLOSED"}),
			LifeInsuranceType:              gofakeit.RandomString([]string{"TERM_LIFE_INSURANCE", "UNIVERSAL_LIFE_INSURANCE"}),
			FullAccountNumber:              gofakeit.Numerify("################"),
			Premium:                        makeMoney(),
			AggregationSource:              gofakeit.RandomString([]string{"SYSTEM", "USER"}),
			OverDraftLimit:                 makeMoney(),
			Nickname:                       gofakeit.Username(),
			Term:                           gofakeit.RandomString([]string{"6_MONTHS", "1_YEAR", "30_YEAR"}),
			InterestRate:                   float64(rand.Intn(20)) / 10.0,
			DeathBenefit:                   makeMoney(),
			Address:                        makeAccountAddress(),
			CashValue:                      makeMoney(),
			Holder:                         []yodlee.AccountHolder{makeAccountHolder()},
			HomeValue:                      makeMoney(),
			AccountNumber:                  gofakeit.Numerify("############"),
			CreatedDate:                    makeDate(),
			InterestPaidYTD:                makeMoney(),
			BusinessInformation:            makeBusinessInformation(),
			MaxRedraw:                      makeMoney(),
			Collateral:                     gofakeit.Word(),
			Dataset:                        []yodlee.AccountDataset{makeAccountDataset()},
			RunningBalance:                 makeMoney(),
			SourceId:                       gofakeit.UUID(),
			DueDate:                        makeDate(),
			Frequency:                      gofakeit.RandomString([]string{"DAILY", "WEEKLY", "BIWEEKLY", "MONTHLY", "ANNUALLY"}),
			SourceAccountOwnership:         gofakeit.RandomString([]string{"INDIVIDUAL", "JOINT"}),
			MaturityAmount:                 makeMoney(),
			AssociatedProviderAccountId:    []int64{makeProviderAccountId()},
			IsAsset:                        gofakeit.Bool(),
			PrincipalBalance:               makeMoney(),
			TotalCashLimit:                 makeMoney(),
			MaturityDate:                   makeDate(),
			MinimumAmountDue:               makeMoney(),
			AnnualPercentageYield:          float64(rand.Intn(50)) / 10.0,
			AccountType:                    gofakeit.RandomString([]string{"CHECKING", "SAVINGS", "CD", "BROKERAGE"}),
			OriginationDate:                makeDate(),
			TotalVestedBalance:             makeMoney(),
			RewardBalance:                  []yodlee.RewardBalance{makeRewardBalance()},
			SourceAccountStatus:            gofakeit.RandomString([]string{"ACTIVE", "CLOSED"}),
			LinkedAccountIds:               []int64{makeAccountId(), makeAccountId()},
			DerivedApr:                     float64(rand.Intn(30)) / 10.0,
			PolicyEffectiveDate:            makeDate(),
			TotalUnvestedBalance:           makeMoney(),
			AnnuityBalance:                 makeMoney(),
			AccountCategory:                gofakeit.RandomString([]string{"INVESTMENT", "RETIREMENT", "OTHER"}),
			TotalCreditLimit:               makeMoney(),
			PolicyStatus:                   gofakeit.RandomString([]string{"ACTIVE", "INACTIVE"}),
			ShortBalance:                   makeMoney(),
			Lender:                         gofakeit.Company(),
			LastEmployeeContributionAmount: makeMoney(),
			ProviderId:                     strconv.Itoa(int(makeProviderAccountId())),
			LastPaymentDate:                makeDate(),
			PrimaryRewardUnit:              gofakeit.RandomString([]string{"MILES", "POINTS", "CASH_BACK"}),
			LastPaymentAmount:              makeMoney(),
			RemainingBalance:               makeMoney(),
			UserClassification:             gofakeit.RandomString([]string{"BUSINESS", "PERSONAL"}),
			BankTransferCode:               []yodlee.BankTransferCode{makeBankTransferCode()},
			ExpirationDate:                 makeDate(),
			Coverage:                       []yodlee.Coverage{makeCoverage()},
			CashApr:                        float64(rand.Intn(30)) / 10.0,
			AutoRefresh:                    makeAutoRefresh(),
			OauthMigrationStatus:           gofakeit.RandomString([]string{"IN_PROGRESS", "COMPLETED"}),
			DisplayedName:                  gofakeit.Name(),
			FullAccountNumberList: yodlee.FullAccountNumberList{
				PaymentAccountNumber:   gofakeit.Numerify("############"),
				TokenizedAccountNumber: gofakeit.UUID(),
				UnmaskedAccountNumber:  gofakeit.Numerify("################"),
			},
			AmountDue:                    makeMoney(),
			CurrentLevel:                 gofakeit.Word(),
			OriginalLoanAmount:           makeMoney(),
			PolicyToDate:                 makeDate(),
			LoanPayoffDetails:            makeLoanPayoffDetails(),
			PaymentProfile:               makePaymentProfile(),
			CONTAINER:                    gofakeit.RandomString([]string{"bank", "creditCard", "investment", "insurance", "loan", "reward", "realEstate"}),
			IsOwnedAtSource:              gofakeit.Bool(),
			LastEmployeeContributionDate: makeDate(),
			LastPayment:                  makeMoney(),
			RecurringPayment:             makeMoney(),
			MinRedraw:                    makeMoney(),
		})
	}
	return yodlee.AccountResponse{Account: arr}
}

func makeTxnResponses(n int) yodlee.TransactionResponse {
	arr := make([]yodlee.TransactionWithDateTime, 0, n)
	for range n {
		arr = append(arr, yodlee.TransactionWithDateTime{
			AccountId:                  makeAccountId(),
			Id:                         makeTransactionId(),
			Date:                       makeDate(),
			Quantity:                   makeAmount(),
			SourceId:                   gofakeit.UUID(),
			Symbol:                     gofakeit.RandomString([]string{"AAPL", "GOOG", "AMZN", "TSLA", "MSFT"}),
			CusipNumber:                gofakeit.Numerify("#########"),
			SourceApcaNumber:           gofakeit.Numerify("######"),
			HighLevelCategoryId:        int64(rand.Intn(100) + 1),
			Memo:                       gofakeit.Sentence(4),
			Type:                       gofakeit.RandomString([]string{"BUY", "SELL", "DIVIDEND", "INTEREST"}),
			Intermediary:               []string{gofakeit.Company()},
			Frequency:                  gofakeit.RandomString([]string{"DAILY", "WEEKLY", "MONTHLY"}),
			LastUpdated:                makeDate(),
			Price:                      makeMoney(),
			SourceMerchantCategoryCode: gofakeit.Numerify("####"),
			CheckNumber:                gofakeit.Numerify("######"),
			TransactionDateTime:        makeDate(),
			TypeAtSource:               gofakeit.Word(),
			Valoren:                    gofakeit.Numerify("#######"),
			IsManual:                   gofakeit.Bool(),
			SourceBillerName:           gofakeit.Company(),
			Merchant:                   makeMerchant(),
			Sedol:                      gofakeit.Lexify("???????"),
			CategoryType:               gofakeit.RandomString([]string{"TRANSFER", "DEFERRED_COMPENSATION", "UNCATEGORIZE", "INCOME", "EXPENSE"}),
			SourceType:                 gofakeit.RandomString([]string{"AGGREGATED", "MANUAL"}),
			SubType:                    gofakeit.RandomString([]string{"REWARD", "ATM_DEPOSIT", "ATM_WITHDRAWAL", "TAX", "REFUND"}),
			HoldingDescription:         gofakeit.Sentence(3),
			Status:                     gofakeit.RandomString([]string{"POSTED", "PENDING", "SCHEDULED", "FAILED", "CLEARED"}),
			DetailCategoryId:           int64(rand.Intn(1000) + 1),
			Description:                makeDescription(),
			SettleDate:                 makeDate(),
			PostDateTime:               makeDate(),
			BaseType:                   gofakeit.RandomString([]string{"CREDIT", "DEBIT"}),
			CategorySource:             gofakeit.RandomString([]string{"SYSTEM", "USER"}),
			Principal:                  makeMoney(),
			SourceBillerCode:           gofakeit.Numerify("######"),
			Interest:                   makeMoney(),
			Commission:                 makeMoney(),
			MerchantType:               gofakeit.RandomString([]string{"GOODS", "SERVICES", "OTHERS"}),
			Amount:                     makeMoney(),
			IsPhysical:                 gofakeit.Bool(),
			IsRecurring:                gofakeit.RandomString([]string{"true", "false"}),
			TransactionDate:            makeDate(),
			CreatedDate:                makeDate(),
			CONTAINER:                  gofakeit.RandomString([]string{"bank", "creditCard", "investment", "insurance", "loan"}),
			BusinessCategory:           makeBusinessCategory(),
			PostDate:                   makeDate(),
			ParentCategoryId:           int64(rand.Intn(100) + 1),
			Category:                   gofakeit.RandomString([]string{"Food", "Travel", "Shopping", "Entertainment", "Utilities", "Income"}),
			RunningBalance:             makeMoney(),
			CategoryId:                 int64(rand.Intn(1000) + 1),
			Isin:                       gofakeit.Lexify("??##########"),
		})
	}
	return yodlee.TransactionResponse{Transaction: arr}
}

func makeHoldResponses(n int) yodlee.HoldingResponse {
	arr := make([]yodlee.Holding, 0, n)
	for range n {
		arr = append(arr, yodlee.Holding{
			AccountId:               makeAccountId(),
			Id:                      makeHoldingId(),
			LastUpdated:             makeDate(),
			HoldingType:             makeHoldingName(),
			Symbol:                  gofakeit.RandomString([]string{"AAPL", "GOOG", "AMZN", "TSLA", "MSFT", "SPY", "QQQ"}),
			ExercisedQuantity:       makeAmount(),
			CusipNumber:             gofakeit.Numerify("#########"),
			AssetClassification:     []yodlee.AssetClassification{makeAssetClassification()},
			VestedQuantity:          makeAmount(),
			Description:             gofakeit.Sentence(5),
			UnvestedValue:           makeMoney(),
			SecurityStyle:           gofakeit.RandomString([]string{"Growth", "Value", "Blend"}),
			VestedValue:             makeMoney(),
			OptionType:              gofakeit.RandomString([]string{"CALL", "PUT"}),
			MatchStatus:             gofakeit.RandomString([]string{"UNCLASSIFIED", "CLASSIFIED"}),
			MaturityDate:            makeDate(),
			Price:                   makeMoney(),
			Term:                    gofakeit.RandomString([]string{"SHORT", "LONG"}),
			ContractQuantity:        makeAmount(),
			IsShort:                 gofakeit.Bool(),
			Value:                   makeMoney(),
			ExpirationDate:          makeDate(),
			InterestRate:            float64(rand.Intn(20)) / 10.0,
			IsManual:                gofakeit.Bool(),
			AccruedInterest:         makeMoney(),
			GrantDate:               makeDate(),
			Sedol:                   gofakeit.Lexify("???????"),
			VestedSharesExercisable: makeAmount(),
			Spread:                  makeMoney(),
			ProviderAccountId:       makeProviderAccountId(),
			EnrichedDescription:     gofakeit.Sentence(4),
			CouponRate:              float64(rand.Intn(100)) / 10.0,
			CreatedDate:             makeDate(),
			AccruedIncome:           makeMoney(),
			SecurityType:            gofakeit.RandomString([]string{"STOCK", "MUTUAL_FUND", "ETF", "BOND", "CD"}),
			UnvestedQuantity:        makeAmount(),
			CostBasis:               makeMoney(),
			VestingDate:             makeDate(),
			Isin:                    gofakeit.Lexify("??##########"),
			StrikePrice:             makeMoney(),
		})
	}
	return yodlee.HoldingResponse{Holding: arr}
}

var Period = time.Second * 1

func ExecuteDemoProducer(serverConfig model.Config, kafkaConfig *sarama.Config) {
	producer := model.MakeSaramaProducer(serverConfig.KafkaBrokers, kafkaConfig)

	slog.Info("starting yodlee ops, starting test producer", "serverConfig", serverConfig, "kafkaConfig", fmt.Sprintf("%+v", kafkaConfig))

	ticker := time.NewTicker(Period)
	for range ticker.C {
		topicKind := rand.Intn(8)

		var topic model.Topic
		var value any

		n := rand.Intn(10)

		switch topicKind {
		case 0:
			topic = model.CnctRefreshTopic
			value = makeCnctRefreshes(n)
		case 1:
			topic = model.AcctRefreshTopic
			value = makeAcctRefreshes(n)
		case 2:
			topic = model.TxnRefreshTopic
			value = makeTxnRefreshes(n)
		case 3:
			topic = model.HoldRefreshTopic
			value = makeHoldRefreshes(n)
		case 4:
			topic = model.CnctResponseTopic
			value = makeCnctResponses(n)
		case 5:
			topic = model.AcctResponseTopic
			value = makeAcctResponses(n)
		case 6:
			topic = model.TxnResponseTopic
			value = makeTxnResponses(n)
		case 7:
			topic = model.HoldResponseTopic
			value = makeHoldResponses(n)
		default:
			continue
		}

		slog.Info("producing message", "topic", topic, "value", value)

		v, err := json.Marshal(value)
		if err != nil {
			slog.Error("failed to marshal produce message", "err", err)
			continue
		}

		producer.Input() <- &sarama.ProducerMessage{
			Topic: string(topic),
			Key:   sarama.StringEncoder(makePartyId()),
			Value: sarama.ByteEncoder(v),
		}
	}
}
