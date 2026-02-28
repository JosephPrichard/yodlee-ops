package yodlee

// models we need from yodlee's swagger spec api. we manually maintain this so we have a strong degree of control over how we store the data after deserializing

type DataExtractsProviderAccount struct {
	DestinationProviderAccountId int64            `json:"destinationProviderAccountId,omitempty"`
	OauthMigrationStatus         string           `json:"oauthMigrationStatus,omitempty"`
	IsManual                     bool             `json:"isManual,omitempty"`
	LastUpdated                  string           `json:"lastUpdated,omitempty"`
	CreatedDate                  string           `json:"createdDate,omitempty"`
	AggregationSource            string           `json:"aggregationSource,omitempty"`
	IsDeleted                    bool             `json:"isDeleted,omitempty"`
	ProviderId                   int64            `json:"providerId,omitempty"`
	RequestId                    string           `json:"requestId,omitempty"`
	SourceProviderAccountIds     []int64          `json:"sourceProviderAccountIds,omitempty"`
	Id                           int64            `json:"id,omitempty"`
	AuthType                     string           `json:"authType,omitempty"`
	Dataset                      []AccountDataset `json:"dataset,omitempty"`
	Status                       string           `json:"status,omitempty"`
}

type DataExtractsAccount struct {
	AvailableCash                  Money               `json:"availableCash,omitempty"`
	IncludeInNetWorth              bool                `json:"includeInNetWorth,omitempty"`
	MoneyMarketBalance             Money               `json:"moneyMarketBalance,omitempty"`
	EnrollmentDate                 string              `json:"enrollmentDate,omitempty"`
	EstimatedDate                  string              `json:"estimatedDate,omitempty"`
	Memo                           string              `json:"memo,omitempty"`
	Guarantor                      string              `json:"guarantor,omitempty"`
	InterestPaidLastYear           Money               `json:"interestPaidLastYear,omitempty"`
	LastUpdated                    string              `json:"lastUpdated,omitempty"`
	Balance                        Money               `json:"balance,omitempty"`
	NsccMemberClearingCode         string              `json:"nsccMemberClearingCode,omitempty"`
	HomeInsuranceType              string              `json:"homeInsuranceType,omitempty"`
	Id                             int64               `json:"id,omitempty"`
	Cash                           Money               `json:"cash,omitempty"`
	TotalCreditLine                Money               `json:"totalCreditLine,omitempty"`
	ProviderName                   string              `json:"providerName,omitempty"`
	ValuationType                  string              `json:"valuationType,omitempty"`
	MarginBalance                  Money               `json:"marginBalance,omitempty"`
	Apr                            float64             `json:"apr,omitempty"`
	AvailableCredit                Money               `json:"availableCredit,omitempty"`
	SourceProductName              string              `json:"sourceProductName,omitempty"`
	CurrentBalance                 Money               `json:"currentBalance,omitempty"`
	IsManual                       bool                `json:"isManual,omitempty"`
	EscrowBalance                  Money               `json:"escrowBalance,omitempty"`
	NextLevel                      string              `json:"nextLevel,omitempty"`
	Classification                 string              `json:"classification,omitempty"`
	LoanPayoffAmount               Money               `json:"loanPayoffAmount,omitempty"`
	InterestRateType               string              `json:"interestRateType,omitempty"`
	LoanPayByDate                  string              `json:"loanPayByDate,omitempty"`
	FaceAmount                     Money               `json:"faceAmount,omitempty"`
	PolicyFromDate                 string              `json:"policyFromDate,omitempty"`
	SourceLoanOffsetEnabled        bool                `json:"sourceLoanOffsetEnabled,omitempty"`
	DtccMemberClearingCode         string              `json:"dtccMemberClearingCode,omitempty"`
	PremiumPaymentTerm             string              `json:"premiumPaymentTerm,omitempty"`
	PolicyTerm                     string              `json:"policyTerm,omitempty"`
	RepaymentPlanType              string              `json:"repaymentPlanType,omitempty"`
	AggregatedAccountType          string              `json:"aggregatedAccountType,omitempty"`
	AvailableBalance               Money               `json:"availableBalance,omitempty"`
	AccountStatus                  string              `json:"accountStatus,omitempty"`
	LifeInsuranceType              string              `json:"lifeInsuranceType,omitempty"`
	Premium                        Money               `json:"premium,omitempty"`
	AggregationSource              string              `json:"aggregationSource,omitempty"`
	IsDeleted                      bool                `json:"isDeleted,omitempty"`
	OverDraftLimit                 Money               `json:"overDraftLimit,omitempty"`
	Nickname                       string              `json:"nickname,omitempty"`
	Term                           string              `json:"term,omitempty"`
	InterestRate                   float64             `json:"interestRate,omitempty"`
	DeathBenefit                   Money               `json:"deathBenefit,omitempty"`
	Address                        AccountAddress      `json:"address,omitempty"`
	SourceLoanRepaymentType        string              `json:"sourceLoanRepaymentType,omitempty"`
	CashValue                      Money               `json:"cashValue,omitempty"`
	Var401kLoan                    Money               `json:"401kLoan,omitempty"`
	HomeValue                      Money               `json:"homeValue,omitempty"`
	AccountNumber                  string              `json:"accountNumber,omitempty"`
	CreatedDate                    string              `json:"createdDate,omitempty"`
	InterestPaidYTD                Money               `json:"interestPaidYTD,omitempty"`
	BusinessInformation            BusinessInformation `json:"businessInformation,omitempty"`
	MaxRedraw                      Money               `json:"maxRedraw,omitempty"`
	ProviderAccountId              int64               `json:"providerAccountId,omitempty"`
	IsSelectedForAggregation       bool                `json:"isSelectedForAggregation,omitempty"`
	Collateral                     string              `json:"collateral,omitempty"`
	Dataset                        []AccountDataset    `json:"dataset,omitempty"`
	RunningBalance                 Money               `json:"runningBalance,omitempty"`
	SourceId                       string              `json:"sourceId,omitempty"`
	DueDate                        string              `json:"dueDate,omitempty"`
	Frequency                      string              `json:"frequency,omitempty"`
	SourceAccountOwnership         string              `json:"sourceAccountOwnership,omitempty"`
	MaturityAmount                 Money               `json:"maturityAmount,omitempty"`
	AssociatedProviderAccountId    []int64             `json:"associatedProviderAccountId,omitempty"`
	IsAsset                        bool                `json:"isAsset,omitempty"`
	PrincipalBalance               Money               `json:"principalBalance,omitempty"`
	TotalCashLimit                 Money               `json:"totalCashLimit,omitempty"`
	MaturityDate                   string              `json:"maturityDate,omitempty"`
	MinimumAmountDue               Money               `json:"minimumAmountDue,omitempty"`
	AnnualPercentageYield          float64             `json:"annualPercentageYield,omitempty"`
	AccountType                    string              `json:"accountType,omitempty"`
	OriginationDate                string              `json:"originationDate,omitempty"`
	TotalVestedBalance             Money               `json:"totalVestedBalance,omitempty"`
	RewardBalance                  []RewardBalance     `json:"rewardBalance,omitempty"`
	SourceAccountStatus            string              `json:"sourceAccountStatus,omitempty"`
	LinkedAccountIds               []int64             `json:"linkedAccountIds,omitempty"`
	DerivedApr                     float64             `json:"derivedApr,omitempty"`
	PolicyEffectiveDate            string              `json:"policyEffectiveDate,omitempty"`
	TotalUnvestedBalance           Money               `json:"totalUnvestedBalance,omitempty"`
	AnnuityBalance                 Money               `json:"annuityBalance,omitempty"`
	AccountCategory                string              `json:"accountCategory,omitempty"`
	AccountName                    string              `json:"accountName,omitempty"`
	TotalCreditLimit               Money               `json:"totalCreditLimit,omitempty"`
	PolicyStatus                   string              `json:"policyStatus,omitempty"`
	ShortBalance                   Money               `json:"shortBalance,omitempty"`
	Lender                         string              `json:"lender,omitempty"`
	LastEmployeeContributionAmount Money               `json:"lastEmployeeContributionAmount,omitempty"`
	ProviderId                     string              `json:"providerId,omitempty"`
	LastPaymentDate                string              `json:"lastPaymentDate,omitempty"`
	PrimaryRewardUnit              string              `json:"primaryRewardUnit,omitempty"`
	LastPaymentAmount              Money               `json:"lastPaymentAmount,omitempty"`
	RemainingBalance               Money               `json:"remainingBalance,omitempty"`
	UserClassification             string              `json:"userClassification,omitempty"`
	BankTransferCode               []BankTransferCode  `json:"bankTransferCode,omitempty"`
	ExpirationDate                 string              `json:"expirationDate,omitempty"`
	Coverage                       []Coverage          `json:"coverage,omitempty"`
	CashApr                        float64             `json:"cashApr,omitempty"`
	OauthMigrationStatus           string              `json:"oauthMigrationStatus,omitempty"`
	DisplayedName                  string              `json:"displayedName,omitempty"`
	SourceProviderAccountId        int64               `json:"sourceProviderAccountId,omitempty"`
	AmountDue                      Money               `json:"amountDue,omitempty"`
	CurrentLevel                   string              `json:"currentLevel,omitempty"`
	OriginalLoanAmount             Money               `json:"originalLoanAmount,omitempty"`
	PolicyToDate                   string              `json:"policyToDate,omitempty"`
	LoanPayoffDetails              LoanPayoffDetails   `json:"loanPayoffDetails,omitempty"`
	Container                      string              `json:"CONTAINER,omitempty"`
	IsOwnedAtSource                bool                `json:"isOwnedAtSource,omitempty"`
	LastEmployeeContributionDate   string              `json:"lastEmployeeContributionDate,omitempty"`
	LastPayment                    Money               `json:"lastPayment,omitempty"`
	RecurringPayment               Money               `json:"recurringPayment,omitempty"`
	MinRedraw                      Money               `json:"minRedraw,omitempty"`
}

type DataExtractsHolding struct {
	Symbol                  string  `json:"symbol,omitempty"`
	ExercisedQuantity       float64 `json:"exercisedQuantity,omitempty"`
	CusipNumber             string  `json:"cusipNumber,omitempty"`
	VestedQuantity          float64 `json:"vestedQuantity,omitempty"`
	Description             string  `json:"description,omitempty"`
	UnvestedValue           Money   `json:"unvestedValue,omitempty"`
	SecurityStyle           string  `json:"securityStyle,omitempty"`
	VestedValue             Money   `json:"vestedValue,omitempty"`
	OptionType              string  `json:"optionType,omitempty"`
	LastUpdated             string  `json:"lastUpdated,omitempty"`
	MatchStatus             string  `json:"matchStatus,omitempty"`
	HoldingType             string  `json:"holdingType,omitempty"`
	MaturityDate            string  `json:"maturityDate,omitempty"`
	Price                   Money   `json:"price,omitempty"`
	Term                    string  `json:"term,omitempty"`
	ContractQuantity        float64 `json:"contractQuantity,omitempty"`
	Id                      int64   `json:"id,omitempty"`
	IsShort                 bool    `json:"isShort,omitempty"`
	Value                   Money   `json:"value,omitempty"`
	ExpirationDate          string  `json:"expirationDate,omitempty"`
	InterestRate            float64 `json:"interestRate,omitempty"`
	Quantity                float64 `json:"quantity,omitempty"`
	AccruedInterest         Money   `json:"accruedInterest,omitempty"`
	GrantDate               string  `json:"grantDate,omitempty"`
	Sedol                   string  `json:"sedol,omitempty"`
	VestedSharesExercisable float64 `json:"vestedSharesExercisable,omitempty"`
	Spread                  Money   `json:"spread,omitempty"`
	AccountId               int64   `json:"accountId,omitempty"`
	EnrichedDescription     string  `json:"enrichedDescription,omitempty"`
	CouponRate              float64 `json:"couponRate,omitempty"`
	CreatedDate             string  `json:"createdDate,omitempty"`
	AccruedIncome           Money   `json:"accruedIncome,omitempty"`
	SecurityType            string  `json:"securityType,omitempty"`
	ProviderAccountId       int64   `json:"providerAccountId,omitempty"`
	UnvestedQuantity        float64 `json:"unvestedQuantity,omitempty"`
	CostBasis               Money   `json:"costBasis,omitempty"`
	VestingDate             string  `json:"vestingDate,omitempty"`
	Isin                    string  `json:"isin,omitempty"`
	StrikePrice             Money   `json:"strikePrice,omitempty"`
}

type DataExtractsTransaction struct {
	Date                       string           `json:"date,omitempty"`
	SourceId                   string           `json:"sourceId,omitempty"`
	Symbol                     string           `json:"symbol,omitempty"`
	CusipNumber                string           `json:"cusipNumber,omitempty"`
	SourceApcaNumber           string           `json:"sourceApcaNumber,omitempty"`
	HighLevelCategoryId        int64            `json:"highLevelCategoryId,omitempty"`
	Memo                       string           `json:"memo,omitempty"`
	Type                       string           `json:"type,omitempty"`
	Intermediary               []string         `json:"intermediary,omitempty"`
	Frequency                  string           `json:"frequency,omitempty"`
	LastUpdated                string           `json:"lastUpdated,omitempty"`
	Price                      Money            `json:"price,omitempty"`
	SourceMerchantCategoryCode string           `json:"sourceMerchantCategoryCode,omitempty"`
	Id                         int64            `json:"id,omitempty"`
	CheckNumber                string           `json:"checkNumber,omitempty"`
	TransactionDateTime        string           `json:"transactionDateTime,omitempty"`
	TypeAtSource               string           `json:"typeAtSource,omitempty"`
	Valoren                    string           `json:"valoren,omitempty"`
	IsManual                   bool             `json:"isManual,omitempty"`
	SourceBillerName           string           `json:"sourceBillerName,omitempty"`
	Merchant                   Merchant         `json:"merchant,omitempty"`
	Sedol                      string           `json:"sedol,omitempty"`
	CategoryType               string           `json:"categoryType,omitempty"`
	AccountId                  int64            `json:"accountId,omitempty"`
	SourceType                 string           `json:"sourceType,omitempty"`
	SubType                    string           `json:"subType,omitempty"`
	HoldingDescription         string           `json:"holdingDescription,omitempty"`
	Status                     string           `json:"status,omitempty"`
	DetailCategoryId           int64            `json:"detailCategoryId,omitempty"`
	Description                Description      `json:"description,omitempty"`
	SettleDate                 string           `json:"settleDate,omitempty"`
	PostDateTime               string           `json:"postDateTime,omitempty"`
	BaseType                   string           `json:"baseType,omitempty"`
	CategorySource             string           `json:"categorySource,omitempty"`
	Principal                  Money            `json:"principal,omitempty"`
	SourceBillerCode           string           `json:"sourceBillerCode,omitempty"`
	IsDeleted                  bool             `json:"isDeleted,omitempty"`
	Interest                   Money            `json:"interest,omitempty"`
	Commission                 Money            `json:"commission,omitempty"`
	MerchantType               string           `json:"merchantType,omitempty"`
	Amount                     Money            `json:"amount,omitempty"`
	IsPhysical                 bool             `json:"isPhysical,omitempty"`
	Quantity                   float64          `json:"quantity,omitempty"`
	IsRecurring                string           `json:"isRecurring,omitempty"`
	TransactionDate            string           `json:"transactionDate,omitempty"`
	CreatedDate                string           `json:"createdDate,omitempty"`
	CONTAINER                  string           `json:"CONTAINER,omitempty"`
	BusinessCategory           BusinessCategory `json:"businessCategory,omitempty"`
	PostDate                   string           `json:"postDate,omitempty"`
	ParentCategoryId           int64            `json:"parentCategoryId,omitempty"`
	Category                   string           `json:"category,omitempty"`
	RunningBalance             Money            `json:"runningBalance,omitempty"`
	CategoryId                 int64            `json:"categoryId,omitempty"`
	Isin                       string           `json:"isin,omitempty"`
}

type ProviderAccountResponse struct {
	ProviderAccount []ProviderAccount `json:"providerAccount,omitempty"`
}

type ProviderAccountPreferences struct {
	IsDataExtractsEnabled   bool  `json:"isDataExtractsEnabled,omitempty"`
	LinkedProviderAccountId int64 `json:"linkedProviderAccountId,omitempty"`
	IsAutoRefreshEnabled    bool  `json:"isAutoRefreshEnabled,omitempty"`
}

type ProviderAccount struct {
	Preferences          ProviderAccountPreferences `json:"preferences,omitempty"`
	OauthMigrationStatus string                     `json:"oauthMigrationStatus,omitempty"`
	IsManual             bool                       `json:"isManual,omitempty"`
	IsRealTimeMFA        bool                       `json:"isRealTimeMFA,omitempty"`
	LastUpdated          string                     `json:"lastUpdated,omitempty"`
	ConsentId            int64                      `json:"consentId,omitempty"`
	CreatedDate          string                     `json:"createdDate,omitempty"`
	AggregationSource    string                     `json:"aggregationSource,omitempty"`
	ProviderId           int64                      `json:"providerId,omitempty"`
	RequestId            string                     `json:"requestId,omitempty"`
	Id                   int64                      `json:"id,omitempty"`
	AuthType             string                     `json:"authType,omitempty"`
	Dataset              []AccountDataset           `json:"dataset,omitempty"`
	Status               string                     `json:"status,omitempty"`
}

type AccountResponse struct {
	Account []Account `json:"account,omitempty"`
}

type Account struct {
	AvailableCash                  Money                 `json:"availableCash,omitempty"`
	IncludeInNetWorth              bool                  `json:"includeInNetWorth,omitempty"`
	MoneyMarketBalance             Money                 `json:"moneyMarketBalance,omitempty"`
	EnrollmentDate                 string                `json:"enrollmentDate,omitempty"`
	EstimatedDate                  string                `json:"estimatedDate,omitempty"`
	Memo                           string                `json:"memo,omitempty"`
	Guarantor                      string                `json:"guarantor,omitempty"`
	InterestPaidLastYear           Money                 `json:"interestPaidLastYear,omitempty"`
	LastUpdated                    string                `json:"lastUpdated,omitempty"`
	Balance                        Money                 `json:"balance,omitempty"`
	NsccMemberClearingCode         string                `json:"nsccMemberClearingCode,omitempty"`
	HomeInsuranceType              string                `json:"homeInsuranceType,omitempty"`
	Id                             int64                 `json:"id,omitempty"`
	Cash                           Money                 `json:"cash,omitempty"`
	TotalCreditLine                Money                 `json:"totalCreditLine,omitempty"`
	ProviderName                   string                `json:"providerName,omitempty"`
	ValuationType                  string                `json:"valuationType,omitempty"`
	MarginBalance                  Money                 `json:"marginBalance,omitempty"`
	Apr                            float64               `json:"apr,omitempty"`
	AvailableCredit                Money                 `json:"availableCredit,omitempty"`
	SourceProductName              string                `json:"sourceProductName,omitempty"`
	CurrentBalance                 Money                 `json:"currentBalance,omitempty"`
	IsManual                       bool                  `json:"isManual,omitempty"`
	Profile                        AccountProfile        `json:"profile,omitempty"`
	EscrowBalance                  Money                 `json:"escrowBalance,omitempty"`
	NextLevel                      string                `json:"nextLevel,omitempty"`
	Classification                 string                `json:"classification,omitempty"`
	LoanPayoffAmount               Money                 `json:"loanPayoffAmount,omitempty"`
	InterestRateType               string                `json:"interestRateType,omitempty"`
	LoanPayByDate                  string                `json:"loanPayByDate,omitempty"`
	FaceAmount                     Money                 `json:"faceAmount,omitempty"`
	PolicyFromDate                 string                `json:"policyFromDate,omitempty"`
	SourceLoanOffsetEnabled        bool                  `json:"sourceLoanOffsetEnabled,omitempty"`
	DtccMemberClearingCode         string                `json:"dtccMemberClearingCode,omitempty"`
	PremiumPaymentTerm             string                `json:"premiumPaymentTerm,omitempty"`
	PolicyTerm                     string                `json:"policyTerm,omitempty"`
	RepaymentPlanType              string                `json:"repaymentPlanType,omitempty"`
	AggregatedAccountType          string                `json:"aggregatedAccountType,omitempty"`
	AvailableBalance               Money                 `json:"availableBalance,omitempty"`
	AccountStatus                  string                `json:"accountStatus,omitempty"`
	LifeInsuranceType              string                `json:"lifeInsuranceType,omitempty"`
	FullAccountNumber              string                `json:"fullAccountNumber,omitempty"`
	Premium                        Money                 `json:"premium,omitempty"`
	AggregationSource              string                `json:"aggregationSource,omitempty"`
	OverDraftLimit                 Money                 `json:"overDraftLimit,omitempty"`
	Nickname                       string                `json:"nickname,omitempty"`
	Term                           string                `json:"term,omitempty"`
	InterestRate                   float64               `json:"interestRate,omitempty"`
	DeathBenefit                   Money                 `json:"deathBenefit,omitempty"`
	Address                        AccountAddress        `json:"address,omitempty"`
	SourceLoanRepaymentType        string                `json:"sourceLoanRepaymentType,omitempty"`
	CashValue                      Money                 `json:"cashValue,omitempty"`
	Holder                         []AccountHolder       `json:"holder,omitempty"`
	Var401kLoan                    Money                 `json:"401kLoan,omitempty"`
	HomeValue                      Money                 `json:"homeValue,omitempty"`
	AccountNumber                  string                `json:"accountNumber,omitempty"`
	CreatedDate                    string                `json:"createdDate,omitempty"`
	InterestPaidYTD                Money                 `json:"interestPaidYTD,omitempty"`
	BusinessInformation            BusinessInformation   `json:"businessInformation,omitempty"`
	MaxRedraw                      Money                 `json:"maxRedraw,omitempty"`
	ProviderAccountId              int64                 `json:"providerAccountId,omitempty"`
	Collateral                     string                `json:"collateral,omitempty"`
	Dataset                        []AccountDataset      `json:"dataset,omitempty"`
	RunningBalance                 Money                 `json:"runningBalance,omitempty"`
	SourceId                       string                `json:"sourceId,omitempty"`
	DueDate                        string                `json:"dueDate,omitempty"`
	Frequency                      string                `json:"frequency,omitempty"`
	SourceAccountOwnership         string                `json:"sourceAccountOwnership,omitempty"`
	MaturityAmount                 Money                 `json:"maturityAmount,omitempty"`
	AssociatedProviderAccountId    []int64               `json:"associatedProviderAccountId,omitempty"`
	IsAsset                        bool                  `json:"isAsset,omitempty"`
	PrincipalBalance               Money                 `json:"principalBalance,omitempty"`
	TotalCashLimit                 Money                 `json:"totalCashLimit,omitempty"`
	MaturityDate                   string                `json:"maturityDate,omitempty"`
	MinimumAmountDue               Money                 `json:"minimumAmountDue,omitempty"`
	AnnualPercentageYield          float64               `json:"annualPercentageYield,omitempty"`
	AccountType                    string                `json:"accountType,omitempty"`
	OriginationDate                string                `json:"originationDate,omitempty"`
	TotalVestedBalance             Money                 `json:"totalVestedBalance,omitempty"`
	RewardBalance                  []RewardBalance       `json:"rewardBalance,omitempty"`
	SourceAccountStatus            string                `json:"sourceAccountStatus,omitempty"`
	LinkedAccountIds               []int64               `json:"linkedAccountIds,omitempty"`
	DerivedApr                     float64               `json:"derivedApr,omitempty"`
	PolicyEffectiveDate            string                `json:"policyEffectiveDate,omitempty"`
	TotalUnvestedBalance           Money                 `json:"totalUnvestedBalance,omitempty"`
	AnnuityBalance                 Money                 `json:"annuityBalance,omitempty"`
	AccountCategory                string                `json:"accountCategory,omitempty"`
	AccountName                    string                `json:"accountName,omitempty"`
	TotalCreditLimit               Money                 `json:"totalCreditLimit,omitempty"`
	PolicyStatus                   string                `json:"policyStatus,omitempty"`
	ShortBalance                   Money                 `json:"shortBalance,omitempty"`
	Lender                         string                `json:"lender,omitempty"`
	LastEmployeeContributionAmount Money                 `json:"lastEmployeeContributionAmount,omitempty"`
	ProviderId                     string                `json:"providerId,omitempty"`
	LastPaymentDate                string                `json:"lastPaymentDate,omitempty"`
	PrimaryRewardUnit              string                `json:"primaryRewardUnit,omitempty"`
	LastPaymentAmount              Money                 `json:"lastPaymentAmount,omitempty"`
	RemainingBalance               Money                 `json:"remainingBalance,omitempty"`
	UserClassification             string                `json:"userClassification,omitempty"`
	BankTransferCode               []BankTransferCode    `json:"bankTransferCode,omitempty"`
	ExpirationDate                 string                `json:"expirationDate,omitempty"`
	Coverage                       []Coverage            `json:"coverage,omitempty"`
	CashApr                        float64               `json:"cashApr,omitempty"`
	AutoRefresh                    AutoRefresh           `json:"autoRefresh,omitempty"`
	OauthMigrationStatus           string                `json:"oauthMigrationStatus,omitempty"`
	DisplayedName                  string                `json:"displayedName,omitempty"`
	FullAccountNumberList          FullAccountNumberList `json:"fullAccountNumberList,omitempty"`
	AmountDue                      Money                 `json:"amountDue,omitempty"`
	CurrentLevel                   string                `json:"currentLevel,omitempty"`
	OriginalLoanAmount             Money                 `json:"originalLoanAmount,omitempty"`
	PolicyToDate                   string                `json:"policyToDate,omitempty"`
	LoanPayoffDetails              LoanPayoffDetails     `json:"loanPayoffDetails,omitempty"`
	PaymentProfile                 PaymentProfile        `json:"paymentProfile,omitempty"`
	CONTAINER                      string                `json:"CONTAINER,omitempty"`
	IsOwnedAtSource                bool                  `json:"isOwnedAtSource,omitempty"`
	LastEmployeeContributionDate   string                `json:"lastEmployeeContributionDate,omitempty"`
	LastPayment                    Money                 `json:"lastPayment,omitempty"`
	RecurringPayment               Money                 `json:"recurringPayment,omitempty"`
	MinRedraw                      Money                 `json:"minRedraw,omitempty"`
}

type HoldingResponse struct {
	Holding []Holding `json:"holding,omitempty"`
}

type Holding struct {
	Symbol                  string                `json:"symbol,omitempty"`
	ExercisedQuantity       float64               `json:"exercisedQuantity,omitempty"`
	CusipNumber             string                `json:"cusipNumber,omitempty"`
	AssetClassification     []AssetClassification `json:"assetClassification,omitempty"`
	VestedQuantity          float64               `json:"vestedQuantity,omitempty"`
	Description             string                `json:"description,omitempty"`
	UnvestedValue           Money                 `json:"unvestedValue,omitempty"`
	SecurityStyle           string                `json:"securityStyle,omitempty"`
	VestedValue             Money                 `json:"vestedValue,omitempty"`
	OptionType              string                `json:"optionType,omitempty"`
	LastUpdated             string                `json:"lastUpdated,omitempty"`
	MatchStatus             string                `json:"matchStatus,omitempty"`
	HoldingType             string                `json:"holdingType,omitempty"`
	MaturityDate            string                `json:"maturityDate,omitempty"`
	Price                   Money                 `json:"price,omitempty"`
	Term                    string                `json:"term,omitempty"`
	ContractQuantity        float64               `json:"contractQuantity,omitempty"`
	Id                      int64                 `json:"id,omitempty"`
	IsShort                 bool                  `json:"isShort,omitempty"`
	Value                   Money                 `json:"value,omitempty"`
	ExpirationDate          string                `json:"expirationDate,omitempty"`
	InterestRate            float64               `json:"interestRate,omitempty"`
	Quantity                float64               `json:"quantity,omitempty"`
	IsManual                bool                  `json:"isManual,omitempty"`
	AccruedInterest         Money                 `json:"accruedInterest,omitempty"`
	GrantDate               string                `json:"grantDate,omitempty"`
	Sedol                   string                `json:"sedol,omitempty"`
	VestedSharesExercisable float64               `json:"vestedSharesExercisable,omitempty"`
	Spread                  Money                 `json:"spread,omitempty"`
	AccountId               int64                 `json:"accountId,omitempty"`
	EnrichedDescription     string                `json:"enrichedDescription,omitempty"`
	CouponRate              float64               `json:"couponRate,omitempty"`
	CreatedDate             string                `json:"createdDate,omitempty"`
	AccruedIncome           Money                 `json:"accruedIncome,omitempty"`
	SecurityType            string                `json:"securityType,omitempty"`
	ProviderAccountId       int64                 `json:"providerAccountId,omitempty"`
	UnvestedQuantity        float64               `json:"unvestedQuantity,omitempty"`
	CostBasis               Money                 `json:"costBasis,omitempty"`
	VestingDate             string                `json:"vestingDate,omitempty"`
	Isin                    string                `json:"isin,omitempty"`
	StrikePrice             Money                 `json:"strikePrice,omitempty"`
}

type TransactionResponse struct {
	Transaction []TransactionWithDateTime `json:"transaction,omitempty"`
}

type TransactionWithDateTime struct {
	Date                       string           `json:"date,omitempty"`
	SourceId                   string           `json:"sourceId,omitempty"`
	Symbol                     string           `json:"symbol,omitempty"`
	CusipNumber                string           `json:"cusipNumber,omitempty"`
	SourceApcaNumber           string           `json:"sourceApcaNumber,omitempty"`
	HighLevelCategoryId        int64            `json:"highLevelCategoryId,omitempty"`
	Memo                       string           `json:"memo,omitempty"`
	Type                       string           `json:"type,omitempty"`
	Intermediary               []string         `json:"intermediary,omitempty"`
	Frequency                  string           `json:"frequency,omitempty"`
	LastUpdated                string           `json:"lastUpdated,omitempty"`
	Price                      Money            `json:"price,omitempty"`
	SourceMerchantCategoryCode string           `json:"sourceMerchantCategoryCode,omitempty"`
	Id                         int64            `json:"id,omitempty"`
	CheckNumber                string           `json:"checkNumber,omitempty"`
	TransactionDateTime        string           `json:"transactionDateTime,omitempty"`
	TypeAtSource               string           `json:"typeAtSource,omitempty"`
	Valoren                    string           `json:"valoren,omitempty"`
	IsManual                   bool             `json:"isManual,omitempty"`
	SourceBillerName           string           `json:"sourceBillerName,omitempty"`
	Merchant                   Merchant         `json:"merchant,omitempty"`
	Sedol                      string           `json:"sedol,omitempty"`
	CategoryType               string           `json:"categoryType,omitempty"`
	AccountId                  int64            `json:"accountId,omitempty"`
	SourceType                 string           `json:"sourceType,omitempty"`
	SubType                    string           `json:"subType,omitempty"`
	HoldingDescription         string           `json:"holdingDescription,omitempty"`
	Status                     string           `json:"status,omitempty"`
	DetailCategoryId           int64            `json:"detailCategoryId,omitempty"`
	Description                Description      `json:"description,omitempty"`
	SettleDate                 string           `json:"settleDate,omitempty"`
	PostDateTime               string           `json:"postDateTime,omitempty"`
	BaseType                   string           `json:"baseType,omitempty"`
	CategorySource             string           `json:"categorySource,omitempty"`
	Principal                  Money            `json:"principal,omitempty"`
	SourceBillerCode           string           `json:"sourceBillerCode,omitempty"`
	Interest                   Money            `json:"interest,omitempty"`
	Commission                 Money            `json:"commission,omitempty"`
	MerchantType               string           `json:"merchantType,omitempty"`
	Amount                     Money            `json:"amount,omitempty"`
	IsPhysical                 bool             `json:"isPhysical,omitempty"`
	Quantity                   float64          `json:"quantity,omitempty"`
	IsRecurring                string           `json:"isRecurring,omitempty"`
	TransactionDate            string           `json:"transactionDate,omitempty"`
	CreatedDate                string           `json:"createdDate,omitempty"`
	CONTAINER                  string           `json:"CONTAINER,omitempty"`
	BusinessCategory           BusinessCategory `json:"businessCategory,omitempty"`
	PostDate                   string           `json:"postDate,omitempty"`
	ParentCategoryId           int64            `json:"parentCategoryId,omitempty"`
	Category                   string           `json:"category,omitempty"`
	RunningBalance             Money            `json:"runningBalance,omitempty"`
	CategoryId                 int64            `json:"categoryId,omitempty"`
	Isin                       string           `json:"isin,omitempty"`
}

type AccountDataset struct {
	LastUpdated               string `json:"lastUpdated,omitempty"`
	UpdateEligibility         string `json:"updateEligibility,omitempty"`
	AdditionalStatus          string `json:"additionalStatus,omitempty"`
	NextUpdateScheduled       string `json:"nextUpdateScheduled,omitempty"`
	Name                      string `json:"name,omitempty"`
	LastUpdateAttempt         string `json:"lastUpdateAttempt,omitempty"`
	AdditionalStatusErrorCode string `json:"additionalStatusErrorCode,omitempty"`
}

type Money struct {
	Amount            float64 `json:"amount,omitempty"`
	ConvertedAmount   float64 `json:"convertedAmount,omitempty"`
	Currency          string  `json:"currency,omitempty"`
	ConvertedCurrency string  `json:"convertedCurrency,omitempty"`
}

type BusinessCategory struct {
	CategoryName string `json:"categoryName,omitempty"`
	CategoryId   int64  `json:"categoryId,omitempty"`
}

type Description struct {
	Security string `json:"security,omitempty"`
	Original string `json:"original,omitempty"`
	Simple   string `json:"simple,omitempty"`
	Consumer string `json:"consumer,omitempty"`
}

type Merchant struct {
	Website       string         `json:"website,omitempty"`
	Address       AccountAddress `json:"address,omitempty"`
	LogoURLs      []LogoURLs     `json:"logoURLs,omitempty"`
	Contact       Contact        `json:"contact,omitempty"`
	CategoryLabel []string       `json:"categoryLabel,omitempty"`
	Coordinates   Coordinates    `json:"coordinates,omitempty"`
	Name          string         `json:"name,omitempty"`
	Id            string         `json:"id,omitempty"`
	Source        string         `json:"source,omitempty"`
	LogoURL       string         `json:"logoURL,omitempty"`
}

type Coverage struct {
	Amount    []CoverageAmount `json:"amount,omitempty"`
	PlanType  string           `json:"planType,omitempty"`
	EndDate   string           `json:"endDate,omitempty"`
	Type      string           `json:"type,omitempty"`
	StartDate string           `json:"startDate,omitempty"`
}

type AccountAddress struct {
	Zip        string `json:"zip,omitempty"`
	Country    string `json:"country,omitempty"`
	Address3   string `json:"address3,omitempty"`
	Address2   string `json:"address2,omitempty"`
	City       string `json:"city,omitempty"`
	SourceType string `json:"sourceType,omitempty"`
	Address1   string `json:"address1,omitempty"`
	Street     string `json:"street,omitempty"`
	State      string `json:"state,omitempty"`
	Type       string `json:"type,omitempty"`
}

type BusinessInformation struct {
	LegalName    string `json:"legalName,omitempty"`
	BusinessName string `json:"businessName,omitempty"`
	Abn          string `json:"abn,omitempty"`
	Acn          string `json:"acn,omitempty"`
}

type RewardBalance struct {
	ExpiryDate      string  `json:"expiryDate,omitempty"`
	BalanceToReward string  `json:"balanceToReward,omitempty"`
	BalanceType     string  `json:"balanceType,omitempty"`
	Balance         float64 `json:"balance,omitempty"`
	Description     string  `json:"description,omitempty"`
	BalanceToLevel  string  `json:"balanceToLevel,omitempty"`
	Units           string  `json:"units,omitempty"`
}

type BankTransferCode struct {
	Id   string `json:"id,omitempty"`
	Type string `json:"type,omitempty"`
}

type LogoURLs struct {
	Type string `json:"type,omitempty"`
	Url  string `json:"url,omitempty"`
}

type LoanPayoffDetails struct {
	PayByDate          string `json:"payByDate,omitempty"`
	PayoffAmount       Money  `json:"payoffAmount,omitempty"`
	OutstandingBalance Money  `json:"outstandingBalance,omitempty"`
}

type CoverageAmount struct {
	Cover     Money  `json:"cover,omitempty"`
	UnitType  string `json:"unitType,omitempty"`
	Type      string `json:"type,omitempty"`
	LimitType string `json:"limitType,omitempty"`
	Met       Money  `json:"met,omitempty"`
}

type Contact struct {
	Phone string `json:"phone,omitempty"`
	Email string `json:"email,omitempty"`
}

type Coordinates struct {
	Latitude  float64 `json:"latitude,omitempty"`
	Longitude float64 `json:"longitude,omitempty"`
}

type PaymentProfile struct {
	Identifier              PaymentIdentifier       `json:"identifier,omitempty"`
	Address                 []AccountAddress        `json:"address,omitempty"`
	PaymentBankTransferCode PaymentBankTransferCode `json:"paymentBankTransferCode,omitempty"`
}

type PaymentBankTransferCode struct {
	Id   string `json:"id,omitempty"`
	Type string `json:"type,omitempty"`
}

type PaymentIdentifier struct {
	Type  string `json:"type,omitempty"`
	Value string `json:"value,omitempty"`
}

type AccountProfile struct {
	Identifier  []Identifier     `json:"identifier,omitempty"`
	Address     []AccountAddress `json:"address,omitempty"`
	PhoneNumber []PhoneNumber    `json:"phoneNumber,omitempty"`
	Email       []Email          `json:"email,omitempty"`
}

type Email struct {
	Type  string `json:"type,omitempty"`
	Value string `json:"value,omitempty"`
}

type PhoneNumber struct {
	Type  string `json:"type,omitempty"`
	Value string `json:"value,omitempty"`
}

type FullAccountNumberList struct {
	PaymentAccountNumber   string `json:"paymentAccountNumber,omitempty"`
	TokenizedAccountNumber string `json:"tokenizedAccountNumber,omitempty"`
	UnmaskedAccountNumber  string `json:"unmaskedAccountNumber,omitempty"`
}

type AssetClassification struct {
	Allocation          float64 `json:"allocation,omitempty"`
	ClassificationType  string  `json:"classificationType,omitempty"`
	ClassificationValue string  `json:"classificationValue,omitempty"`
}

type AutoRefresh struct {
	AdditionalStatus string `json:"additionalStatus,omitempty"`
	AsOfDate         string `json:"asOfDate,omitempty"`
	Status           string `json:"status,omitempty"`
}

type AccountHolder struct {
	Identifier []Identifier `json:"identifier,omitempty"`
	Gender     string       `json:"gender,omitempty"`
	Ownership  string       `json:"ownership,omitempty"`
	Name       Name         `json:"name,omitempty"`
}

type Identifier struct {
	Type  string `json:"type,omitempty"`
	Value string `json:"value,omitempty"`
}

type Name struct {
	Middle   string `json:"middle,omitempty"`
	Last     string `json:"last,omitempty"`
	FullName string `json:"fullName,omitempty"`
	First    string `json:"first,omitempty"`
}
