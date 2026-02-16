package templates

type LogArgs struct {
	Topic     string
	Timestamp string
	Json      string
	JsonTree  map[string]any
}

type AdminPageKind int

const (
	ConnectionsPage AdminPageKind = iota
	AccountsPage
	TransactionsPage
	HoldingsPage
)

type AdminPageArgs struct {
	Kind      AdminPageKind
	ProfileID string
	//Connections  []Ops
}

type LogsPageArgs struct {
	ProfileIDs        []string
	Topics            []string
	InputTopicOptions []string
}
